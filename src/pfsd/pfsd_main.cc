/*
 * Copyright (c) 2017-2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <errno.h>
#include <signal.h>
#include <semaphore.h>

#include "pfsd_common.h"
#include "pfsd_shm.h"
#include "pfsd_worker.h"
#include "pfsd_option.h"

#include "pfs_trace.h"
#include "pfsd_zlog.h"

#include "pfsd_chnl.h"

static sem_t main_sem;
static void
signal_handler(int num)
{
	g_stop = true;
	sem_post(&main_sem);
}

static void
reload_handler(int num)
{
}

/* used for libpfs logger */
zlog_category_t *original_zlog_cat = NULL;

int main(int ac, char *av[])
{
	const char *pbdname;
	int err;
	if (pfsd_parse_option(ac, av) != 0) {
		pfsd_usage(av[0]);
		return -1;
	}

	if (ac == 1)
		pfsd_usage(av[0]);

	sem_init(&main_sem, 0, 0);
	pbdname = g_option.o_pbdname;
	err = pfsd_write_pid(pbdname);
	if (err != 0) {
		fprintf(stderr, "pfsd %s may already running, err %d.\n", 
		    pbdname, err);
		return -1;
	}

	/* init signal */
	struct sigaction sig;
	memset(&sig, 0, sizeof(sig));
	sig.sa_handler = signal_handler;
	sigaction(SIGINT, &sig, NULL);
	sig.sa_handler = reload_handler;
	sigaction(SIGHUP, &sig, NULL);
	sig.sa_handler = SIG_IGN;
	sigaction(SIGPIPE, &sig, NULL);

	if (g_option.o_daemon)
		daemon(1, 1);

	/* init logger: use env for pass logdir to zlog */
	if (setenv("PFSD_PBDNAME", pbdname, 1) != 0) {
		fprintf(stderr, "set env [%s] failed: %s\n", pbdname, strerror(errno));
		return -1;
	}
	char logdir[PFS_MAX_PATHLEN] = "";
	snprintf(logdir, PFS_MAX_PATHLEN-1, "/var/log/pfsd-%s", pbdname);
	mkdir(logdir, 0777);
	int rv = LogInit(g_option.o_log_cfg, (char *)"pfsd_cat");
	if (rv != 0) {
		fprintf(stderr, "Error: init log failed, ret:%d\n", rv);
		return rv;
	}

	/* init libpfs logger */
	original_zlog_cat = zlog_get_category("original_cat");
	if (original_zlog_cat == NULL) {
		pfsd_error("why no original category");
		original_zlog_cat = zlog_get_category("pfsd_cat");
	}

	pfs_set_trace_func(wrapper_zlog);

	fprintf(stderr, "starting pfsd[%d] %s\n", getpid(), pbdname);
	pfsd_info("starting pfsd[%d] %s", getpid(), pbdname);

	/* init communicate shm and inotify stuff */
	if (pfsd_chnl_listen(PFSD_USER_PID_DIR, pbdname, g_option.o_workers, 
	    g_shm_fname, g_option.o_shm_dir) != 0) {
		pfsd_error("[pfsd]pfsd_chnl_listen %s failed, errno %d", 
		    PFSD_USER_PID_DIR, errno);
		return -1;
	}

	/* notify worker start */
	worker_t *wk = g_worker ;
	sem_post(&wk->w_sem);

	int windex = 0;
	while (!g_stop) {
		/* recycle zombie */
		for (int ci = 0; ci < g_worker->w_nch; ++ci) {
			pfsd_iochannel_t *ch = g_worker->w_channels[ci];
			pfsd_shm_recycle_request(ch);
		}

		struct timespec ts;
		clock_gettime(CLOCK_REALTIME, &ts);
		ts.tv_sec++;
		sem_timedwait(&main_sem, &ts);
	}

	/* exit */
	if (g_worker != NULL && g_worker->w_nch != 0) {
		sem_post(&g_worker->w_sem);
		pfsd_info("[pfsd]pthread_join worker");
		pthread_join(g_worker->w_tid, NULL);
	}

	pfsd_destroy_workers(&g_worker);

	pfsd_info("[pfsd]bye bye");
	return 0;
}

