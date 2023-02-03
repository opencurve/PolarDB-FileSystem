/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/* Author: Xu Yifeng */

#include <stdio.h>
#include <errno.h>
#include <signal.h>
#include <semaphore.h>
#include <fcntl.h>

#include "pfsd_common.h"
#include "pfsd_shm.h"
#include "pfsd_worker.h"
#include "pfsd_option.h"
#include "pfsd_log.h"
#include "pfsd_chnl.h"
#include "pfsd_api.h"

static int       g_pfsd_started = 0;
static pthread_t g_pfsd_main_thread = 0;
static int       g_pfsd_pidfile = -1;
static sem_t     g_pfsd_main_sem;

static void *pfsd_main_thread_entry(void *arg);

static int
sanity_check(const pfsd_option_t *opt)
{
	if (opt->o_workers < 1 || opt->o_workers > PFSD_WORKER_MAX) {
		pfsd_error("o_workers should be between 1..%d", PFSD_WORKER_MAX);
		return -1;
	}

	if (opt->o_usleep < 0 || opt->o_usleep > 1000) {
		pfsd_error("o_usleep should be between 0..%d", 1000);
		return -1;
	}

	if (strlen(opt->o_pbdname) == 0) {
		pfsd_error("pbdname is empty\n");
		return -1;
	}

	return 0;
}

extern "C" int
pfsd_start(struct pfsd_option *opt)
{
	const char *pbdname;
	int rc;

	if (g_pfsd_started) {
		pfsd_error("pfsd already started\n");
		return -1;
	}

	if (pfsd_prepare_env()) {
		pfsd_error("pfsd_prepare_env failed\n");
		return -1;
	}

	if (sanity_check(opt)) {
		pfsd_error("pfsd_option sanity check failed\n");
		return -1;
	}

	/* copy option */
	g_pfsd_option = *opt;

	g_pfsd_stop = false;
	sem_init(&g_pfsd_main_sem, 0, 0);
	pbdname = g_pfsd_option.o_pbdname;
	g_pfsd_pidfile = pfsd_pidfile_open(pbdname);
	if (g_pfsd_pidfile < 0) {
		pfsd_error("failed to open pid file.");
		return -1;
	}

	/* for pfsdaemon program */
	if (g_pfsd_option.o_daemon)
		daemon(1, 1);

	pfsd_pidfile_write(g_pfsd_pidfile);

	pfsd_info("starting pfsd[%d] %s", getpid(), pbdname);

	/* init communicate shm and inotify stuff */
	if (pfsd_chnl_listen(PFSD_USER_PID_DIR, pbdname, g_pfsd_option.o_workers, 
	    g_shm_fname, g_pfsd_option.o_shm_dir) != 0) {
		pfsd_error("[pfsd]pfsd_chnl_listen %s failed, errno %d", 
		    PFSD_USER_PID_DIR, errno);
		pfsd_pidfile_close(g_pfsd_pidfile);
		g_pfsd_pidfile = -1;
		return -1;
	}

	/* notify worker to start */
	worker_t *wk = g_pfsd_worker;
	sem_post(&wk->w_sem);

	rc = pthread_create(&g_pfsd_main_thread, NULL, pfsd_main_thread_entry,
		NULL);
	if (rc) {
		pfsd_error("create not create thread, error: %d", rc);
		pfsd_pidfile_close(g_pfsd_pidfile);
		g_pfsd_pidfile = -1;
		return -1;
	}

	g_pfsd_started = 1;

	pfsd_info("pfsd started [%s]", pbdname);
	return 0;
}

/* async stop pfsd backgroupd workers */
extern "C" int
pfsd_stop(void)
{
	g_pfsd_stop = true;
	sem_post(&g_pfsd_main_sem);
	return 0;
}

/* wait pfsd backgroupd workers to stop */
extern "C" int
pfsd_wait_stop(void)
{
	if (!g_pfsd_started)
		return -1;
	pthread_join(g_pfsd_main_thread, NULL);
	pfsd_pidfile_close(g_pfsd_pidfile);
	g_pfsd_pidfile = -1;
	g_pfsd_started = 0;
	g_pfsd_stop = 0;
	return 0;
}

extern "C" int
pfsd_is_started(void)
{
	return g_pfsd_started;
}

static void *
pfsd_main_thread_entry(void *arg)
{
	const int ZOMBIE_RECYCLE_WAIT = 5;

	while (!g_pfsd_stop) {
		/* recycle zombie */
		for (int ci = 0; ci < g_pfsd_worker->w_nch; ++ci) {
			pfsd_iochannel_t *ch = g_pfsd_worker->w_channels[ci];
			pfsd_shm_recycle_request(ch);
		}

		struct timespec ts;
		clock_gettime(CLOCK_REALTIME, &ts);
		ts.tv_sec += ZOMBIE_RECYCLE_WAIT;
		sem_timedwait(&g_pfsd_main_sem, &ts);
	}

	/* exit */
	if (g_pfsd_worker != NULL && g_pfsd_worker->w_nch != 0) {
		pfsd_info("pthread_join worker");
		pthread_join(g_pfsd_worker->w_tid, NULL);
	}

	pfsd_destroy_workers(&g_pfsd_worker);
	pfsd_info("bye bye");
	return 0;
}
