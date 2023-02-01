/*
 * Copyright (c) 2023 Netease Inc.
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

/*
 * Author: Xu Yifeng
 */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <signal.h>
#include <string.h>                                                             
#include <sys/stat.h> /* for mkdir */
#include <sys/types.h>
#include <unistd.h>

/* In the daemon, we use zlog for logging */
#include <zlog.h> 

/* Use libpfs and libpfs_svr api */
#include "pfs_option_api.h"
#include "pfs_trace_func.h"
#include "pfsd_api.h"
#include "pfsd_option.h"

static zlog_category_t *original_zlog_cat = NULL;
static int zlog_inited;
static char o_zlog_cfg[1024];

/* Ctrl+C handler */
static void
signal_int_handler(int num)
{
	pfsd_stop();
}

static void
reload_handler(int num)
{
}

static void
setup_sigaction(void)
{
	struct sigaction sig;
	memset(&sig, 0, sizeof(sig));
	sig.sa_handler = signal_int_handler;
	sigaction(SIGINT, &sig, NULL);
	sig.sa_handler = reload_handler;
	sigaction(SIGHUP, &sig, NULL);
	sig.sa_handler = SIG_IGN;
	sigaction(SIGPIPE, &sig, NULL);
}

static void
wrapper_zlog(int level, const char *filename, const char *func, int line,
	     const char *fmt, va_list ap)
{
	int l = ZLOG_LEVEL_INFO;
	switch(level) {
	case PFS_TRACE_FATAL:
		l = ZLOG_LEVEL_FATAL;
		break;
        case PFS_TRACE_ERROR:
		l = ZLOG_LEVEL_ERROR;
		break;
        case PFS_TRACE_WARN:
		l = ZLOG_LEVEL_WARN;
		break;                                                         
	case PFS_TRACE_INFO:
		l = ZLOG_LEVEL_INFO;
		break;
        case PFS_TRACE_DBG:
        case PFS_TRACE_VERB:
		l = ZLOG_LEVEL_DEBUG;
		break;
	}
	vzlog(original_zlog_cat, filename, strlen(filename),
	      func, strlen(func), line, l, fmt, ap);
}

/* setup zlog */
static int
setup_log(const char *pbdname)
{
	if (o_zlog_cfg[0] == '\0') {
		/* zlog not used */
		return 0;
	}

	if (strlen(pbdname) == 0) {
		fprintf(stderr, "pbdname is empty when initializing zlog\n");
		return -1;
	}

	/* init logger: use env for pass logdir to zlog */
	if (setenv("PFSD_PBDNAME", pbdname, 1) != 0) {
		fprintf(stderr, "set env [%s] failed: %s\n", pbdname,
			strerror(errno));
		return -1;
	}

	int rv = dzlog_init(o_zlog_cfg, (char *)"pfsd_cat");
	if (rv != 0) {
		fprintf(stderr, "Error: init log failed, ret:%d\n", rv);
		return rv;
	}

	/* init libpfs logger */
	original_zlog_cat = zlog_get_category("original_cat");
	if (original_zlog_cat == NULL) {
		fprintf(stderr, "why no original category");
		original_zlog_cat = zlog_get_category("pfsd_cat");
	}

	/* Set pfs log callback */
	pfs_set_trace_func(wrapper_zlog);
	zlog_inited = 1;
	return 0;
}

static void shutdown_log()
{
	if (zlog_inited)
		zlog_fini();                                                                
}

static char *
safe_strncpy(char *dest, const char *source, size_t size,
	const char *msg)
{
	dest[size-1] = '\0';
	strncpy(dest, source, size);
	if (dest[size-1] != '\0') {
		fprintf(stderr, "%s too long, max len %zd\n", msg, size-1);
		return NULL;
	}
	return dest;
}

static int
pfsd_parse_option(int ac, char *av[], pfsd_option_t *opt)
{
	int ch = 0;
	while ((ch = getopt(ac, av, "w:s:i:c:p:a:l:e:fd:r:q:C:")) != -1) {
		switch (ch) {
		case 'f':
			opt->o_daemon = 0;
			break;
		case 'd':
			opt->o_daemon = 1;
			break;
		case 'w':
			{
				errno = 0;
				long w = strtol(optarg, NULL, 10);
				if (errno == 0)
					opt->o_workers = int(w);
			}
			break;
		case 's':
			{
				errno = 0;
				long us = strtol(optarg, NULL, 10);
				if (errno == 0)
					opt->o_usleep = int(us);
			}
			break;
		case 'i':
			break;
		case 'e':
			{
				errno = 0;
				long w = strtol(optarg, NULL, 10);
				if (errno == 0)
					opt->o_server_id = (unsigned int)(w);
			}
			break;
		case 'c':
			if (!safe_strncpy(o_zlog_cfg, optarg, sizeof o_zlog_cfg,
					"log cfg file name")) {
				return -1;
			}
			break;
		case 'p':
			if (!safe_strncpy(opt->o_pbdname, optarg,
			    sizeof opt->o_pbdname, "pbd name")) {
				return -1;
			}
			break;
		case 'a':
			if (!safe_strncpy(opt->o_shm_dir, optarg, 
			    sizeof opt->o_shm_dir, "shm dir")) {
				return -1;
			}
			break;
		case 'r':
			{
				errno = 0;
				long w = strtol(optarg, NULL, 10);
				if (errno == 0)
					opt->o_pollers = (unsigned int)(w);
			}
			break;
		case 'q':
			opt->o_auto_increase_epoch = 1;
			break;
		default:
			return -1;
		}
	}

	if (optind != ac) {
		fprintf(stderr, "Unknown option: %s", optarg);
		return -1;
	}

	return 0;
}

void
pfsd_usage(const char *prog)
{
	fprintf(stderr, "Usage: %s \n"
			" -f (not daemon mode)\n"
			" -w #nworkers\n"
			" -c log_config_file\n"
			" -p pbdname\n"
			" -e db ins id\n"
			" -a shm directory\n"
			" -i #inode_list_size\n", prog);
}

int main(int ac, char *av[])
{
	int err;
	pfsd_option_t opt;

	pfsd_option_init(&opt);

	if (ac == 1 || pfsd_parse_option(ac, av, &opt) != 0) {
		pfsd_usage(av[0]);
		return EXIT_FAILURE;
	}

	if (setup_log(opt.o_pbdname)) {
		return EXIT_FAILURE;
	}

	setup_sigaction();

	/* we are stand-alone app, pass 1 to allow daemon mode */
	if (pfsd_start(&opt))
        	return 1;
	pfsd_wait_stop();

	/* close zlog */
	shutdown_log();
	return  EXIT_SUCCESS;
}
