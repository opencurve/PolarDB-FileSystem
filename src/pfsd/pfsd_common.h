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

#ifndef _PFSD_COMMON_H_
#define _PFSD_COMMON_H_

#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <errno.h>
#include <execinfo.h>
#include <syslog.h>

#include <pfsd_log.h>

#ifndef likely                                                                 
	#define likely(c) __builtin_expect(!!(c), 1)                            
#endif                                                                         
#ifndef unlikely                                                               
	#define unlikely(c)  __builtin_expect(!!(c), 0)                         
#endif

#define PFSD_SHM_MAGIC (0x0133C96C)

#define PFSD_INVALID_PID (pid_t(0))
#define PFSD_SHM_PATH "/dev/shm/pfsd/"

/* 1K, 8K, 16K, 64K, 256K, 1M, 4M*/
#define PFSD_SHM_MAX (7)
#define PFSD_WORKER_MAX (10000)

#define PFSD_SHM_CHNL_DEFAULT (32)

#define PFSD_MAX_IOSIZE (4 * 1024 * 1024)

#define PFSD_USER_PID_DIR "/var/run/pfsd/"

#define PID_FORMAT			  "pfsd.%d.pid"

struct pfsd_request;
typedef pfsd_request pfsd_request_t;

static inline
int pfsd_make_pid_name(char* buf, size_t size)
{
	return snprintf(buf, size, PID_FORMAT, int(getpid()));
}

/* make shm pathname */
static inline
int pfsd_make_shm_path(int seq, const char* dir, const char* pbdname, char* buf,
    size_t size)
{
	return snprintf(buf, size, "%s/shm_pfsd-%s_%02d", dir, pbdname, seq);
}

int pfsd_prepare_env(void);

long pfsd_tolong(void* ptr);

bool pfsd_request_alive(pfsd_request_t* req);

void pfsd_robust_mutex_init(pthread_mutex_t* mutex);
int pfsd_robust_mutex_lock(pthread_mutex_t* mutex);
int pfsd_robust_mutex_trylock(pthread_mutex_t* mutex);
int pfsd_robust_mutex_unlock(pthread_mutex_t* mutex);

#define PFSD_MUTEX_LOCK_EX(m, rv) do {\
	int __r = pfsd_robust_mutex_lock(&(m));\
	if (__r != 0) { \
		return rv; \
	} \
} while(0)

#define PFSD_MUTEX_LOCK(m) do {\
	if (pfsd_robust_mutex_lock(&(m)) != 0) { \
		return; \
	} \
} while(0)

#define PFSD_MUTEX_TRYLOCK_EX(m, rv) do {\
	int __r = pfsd_robust_mutex_trylock(&(m));\
	if (__r != 0) { \
		return rv; \
	} \
} while(0)

#define PFSD_MUTEX_TRYLOCK(m) do {\
	if (pfsd_robust_mutex_trylock(&(m)) != 0) { \
		return; \
	} \
} while(0)

#define PFSD_MUTEX_UNLOCK(m) do { \
	pfsd_robust_mutex_unlock(&(m));\
} while(0)


/* get pbdname /86-1/hello.txt -> 86-1 */
int pfsd_sdk_pbdname(const char* pbdpath, char* pbdname);

/* only one instance running for each pbd */
int pfsd_pidfile_open(const char* pbdname);
int pfsd_pidfile_write(int fd);
int pfsd_pidfile_close(int fd);

#define FILE_MAX_FNAME 512

#define PFSD_MALLOC(T)  (T*)malloc(sizeof(T))
#define PFSD_MALLOC_ARR(n, T)  (T*)malloc((n) * sizeof(T))
#define PFSD_FREE(p)	free(p)

#define PFSD_ARRAY_SIZE(array) (sizeof(array) / sizeof(array[0]))

#define IS_2_POWER(n) (n > 0 && ((n & (n - 1)) == 0))

#define PFSD_ASSERT(cond)  do { \
		if (!(cond)) { \
			fprintf(stderr, "[%s:%d] %s", __func__, __LINE__, #cond); \
			pfsd_abort("assert", #cond, __func__, __LINE__);\
		} \
} while(0)

#ifndef PFSD_SERVER

#ifdef NDEBUG
#define PFSD_CLIENT_LOG		pfsd_debug
#else
#define PFSD_CLIENT_LOG		pfsd_info
#endif

#define PFSD_CLIENT_ELOG	pfsd_error

#endif

/* PFSD_CPUSET_FILE must in same volume for all pods */
#define PFSD_CPUSET_FILE "/var/run/pfsd/pfsd.cpuset"
#define PFSD_CPUSET_TIMEOUT_SECONDS (60)
/* bind at most 6 workers on a cpu for now, may be modified in future */
#define PFSD_THREADS_PERCPU (6)

typedef struct pfsd_cpu_record {
	int cr_index;                           /* cpu index */
	char cr_pbdname[64];                    /* bind by which pfsd */
	time_t cr_ts;                           /* timestamp in seconds */
	int cr_tindices[PFSD_THREADS_PERCPU];   /* worker threads index */
} __attribute__((aligned(512))) pfsd_cpu_record_t;

/* For example, if 16 workers, should split into 6,5,5 by PFSD_THREADS_PERCPU */
static inline
int* pfsd_calc_threads_per_cpu(int workers, int* groups)
{
	if (workers == 0)
		return NULL;

	int *arr = NULL;
	int ngroup = 0;
	/* special process for small workers */
	if (workers < 2 * (PFSD_THREADS_PERCPU - 1)) {
		int n[2];
		n[0] = (workers+1) / 2;
		n[1] = workers - n[0];
		ngroup = 1;
		if (n[1] != 0)
			ngroup++;

		arr = PFSD_MALLOC_ARR(ngroup, int);
		for (int i = 0; i < ngroup; ++i) {
			arr[i] = n[i];
		}
	} else {
		ngroup = (workers + PFSD_THREADS_PERCPU - 1) / PFSD_THREADS_PERCPU;
		int lacked = ngroup * PFSD_THREADS_PERCPU - workers;

		arr = PFSD_MALLOC_ARR(ngroup, int);
		for (int i = 0; i < ngroup; ++i) {
			if (i < lacked)
				arr[i] = PFSD_THREADS_PERCPU - 1;
			else
				arr[i] = PFSD_THREADS_PERCPU;
		}
	}

	if (groups != NULL)
		*groups = ngroup;

	return arr;
}

static inline void
pfsd_abort(const char *action, const char *cond, const char *func, int line)
{
#define	SYMBOL_SIZE	128
	void *buf[SYMBOL_SIZE];
	int nsym;
	char **syms;

	pfsd_error("failed to %s %s at %s: %d\n", action, cond, func, line);
	nsym = backtrace(buf, SYMBOL_SIZE);
	syms = backtrace_symbols(buf, nsym);
	for (int i = 0; i < nsym; i++)
		pfsd_error("%s\n", syms[i]);
	free(syms);

	abort();
}

/* EOF directory */
#define PFSD_DIR_END 1

#endif

