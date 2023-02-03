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

#include <errno.h>
#include <string.h>
#include <assert.h>
#include <sys/file.h>

#include "pfsd_common.h"
#include "pfsd_proto.h"

void
pfsd_robust_mutex_init(pthread_mutex_t *mutex)
{
	int r = 0;

	pthread_mutexattr_t attr;
	r |= pthread_mutexattr_init(&attr);
	r |= pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
	r |= pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	r |= pthread_mutex_init(mutex, &attr);
	pthread_mutexattr_destroy(&attr);
	assert (r == 0);
}

int
pfsd_robust_mutex_lock(pthread_mutex_t *mutex)
{
	if (mutex == NULL)
		return -1;

	int r = pthread_mutex_lock(mutex);
	if (r == EOWNERDEAD)
		r = pthread_mutex_consistent(mutex);

	assert (r == 0);
	return r;
}

int
pfsd_robust_mutex_trylock(pthread_mutex_t *mutex)
{
	if (mutex == NULL)
		return -1;

	int r = pthread_mutex_trylock(mutex);
	if (r != 0 && r != EOWNERDEAD)
		return -1;

	if (r == EOWNERDEAD)
		r = pthread_mutex_consistent(mutex);

	assert (r == 0);
	return r;
}

int
pfsd_robust_mutex_unlock(pthread_mutex_t *mutex)
{
	if (mutex == NULL)
		return -1;

	int r = pthread_mutex_unlock(mutex);
	r = r;
	assert (r == 0);
	return 0;
}

long
pfsd_tolong(void *ptr)
{
	union {
		long v;
		void *p;
	} vp;
	vp.p = ptr;
	return vp.v;
}

bool
pfsd_request_alive(pfsd_request_t *req)
{
	//This is special for thread mode mount to avoid alive check that only
	//relies on fd close detection.
	if (req->connid % 2)
		return true;
	int r = kill(req->owner, 0);
	if (r == -1 && errno == ESRCH)
		return false;

	return true;
}

int
pfsd_sdk_pbdname(const char *pbdpath, char *pbdname)
{
	if (pbdpath == NULL || pbdpath[0] != '/')
		return -1;

	int i = 1;
	while (pbdpath[i] != '\0' && pbdpath[i] == '/')
		i++;

	if (pbdpath[i] == '\0')
		return -1;

	const char *slash = strchr(pbdpath + i, '/');
	if (slash == NULL)
		return -1;

	size_t len = slash - (pbdpath + i);
	if (len == 0)
		return -1;
	else if (len >= PFS_MAX_NAMELEN)
		return -1;

	strncpy(pbdname, pbdpath + i, len);
	pbdname[len] = 0;
	return 0;
}

#define PATH_PFSD_RUN   "/var/run/pfsd"
#define PATH_PFS_RUN    "/var/run/pfs"
#define PATH_PFSD_SHM   "/dev/shm/pfsd"

static int
pfsd_mkdir(const char *dir)
{
	int rc;

	rc = access(dir, R_OK|W_OK|X_OK);
	if (rc == -1) {
		if (errno == ENOENT) {
			pfsd_info("directory %s does not exists, create it!", dir);
			rc = mkdir(dir, 0777);
			if (rc == -1) {
				pfsd_error("can not make directory: %s", dir);
				return -1;
			}
		} else if (errno) {
			pfsd_error("can not access directory: %s, error:%s", dir,
					strerror(errno));
			return -1;
		}
	}

	return 0;
}

int
pfsd_prepare_env(void)
{
	return pfsd_mkdir(PATH_PFSD_RUN) || pfsd_mkdir(PATH_PFS_RUN) ||
	       pfsd_mkdir(PATH_PFSD_SHM);
}

int
pfsd_pidfile_open(const char *pbdname)
{
	int fd = -1;
	char file[1024] = "";

	snprintf(file, sizeof(file), "/var/run/pfsd/pfsd_%s.pid", pbdname);
	fd = ::open(file, O_RDWR | O_CREAT | O_CLOEXEC, 0644);
	if (fd < 0) {
		pfsd_error("can not open/create file: %s, error: %s", file,
			   strerror(errno));
		return -errno;
	}

	if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
		pfsd_error("can not lock file: %s, error: %s", file,
			   strerror(errno));

		close(fd);
		return -errno;
	}

	return fd;
}

int
pfsd_pidfile_write(int fd)
{
	char buf[128];
	size_t size = snprintf(buf, sizeof(buf), "%ld", (long)getpid());
	int ret = write(fd, buf, size);
	if (ret != (int)size) {
		return -EIO;
	}
	return 0;
}

int
pfsd_pidfile_close(int fd) 
{
	return close(fd);
}

