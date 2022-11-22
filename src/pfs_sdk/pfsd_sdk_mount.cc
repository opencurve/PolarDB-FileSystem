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
#include <pthread.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/queue.h>

#include "pfs_mount.h"
#include "pfsd_sdk_mount.h"
#include "pfsd_common.h"
#include "pfsd_sdk_file.h"

#define FLK_LEN	1024
#define MOUNT_PREPARE_TIMEOUT_MS (30 * 1000)

int pfs_mount_epoch;                                                     
pthread_mutex_t pfs_mount_epoch_mtx = PTHREAD_MUTEX_INITIALIZER;
extern pthread_mutex_t pfs_init_mtx;
typedef TAILQ_HEAD(mount_list, mountargs) mount_list_t;
static mount_list_t mount_list = TAILQ_HEAD_INITIALIZER(mount_list);

struct mountargs *
pfs_mountargs_alloc(void)
{
	struct mountargs *mp;

	mp = PFSD_MALLOC(struct mountargs);
	bzero(mp, sizeof(*mp));
	pthread_rwlock_init(&mp->rwlock, NULL);
	mp->hostid_lock_fd = -1;
	mp->meta_lock_fd = -1;
	mp->conn_id = -1;
	mp->host_id = -1;
	return mp;
}

void
pfs_mountargs_free(struct mountargs *mp)
{
	assert(mp->ref_count == 0);		
	pthread_rwlock_destroy(&mp->rwlock);
	PFSD_FREE(mp);
}

static void
pfs_mountargs_register(struct mountargs *mp, int locked)
{
	if (!locked)
		pthread_mutex_lock(&pfs_init_mtx);
	if (!mp->on_list) {
		TAILQ_INSERT_HEAD(&mount_list, mp, link);
		mp->on_list = 1;
		mp->ref_count++;
	}
	if (!locked)
		pthread_mutex_unlock(&pfs_init_mtx);
}

static void
pfs_mountargs_unregister(struct mountargs *mp)
{
	pthread_mutex_lock(&pfs_init_mtx);
	if (mp->on_list) {
		TAILQ_REMOVE(&mount_list, mp, link);
		mp->on_list = 0;
		mp->ref_count--;
		if (mp->ref_count == 0)
			pfs_mountargs_free(mp);
	}
	pthread_mutex_unlock(&pfs_init_mtx);
}

int
pfs_mountargs_rdlock(struct mountargs *mp)
{
	return pthread_rwlock_rdlock(&mp->rwlock);
}

int
pfs_mountargs_wrlock(struct mountargs *mp)
{
	return pthread_rwlock_wrlock(&mp->rwlock);
}

int
pfs_mountargs_unlock(struct mountargs *mp)
{
	return pthread_rwlock_unlock(&mp->rwlock);
}

struct mountargs *
pfs_mountargs_find(const char *pbdname, int lock_mode)
{
	struct mountargs *mp = NULL;

again:
	pthread_mutex_lock(&pfs_init_mtx);
	TAILQ_FOREACH(mp, &mount_list, link) {
		if (!strcmp(pbdname, mp->pbd_name)) {
			break;
		}
	}
	if (mp)
		mp->ref_count++;
	pthread_mutex_unlock(&pfs_init_mtx);
	if (!mp)
		return NULL;
	if (lock_mode == RDLOCK) {
		pthread_rwlock_rdlock(&mp->rwlock);
	} else {
		pthread_rwlock_wrlock(&mp->rwlock);
	}

	if (mp->on_list == 0) {
		pfs_mountargs_put(mp);
		goto again;
	}
	return mp;
}

int
pfs_mountargs_exists(const char *pbdname)
{
	struct mountargs *mp = NULL;

	TAILQ_FOREACH(mp, &mount_list, link) {
		if (!strcmp(pbdname, mp->pbd_name)) {
			break;
		}
	}
	return mp != NULL;
}

void
pfs_mountargs_put(struct mountargs *mp)
{
	pthread_rwlock_unlock(&mp->rwlock);

	pthread_mutex_lock(&pfs_init_mtx);
	mp->ref_count--;
	if (mp->ref_count == 0) {
		pfs_mountargs_free(mp);
	}
	pthread_mutex_unlock(&pfs_init_mtx);
}

int
pfs_mountargs_foreach(int (*cb)(struct mountargs *, void *arg), void *arg)
{
	struct mountargs *mp = NULL;
	int rc = 0;
	pthread_mutex_lock(&pfs_init_mtx);
	TAILQ_FOREACH(mp, &mount_list, link) {
		rc |= cb(mp, arg);
	}
	pthread_mutex_unlock(&pfs_init_mtx);
	return rc;
}

int
pfsd_paxos_hostid_local_lock(const char *pbdname, int hostid, const char *caller)
{
	char pathbuf[PFS_MAX_PATHLEN];
	struct flock flk;
	mode_t omask;
	ssize_t size;
	int err, fd;

	size = snprintf(pathbuf, sizeof(pathbuf),
	    "/var/run/pfs/%s-paxos-hostid", pbdname);
	if (size >= (ssize_t)sizeof(pathbuf)) {
		errno = ENAMETOOLONG;
		return -1;
	}

	omask = umask(0000);
	err = fd = open(pathbuf, O_CREAT | O_RDWR | O_CLOEXEC, 0666);
	(void)umask(omask);
	if (err < 0) {
		PFSD_CLIENT_ELOG("cant open file %s, err=%d, errno=%d",
			pathbuf, err, errno);
		errno = EACCES;
		return -1;
	}

	/*
	 * Writer with host N will try to lock FLK_LEN*[N, N+1) region
	 * of access file. If the writer is a mkfs/growfs which's hostid
	 * is 0, then both l_start and l_len are zero, the whole file will
	 * be locked according to fcntl(2).
	 */
	memset(&flk, 0, sizeof(flk));
	flk.l_type = F_WRLCK;
	flk.l_whence = SEEK_SET;
	flk.l_start = hostid * FLK_LEN;
	flk.l_len = hostid > 0 ? FLK_LEN : 0;
	err = fcntl(fd, F_SETLK, &flk);
	if (err < 0) {
		PFSD_CLIENT_ELOG("%s cant lock file %s [%ld, %ld), err=%d,"
		   " errno=%d", caller, pathbuf, flk.l_start,
		   flk.l_start + flk.l_len, err, errno);
		(void)close(fd);
		errno = EACCES;
		return -1;
	}

	return fd;
}

void
pfsd_paxos_hostid_local_unlock(int fd)
{
	if (fd >= 0)
		close(fd);
}


struct mountargs *
pfs_mount_prepare(const char *cluster, const char *pbdname, int host_id,
    int flags)
{
	int fd = -1;
	mountargs_t *result = NULL;
	if (!cluster || !pbdname) {
		PFSD_CLIENT_ELOG("invalid cluster(%p) or pbdname(%p)",
		    cluster, pbdname);
		errno = EINVAL;
		return NULL;
	}
	if (strlen(pbdname) >= PFS_MAX_PBDLEN) {
		PFSD_CLIENT_ELOG("pbdname(%p) too long", pbdname);
		errno = EINVAL;
		return NULL;
	}
	PFSD_CLIENT_LOG("begin prepare mount cluster(%s), PBD(%s), hostid(%d),"
	    "flags(0x%x)", cluster, pbdname, host_id, flags);

	result = pfs_mountargs_alloc();
	if (result == NULL) {
		errno = ENOMEM;
		return NULL;
	}
	if ((flags & MNTFLG_WR) == 0) {
		// read only
		goto out;
	}

	if ((flags & MNTFLG_TOOL) == 0) {
		/*
		* Don't conflict with growfs.
		* growfs can run when DB is running, so it can't lock the whole
		* file like mkfs. growfs will lock the region after normal paxos
		* regions.
		*/
		int timeout_ms = MOUNT_PREPARE_TIMEOUT_MS;
		while (timeout_ms >= 0) {
			fd = pfsd_paxos_hostid_local_lock(pbdname,
			    DEFAULT_MAX_HOSTS + 1, __func__);
			if (fd >= 0)
				break;

			PFSD_CLIENT_ELOG("can't got locallock when prepare"
			    " mount PBD(%s), hostid(%d) %s", pbdname, host_id,
			    strerror(errno));
			if (errno != EACCES)
				goto err_handle;

			usleep(10 * 1000);
			timeout_ms -= 10;
		}

		if (fd < 0) {
			errno = ETIMEDOUT;
			goto err_handle;
		}

		result->meta_lock_fd = fd;
	}

	if ((flags & PFS_TOOL) != 0 && host_id == 0) {
		fd = pfsd_paxos_hostid_local_lock(pbdname, DEFAULT_MAX_HOSTS + 2,
		    __func__);
	} else {
		fd = pfsd_paxos_hostid_local_lock(pbdname, host_id, __func__);
	}

	if (fd < 0) {
		PFSD_CLIENT_ELOG("fail got locallock when prepare mount PBD(%s),"
		   " hostid(%d) %s", pbdname, host_id, strerror(errno));
		goto err_handle;
	}

	result->hostid_lock_fd = fd;

out:
	strncpy(result->pbd_name, pbdname, sizeof(result->pbd_name));
	result->pbd_name[sizeof(result->pbd_name)-1] = '\0';
	result->host_id = host_id;
	result->flags = flags;
	PFSD_CLIENT_LOG("pfs_mount_prepare success for %s hostid %d", pbdname,
	    host_id);
	return result;

err_handle:
	pfsd_paxos_hostid_local_unlock(result->hostid_lock_fd);
	pfsd_paxos_hostid_local_unlock(result->meta_lock_fd);
	pfs_mountargs_free(result);
	if (errno == 0)
		errno = EINVAL;
	PFSD_CLIENT_ELOG("pfs_mount_prepare failed for %s hostid %d, err %s",
	    pbdname, host_id, strerror(errno));
	return NULL;
}

void
pfs_mount_atfork_child()
{
	TAILQ_INIT(&mount_list);
	pthread_mutex_init(&pfs_init_mtx, NULL);
	pthread_mutex_init(&pfs_mount_epoch_mtx, NULL);
}

void
pfs_mount_post(void *handle, int err)
{
	mountargs_t *result = (mountargs_t*)handle;

	if (result->meta_lock_fd >= 0) {
		pfsd_paxos_hostid_local_unlock(result->meta_lock_fd);
		result->meta_lock_fd = -1;
	}

	if (err < 0) {
		pfsd_paxos_hostid_local_unlock(result->hostid_lock_fd);
		pfs_mountargs_free(result);
	} else {
		pfs_mountargs_register(result, true);
	}
	PFSD_CLIENT_LOG("pfs_mount_post err : %d", err);
}

int
pfs_remount_prepare(struct mountargs *mp, const char *cluster,
    const char *pbdname, int host_id, int flags)
{
	int fd = -1;
	if (!pbdname || !cluster) {
		PFSD_CLIENT_ELOG("invalid cluster(%p) or pbdname(%p)",
		    cluster, pbdname);
		errno = EINVAL;
		return -1;
	}
	if ((flags & MNTFLG_TOOL) != 0 || (flags & MNTFLG_WR) == 0 ) {
		PFSD_CLIENT_ELOG("invalid remount flags(%#x)", flags);
		errno = EINVAL;
		return -1;
	}
	PFSD_CLIENT_LOG("remount cluster(%s), PBD(%s), hostid(%d),flags(%#x)",
	    cluster, pbdname, host_id, flags);
	fd = pfsd_paxos_hostid_local_lock(pbdname, host_id, __func__);
	if (fd < 0) {
		goto err_handle;
	}
	mp->hostid_lock_fd = fd;
	return 0;
err_handle:
	pfsd_paxos_hostid_local_unlock(mp->hostid_lock_fd);
	mp->hostid_lock_fd = -1;
	return -1;
}

void
pfs_remount_post(void *handle, int err)
{
	mountargs_t *result = (mountargs_t*)handle;
	if (err < 0) {
		PFSD_CLIENT_ELOG("remount failed %d", err);
		pfsd_paxos_hostid_local_unlock(result->hostid_lock_fd);
		result->hostid_lock_fd = -1;
	}
}

void
pfs_umount_prepare(const char *pbdname, void *handle)
{
	mountargs_t *result = (mountargs_t*)handle;
	if (result->meta_lock_fd >= 0) {
		pfsd_paxos_hostid_local_unlock(result->meta_lock_fd);
		result->meta_lock_fd = -1;
	}
	PFSD_CLIENT_LOG("pfs_umount_prepare. pbdname:%s", pbdname);
}

void
pfs_umount_post(const char *pbdname, void *handle)
{
	mountargs_t *result = (mountargs_t*)handle;
	assert (result->meta_lock_fd < 0);

	pfsd_paxos_hostid_local_unlock(result->hostid_lock_fd);
	pfs_mountargs_unregister(result);
	PFSD_CLIENT_LOG("pfs_umount_post. pbdname:%s", pbdname);
}

