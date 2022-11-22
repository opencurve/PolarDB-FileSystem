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


#ifndef PFSD_MOUNT_SHARE_H_
#define PFSD_MOUNT_SHARE_H_

#include <sys/queue.h>

typedef struct mountargs {
	TAILQ_ENTRY(mountargs) link;
	pthread_rwlock_t rwlock;
	int meta_lock_fd;
	int hostid_lock_fd;
	int flags;
	int host_id;
	int conn_id;
	int on_list;
	int ref_count;
	char pbd_name[PFS_MAX_PBDLEN];
} mountargs_t;

enum {RDLOCK, WRLOCK};

struct mountargs *pfs_mount_prepare(const char *cluster, const char *pbdname,
	int host_id, int flags);
int	pfs_remount_prepare(struct mountargs *ma, const char *cluster,
			const char *pbdname, int host_id, int flags);
void 	pfs_mount_post(void *handle, int err);
void 	pfs_mount_atfork_child(void);
void 	*pfs_remount_prepare(const char *cluster, const char *pbdname,
    int host_id, int flags);
void 	pfs_remount_post(void *handle, int err);

void	pfs_umount_prepare(const char *pbdname, void *handle);
void	pfs_umount_post(const char *pbdname, void *handle);
struct mountargs *pfs_mountargs_find(const char *pbdname, int lock_mode);
void	pfs_mountargs_put(struct mountargs *mp);
int	pfs_mountargs_foreach(int (*cb)(struct mountargs *, void *arg),
		void *arg);
int	pfs_mountargs_exists(const char *pbdname);
int	pfs_mountargs_rdlock(struct mountargs *mp);
int	pfs_mountargs_wrlock(struct mountargs *mp);
int	pfs_mountargs_unlock(struct mountargs *mp);
#endif //PFSD_MOUNT_SHARE_H_
