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

#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <time.h>

#include "pfsd_common.h"
#include "pfsd_proto.h"
#include "pfsd_shm.h"
#include "pfsd_sdk_file.h"
#include "pfsd_sdk.h"
#include "pfsd_sdk_mount.h"

#include "pfsd_chnl.h"
#include "pfsd_chnl_shm.h"

/* init once */
pthread_mutex_t pfs_init_mtx = PTHREAD_MUTEX_INITIALIZER;
static int s_inited = 0;

static int s_mode = PFSD_SDK_PROCESS;
static char s_svraddr[PFS_MAX_PATHLEN];
static int s_timeout_ms = 20 * 1000;
static int s_remount_timeout_ms = 2000 * 1000;

#define CHECK_WRITABLE(mp) do { \
	if (unlikely(!pfsd_writable((mp)->flags))) { \
		pfs_mountargs_put(mp); \
		errno = EROFS; \
		return -1; \
	} \
} while(0)

#define CHECK_MOUNT2(mp, pbdname, mode) do { \
	mp = pfs_mountargs_find((pbdname), mode); \
	if (unlikely(mp == NULL)) { \
		PFSD_CLIENT_ELOG("No such device %s mounted", (pbdname));\
		errno = ENODEV; \
		return -1; \
	} \
} while(0)

#define CHECK_MOUNT(mp, pbdname) CHECK_MOUNT2(mp, pbdname, RDLOCK) 

#define CHECK_MOUNT_RETVAL(mp, pbdname, retval) do { \
	mp = pfs_mountargs_find((pbdname), RDLOCK); \
	if (unlikely(mp == NULL)) { \
		PFSD_CLIENT_ELOG("No such device %s mounted", (pbdname));\
		errno = ENODEV; \
		return retval; \
	} \
} while(0)

#define PUT_MOUNT(mp) pfs_mountargs_put((mp))

static ssize_t pfsd_file_pread(pfsd_file_t *file, void *buf, size_t len,
	off_t off);
static ssize_t pfsd_file_pwrite(pfsd_file_t *file, const void *buf, size_t len,
	off_t off);
static off_t pfsd_file_lseek(pfsd_file_t *file, off_t offset, int whence);

void
pfsd_set_mode(int mode)
{
	if (mode == PFSD_SDK_THREADS || mode == PFSD_SDK_PROCESS)
		s_mode = mode;
	else
		PFSD_CLIENT_ELOG("Wrong mode %d, expect 0(threads), 1(processes)", mode);
}

void
pfsd_set_svr_addr(const char *svraddr, size_t len)
{
	if (len >= PFS_MAX_PATHLEN) {
		PFSD_CLIENT_ELOG("Too long path %s", svraddr);
		return;
	}

	strncpy(s_svraddr, svraddr, len);
	s_svraddr[len-1] = '\0';
}

void
pfsd_set_connect_timeout(int timeout_ms)
{
	if (timeout_ms <= 0)
		return;
	if (timeout_ms > 24 * 3600 * 1000)
		return;

	s_timeout_ms = timeout_ms;
}

/* when child process is ready */
void
pfsd_atfork_child_post()
{
	/* init rand seed for each process */

	struct timeval now;
	gettimeofday(&now, NULL);
	srand((unsigned)((now.tv_sec + now.tv_usec) ^ getpid()));

	pthread_mutex_init(&pfs_init_mtx, NULL);
	pfsd_sdk_file_reinit();
	pfsd_connect_child_post();  
	pfs_mount_atfork_child();
}

int
pfsd_sdk_init(int mode, const char *svraddr, int timeout_ms,
    const char *cluster, const char *pbdname, int host_id, int flags)
{
	int conn_id;
	struct mountargs *mp = NULL;

	if (cluster == NULL)
		cluster = "polarstore";

	pthread_mutex_lock(&pfs_init_mtx);
	if (s_inited == 1) {
		PFSD_CLIENT_LOG("sdk has already been initialized by other threads");
		goto mount_vol;
	}

	pfsd_chnl_shm_client_init(); /* ! forced link pfsd_chnl_shm.o in libpfsd.a */

	if (flags & MNTFLG_TOOL) {
		char logfile[1024] = "";
		(void)snprintf(logfile, sizeof(logfile), "/var/log/pfs-%s.log", pbdname);
		int fd = open(logfile, O_CREAT | O_WRONLY | O_APPEND | O_CLOEXEC, 0666);
		if (fd < 0) {
			fprintf(stderr, "cant open logfile %s\n", logfile);
		} else  {
			if (dup2(fd, STDERR_FILENO) < 0) {
				fprintf(stderr, "cant dup fd %d to stderr\n", fd);
				close(fd);
				fd = -1;
			}
			chmod(logfile, 0666);
			close(fd);
		}
	}

	pfsd_sdk_file_init();

	if (s_svraddr[0] == '\0') {
		if (svraddr[0] == '\0') {
			strncpy(s_svraddr, PFSD_USER_PID_DIR, sizeof(s_svraddr));
		} else {
			strncpy(s_svraddr, svraddr, sizeof(s_svraddr));
		}
		s_svraddr[sizeof(s_svraddr)-1] = '\0';
	}

	srand(time(NULL));

	if (mode == PFSD_SDK_PROCESS) {
		pthread_atfork(NULL, NULL, pfsd_atfork_child_post);
	}

	s_inited = 1;

mount_vol:
	if (pfs_mountargs_exists(pbdname) ||
            pfs_mountargs_inprogress(pbdname)) {
		PFSD_CLIENT_ELOG("pbd %s is already mounted", pbdname);
		pthread_mutex_unlock(&pfs_init_mtx);
		return -1;
	}

	/* local hostid lock */
	errno = 0;
	mp = pfs_mount_prepare(cluster, pbdname, host_id, flags);
	if (mp == NULL) {
		PFSD_CLIENT_ELOG("pfs_mount_prepare failed, maybe hostid %d used, err %s", host_id, strerror(errno));
		goto failed;
	}
	pfs_mountargs_add_inprogress(mp);	

	pthread_mutex_unlock(&pfs_init_mtx);
	conn_id = pfsd_chnl_connect(s_svraddr, cluster, timeout_ms, pbdname, host_id, flags);
	pthread_mutex_lock(&pfs_init_mtx);

	PFSD_CLIENT_LOG("pfsd_chnl_connect %s", conn_id > 0 ? "success" : "failed");
	if (conn_id <= 0)
		goto failed;

	pfs_mountargs_remove_inprogress(mp);
	mp->conn_id = conn_id;
	pfs_mount_post(mp, 0);
	pthread_mutex_unlock(&pfs_init_mtx);
	return 0;

failed:
	if (mp) {
		pfs_mountargs_remove_inprogress(mp);	
		pfs_mount_post(mp, -1);
	}
	pthread_mutex_unlock(&pfs_init_mtx);
	return -1;
}

#define CHECK_STALE(rsp) do {\
	if (unlikely(rsp->error == ESTALE)) { \
		PFSD_CLIENT_LOG("Stale request, rsp type %d!!!", rsp->type); \
		rsp->error = 0; \
		pfsd_chnl_update_meta(conn_id, req->mntid); \
		pfsd_chnl_buffer_free(conn_id, req, rsp, NULL, pfsd_tolong(ch)); \
		goto retry;\
	} \
} while(0)\

int
pfsd_mount(const char *cluster, const char *pbdname, int hostid, int flags)
{
	return pfsd_sdk_init(s_mode, s_svraddr, s_timeout_ms, cluster, pbdname, hostid, flags);
}

int
pfsd_increase_epoch(const char *pbdname)
{
	struct mountargs *mp = NULL;
	int err = 0;
	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	pfsd_response_t *rsp = NULL;
	int conn_id = -1;

	CHECK_MOUNT(mp, pbdname);
	conn_id = mp->conn_id;
retry:
	if (pfsd_chnl_buffer_alloc(conn_id, 0, (void**)&req, 0, (void**)&rsp,
	    NULL, (long*)(&ch)) != 0) {
		PUT_MOUNT(mp);
		errno = ENOMEM;
		return -1;
	}

	PFSD_CLIENT_LOG("increae epoch for %s", pbdname);

	/* fill request */
	req->type = PFSD_REQUEST_INCREASEEPOCH;
	strncpy(req->i_req.g_pbd, pbdname, PFS_MAX_PBDLEN);

	pfsd_chnl_send_recv(conn_id, req, 0, rsp, 0, NULL, pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	if (rsp->error != 0) {
		errno = rsp->error;
		err = -1;
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, NULL, pfsd_tolong(ch));
	PUT_MOUNT(mp);
	return err;
}

int
pfsd_umount_force(const char *pbdname)
{
	struct mountargs *mp = NULL;
	PFSD_CLIENT_LOG("pbdname %s", pbdname);

	CHECK_MOUNT2(mp, pbdname, WRLOCK);

	pfs_umount_prepare(pbdname, mp);
	int err = pfsd_chnl_close(mp->conn_id, true);
	if (err == 0) {
		pfsd_close_all_files(mp);
		pfs_umount_post(pbdname, mp);
		PFSD_CLIENT_LOG("umount success for %s", pbdname);
	} else {
		PFSD_CLIENT_ELOG("umount failed for %s", pbdname);
	}
	PUT_MOUNT(mp);

	return err;
}

int
pfsd_umount(const char *pbdname)
{
	struct mountargs *mp = NULL;
	PFSD_CLIENT_LOG("pbdname %s", pbdname);

	CHECK_MOUNT2(mp, pbdname, WRLOCK);
	int err = pfsd_chnl_close(mp->conn_id, false);
	if (err == 0) {
		pfsd_close_all_files(mp);
		pfs_umount_post(pbdname, mp);
		PFSD_CLIENT_LOG("umount success for %s", pbdname);
	} else {
		PFSD_CLIENT_ELOG("umount failed for %s", pbdname);
	}
	PUT_MOUNT(mp);
	return err;
}

int
pfsd_remount(const char *cluster, const char *pbdname, int hostid, int flags)
{
	struct mountargs *mp = NULL;;
	int res;

	CHECK_MOUNT2(mp, pbdname, WRLOCK);

	if (hostid != mp->host_id) {
		PFSD_CLIENT_ELOG("pfs_remount with diff hostid %d, expect %d",
				 hostid, mp->host_id);
		PUT_MOUNT(mp);
		errno = EINVAL;
		return -1;
	}

	if (mp->flags & MNTFLG_WR) {
		PFSD_CLIENT_ELOG("pfs_remount no need, already rw mount: %#x",
				 mp->flags);
		PUT_MOUNT(mp);
		errno = EINVAL;
		return -1;
	}

	if (cluster == NULL)
		cluster = "polarstore";

	errno = 0;
	if (pfs_remount_prepare(mp, cluster, pbdname, hostid, flags)) {
		PFSD_CLIENT_ELOG("pfs_remount_prepare failed, maybe hostid %d used, err %s", hostid, strerror(errno));
		goto failed;
	}
	/* reconnect, use same connid */
	res = pfsd_chnl_reconnect(mp->conn_id, cluster, s_remount_timeout_ms, pbdname, hostid, flags);
	if (res == 0) {
		mp->flags = flags;
	} else {
		goto failed;
	}

	pfs_remount_post(mp, 0);
	PUT_MOUNT(mp);

	return 0;

failed:
	if (mp) {
		pfs_remount_post(mp, -1);
		PUT_MOUNT(mp);
	}

	return -1;
}

static int
abort_conn(struct mountargs *mp, void *arg)
{
	int pid = (int)(intptr_t)arg;
	return pfsd_chnl_abort(mp->conn_id, pid);
}

int
pfsd_abort_request(pid_t pid)
{
	int rc = pfs_mountargs_foreach(abort_conn, (void *)(intptr_t)pid);
	if (rc)
		return -1;
	return 0;
}

int
pfsd_mount_growfs(const char *pbdname)
{
	struct mountargs *mp = NULL;

	CHECK_MOUNT(mp, pbdname);

	const int conn_id = mp->conn_id;
	int err = 0;
	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	pfsd_response_t *rsp = NULL;

retry:
	if (pfsd_chnl_buffer_alloc(mp->conn_id, 0, (void**)&req, 0, (void**)&rsp,
	    NULL, (long*)(&ch)) != 0) {
		PUT_MOUNT(mp);
		errno = ENOMEM;
		return -1;
	}

	PFSD_CLIENT_LOG("growfs for %s", pbdname);

	/* fill request */
	req->type = PFSD_REQUEST_GROWFS;
	strncpy(req->g_req.g_pbd, pbdname, PFS_MAX_PBDLEN);

	pfsd_chnl_send_recv(conn_id, req, 0, rsp, 0, NULL, pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	if (rsp->error != 0) {
		errno = rsp->error;
		err = -1;
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, NULL, pfsd_tolong(ch));
	PUT_MOUNT(mp);
	return err;
}

int
pfsd_rename(const char *oldpbdpath, const char *newpbdpath)
{
	struct mountargs *mp = NULL;

	if (oldpbdpath == NULL || newpbdpath == NULL) {
		errno = EINVAL;
		PFSD_CLIENT_ELOG("NULL args");
		return -1;
	}

	char oldpath[PFS_MAX_PATHLEN], newpath[PFS_MAX_PATHLEN];

	oldpbdpath = pfsd_name_init(oldpbdpath, oldpath, sizeof oldpath);
	if (oldpbdpath == NULL) {
		PFSD_CLIENT_ELOG("wrong oldpbdpath %s", oldpbdpath);
		return -1;
	}

	newpbdpath = pfsd_name_init(newpbdpath, newpath, sizeof newpath);
	if (newpbdpath == NULL) {
		PFSD_CLIENT_ELOG("wrong newpbdpath %s", oldpbdpath);
		return -1;
	}

	int err = 0;

	char oldpbd[PFS_MAX_NAMELEN];
	char newpbd[PFS_MAX_NAMELEN];
	if (pfsd_sdk_pbdname(oldpbdpath, oldpbd) != 0 ||
		pfsd_sdk_pbdname(newpbdpath, newpbd) != 0) {
		PFSD_CLIENT_ELOG("wrong pbdpath:  old %s, new %s", oldpbdpath, newpbdpath);
		errno = EINVAL;
		return -1;
	}

	/* Don't support rename between different PBD */
	if (strncmp(oldpbd, newpbd, PFS_MAX_NAMELEN) != 0) {
		PFSD_CLIENT_ELOG("Rename must in same pbd: [%s] != [%s]", oldpbd, newpbd);
		errno = EXDEV;
		return -1;
	}

	CHECK_MOUNT(mp, newpbd);
	CHECK_WRITABLE(mp);

	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	unsigned char *buf = NULL;
	pfsd_response_t *rsp = NULL;
	int64_t iolen = 2 * PFS_MAX_PATHLEN;
	int const conn_id = mp->conn_id;

retry:
	if (pfsd_chnl_buffer_alloc(conn_id, iolen, (void**)&req, 0, (void**)&rsp,
	    (void**)&buf, (long*)(&ch)) != 0) {
		PUT_MOUNT(mp);
		errno = ENOMEM;
		return -1;
	}

	/* fill request */
	req->type = PFSD_REQUEST_RENAME;
	/* copy pbdpath to iobuf */
	strncpy((char*)buf, oldpath, PFS_MAX_PATHLEN);
	strncpy((char*)buf+PFS_MAX_PATHLEN, newpath, PFS_MAX_PATHLEN);

	pfsd_chnl_send_recv(conn_id, req, iolen,
	    rsp, 0, buf, pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	if (rsp->error != 0) {
		PFSD_CLIENT_ELOG("rename %s -> %s error: %d", oldpbdpath, newpbdpath, rsp->error);
		errno = rsp->error;
		err = -1;
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, buf, pfsd_tolong(ch));
	PUT_MOUNT(mp);
	return err;
}

int
pfsd_open(const char *pbdpath, int flags, mode_t mode)
{
	struct mountargs *mp = NULL;
	char abspath[PFS_MAX_PATHLEN];
	pbdpath = pfsd_name_init(pbdpath, abspath, sizeof abspath);
	if (pbdpath == NULL)
		return -1;

	char pbdname[PFS_MAX_NAMELEN];
	if (pfsd_sdk_pbdname(pbdpath, pbdname) != 0) {
		errno = EINVAL;
		return -1;
	}

	CHECK_MOUNT(mp, pbdname);
	int const conn_id = mp->conn_id;

	if (flags & (O_CREAT | O_TRUNC)) {
		CHECK_WRITABLE(mp);
	}

	pfsd_file_t *file = pfsd_alloc_file();
	if (file == NULL) {
		PUT_MOUNT(mp);
		errno = ENOMEM;
		return -1;
	}

	int fd = pfsd_alloc_fd(file);
	if (fd == -1) {
		errno = EMFILE;
		pfsd_free_file(file);
		PUT_MOUNT(mp);
		return -1;
	}
	file->f_flags = flags;

	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	pfsd_response_t *rsp = NULL;
	unsigned char *buf = NULL;

retry:
	if (pfsd_chnl_buffer_alloc(conn_id, PFS_MAX_PATHLEN, (void**)&req, 0,
	    (void**)&rsp, (void**)&buf, (long*)(&ch)) != 0) {
		errno = ENOMEM;
		pfsd_close_file(file);
		PUT_MOUNT(mp);
		return -1;
	}

	/* fill request */
	req->type = PFSD_REQUEST_OPEN;
	req->o_req.o_flags = flags;
	req->o_req.o_mode = mode;
	/* copy pbdpath to iobuf */
	strncpy((char*)buf, pbdpath, PFS_MAX_PATHLEN);

	pfsd_chnl_send_recv(conn_id, req, PFS_MAX_PATHLEN, rsp, 0, buf,
	    pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	file->f_inode = rsp->o_rsp.o_ino;
	file->f_common_pl = rsp->common_pl_rsp;
	if (file->f_inode == -1) {
		pfsd_close_file(file);
		errno = rsp->error;
		fd = -1;
		if (errno != ENOENT)
			PFSD_CLIENT_ELOG("open %s failed %s", pbdpath,
			    strerror(errno));
	} else {
		file->f_offset = rsp->o_rsp.o_off;
		file->f_conn_id = mp->conn_id;
		file->f_mp = mp;
		if (flags & O_CREAT)
			PFSD_CLIENT_LOG("open %s with inode %ld, fd %d",
			    pbdpath, file->f_inode, fd);
	}

	pfsd_chnl_buffer_free(mp->conn_id, req, rsp, buf, pfsd_tolong(ch));
	PUT_MOUNT(mp);

	if (fd < 0)
		return -1;

	return PFSD_FD_MAKE(fd);
}

int
pfsd_creat(const char *pbdpath, mode_t mode)
{
	return pfsd_open(pbdpath, O_CREAT | O_TRUNC | O_WRONLY, mode);
}

#define PFSD_SDK_GET_FILE(fd) do {\
	if (unlikely(!PFSD_FD_ISVALID(fd))) {\
		errno = EBADF; \
		return -1; \
	}\
	fd = PFSD_FD_RAW(fd); \
	file = pfsd_get_file(fd, false); \
	if (unlikely(file == NULL)) { \
		PFSD_CLIENT_ELOG("bad fd %d", fd);\
		errno = EBADF; \
		return -1; \
	} \
	mp = file->f_mp; \
	if (unlikely(mp == NULL)) { \
		pfsd_put_file(file, NULL); \
		errno = ENODEV; \
		return -1; \
	} \
	pfs_mountargs_rdlock(mp); \
} while(0)

#define OFFSET_FILE_POS     (-1)    /* offset is current file position */
#define OFFSET_FILE_SIZE    (-2)    /* offset is file size */

ssize_t
pfsd_read(int fd, void *buf, size_t len)
{
	pfsd_file_t *file = NULL;
	mountargs_t *mp = NULL;
	char *cbuf = (char *)buf;
	ssize_t rc = 0, total = 0, to_read = 0;
	int err = 0;

	PFSD_SDK_GET_FILE(fd);
	pthread_mutex_lock(&file->f_lseek_lock);
	while (len > 0) {
		to_read = len;
		if (to_read > PFSD_MAX_IOSIZE)
			to_read = PFSD_MAX_IOSIZE;
		rc = pfsd_file_pread(file, cbuf+total, to_read, file->f_offset);
		if (rc > 0) {
			file->f_offset += rc;
			total += rc;
			len -= rc;
		} else {
			err = rc;
			break;
		}
	}
	pthread_mutex_unlock(&file->f_lseek_lock);
	pfsd_put_file(file, mp);
	return err ? err : total;
}

ssize_t
pfsd_pread(int fd, void *buf, size_t len, off_t off)
{
	pfsd_file_t *file = NULL;
	mountargs_t *mp = NULL;
	char *cbuf = (char *)buf;
	ssize_t rc = 0, total = 0, to_read = 0;
	int err = 0;

	PFSD_SDK_GET_FILE(fd);
	while (len > 0) {
		to_read = len;
		if (to_read > PFSD_MAX_IOSIZE)
			to_read = PFSD_MAX_IOSIZE;		
		rc = pfsd_file_pread(file, cbuf+total, to_read, off+total);
		if (rc > 0) {
			total += rc;
			len -= rc;
		} else {
			err = rc;
			break;
		}
	}
	pfsd_put_file(file, mp);
	return err ? err : total; 
}

static ssize_t
pfsd_file_pread(pfsd_file_t *file, void *buf, size_t len, off_t off)
{
	if (unlikely(buf == NULL)) {
		errno = EINVAL;
		return -1;
	}

	if (unlikely(len > PFSD_MAX_IOSIZE)) {
		/* may shorten read */
		PFSD_CLIENT_LOG("pread len %lu is too big for fd %d, cast to 4MB.", len, file->f_fd);
		len = PFSD_MAX_IOSIZE;
	}

	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	unsigned char *rbuf = NULL;
	ssize_t ss = -1;
	pfsd_response_t *rsp = NULL;
	const int conn_id = file->f_conn_id;

	off_t off2 = off;

	if (unlikely(off2 < 0)) {
		errno = EINVAL;
		return -1;
	}

retry:
	if (unlikely(pfsd_chnl_buffer_alloc(conn_id, 0, (void**)&req, len,
	    (void**)&rsp, (void**)&rbuf, (long*)(&ch)) != 0)) {
		errno = ENOMEM;
		return -1;
	}

	/* fill request */
	req->type = PFSD_REQUEST_READ;
	req->r_req.r_ino = file->f_inode;
	req->r_req.r_len = len;
	req->r_req.r_off = off2;
	req->common_pl_req = file->f_common_pl;

	pfsd_chnl_send_recv(conn_id, req, 0, rsp, len, buf, pfsd_tolong(ch),
	    0);
	CHECK_STALE(rsp);

	if (rsp->r_rsp.r_len > 0)
		memcpy(buf, rbuf, rsp->r_rsp.r_len);

	ss = rsp->r_rsp.r_len;
	if (unlikely(ss < 0)) {
		errno = rsp->error;
		PFSD_CLIENT_ELOG("pread fd %d ino %ld error: %s", file->f_fd,
		    file->f_inode, strerror(errno));
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, buf, pfsd_tolong(ch));
	return ss;
}

ssize_t
pfsd_write(int fd, const void *buf, size_t len)
{
	pfsd_file_t *file = NULL;
	mountargs_t *mp = NULL;
	char *cbuf = (char *)buf;
	ssize_t rc = 0, total = 0, to_write = 0;
	int err = 0;

	PFSD_SDK_GET_FILE(fd);
	pthread_mutex_lock(&file->f_lseek_lock);
	while (len > 0) {
		to_write = len;
		if (to_write > PFSD_MAX_IOSIZE)
			to_write = PFSD_MAX_IOSIZE;
		rc = pfsd_file_pwrite(file, cbuf+total, to_write, OFFSET_FILE_POS);
		if (likely(rc > 0)) {
			total += rc;
			len -= rc;
		} else {
			err = rc;
			break;
		}
	}
	pthread_mutex_unlock(&file->f_lseek_lock);
	pfsd_put_file(file, mp);
	return err ? err : total;
}

ssize_t
pfsd_pwrite(int fd, const void *buf, size_t len, off_t off)
{
	pfsd_file_t *file = NULL;
	mountargs_t *mp = NULL;
	char *cbuf = (char *)buf;
	ssize_t rc = 0, total = 0, to_write = 0;
	int err = 0;

	if (unlikely(off < 0)) {
		errno = EINVAL;
		return -1;
	}
	PFSD_SDK_GET_FILE(fd);
	if (file->f_flags & O_APPEND) {
		pthread_mutex_lock(&file->f_lseek_lock);
	}
	while (len > 0) {
		to_write = len;
		if (to_write > PFSD_MAX_IOSIZE)
			to_write = PFSD_MAX_IOSIZE;
		rc = pfsd_file_pwrite(file, cbuf+total, to_write, off+total);
		if (likely(rc > 0)) {
			total += rc;
			len -= rc;
		} else {
			err = rc;
			break;
		}
	}
	if (file->f_flags & O_APPEND) {
		pthread_mutex_unlock(&file->f_lseek_lock);
	}
	pfsd_put_file(file, mp);
	return err ? err : total;
}

static ssize_t
pfsd_file_pwrite(pfsd_file_t *file, const void *buf, size_t len, off_t off)
{
	if (unlikely(buf == NULL)) {
		errno = EINVAL;
		return -1;
	}

	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	unsigned char *wbuf = NULL;
	pfsd_response_t *rsp = NULL;
	ssize_t ss = -1;
	const int conn_id = file->f_conn_id;

	if (unlikely(!pfsd_writable(file->f_mp->flags))) {
		errno = EROFS;
		return -1;
	}

	if (unlikely(len == 0)) {
		return 0;
	}

	if (unlikely(len > PFSD_MAX_IOSIZE)) {
		PFSD_CLIENT_ELOG("pwrite len %lu is too big for fd %d.", len, file->f_fd);
		errno = EFBIG;
		return -1;
	}

	off_t off2 = off;
	if (file->f_flags & O_APPEND)
		off2 = OFFSET_FILE_SIZE;
	else if (off == OFFSET_FILE_POS)
		 off2 = file->f_offset;

	if (unlikely(off2 < 0 && off2 != OFFSET_FILE_SIZE)) {
		PFSD_CLIENT_ELOG("pwrite wrong off2 %lu for fd %d.", off2, file->f_fd);
		errno = EINVAL;
		return -1;
	}

retry:
	if (unlikely(pfsd_chnl_buffer_alloc(conn_id, len, (void**)&req, 0,
	    (void**)&rsp, (void**)&wbuf, (long*)(&ch)) != 0)) {
		errno = ENOMEM;
		return -1;
	}

	/* fill request */
	req->type = PFSD_REQUEST_WRITE;
	req->w_req.w_ino = file->f_inode;
	req->w_req.w_len = len;
	req->w_req.w_off = off2;
	req->w_req.w_flags = file->f_flags;
	req->common_pl_req = file->f_common_pl;

	memcpy(wbuf, buf, len);

	pfsd_chnl_send_recv(conn_id, req, len, rsp, 0, wbuf, pfsd_tolong(ch),
	    0);

	if ((file->f_flags & O_APPEND) == 0)
		CHECK_STALE(rsp);

	ss = rsp->w_rsp.w_len;
	if (unlikely(ss < 0)) {
		errno = rsp->error;
		PFSD_CLIENT_ELOG("pwrite fd %d ino %ld error: %s", file->f_fd,
		    file->f_inode, strerror(errno));
	} else {
		if (ss >= 0 && off == -1) {
			file->f_offset += ss;
		}
		if ((file->f_flags & O_APPEND) != 0 && OFFSET_FILE_POS == off)
			file->f_offset = rsp->w_rsp.w_file_size;
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, wbuf, pfsd_tolong(ch));
	return ss;
}

int
pfsd_posix_fallocate(int fd, off_t offset, off_t len)
{
	return pfsd_fallocate(fd, 0, offset, len);
}

#define FALLOC_PFSFL_FIXED_OFFSET   0x0100  /* lower bits defined in falloc.h */

int
pfsd_fallocate(int fd, int mode, off_t offset, off_t len)
{
	if (fd < 0 || offset < 0 || len <= 0) {
		errno = (fd < 0) ? EBADF : EINVAL;
		return -1;
	}


	pfsd_file_t *file = NULL;
	mountargs_t *mp = NULL;
	PFSD_SDK_GET_FILE(fd);

	if (!pfsd_writable(mp->flags)) {
		pfsd_put_file(file, mp);
		errno = EROFS;
		return -1;
	}

	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	pfsd_response_t *rsp = NULL;
        const int conn_id = file->f_conn_id;
	int rv = -1;

retry:
	if (pfsd_chnl_buffer_alloc(conn_id, 0, (void**)&req, 0, (void**)&rsp,
	    NULL, (long*)(&ch)) != 0) {
		errno = ENOMEM;
		pfsd_put_file(file, mp);
		return -1;
	}

	PFSD_CLIENT_LOG("fallocate ino %ld off %ld len %ld", file->f_inode, offset, len);
	/* fill request */
	req->type = PFSD_REQUEST_FALLOCATE;
	req->fa_req.f_ino = file->f_inode;
	req->fa_req.f_len = len;
	req->fa_req.f_off = offset;
	req->fa_req.f_mode = mode;
	req->common_pl_req = file->f_common_pl;

	pfsd_chnl_send_recv(conn_id, req, 0,
	    rsp, 0, NULL, pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	rv = rsp->fa_rsp.f_res;
	if (rv != 0) {
		errno = rsp->error;
		PFSD_CLIENT_ELOG("fallocate ino %ld error: %s", file->f_inode, strerror(errno));
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, NULL, pfsd_tolong(ch));
	pfsd_put_file(file, mp);
	return rv;
}

int
pfsd_truncate(const char *pbdpath, off_t len)
{
	struct mountargs *mp = NULL;
	if (!pbdpath || len < 0) {
		errno = EINVAL;
		return -1;
	}

	char abspath[PFS_MAX_PATHLEN];
	pbdpath = pfsd_name_init(pbdpath, abspath, sizeof abspath);
	if (pbdpath == NULL)
		return -1;

	char pbdname[PFS_MAX_NAMELEN];
	if (pfsd_sdk_pbdname(pbdpath, pbdname) != 0) {
		errno = EINVAL;
		return -1;
	}

	CHECK_MOUNT(mp, pbdname);
	CHECK_WRITABLE(mp);

	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	unsigned char *buf = NULL;
	int rv = -1;
	pfsd_response_t *rsp = NULL;
	const int conn_id = mp->conn_id;

retry:
	if (pfsd_chnl_buffer_alloc(conn_id, PFS_MAX_PATHLEN, (void**)&req, 0,
	    (void**)&rsp, (void**)&buf, (long*)(&ch)) != 0) {
		PUT_MOUNT(mp);
		errno = ENOMEM;
		return -1;
	}

	PFSD_CLIENT_LOG("truncate %s len %ld", pbdpath, len);

	/* fill request */
	req->type = PFSD_REQUEST_TRUNCATE;
	req->t_req.t_len = len;
	/* copy pbdpath to iobuf */
	strncpy((char*)buf, pbdpath, PFS_MAX_PATHLEN);

	pfsd_chnl_send_recv(conn_id, req, PFS_MAX_PATHLEN, rsp, 0, buf,
	    pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	rv = rsp->t_rsp.t_res;
	if (rv != 0) {
		errno = rsp->error;
		PFSD_CLIENT_ELOG("truncate %s len %ld error: %s", pbdpath, len, strerror(errno));
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, buf, pfsd_tolong(ch));
	PUT_MOUNT(mp);
	return rv;
}

int
pfsd_ftruncate(int fd, off_t len)
{
	if (fd < 0 || len < 0) {
		errno = (fd < 0) ? EBADF : EINVAL;
		return -1;
	}


	pfsd_file_t *file = NULL;
	mountargs_t *mp = NULL;
	PFSD_SDK_GET_FILE(fd);
	if (!pfsd_writable(mp->flags)) {
		pfsd_put_file(file, mp);
		errno = EROFS;
		return -1;
	}

	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	int rv = -1;
	pfsd_response_t *rsp = NULL;
	const int conn_id = file->f_conn_id;

retry:
	if (pfsd_chnl_buffer_alloc(conn_id, 0, (void**)&req, 0,
	    (void**)&rsp, NULL, (long*)(&ch)) != 0) {
		pfsd_put_file(file, mp);
		errno = ENOMEM;
		return -1;
	}

	PFSD_CLIENT_LOG("ftruncate ino %ld, len %lu", file->f_inode, len);

	/* fill request */
	req->type = PFSD_REQUEST_FTRUNCATE;
	req->ft_req.f_ino = file->f_inode;
	req->ft_req.f_len = len;
	req->common_pl_req = file->f_common_pl;

	pfsd_chnl_send_recv(conn_id, req, 0, rsp, 0, NULL, pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	rv = rsp->ft_rsp.f_res;
	if (rv != 0) {
		errno = rsp->error;
		PFSD_CLIENT_ELOG("ftruncate ino %ld, len %lu: %s", file->f_inode, len, strerror(errno));
	}

	pfsd_put_file(file, mp);
	pfsd_chnl_buffer_free(conn_id, req, rsp, NULL, pfsd_tolong(ch));
	return rv;
}

int
pfsd_unlink(const char *pbdpath)
{
	struct mountargs *mp = NULL;
	char abspath[PFS_MAX_PATHLEN];
	pbdpath = pfsd_name_init(pbdpath, abspath, sizeof abspath);
	if (pbdpath == NULL)
		return -1;

	char pbdname[PFS_MAX_NAMELEN];
	if (pfsd_sdk_pbdname(pbdpath, pbdname) != 0) {
		errno = EINVAL;
		return -1;
	}

	CHECK_MOUNT(mp, pbdname);
	/* check writable */
	CHECK_WRITABLE(mp);

	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	unsigned char *buf = NULL;
	int rv = -1;
	pfsd_response_t *rsp = NULL;
	const int conn_id = mp->conn_id;

retry:
	if (pfsd_chnl_buffer_alloc(conn_id, PFS_MAX_PATHLEN, (void**)&req, 0,
	    (void**)&rsp, (void**)&buf, (long*)(&ch)) != 0) {
		PUT_MOUNT(mp);
		errno = ENOMEM;
		return -1;
	}

	PFSD_CLIENT_LOG("unlink %s", pbdpath);
	/* fill request */
	req->type = PFSD_REQUEST_UNLINK;
	/* copy pbdpath to iobuf */
	strncpy((char*)buf, pbdpath, PFS_MAX_PATHLEN);

	pfsd_chnl_send_recv(conn_id, req, PFS_MAX_PATHLEN,
	    rsp, 0, buf, pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	rv = rsp->un_rsp.u_res;
	if (rv != 0) {
		errno = rsp->error;
		if (errno != ENOENT)
			PFSD_CLIENT_ELOG("unlink %s: %s", pbdpath, strerror(errno));
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, buf, pfsd_tolong(ch));
	PUT_MOUNT(mp);
	return rv;
}

int
pfsd_stat(const char *pbdpath, struct stat *st)
{
	struct mountargs *mp = NULL;
	if (!pbdpath || !st) {
		errno = EINVAL;
		return -1;
	}

	char abspath[PFS_MAX_PATHLEN];
	pbdpath = pfsd_name_init(pbdpath, abspath, sizeof abspath);
	if (pbdpath == NULL)
		return -1;

	char pbdname[PFS_MAX_NAMELEN];
	if (pfsd_sdk_pbdname(pbdpath, pbdname) != 0) {
		errno = EINVAL;
		return -1;
	}

	CHECK_MOUNT(mp, pbdname);
	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	unsigned char *buf = NULL;
	int rv = -1;
	pfsd_response_t *rsp = NULL;
	int const conn_id = mp->conn_id;

retry:
	if (pfsd_chnl_buffer_alloc(conn_id, PFS_MAX_PATHLEN, (void**)&req, 0,
	    (void**)&rsp, (void**)&buf, (long*)(&ch)) != 0) {
		PUT_MOUNT(mp);
		errno = ENOMEM;
		return -1;
	}

	/* fill request */
	req->type = PFSD_REQUEST_STAT;
	/* copy pbdpath to iobuf */
	strncpy((char*)buf, pbdpath, PFS_MAX_PATHLEN);

	pfsd_chnl_send_recv(conn_id, req, PFS_MAX_PATHLEN,
	    rsp, 0, buf, pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	rv = rsp->s_rsp.s_res;
	if (rv != 0) {
		errno = rsp->error;
		if (errno != ENOENT)
			PFSD_CLIENT_ELOG("stat %s: %s", pbdpath, strerror(errno));
	} else {
		memcpy(st, &rsp->s_rsp.s_st, sizeof(*st));
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, buf, pfsd_tolong(ch));
	PUT_MOUNT(mp);
	return rv;
}

int
pfsd_fstat(int fd, struct stat *st)
{
	if (fd < 0 || !st) {
		errno = (fd < 0) ? EBADF : EINVAL;
		return -1;
	}

	pfsd_file_t *file = NULL;
	mountargs_t *mp = NULL;
	PFSD_SDK_GET_FILE(fd);

	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	int rv = -1;
	pfsd_response_t *rsp = NULL;
	int const conn_id = file->f_conn_id;

retry:
	if (pfsd_chnl_buffer_alloc(conn_id, 0, (void**)&req, 0,
	    (void**)&rsp, NULL, (long*)(&ch)) != 0) {
		pfsd_put_file(file, mp);
		errno = ENOMEM;
		return -1;
	}

	/* fill request */
	req->type = PFSD_REQUEST_FSTAT;
	req->f_req.f_ino = file->f_inode;
	req->common_pl_req = file->f_common_pl;

	pfsd_chnl_send_recv(conn_id, req, 0, rsp, 0, NULL, pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	rv = rsp->f_rsp.f_res;
	if (rv != 0) {
		errno = rsp->error;
		PFSD_CLIENT_ELOG("fstat %ld error: %s", file->f_inode, strerror(errno));
	} else {
		memcpy(st, &rsp->f_rsp.f_st, sizeof(*st));
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, NULL, pfsd_tolong(ch));
	pfsd_put_file(file, mp);
	return rv;
}

static off_t
local_file_lseek(pfsd_file_t *file, off_t offset, int whence)
{
	off_t old_offset, new_offset;

	switch (whence) {
		case SEEK_SET:
			old_offset = file->f_offset;
			new_offset = offset;
			goto check_file_offset;

		case SEEK_CUR:
			old_offset = file->f_offset;
			new_offset = old_offset + offset;
			break;

		case SEEK_END:
			errno = 0;
			return off_t(-1);

		default:
			errno = EINVAL;
			return off_t(-1);
	}

	if (offset > 0 && new_offset < old_offset) {
		errno = EOVERFLOW;
		return off_t(-1);
	}

	/*
	 * when offset < 0 with SEEK_END, f_offset is less than filesize,
	 * new_offset maybe bigger than f_offset. So we compare new_offset and
	 * file size.
	 */
	if (offset < 0 && new_offset > old_offset) {
		errno = EOVERFLOW;
		return off_t(-1);
	}

check_file_offset:
	if (new_offset < 0) {
		errno = EINVAL;
		return off_t(-1);
	} else {
		file->f_offset = new_offset;
		return file->f_offset;
	}
}

off_t
pfsd_lseek(int fd, off_t offset, int whence)
{
	off_t rc = -1;
	pfsd_file_t *file = NULL;
	mountargs_t *mp = NULL;

	PFSD_SDK_GET_FILE(fd);
	pthread_mutex_lock(&file->f_lseek_lock);
	rc = pfsd_file_lseek(file, offset, whence);
	pthread_mutex_unlock(&file->f_lseek_lock);
	pfsd_put_file(file, mp);
	return rc;
}

static off_t
pfsd_file_lseek(pfsd_file_t *file, off_t offset, int whence)
{
	/* for ask pfsd if SEEK_END */
	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	pfsd_response_t *rsp = NULL;
	int const conn_id = file->f_conn_id;

	off_t rv = -1;
	rv = local_file_lseek(file, offset, whence);
	if (rv >= 0)
		goto finish;
	if (rv == off_t(-1) && errno != 0)
		goto finish;

retry:
	/* ask pfsd to seek end */
	if (unlikely(pfsd_chnl_buffer_alloc(conn_id, 0, (void**)&req, 0,
	    (void**)&rsp, NULL, (long*)(&ch)) != 0)) {
		errno = ENOMEM;
		return -1;
	}

	/* fill request */
	req->type = PFSD_REQUEST_LSEEK;
	req->l_req.l_ino = file->f_inode;
	req->l_req.l_offset = offset;
	req->l_req.l_whence = whence;
	req->common_pl_req = file->f_common_pl;
	assert (whence == SEEK_END); /* must be SEED_END */

	pfsd_chnl_send_recv(conn_id, req, 0, rsp, 0, NULL, pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	if (unlikely(rsp->l_rsp.l_offset < 0)) {
		errno = rsp->error;
		rv = off_t(-1);
		PFSD_CLIENT_ELOG("lseek %ld off %ld error: %s", file->f_inode,
		    offset, strerror(errno));
	} else {
		file->f_offset = rsp->l_rsp.l_offset;
		rv = rsp->l_rsp.l_offset;
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, NULL, pfsd_tolong(ch));

finish:
	return rv;
}

int
pfsd_close(int fd)
{
	pfsd_file_t *file = NULL;
	int err = -EAGAIN;
	bool fdok = PFSD_FD_ISVALID(fd);
	if (!fdok)
		err = -EBADF;

	fd = PFSD_FD_RAW(fd);

	while (err == -EAGAIN){
		file = pfsd_get_file(fd, true);
		if (file == NULL) {
			err = -EBADF;
			break;
		}

		err = pfsd_close_file(file);
		if (err != 0) {
			PFSD_CLIENT_ELOG("close fd %d failed, err:%d", fd, err);
			pfsd_put_file(file, NULL);
		}
	}
	if (err < 0) {
		errno = -err;
		return -1;
	}
	return 0;
}

int
pfsd_chdir(const char *pbdpath)
{
	struct mountargs *mp = NULL;
	char abspath[PFS_MAX_PATHLEN];
	pbdpath = pfsd_name_init(pbdpath, abspath, sizeof abspath);
	if (pbdpath == NULL)
		return -1;

	assert (pbdpath == abspath);

	char pbdname[PFS_MAX_NAMELEN];
	if (pfsd_sdk_pbdname(pbdpath, pbdname) != 0) {
		errno = EINVAL;
		return -1;
	}

	CHECK_MOUNT(mp, pbdname);
	if (!pfsd_chdir_begin()) {
		PUT_MOUNT(mp);
		return -1;
	}

	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	unsigned char *buf = NULL;
	int rv = -1;
	pfsd_response_t *rsp = NULL;
	int const conn_id = mp->conn_id;

retry:
	if (pfsd_chnl_buffer_alloc(conn_id, PFS_MAX_PATHLEN, (void**)&req, 0,
	    (void**)&rsp, (void**)&buf, (long*)(&ch)) != 0) {
		pfsd_chdir_end();
		PUT_MOUNT(mp);
		errno = ENOMEM;
		return -1;
	}

	/* fill request */
	req->type = PFSD_REQUEST_CHDIR;
	/* copy pbdpath to iobuf */
	strncpy((char*)buf, pbdpath, PFS_MAX_PATHLEN);

	pfsd_chnl_send_recv(conn_id, req, PFS_MAX_PATHLEN,
	    rsp, 0, buf, pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	rv = rsp->cd_rsp.c_res;
	if (rv != 0) {
		errno = rsp->error;
		PFSD_CLIENT_ELOG("chdir %s error: %s", pbdpath, strerror(errno));
	} else {
		int err = pfsd_normalize_path(abspath);
		if (err == 0) {
			err = pfsd_dir_xsetwd(abspath, strlen(abspath));
		}
		if (err != 0) {
			errno = err;
			rv = -1;
		}
	}

	pfsd_chdir_end();
	pfsd_chnl_buffer_free(conn_id, req, rsp, buf, pfsd_tolong(ch));
	PUT_MOUNT(mp);
	return rv;
}

char *
pfsd_getwd(char *buf)
{
	return pfsd_getcwd(buf, PFS_MAX_PATHLEN);
}

char *
pfsd_getcwd(char *buf, size_t size)
{
	int err = -EAGAIN;

	if (!buf)
		err = -EINVAL;

	while (err == -EAGAIN) {
		err = pfsd_dir_xgetwd(buf, size);
	}

	if (err < 0) {
		errno = -err;
		PFSD_CLIENT_ELOG("getcwd error: %s", strerror(errno));
		return NULL;
	}

	return buf;
}

int
pfsd_mkdir(const char *pbdpath, mode_t mode)
{
	struct mountargs *mp = NULL;
	char abspath[PFS_MAX_PATHLEN];
	pbdpath = pfsd_name_init(pbdpath, abspath, sizeof abspath);
	if (pbdpath == NULL)
		return -1;

	char pbdname[PFS_MAX_NAMELEN];
	if (pfsd_sdk_pbdname(pbdpath, pbdname) != 0) {
		errno = EINVAL;
		return -1;
	}

	CHECK_MOUNT(mp, pbdname);
	CHECK_WRITABLE(mp);

	int err = 0;
	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	unsigned char *buf = NULL;
	pfsd_response_t *rsp = NULL;
	int const conn_id = mp->conn_id;

retry:
	if (pfsd_chnl_buffer_alloc(conn_id, PFS_MAX_PATHLEN, (void**)&req, 0,
	    (void**)&rsp, (void**)&buf, (long*)(&ch)) != 0) {
		PUT_MOUNT(mp);
		errno = ENOMEM;
		return -1;
	}

	PFSD_CLIENT_LOG("mkdir %s", pbdpath);
	/* fill request */
	req->type = PFSD_REQUEST_MKDIR;
	/* copy pbdpath to iobuf */
	strncpy((char*)buf, pbdpath, PFS_MAX_PATHLEN);

	pfsd_chnl_send_recv(conn_id, req, PFS_MAX_PATHLEN,
	    rsp, 0, buf, pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	if (rsp->mk_rsp.m_res != 0) {
		err = -1;
		errno = rsp->error;
		PFSD_CLIENT_ELOG("mkdir %s error: %s", pbdpath, strerror(errno));
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, buf, pfsd_tolong(ch));
	PUT_MOUNT(mp);
	return err;
}

int
pfsd_rmdir(const char *pbdpath)
{
	struct mountargs *mp = NULL;
	char abspath[PFS_MAX_PATHLEN];
	pbdpath = pfsd_name_init(pbdpath, abspath, sizeof abspath);
	if (pbdpath == NULL)
		return -1;

	char pbdname[PFS_MAX_NAMELEN];
	if (pfsd_sdk_pbdname(pbdpath, pbdname) != 0) {
		errno = EINVAL;
		return -1;
	}

	CHECK_MOUNT(mp, pbdname);
	CHECK_WRITABLE(mp);

	int err = 0;
	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	unsigned char *buf = NULL;
	pfsd_response_t *rsp = NULL;
	int const conn_id = mp->conn_id;

retry:
	if (pfsd_chnl_buffer_alloc(conn_id, PFS_MAX_PATHLEN, (void**)&req, 0,
	    (void**)&rsp, (void**)&buf, (long*)(&ch)) != 0) {
		PUT_MOUNT(mp);
		errno = ENOMEM;
		return -1;
	}

	PFSD_CLIENT_LOG("rmdir %s", pbdpath);
	/* fill request */
	req->type = PFSD_REQUEST_RMDIR;
	/* copy pbdpath to iobuf */
	strncpy((char*)buf, pbdpath, PFS_MAX_PATHLEN);

	pfsd_chnl_send_recv(conn_id, req, PFS_MAX_PATHLEN,
	    rsp, 0, buf, pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	if (rsp->rm_rsp.r_res != 0) {
		err = -1;
		errno = rsp->error;
		PFSD_CLIENT_ELOG("rmdir %s error: %s", pbdpath, strerror(errno));
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, buf, pfsd_tolong(ch));
	PUT_MOUNT(mp);
	return err;
}

DIR *
pfsd_opendir(const char *pbdpath)
{
	struct mountargs *mp = NULL;
	char abspath[PFS_MAX_PATHLEN];
	pbdpath = pfsd_name_init(pbdpath, abspath, sizeof abspath);
	if (pbdpath == NULL)
		return NULL;

	char pbdname[PFS_MAX_NAMELEN];
	if (pfsd_sdk_pbdname(pbdpath, pbdname) != 0) {
		errno = EINVAL;
		return NULL;
	}

	CHECK_MOUNT_RETVAL(mp, pbdname, NULL);

	DIR *dir = NULL;

	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	unsigned char *buf = NULL;
	pfsd_response_t *rsp = NULL;
	int const conn_id = mp->conn_id;
retry:
	if (pfsd_chnl_buffer_alloc(conn_id, PFS_MAX_PATHLEN, (void**)&req, 0,
	    (void**)&rsp, (void**)&buf, (long*)(&ch)) != 0) {
		PUT_MOUNT(mp);
		errno = ENOMEM;
		return NULL;
	}

	/* fill request */
	req->type = PFSD_REQUEST_OPENDIR;
	/* copy pbdpath to iobuf */
	strncpy((char*)buf, pbdpath, PFS_MAX_PATHLEN);

	pfsd_chnl_send_recv(conn_id, req, PFS_MAX_PATHLEN,
	    rsp, 0, buf, pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	if (rsp->od_rsp.o_res != 0) {
		dir = NULL;
		errno = rsp->error;
		PFSD_CLIENT_ELOG("opendir %s error: %s", pbdpath, strerror(errno));
	} else {
		dir = PFSD_MALLOC(DIR);
		if (dir == NULL) {
			errno = ENOMEM;
		} else {
			memset(dir, 0, sizeof(*dir));
			dir->d_ino = rsp->od_rsp.o_dino;
			dir->d_next_ino = rsp->od_rsp.o_first_ino;
			dir->d_conn_id = conn_id;
		}
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, buf, pfsd_tolong(ch));
	PUT_MOUNT(mp);

	if (dir == NULL)
		return NULL;

	return PFSD_DIR_MAKE(dir);
}

struct dirent *
pfsd_readdir(DIR *dir)
{
	if (!PFSD_DIR_ISVALID(dir)) {
		errno = EINVAL;
		return NULL;
	}

	DIR *raw_dir = PFSD_DIR_RAW(dir);
	if (!raw_dir) {
		errno = EINVAL;
		return NULL;
	}

	struct dirent *ent = &raw_dir->d_sysde;
	struct dirent *sysent = NULL;
	int err = pfsd_readdir_r(dir, ent, &sysent);
	if (err != 0) {
		sysent = NULL;
	}

	return sysent;
}

int
pfsd_readdir_r(DIR *dir, struct dirent *entry, struct dirent **result)
{
	if (!PFSD_DIR_ISVALID(dir)) {
		errno = EINVAL;
		return -1;
	}

	dir = PFSD_DIR_RAW(dir);
	if (!dir || !entry || !result) {
		errno = EINVAL;
		return -1;
	}

	/* Try read from dirent buffer */
	if (dir->d_data_offset < dir->d_data_size) {
		*result = entry;
		memcpy(entry, &dir->d_data[dir->d_data_offset], sizeof(*entry));

		dir->d_data_offset += sizeof(struct dirent);
		assert (dir->d_data_offset <= dir->d_data_size);

		return 0;
	} else {
		dir->d_data_offset = 0;
		dir->d_data_size = 0;
	}

	if (dir->d_next_ino == 0) {
		*result = NULL;
		return 0;
	}

	int err = 0;

	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	pfsd_response_t *rsp = NULL;
	unsigned char *dbuf = NULL;
	int const conn_id = dir->d_conn_id;

retry:
	if (pfsd_chnl_buffer_alloc(conn_id, 0, (void**)&req,
	    PFSD_DIRENT_BUFFER_SIZE, (void**)&rsp, (void**)&dbuf, (long*)(&ch))
	    != 0) {
		errno = ENOMEM;
		return -1;
	}
	/* fill request */
	req->type = PFSD_REQUEST_READDIR;
	req->rd_req.r_dino = dir->d_ino;
	req->rd_req.r_ino = dir->d_next_ino;
	req->rd_req.r_offset = dir->d_next_offset;

	pfsd_chnl_send_recv(conn_id, req, 0,
	    rsp, PFSD_DIRENT_BUFFER_SIZE, dbuf, pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	if (rsp->rd_rsp.r_res != 0) {
		*result = NULL;

		/* Dir EOF is not error */
		if (rsp->rd_rsp.r_res != PFSD_DIR_END) {
			err = -1;
			errno = rsp->error;
		}
	} else {
		*result = entry;

		dir->d_data_size = rsp->rd_rsp.r_data_size;
		memcpy(dir->d_data, dbuf, dir->d_data_size);

		memcpy(entry, &dir->d_data[0], sizeof(*entry));
		dir->d_data_offset = sizeof(*entry);
		dir->d_next_ino = rsp->rd_rsp.r_ino;
		dir->d_next_offset = rsp->rd_rsp.r_offset;
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, dbuf, pfsd_tolong(ch));
	return err;
}

int
pfsd_closedir(DIR *dir)
{
	if (!PFSD_DIR_ISVALID(dir)) {
		errno = EINVAL;
		return -1;
	}

	dir = PFSD_DIR_RAW(dir);
	if (!dir) {
		errno = EINVAL;
		return -1;
	}

	PFSD_FREE(dir);
	return 0;
}

int
pfsd_access(const char *pbdpath, int amode)
{
	struct mountargs *mp = NULL;
	if (amode != F_OK &&
		(amode & (R_OK | W_OK | X_OK)) == 0) {
		errno = EINVAL;
		return -1;
	}

	char abspath[PFS_MAX_PATHLEN];
	pbdpath = pfsd_name_init(pbdpath, abspath, sizeof abspath);
	if (pbdpath == NULL) {
		errno = EFAULT;
		return -1;
	}

	char pbdname[PFS_MAX_NAMELEN];
	if (pfsd_sdk_pbdname(pbdpath, pbdname) != 0) {
		errno = EINVAL;
		return -1;
	}

	CHECK_MOUNT(mp, pbdname);

	if (amode & W_OK) {
		CHECK_WRITABLE(mp);
	}

	int err = 0;
	pfsd_iochannel_t *ch = NULL;
	pfsd_request_t *req = NULL;
	pfsd_response_t *rsp = NULL;
	unsigned char *buf = NULL;
	int const conn_id = mp->conn_id;
retry:
	if (pfsd_chnl_buffer_alloc(conn_id, PFS_MAX_PATHLEN, (void**)&req, 0,
	    (void**)&rsp, (void**)&buf, (long*)(&ch)) != 0) {
		PUT_MOUNT(mp);
		errno = ENOMEM;
		return -1;
	}

	/* fill request */
	req->type = PFSD_REQUEST_ACCESS;
	req->a_req.a_mode = amode;
	/* copy pbdpath to iobuf */
	strncpy((char*)buf, pbdpath, PFS_MAX_PATHLEN);

	pfsd_chnl_send_recv(conn_id, req, PFS_MAX_PATHLEN, rsp, 0, buf,
	    pfsd_tolong(ch), 0);
	CHECK_STALE(rsp);

	if (rsp->a_rsp.a_res != 0) {
		err = -1;
		errno = rsp->error;
		if (errno != ENOENT)
			PFSD_CLIENT_ELOG("access %s: %s", pbdpath,
			    strerror(errno));
	}

	pfsd_chnl_buffer_free(conn_id, req, rsp, buf, pfsd_tolong(ch));
	PUT_MOUNT(mp);

	return err;
}

int
pfsd_fsync(int fd)
{
	return 0;
}

ssize_t
pfsd_readlink(const char *pbdpath, char *buf, size_t bufsize)
{
	errno = EINVAL;
	return -1;
}

int
pfsd_chmod(const char *pbdpath, mode_t mode)
{
	return 0;
}

int
pfsd_fchmod(int fd, mode_t mode)
{
	return 0;
}

int
pfsd_chown(const char *pbdpath, uid_t owner, gid_t group)
{
	return 0;
}

static const uint64_t
pfsd_current_version = 2;

unsigned long
pfsd_meta_version_get() {
	return pfsd_current_version;
}

/* libpfs version, 'strings libpfs.a' can get this info */
#define _TOSTR(a)   #a
#define TOSTR(a)    _TOSTR(a)
char pfsd_build_version[] = "libpfs_version_" TOSTR(VERSION_DETAIL);
const char*
pfsd_build_version_get() {
	return pfsd_build_version;
}

