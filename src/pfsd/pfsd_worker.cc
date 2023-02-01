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

#include <signal.h>
#include <fcntl.h>
#include <sys/prctl.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <utility>

#include <sys/types.h>
#include <sys/socket.h>

#include "pfs_api.h"
#include "pfs_inode.h"
#include "pfsd_svr.h"
#include "pfs_mount.h"

#include "pfsd_shm.h"
#include "pfsd_option.h"

#include "pfsd_log.h"
#include "pfsd_chnl.h"
#include "pfsd_chnl_shm.h"
#include "pfsd_worker.h"

#include "pfsd_option.h"

#include "blockingconcurrentqueue.h"

typedef std::pair<pfsd_iochannel_t *, int> WorkItem;

static moodycamel::BlockingConcurrentQueue<WorkItem> g_work_queue;

static void *io_worker(void *arg);
static void *io_poller(void *arg);
#define IO_WORKERS 100
static pthread_t io_worker_h[IO_WORKERS];

worker_t *g_pfsd_worker;

volatile bool g_pfsd_stop = false;

/* current processing request's pid */
__thread pid_t g_currentPid;

pid_t
pfsd_worker_current_processing_pid()
{
	return g_currentPid;
}

worker_t*
pfsd_create_workers(int nworkers)
{
	if (nworkers <= 0)
		return NULL;

	worker_t *worker = PFSD_MALLOC(worker_t);
	if (worker == NULL)
		return NULL;

	worker->w_nch = 0;
	worker->w_nworkers = nworkers;
	worker->w_npollers = g_pfsd_option.o_pollers;
	worker->w_io_workers = (pthread_t *)calloc(nworkers, sizeof(pthread_t));
	worker->w_io_pollers = (pthread_t *)calloc(worker->w_npollers, sizeof(pthread_t));
	sem_init(&worker->w_sem, 0, 0);

	return worker;
}

void
pfsd_destroy_workers(worker_t **workers)
{
	if (workers && *workers) {
		sem_destroy(&(*workers)->w_sem);
		free((*workers)->w_io_workers);
		free(*workers);
		*workers = NULL;
	}
}

static int init_io_workers(worker_t *wk)
{
	int i;

	pfsd_info("create %d io workers", wk->w_nworkers);

	for (i = 0; i < wk->w_nworkers; ++i) {
		if (pthread_create(&wk->w_io_workers[i], NULL, io_worker, wk)) {
			pfsd_error("can not create io worker thread, idx = %d", i);
		}
	}
	return 0;
}

static int init_io_pollers(worker_t *wk)
{
	int i;

	pfsd_info("create %d io pollers", wk->w_npollers);

	for (i = 0; i < wk->w_npollers; ++i) {
		if (pthread_create(&wk->w_io_pollers[i], NULL, io_poller, wk)) {
			pfsd_error("can not create io poller thread, idx = %d", i);
		}
	}
	return 0;
}

static void stop_io_workers(worker_t *wk)
{
	int i;

	for (i = 0; i < wk->w_nworkers; ++i) {
		g_work_queue.enqueue({nullptr, -1});
	}

	for (i = 0; i < wk->w_nworkers; ++i) {
		pthread_join(wk->w_io_workers[i], NULL);
	}
}

static void stop_io_pollers(worker_t *wk)
{
	int i;

	for (i = 0; i < wk->w_npollers; ++i) {
		pthread_join(wk->w_io_pollers[i], NULL);
	}
}

static void drain_work_queue(void)
{
	WorkItem w;

	while(g_work_queue.try_enqueue(w))
		;
}

void*
pfsd_worker_routine(void *arg)
{
	worker_t *wk = (worker_t*)(arg);

	char name[32];
	snprintf(name, sizeof(name), "pfsd-poller");
	prctl(PR_SET_NAME,(unsigned long)name);

	/* assign channels to worker */
	for (int si = 0; si < PFSD_SHM_MAX; ++si) {
		char *channels = (char*)(g_shm[si]+1);
		size_t unit_size = ((pfsd_iochannel_t*)channels)->ch_unitsize;
		int nrequests = ((pfsd_iochannel_t*)channels)->ch_max_req;
		size_t chsize = pfsd_channel_size(nrequests, unit_size);
		for (int ci = 0; ci < g_shm[si]->sh_nch; ++ci) {
			if (wk->w_nch >= (int)PFSD_ARRAY_SIZE(wk->w_channels)) {
				pfsd_error("channel array size %d is too small, FIX it.", 
				    wk->w_nch);
				exit(0);
			}

			wk->w_channels[wk->w_nch] = (pfsd_iochannel_t* )(channels + ci * chsize);
			wk->w_nch++;
		}
	}

	if (wk->w_nch == 0) {
		g_pfsd_stop = true;
		pfsd_error("no avail channel for job poller");
		return NULL;
	}

	/* wait main thread ready */
	sem_wait(&wk->w_sem);

	init_io_workers(wk);

	init_io_pollers(wk);

	while (!g_pfsd_stop)
		usleep(1000);

	stop_io_workers(wk);
	stop_io_pollers(wk);
	drain_work_queue();
	return NULL;
}

static void* io_poller(void *arg)
{
	worker_t *wk = (worker_t*)(arg);

	moodycamel::ProducerToken ptok(g_work_queue);

	while (!g_pfsd_stop) {
		for (int i = 0; i < wk->w_nch; ++i) {
			int index = -1;
			pfsd_iochannel_t *ch = wk->w_channels[i];
			pfsd_request_t *req = pfsd_shm_fetch_request(ch);

			if (req != NULL) {
				g_currentPid = req->owner;
				index = req - ch->ch_requests;
				PFSD_ASSERT(index < ch->ch_max_req);
				pfsd_response_t *rsp = &ch->ch_responses[index];

				if (req->shm_epoch != ch->ch_epoch) {
					pfsd_warn("ESTALE: req epoch %u, ch epoch %u",
					    req->shm_epoch, ch->ch_epoch);
					pfsd_warn("Notify retry request: owner %d type %s state %s connid %d",
					    req->owner, pfsd_req_type_string(req->type),
					    pfsd_req_state_string(req->state), (int)req->connid);
					req->mntid = pfsd_chnl_get_logic_id(req->connid);
					rsp->error = ESTALE;
				} else {
					rsp->error = 0;
				}

				g_work_queue.enqueue(ptok, {ch, index});
				g_currentPid = PFSD_INVALID_PID;
			}
		}
		wk->w_wait_io(wk);
	}
	return NULL;
}

static void *io_worker(void *arg)
{
	worker_t *wk = (worker_t*)(arg);
	moodycamel::ConsumerToken ctok(g_work_queue);

	char name[32];
	snprintf(name, sizeof(name), "pfsd-worker");
	prctl(PR_SET_NAME,(unsigned long)name);

	for (;;) {
		WorkItem w;
		g_work_queue.wait_dequeue(ctok, w);
		pfsd_iochannel_t *ch = w.first;
		if (ch == nullptr)
			break;
		int index = w.second;
		pfsd_request_t *req = ch->ch_requests + index;
		g_currentPid = req->owner;
		index = req - ch->ch_requests;
		pfsd_worker_handle_request(w.first, w.second);
		pfsd_shm_done_request(w.first, w.second);
		g_currentPid = PFSD_INVALID_PID;
	}
	return NULL;
}

int
pfsd_worker_handle_request(pfsd_iochannel_t *ch, int req_index)
{
	PFSD_ASSERT(ch && ch->ch_magic == PFSD_SHM_MAGIC);

	const pfsd_request_t *req = &ch->ch_requests[req_index];
	pfsd_response_t *rsp = &ch->ch_responses[req_index];

	switch (pfsd_request_type(req)) {
        case PFSD_REQUEST_INCREASEEPOCH:
			pfsd_worker_handle_increase_epoch(ch, req_index, &req->i_req, &rsp->i_rsp);
			return 0;

		case PFSD_REQUEST_GROWFS:
			pfsd_worker_handle_growfs(ch, req_index, &req->g_req, &rsp->g_rsp);
			return 0;

		case PFSD_REQUEST_RENAME:
			pfsd_worker_handle_rename(ch, req_index, &req->re_req, &rsp->re_rsp);
			return 0;

		case PFSD_REQUEST_OPEN: {
			MNT_STAT_API_BEGIN(MNT_STAT_API_OPEN);
			pfsd_worker_handle_open(ch, req_index, &req->o_req, &rsp->o_rsp);
			MNT_STAT_API_END(MNT_STAT_API_OPEN);
			return 0;
		}

		case PFSD_REQUEST_READ: {
			MNT_STAT_API_BEGIN(MNT_STAT_API_PREAD);
			pfs_mntstat_set_file_type(req->common_pl_req.pl_file_type);
			pfsd_worker_handle_read(ch, req_index, &req->r_req,
			    &rsp->r_rsp);
			MNT_STAT_API_END_BANDWIDTH(MNT_STAT_API_PREAD,
			    req->r_req.r_len);
			return 0;
		}

		case PFSD_REQUEST_WRITE: {
			MNT_STAT_API_BEGIN(MNT_STAT_API_PWRITE);
			pfs_mntstat_set_file_type(req->common_pl_req.pl_file_type);
			pfsd_worker_handle_write(ch, req_index, &req->w_req, &rsp->w_rsp);
			MNT_STAT_API_END_BANDWIDTH(MNT_STAT_API_PWRITE,
			    req->w_req.w_len);
			return 0;
		}

		case PFSD_REQUEST_TRUNCATE: {
			MNT_STAT_API_BEGIN(MNT_STAT_API_TRUNCATE);
			pfsd_worker_handle_truncate(ch, req_index, &req->t_req, &rsp->t_rsp);
			MNT_STAT_API_END(MNT_STAT_API_TRUNCATE);
			return 0;
		}

		case PFSD_REQUEST_FTRUNCATE: {
			MNT_STAT_API_BEGIN(MNT_STAT_API_FTRUNCATE);
			pfs_mntstat_set_file_type(req->common_pl_req.pl_file_type);
			pfsd_worker_handle_ftruncate(ch, req_index, &req->ft_req, &rsp->ft_rsp);
			MNT_STAT_API_END(MNT_STAT_API_FTRUNCATE);
			return 0;
		}

		case PFSD_REQUEST_UNLINK:
			pfsd_worker_handle_unlink(ch, req_index, &req->un_req, &rsp->un_rsp);
			return 0;

		case PFSD_REQUEST_STAT: {
			MNT_STAT_API_BEGIN(MNT_STAT_API_STAT);
			pfsd_worker_handle_stat(ch, req_index, &req->s_req, &rsp->s_rsp);
			MNT_STAT_API_END(MNT_STAT_API_STAT);
			return 0;
		}

		case PFSD_REQUEST_FSTAT: {
			MNT_STAT_API_BEGIN(MNT_STAT_API_FSTAT);
			pfs_mntstat_set_file_type(req->common_pl_req.pl_file_type);
			pfsd_worker_handle_fstat(ch, req_index, &req->f_req, &rsp->f_rsp);
			MNT_STAT_API_END(MNT_STAT_API_FSTAT);
			return 0;
		}

		case PFSD_REQUEST_FALLOCATE: {
			MNT_STAT_API_BEGIN(MNT_STAT_API_FALLOCATE);
			pfs_mntstat_set_file_type(req->common_pl_req.pl_file_type);
			pfsd_worker_handle_fallocate(ch, req_index, &req->fa_req, &rsp->fa_rsp);
			MNT_STAT_API_END(MNT_STAT_API_FALLOCATE);
			return 0;
		}

		case PFSD_REQUEST_CHDIR:
			pfsd_worker_handle_chdir(ch, req_index, &req->cd_req, &rsp->cd_rsp);
			return 0;

		case PFSD_REQUEST_MKDIR:
			pfsd_worker_handle_mkdir(ch, req_index, &req->mk_req, &rsp->mk_rsp);
			return 0;

		case PFSD_REQUEST_RMDIR:
			pfsd_worker_handle_rmdir(ch, req_index, &req->rm_req, &rsp->rm_rsp);
			return 0;

		case PFSD_REQUEST_OPENDIR:
			pfsd_worker_handle_opendir(ch, req_index, &req->od_req, &rsp->od_rsp);
			return 0;

		case PFSD_REQUEST_READDIR:
			pfsd_worker_handle_readdir(ch, req_index, &req->rd_req, &rsp->rd_rsp);
			return 0;

		case PFSD_REQUEST_ACCESS:
			pfsd_worker_handle_access(ch, req_index, &req->a_req, &rsp->a_rsp);
			return 0;

		case PFSD_REQUEST_LSEEK: {
			MNT_STAT_API_BEGIN(MNT_STAT_API_LSEEK);
			pfs_mntstat_set_file_type(req->common_pl_req.pl_file_type);
			pfsd_worker_handle_lseek(ch, req_index, &req->l_req, &rsp->l_rsp);
			MNT_STAT_API_END(MNT_STAT_API_LSEEK);
			return 0;
		}

		default:
			pfsd_error("worker: unknown request %d", pfsd_request_type(req));
			return -1;
	}

	return 0;
}

#define CHECK_RSP_ERROR(rsp) do {\
	if (rsp->error != 0) { \
		return; \
	} \
} while(0)\


void
pfsd_worker_handle_increase_epoch(pfsd_iochannel_t *ch, int , const increase_epoch_request_t *req,
    increase_epoch_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_INCREASEEPOCH;
	rsp->err = -1;

	CHECK_RSP_ERROR(rsp);

	rsp->err = pfs_mount_increase_epoch(req->g_pbd);

	if (rsp->err == -1) {
		rsp->error = errno;
		pfsd_error("increase epoch %s error %d", req->g_pbd, errno);
	} else
		pfsd_info("increase epoch %s success", req->g_pbd);
}

void
pfsd_worker_handle_growfs(pfsd_iochannel_t *ch, int , const growfs_request_t *req,
    growfs_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_GROWFS;
	rsp->err = -1;

	CHECK_RSP_ERROR(rsp);

	rsp->err = pfs_mount_growfs(req->g_pbd);

	if (rsp->err == -1) {
		rsp->error = errno;
		pfsd_error("growfs %s error %d", req->g_pbd, errno);
	} else
		pfsd_info("growfs %s success", req->g_pbd);
}

void
pfsd_worker_handle_rename(pfsd_iochannel_t *ch, int req_index,
    const rename_request_t *req, rename_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_RENAME;
	rsp->r_res = -1;

	CHECK_RSP_ERROR(rsp);

	const char *oldpath = (const char*)ch->ch_buf + req_index * ch->ch_unitsize;
	const char *newpath = (const char*)ch->ch_buf + req_index * ch->ch_unitsize + PFS_MAX_PATHLEN;

	pfsd_info("pid %d %s -> %s", g_currentPid, oldpath, newpath);

	rsp->r_res = pfs_rename(oldpath, newpath);
	if (rsp->r_res < 0) {
		rsp->error = errno;
		pfsd_error("rename %s -> %s error: %d", oldpath, newpath, errno);
	} else
		pfsd_info("rename %s -> %s success", oldpath, newpath);
}

void
pfsd_worker_handle_open(pfsd_iochannel_t *ch, int req_index, 
    const open_request_t *req, open_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_OPEN;
	rsp->o_ino = -1;
	rsp->o_off = 0;

	CHECK_RSP_ERROR(rsp);

	const char *pbdpath = (const char*)ch->ch_buf + req_index * ch->ch_unitsize;

	memset(&rsp->common_pl_rsp, 0, sizeof(rsp->common_pl_rsp));
	rsp->o_ino = pfsd_open_svr(pbdpath, req->o_flags, req->o_mode,
	    &rsp->common_pl_rsp.pl_btime,
	    &rsp->common_pl_rsp.pl_file_type);
	rsp->o_off = 0;
	if (rsp->o_ino < 0)
		rsp->error = errno;
}

#define PFSD_GET_MOUNT(mntid, rsp) do {\
	mnt = pfs_get_mount_byid(mntid); \
	if (mnt == NULL) { \
		pfsd_error("Cant find mntid %d", mntid); \
		rsp->error = ENODEV; \
		return; \
	} \
} while(0)

#define PFSD_PUT_MOUNT(mnt) do {\
	if (mnt) { \
		pfs_put_mount(mnt); \
		mnt = NULL; \
	} \
} while(0)

extern pfs_inode_t *pfs_inode_get_and_load(pfs_mount_t *mnt, pfs_ino_t ino);

#define PFSD_GET_MOUNT_AND_INODE(mntid, ino, rsp) do {\
	mnt = pfs_get_mount_byid(mntid); \
	if (mnt == NULL) { \
		pfsd_error("Cant find mntid %d", mntid); \
		rsp->error = ENODEV; \
		return; \
	} \
	inode = pfs_inode_get_and_load(mnt, ino); \
	if (inode == NULL) { \
		pfs_put_mount(mnt); \
		rsp->error = EBADF; \
		return; \
	} \
} while(0)

/*
 * LRU inode-list
 */
#define PFSD_PUT_MOUNT_AND_INODE(mnt, in) do {\
	if (mnt) { \
		pfs_put_inode(mnt, in); \
		pfs_put_mount(mnt); \
		mnt = NULL; \
	} \
} while(0)

void
pfsd_worker_handle_read(pfsd_iochannel_t *ch, int req_index,
    const read_request_t *req, read_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_READ;
	rsp->r_len = -1;

	CHECK_RSP_ERROR(rsp);

	rsp->r_ino = req->r_ino;
	size_t read_len = req->r_len;

	unsigned char *rbuf = ch->ch_buf;
	rbuf += ch->ch_unitsize * req_index;

	if (req->r_off < 0 || req->r_ino < 0) {
		pfsd_error("pid %d read invalid ino %ld or offset %lu", g_currentPid,
		    req->r_ino, req->r_off);
		errno = EINVAL;
		return;
	}
	pfs_mount_t *mnt = NULL;
	pfs_inode_t *inode = NULL;
	PFSD_GET_MOUNT_AND_INODE(req->mntid, req->r_ino, rsp);

	rsp->r_len = pfsd_pread_svr(mnt, inode, rbuf, read_len, req->r_off,
	    req->common_pl_req.pl_btime);

	PFSD_PUT_MOUNT_AND_INODE(mnt, inode);

	if (rsp->r_len < 0) {
		pfsd_error("read ino %ld failed %d", req->r_ino, errno);
		rsp->error = errno;
	} else
		pfsd_debug("read ino %ld return %ld bytes", req->r_ino, rsp->r_len);
}

void
pfsd_worker_handle_write(pfsd_iochannel_t *ch, int req_index,
    const write_request_t *req, write_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_WRITE;
	rsp->w_ino = req->w_ino;
	rsp->w_len = -1;

	CHECK_RSP_ERROR(rsp);

	unsigned char *wbuf = ch->ch_buf;
	wbuf += ch->ch_unitsize * req_index;
	if (req->w_ino < 0) {
		pfsd_error("pid %d error inode %ld, offset %lu", g_currentPid,
		    req->w_ino, req->w_off);
		rsp->error = EINVAL;
		return;
	}

	pfs_mount_t* mnt = NULL;
	pfs_inode_t* inode = NULL;
	PFSD_GET_MOUNT_AND_INODE(req->mntid, req->w_ino, rsp);

	rsp->w_len = pfsd_pwrite_svr(mnt, inode, req->w_flags, wbuf, req->w_len,
	    req->w_off, &rsp->w_file_size, req->common_pl_req.pl_btime);

	PFSD_PUT_MOUNT_AND_INODE(mnt, inode);

	if (rsp->w_len < 0) {
		rsp->error = errno;
		pfsd_error("pid %d write ino %ld failed: %d", g_currentPid, req->w_ino, errno);
	} else
		pfsd_debug("write ino %ld return %ld bytes", req->w_ino, rsp->w_len);
}

void
pfsd_worker_handle_truncate(pfsd_iochannel *ch, int req_index,
    const truncate_request_t *req, truncate_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_TRUNCATE;
	rsp->t_res = -1;
	CHECK_RSP_ERROR(rsp);

	const char *pbdpath = (const char*)ch->ch_buf + req_index * ch->ch_unitsize;

	rsp->t_res = pfsd_truncate_svr(pbdpath, req->t_len);
	if (rsp->t_res < 0) {
		rsp->error = errno;
		pfsd_error("pid %d truncate %s to len %ld error: %d",
		    g_currentPid, pbdpath, req->t_len, errno);
	} else
		pfsd_info("pid %d truncate %s to len %ld success", g_currentPid,
		    pbdpath, req->t_len);
}

void
pfsd_worker_handle_ftruncate(pfsd_iochannel *ch, int index,
    const ftruncate_request_t *req, ftruncate_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_FTRUNCATE;
	rsp->f_res = -1;

	CHECK_RSP_ERROR(rsp);

	pfs_mount_t *mnt = NULL;
	pfs_inode_t *inode = NULL;
	PFSD_GET_MOUNT_AND_INODE(req->mntid, req->f_ino, rsp);

	rsp->f_res = pfsd_ftruncate_svr(mnt, inode, req->f_len,
	    req->common_pl_req.pl_btime);

	PFSD_PUT_MOUNT_AND_INODE(mnt, inode);

	if (rsp->f_res < 0) {
		rsp->error = errno;
		pfsd_error("pid %d ftruncate ino %ld to len %ld err: %d",
		    g_currentPid, req->f_ino, req->f_len, errno);
	} else
		pfsd_info("pid %d ftruncate ino %ld to len %ld success",
		    g_currentPid, req->f_ino, req->f_len);
}

void
pfsd_worker_handle_unlink(pfsd_iochannel *ch, int req_index,
    const unlink_request_t *req, unlink_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_UNLINK;
	rsp->u_res = -1;

	CHECK_RSP_ERROR(rsp);

	const char *pbdpath = (const char*)ch->ch_buf + req_index * ch->ch_unitsize;
	rsp->u_res = pfsd_unlink_svr(pbdpath);
	if (rsp->u_res < 0) {
		rsp->error = errno;
		pfsd_error("pid %d unlink %s error %d", g_currentPid, pbdpath, rsp->error);
	} else
		pfsd_info("pid %d unlink %s success", g_currentPid, pbdpath);
}

void
pfsd_worker_handle_stat(pfsd_iochannel *ch, int req_index,
    const stat_request_t *req, stat_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_STAT;
	rsp->s_res = -1;

	CHECK_RSP_ERROR(rsp);

	const char *pbdpath = (const char*)ch->ch_buf + req_index * ch->ch_unitsize;
	rsp->s_res = pfsd_stat_svr(pbdpath, &rsp->s_st);
	if (rsp->s_res < 0) {
		rsp->error = errno;
		pfsd_error("pid %d stat %s error %d", g_currentPid, pbdpath, rsp->error);
	} else
		pfsd_debug("pid %d stat %s size is %lu", g_currentPid, pbdpath, rsp->s_st.st_size);
}

void
pfsd_worker_handle_fstat(pfsd_iochannel *ch, int index,
    const fstat_request_t *req, fstat_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_FSTAT;
	rsp->f_res = -1;

	CHECK_RSP_ERROR(rsp);

	pfs_mount_t *mnt = NULL;
	pfs_inode_t *inode = NULL;
	PFSD_GET_MOUNT_AND_INODE(req->mntid, req->f_ino, rsp);

	rsp->f_res = pfsd_fstat_svr(mnt, inode, &rsp->f_st,
	    req->common_pl_req.pl_btime);

	PFSD_PUT_MOUNT_AND_INODE(mnt, inode);

	if (rsp->f_res < 0) {
		rsp->error = errno;
		pfsd_error("pid %d fstat ino %ld error %d", g_currentPid, req->f_ino, errno);
	} else
		pfsd_debug("pid %d fstat ino %ld success, size is %lu",
		    g_currentPid, req->f_ino, rsp->f_st.st_size);
}

void
pfsd_worker_handle_fallocate(pfsd_iochannel_t *ch, int req_index,
    const fallocate_request_t *req, fallocate_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_FALLOCATE;
	rsp->f_ino = req->f_ino;
	rsp->f_res = -1;

	CHECK_RSP_ERROR(rsp);

	pfs_mount_t *mnt = NULL;
	pfs_inode_t *inode = NULL;
	PFSD_GET_MOUNT_AND_INODE(req->mntid, req->f_ino, rsp);

	rsp->f_res = pfsd_fallocate_svr(mnt, inode, req->f_off, req->f_len, req->f_mode,
	    req->common_pl_req.pl_btime);

	PFSD_PUT_MOUNT_AND_INODE(mnt, inode);

	if (rsp->f_res < 0) {
		rsp->error = errno;
		rsp->f_res = -1;
		pfsd_error("pid %d fallocate ino %ld, off %ld len %ld error: %d",
		    g_currentPid, req->f_ino, req->f_off, req->f_len, errno);
	} else
		pfsd_info("pid %d fallocate ino %ld, off %ld len %ld success",
		    g_currentPid, req->f_ino, req->f_off, req->f_len);
}

void
pfsd_worker_handle_chdir(pfsd_iochannel *ch, int req_index,
    const chdir_request_t *req, chdir_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_CHDIR;
	rsp->c_res = -1;

	CHECK_RSP_ERROR(rsp);

	const char *pbdpath = (const char*)ch->ch_buf + req_index * ch->ch_unitsize;
	rsp->c_res = pfsd_chdir_svr(pbdpath);
	if (rsp->c_res < 0) {
		rsp->error = errno;
		pfsd_error("pid %d chdir to %s error %d", g_currentPid, pbdpath, errno);
	} else
		pfsd_info("pid %d chdir to %s success", g_currentPid, pbdpath);
}

void
pfsd_worker_handle_mkdir(pfsd_iochannel *ch, int req_index,
    const mkdir_request_t *req, mkdir_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_MKDIR;
	rsp->m_res = -1;

	CHECK_RSP_ERROR(rsp);

	const char *pbdpath = (const char*)ch->ch_buf + req_index * ch->ch_unitsize;

	rsp->m_res = pfs_mkdir(pbdpath, req->m_mode);
	if (rsp->m_res < 0) {
		rsp->error = errno;
		pfsd_error("pid %d mkdir %s error: %d", g_currentPid, pbdpath, errno);
	} else
		pfsd_info("pid %d mkdir %s success", g_currentPid, pbdpath);
}

void
pfsd_worker_handle_rmdir(pfsd_iochannel *ch, int req_index,
    const rmdir_request_t *req, rmdir_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_RMDIR;
	rsp->r_res = -1;

	CHECK_RSP_ERROR(rsp);

	const char *pbdpath = (const char*)ch->ch_buf + req_index * ch->ch_unitsize;

	rsp->r_res = pfs_rmdir(pbdpath);
	if (rsp->r_res < 0) {
		rsp->error = errno;
		pfsd_error("pid %d rmdir %s error: %d", g_currentPid, pbdpath, errno);
	} else
		pfsd_info("pid %d rmdir %s success", g_currentPid, pbdpath);
}

void
pfsd_worker_handle_opendir(pfsd_iochannel_t *ch, int req_index,
    const opendir_request_t *req, opendir_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_OPENDIR;
	rsp->o_res = -1;

	CHECK_RSP_ERROR(rsp);

	const char *pbdpath = (const char*)ch->ch_buf + req_index * ch->ch_unitsize;
	pfsd_debug("pid %d, opendir %s", g_currentPid, pbdpath);

	rsp->o_res = pfsd_opendir_svr(pbdpath, &rsp->o_dino, &rsp->o_first_ino);
	if (rsp->o_res != 0) {
		rsp->error = errno;
		pfsd_error("pid %d opendir %s error: %d", g_currentPid, pbdpath, rsp->error);
	}
	/* pfsd_opendir_svr will print detail logs */
}

void
pfsd_worker_handle_readdir(pfsd_iochannel_t *ch, int req_index,
    const readdir_request_t *req, readdir_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_READDIR;
	rsp->r_res = -1;

	CHECK_RSP_ERROR(rsp);

	if (ch->ch_unitsize < PFSD_DIRENT_BUFFER_SIZE) {
		rsp->error = EFAULT;
		rsp->r_res = -1;
		return;
	}

	pfs_mount_t *mnt = NULL;
	PFSD_GET_MOUNT(req->mntid, rsp);

	rsp->r_res = 0;

	unsigned char *rbuf = ch->ch_buf;
	rbuf += ch->ch_unitsize * req_index;

	int64_t cur_ino = req->r_ino;
	uint64_t cur_offset = req->r_offset;
	int64_t next_ino = 0;
	uint64_t data_size = 0;
	while (cur_ino != 0 && data_size + sizeof(struct dirent) <= PFSD_DIRENT_BUFFER_SIZE) {
		int err = pfsd_readdir_svr(mnt, req->r_dino, cur_ino, cur_offset,
		    (struct dirent*)&rbuf[data_size], &next_ino);
		if (err != 0) {
			if (data_size == 0) {
				rsp->r_res = err;
				rsp->error = errno;
				rsp->r_data_size = 0;
				rsp->r_ino = 0;
			}

			if (err == PFSD_DIR_END)
				rsp->r_ino = 0; /* Dir EOF */

			break;
		} else {
			data_size += sizeof(struct dirent);
			pfsd_debug("got ino %ld at offset %ld", cur_ino, cur_offset);
			cur_ino = next_ino;
			++cur_offset;

			rsp->r_data_size = data_size;
			rsp->r_ino = next_ino;
			rsp->r_offset = cur_offset;
		}
	}
	pfsd_debug("pid %d, readdir err %d, req offset %ld rsp offset %ld",
	    g_currentPid, rsp->error, req->r_offset, rsp->r_offset);

	PFSD_PUT_MOUNT(mnt);
}

void
pfsd_worker_handle_access(pfsd_iochannel *ch, int req_index,
    const access_request_t *req, access_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_ACCESS;
	rsp->a_res = -1;

	CHECK_RSP_ERROR(rsp);

	const char *pbdpath = (const char*)ch->ch_buf + req_index * ch->ch_unitsize;
	rsp->a_res = pfs_access(pbdpath, req->a_mode);
	if (rsp->a_res < 0) {
		rsp->error = errno;
		pfsd_error("pid %d access %s mode %u error: %d", g_currentPid,
		    pbdpath, req->a_mode, errno);
	} else
		pfsd_debug("pid %d access %s mode %u success", g_currentPid,
		    pbdpath, req->a_mode);
}

void
pfsd_worker_handle_lseek(pfsd_iochannel *ch, int req_index,
    const lseek_request_t *req, lseek_response_t *rsp)
{
	rsp->type = PFSD_RESPONSE_LSEEK;
	rsp->l_offset = off_t(-1);

	CHECK_RSP_ERROR(rsp);

	int64_t ino = req->l_ino;
	off_t off = req->l_offset;
	if (req->l_whence != SEEK_END) {
		rsp->error = EINVAL;
		pfsd_error("pid %d lseek not SEEK_END: whence is %d",
		    g_currentPid, req->l_whence);
		return;
	}

	pfs_mount_t *mnt = NULL;
	pfs_inode_t *inode = NULL;
	PFSD_GET_MOUNT_AND_INODE(req->mntid, ino, rsp);

	rsp->l_offset = pfsd_lseek_end_svr(mnt, inode, off,
	    req->common_pl_req.pl_btime);

	PFSD_PUT_MOUNT_AND_INODE(mnt, inode);

	if (rsp->l_offset < 0) {
		rsp->error = errno;
		pfsd_error("pid %d lseek error %d, ino %ld, off %ld",
		    g_currentPid, rsp->error, ino, off);
	} else
		pfsd_debug("pid %d lseek ino %ld, off %ld", g_currentPid, ino, off);
}

