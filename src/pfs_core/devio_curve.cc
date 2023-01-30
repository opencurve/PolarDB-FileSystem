/*
 *  Copyright (c) 2021 NetEase Inc.
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

/*
 * Project: curve
 * File Created: 2021-11-3
 * Author: XuYifeng
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <libcurve.h>

#include "pfs_trace.h"
#include "pfs_devio.h"
#include "pfs_memory.h"
#include "pfs_option.h"
#include "pfs_impl.h"

typedef struct pfs_curvedev {
    pfs_dev_t   dk_base;
    int         dk_fd;
    size_t      dk_sectsz;
    char        dk_path[PATH_MAX];
} pfs_curvedev_t;

typedef struct pfs_curveiocb {
    CurveAioContext 	 ctx;
    pfs_devio_t          *pfs_io;
} pfs_curveiocb_t;

typedef struct pfs_curveioq {
    pfs_ioq_t       dkq_ioq;
#define dkq_destroy     dkq_ioq.ioq_destroy
    pthread_mutex_t dkq_mutex;
    pthread_cond_t  dkq_cond;

    int         dkq_inflight_count;
    int         dkq_complete_count;
    TAILQ_HEAD(, pfs_devio) dkq_inflight_queue;
    TAILQ_HEAD(, pfs_devio) dkq_complete_queue;
} pfs_curveioq_t;

static int64_t curve_iodepth = 65536;
PFS_OPTION_REG(curve_iodepth, pfs_check_ival_normal);

using namespace curve::client;
static pthread_mutex_t curve_init_lock = PTHREAD_MUTEX_INITIALIZER;
static curve::client::CurveClient *g_curve;

#define CURVE_CONF_PATH "/etc/curve/client.conf"

static int
pfs_init_curve(void)
{
    if (g_curve)
        return 0;

    int ret = 0;
    pthread_mutex_lock(&curve_init_lock);
    if (!g_curve) {
	curve::client::CurveClient *curve_client;
    	curve_client = new curve::client::CurveClient;
        if (curve_client->Init(CURVE_CONF_PATH)) {
            pfs_etrace("can not init nebd client, errno=%d\n", errno);
            delete curve_client;
            ret = -1;
	}
	__sync_synchronize();
	g_curve = curve_client;
    }
    pthread_mutex_unlock(&curve_init_lock);
    return ret;
}

static void
pfs_curvedev_destroy_ioq(pfs_ioq_t *ioq)
{
    pfs_curveioq_t *dkioq = (pfs_curveioq_t *)ioq;
    int err;

    PFS_ASSERT(dkioq->dkq_inflight_count == 0);
    PFS_ASSERT(TAILQ_EMPTY(&dkioq->dkq_inflight_queue));

    PFS_ASSERT(dkioq->dkq_complete_count == 0);
    PFS_ASSERT(TAILQ_EMPTY(&dkioq->dkq_complete_queue));

    mutex_destroy(&dkioq->dkq_mutex);
    cond_destroy(&dkioq->dkq_cond);
    pfs_mem_free(dkioq, M_CURVE_IOQ);
}

static pfs_ioq_t *
pfs_curvedev_create_ioq(pfs_dev_t *dev)
{
    pfs_curveioq_t *dkioq;
    int err;

    dkioq = (pfs_curveioq_t *)pfs_mem_malloc(sizeof(*dkioq), M_CURVE_IOQ);
    if (dkioq == NULL) {
        pfs_etrace("create diks ioq data failed: ENOMEM\n");
        return NULL;
    }
    memset(dkioq, 0, sizeof(*dkioq));
    dkioq->dkq_destroy = pfs_curvedev_destroy_ioq;
    dkioq->dkq_inflight_count = 0;
    dkioq->dkq_complete_count = 0;
    TAILQ_INIT(&dkioq->dkq_inflight_queue);
    TAILQ_INIT(&dkioq->dkq_complete_queue);

    mutex_init(&dkioq->dkq_mutex);
    cond_init(&dkioq->dkq_cond, NULL);
    return (pfs_ioq_t *)dkioq;
}

static bool
pfs_curvedev_need_throttle(pfs_dev_t *dev, pfs_ioq_t *ioq)
{
    pfs_curveioq_t *dkioq = (pfs_curveioq_t *)ioq;
    return (dkioq->dkq_inflight_count >= curve_iodepth);
}

static int pfs_curvedev_get_path(pfs_dev_t *dev, char* path) {
    strcpy(path, dev->d_devname);
    char *p = strstr(path, "@@");
    if (p == NULL) {
        errno = EINVAL;
        ERR_RETVAL(EINVAL);
    }
    strcpy(path, p+1);
    path[0] = '/';
    return 0;
}

static int
pfs_curvedev_increase_epoch_i(const char *path) {
    int err = g_curve->IncreaseEpoch(path);
    if (err < 0) {
        pfs_etrace("curve Increase epoch, err=%d\n", err);
        if (-LIBCURVE_ERROR::EPOCH_TOO_OLD == err) {
            errno = EROFS;
        } else {
            errno = EIO;
        }
        ERR_RETVAL(errno);
    }
    return 0;
}

static int
pfs_curvedev_increase_epoch(pfs_dev_t *dev) {
    char path[PATH_MAX];
    int err = pfs_curvedev_get_path(dev, path);
    if (err != 0) {
        pfs_etrace("curvedev get path, errno=%d\n", err);
        return err;
    }
    return pfs_curvedev_increase_epoch_i(path);
}

static int
pfs_curvedev_open(pfs_dev_t *dev)
{
    pfs_curvedev_t *dkdev = (pfs_curvedev_t *)dev;
    char path[PATH_MAX];
    int fd, err, sectsz;
    OpenFlags openflags;

    openflags.exclusive = false;
    if (pfs_init_curve())
        ERR_RETVAL(EINVAL);

    dkdev->dk_fd = -1;

    err = pfs_curvedev_get_path(dev, path);
    if (err != 0) {
        pfs_etrace("curvedev get path, errno=%d\n", err);
        return err;
    }

    pfs_itrace("open curve disk: %s, d_flags:0x%x\n", path, dev->d_flags);
    fd = g_curve->Open(path, openflags);

    if (fd < 0) {
        errno = EIO;
        pfs_etrace("can not open curve disk %s, %s\n", path, strerror(errno));
        ERR_RETVAL(errno);
    }
    strcpy(dkdev->dk_path, path);
    sectsz = 4096; //FIXME

    dkdev->dk_fd = fd;
    dkdev->dk_sectsz = sectsz;

    if ((dev->d_flags & DEVFLG_AUTO_INCREASE_EPOCH) &&
        (dev->d_flags & DEVFLG_WR)) {
        return pfs_curvedev_increase_epoch_i(path);
    }
    return 0;
}

static int
pfs_curvedev_reopen(pfs_dev_t *dev)
{
    pfs_curvedev_t *dkdev = (pfs_curvedev_t *)dev;
    char path[PATH_MAX];
    int fd, err, sectsz;

    err = pfs_curvedev_get_path(dev, path);
    if (err != 0) {
        pfs_etrace("curvedev get path, errno=%d\n", err);
        return err;
    }

    /*
     * RW should guarantee the data is written to disk,
     * while RO should bypass page cache.
     */
    if (dkdev->dk_fd >= 0) {
        g_curve->Close(dkdev->dk_fd);
        dkdev->dk_fd = -1;
    }

    pfs_itrace("reopen curve disk: %s, now d_flags:0x%x", path, dev->d_flags);

    OpenFlags openflags;
    openflags.exclusive = false;
    fd = g_curve->Open(path, openflags);
    if (fd < 0) {
        errno = EIO;
        pfs_etrace("cant open %s: %s\n", path, strerror(errno));
        ERR_RETVAL(errno);
    }

    sectsz = 4096; //FIXME

    strcpy(dkdev->dk_path, path);
    dkdev->dk_fd = fd;
    dkdev->dk_sectsz = (size_t)sectsz;

    if ((dev->d_flags & DEVFLG_AUTO_INCREASE_EPOCH) &&
        (dev->d_flags & DEVFLG_WR)) {
        return pfs_curvedev_increase_epoch_i(path);
    }
    return 0;
}

static int
pfs_curvedev_close(pfs_dev_t *dev)
{
    pfs_curvedev_t *dkdev = (pfs_curvedev_t *)dev;
    int err = 0;

    if (dkdev->dk_fd >= 0)
        err = g_curve->Close(dkdev->dk_fd);
    dkdev->dk_fd = -1;
    return err;
}

static int
pfs_curvedev_info(pfs_dev_t *dev, struct pbdinfo *pi)
{
    pfs_curvedev_t *dkdev = (pfs_curvedev_t *)dev;
    int64_t size;
    int err = 0;

    size = g_curve->StatFile(dkdev->dk_path);
    if (size < 0) {
        errno = EIO;
        pfs_etrace("curve failed to get disk size, errno=%d\n", errno);
        ERR_RETVAL(errno);
    }

    pi->pi_pbdno = 0;
    pi->pi_unitsize = (4UL << 20);
    pi->pi_chunksize = (10ULL << 30);
    pi->pi_disksize = (size / pi->pi_chunksize) * pi->pi_chunksize;
    pi->pi_rwtype = 1; // FIXME

    pfs_itrace("pfs_curvedev_info get pi_pbdno %u, pi_rwtype %d, pi_unitsize %" PRIu64 ", "
        "pi_chunksize %" PRIu64 ", pi_disksize %" PRIu64 "\n", pi->pi_pbdno, pi->pi_rwtype,
        pi->pi_unitsize, pi->pi_chunksize, pi->pi_disksize);
    pfs_itrace("pfs_curvedev_info waste size: %zd\n", size - pi->pi_disksize);
    return err;
}

static int
pfs_curvedev_reload(pfs_dev_t *dev)
{
    return 0;
}

static inline bool
pfs_curvedev_dio_aligned(pfs_curvedev_t *dkdev, uint64_t val)
{
    return (val & (dkdev->dk_sectsz-1)) == 0;
}

static void
pfs_curvedev_enq_inflight_io(pfs_curveioq_t *dkioq, pfs_devio_t *io)
{
    TAILQ_INSERT_TAIL(&dkioq->dkq_inflight_queue, io, io_next);
    dkioq->dkq_inflight_count++;
}

static void
pfs_curvedev_deq_inflight_io(pfs_curveioq_t *dkioq, pfs_devio_t *io)
{
    TAILQ_REMOVE(&dkioq->dkq_inflight_queue, io, io_next);
    dkioq->dkq_inflight_count--;
}

static void
pfs_curvedev_enq_complete_io(pfs_curveioq_t *dkioq, pfs_devio_t *io)
{
    TAILQ_INSERT_TAIL(&dkioq->dkq_complete_queue, io, io_next);
    dkioq->dkq_complete_count++;
}

static void
pfs_curvedev_deq_complete_io(pfs_curveioq_t *dkioq, pfs_devio_t *io)
{
    TAILQ_REMOVE(&dkioq->dkq_complete_queue, io, io_next);
    dkioq->dkq_complete_count--;
}

static void
pfs_curvedev_aio_callback(struct CurveAioContext* ctx)
{
    pfs_curveiocb_t *iocb = container_of(ctx, pfs_curveiocb_t, ctx);
    pfs_devio_t *io = iocb->pfs_io;
    pfs_curveioq_t *dkioq = (pfs_curveioq_t *)io->io_queue;

    PFS_ASSERT(io->io_error == PFSDEV_IO_DFTERR);

    if (iocb->ctx.ret < 0) {
        if (-LIBCURVE_ERROR::EPOCH_TOO_OLD == iocb->ctx.ret) {
            io->io_error = -EROFS;
            errno = EROFS;
        } else {
            io->io_error = -EIO;
            errno = EIO;
        }
    } else {
        io->io_error = 0;
    }
    io->io_private = nullptr;

    mutex_lock(&dkioq->dkq_mutex);
    pfs_curvedev_deq_inflight_io(dkioq, io);
    pfs_curvedev_enq_complete_io(dkioq, io);
    cond_broadcast(&dkioq->dkq_cond);
    mutex_unlock(&dkioq->dkq_mutex);

    delete iocb;
}

static int
pfs_curvedev_io_prep_pread(pfs_curvedev_t *dkdev, pfs_devio_t *io, pfs_curveiocb_t *iocb)
{
    PFS_ASSERT(pfs_curvedev_dio_aligned(dkdev, io->io_bda));
    PFS_ASSERT(pfs_curvedev_dio_aligned(dkdev, io->io_len));

    iocb->ctx.offset = io->io_bda;
    iocb->ctx.length = io->io_len;
    iocb->ctx.buf = io->io_buf;
    iocb->ctx.op = LIBCURVE_OP_READ;
    iocb->ctx.cb = pfs_curvedev_aio_callback;
    return 0;
}

static int
pfs_curvedev_io_prep_pwrite(pfs_curvedev_t *dkdev, pfs_devio_t *io, pfs_curveiocb_t *iocb)
{
    PFS_ASSERT(pfs_curvedev_dio_aligned(dkdev, io->io_bda));
    PFS_ASSERT(pfs_curvedev_dio_aligned(dkdev, io->io_len));

    iocb->ctx.offset = io->io_bda;
    iocb->ctx.length = io->io_len;
    iocb->ctx.buf = io->io_buf;
    iocb->ctx.op = LIBCURVE_OP_WRITE;
    iocb->ctx.cb = pfs_curvedev_aio_callback;
    return 0;
}

static int
pfs_curvedev_io_prep_trim(pfs_curvedev_t *dkdev, pfs_devio_t *io, pfs_curveiocb_t *iocb)
{
    PFS_ASSERT(pfs_curvedev_dio_aligned(dkdev, io->io_bda));
    PFS_ASSERT(pfs_curvedev_dio_aligned(dkdev, io->io_len));

    iocb->ctx.offset = io->io_bda;
    iocb->ctx.length = io->io_len;
    iocb->ctx.buf = io->io_buf;
//  iocb->ctx.op = LIBAIO_OP::LIBAIO_OP_DISCARD;
    iocb->ctx.cb = pfs_curvedev_aio_callback;
    return 0;
}

static int
pfs_curvedev_submit_io(pfs_dev_t *dev, pfs_ioq_t *ioq, pfs_devio_t *io)
{
    pfs_curvedev_t *dkdev = (pfs_curvedev_t *)dev;
    pfs_curveioq_t *dkioq = (pfs_curveioq_t *)ioq;
    pfs_curveiocb_t *iocb = nullptr;
    int err = 0;

    iocb = new pfs_curveiocb_t;
    iocb->pfs_io = io;
    io->io_private = iocb;
    io->io_error = PFSDEV_IO_DFTERR;
    mutex_lock(&dkioq->dkq_mutex);
    pfs_curvedev_enq_inflight_io(dkioq, io);
    mutex_unlock(&dkioq->dkq_mutex);

    switch (io->io_op) {
    case PFSDEV_REQ_RD:
        err = pfs_curvedev_io_prep_pread(dkdev, io, iocb);
        if (err == 0){
            if (g_curve->AioRead(dkdev->dk_fd, &iocb->ctx, UserDataType::RawBuffer)) {
                err = errno;
            }
        }
        break;
    case PFSDEV_REQ_WR:
        err = pfs_curvedev_io_prep_pwrite(dkdev, io, iocb);
        if (err == 0) {
            if (g_curve->AioWrite(dkdev->dk_fd, &iocb->ctx, UserDataType::RawBuffer)) {
                err = errno;
            }
        }
        break;
    case PFSDEV_REQ_TRIM:
        err = pfs_curvedev_io_prep_trim(dkdev, io, iocb);
        if (err == 0) {
            iocb->ctx.ret = 0;
            pfs_curvedev_aio_callback(&iocb->ctx);
        }
        break;
    default:
        err = EINVAL;
        pfs_etrace("invalid io task! op: %d, bufp: %p, len: %zu, bda%lu\n",
            io->io_op, io->io_buf, io->io_len, io->io_bda);
        PFS_ASSERT("unsupported io type" == NULL);
    }

    if (err) {
        /* io submit failure */
        mutex_lock(&dkioq->dkq_mutex);
        pfs_curvedev_deq_inflight_io(dkioq, io);
        io->io_private = nullptr;
        io->io_error = -err;
        mutex_unlock(&dkioq->dkq_mutex);
        delete iocb;

        pfs_etrace("failed to prep iocb\n");
        ERR_RETVAL(err);
    }

    return 0;
}

static pfs_devio_t *
pfs_curvedev_wait_io(pfs_dev_t *dev, pfs_ioq_t *ioq, pfs_devio_t *io)
{
    pfs_curvedev_t *dkdev = (pfs_curvedev_t *)dev;
    pfs_curveioq_t *dkioq = (pfs_curveioq_t *)ioq;
    pfs_devio_t *nio = nullptr;

    mutex_lock(&dkioq->dkq_mutex);
    while (!TAILQ_EMPTY(&dkioq->dkq_inflight_queue) ||
           !TAILQ_EMPTY(&dkioq->dkq_complete_queue)) {
        TAILQ_FOREACH(nio, &dkioq->dkq_complete_queue, io_next) {
            if (io == nullptr || nio == io)
                break;
        }

        if (nio == nullptr) {
            cond_wait(&dkioq->dkq_cond, &dkioq->dkq_mutex);
        } else {
            pfs_curvedev_deq_complete_io(dkioq, nio);
            break;
        }
    }
    mutex_unlock(&dkioq->dkq_mutex);
    return nio;
}

struct pfs_devops pfs_curvedev_ops = {
    .dop_name           = "curve",
    .dop_type           = PFS_DEV_CURVE,
    .dop_size           = sizeof(pfs_curvedev_t),
    .dop_memtag         = M_CURVE_DEV,
    .dop_open           = pfs_curvedev_open,
    .dop_reopen         = pfs_curvedev_reopen,
    .dop_close          = pfs_curvedev_close,
    .dop_info           = pfs_curvedev_info,
    .dop_reload         = pfs_curvedev_reload,
    .dop_create_ioq     = pfs_curvedev_create_ioq,
    .dop_need_throttle  = pfs_curvedev_need_throttle,
    .dop_submit_io      = pfs_curvedev_submit_io,
    .dop_wait_io        = pfs_curvedev_wait_io,
    .dop_increase_epoch = pfs_curvedev_increase_epoch,
};

