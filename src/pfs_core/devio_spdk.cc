/* vim: set ts=4 sw=4 expandtab: */
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

/*
 * Project: curve
 * File Created: 2022-5-7
 * Author: XuYifeng
 */

#include <sys/param.h>

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <semaphore.h>
#include <unistd.h>

#include <spdk/env.h>
#include <spdk/log.h>
#include <spdk/string.h>
#include <spdk/nvme.h>

#include <rte_common.h>
#include <rte_thread.h>
#include <rte_pause.h>
#include <rte_prefetch.h>

#include "pfs_trace.h"
#include "pfs_devio.h"
#include "pfs_memory.h"
#include "pfs_option.h"
#include "pfs_impl.h"
#include "pfs_spdk.h"
#include "pfs_util.h"
#include "pfs_iomem.h"
#include "pfs_futex_event.h"
#include "pfs_option.h"

#include "pfs_errno_wrapper.h"

extern "C" {
// FIXME
// The following function is declared in bdev_nvme.h which is not
// installed by spdk.
struct spdk_nvme_ctrlr *bdev_nvme_get_ctrlr(struct spdk_bdev *bdev);
}

typedef struct pfs_spdk_iocb pfs_spdk_iocb_t;

typedef struct pfs_spdk_dev {
    /* must be first member */
    pfs_dev_t   dk_base;
    struct spdk_bdev_desc *dk_desc;
    struct spdk_bdev      *dk_bdev;
    cpu_set_t   dk_cpuset;
    uint32_t    dk_sect_size;
    uint64_t    dk_size;
    uint64_t    dk_block_num;
    uint32_t    dk_block_size;
    uint32_t    dk_unit_size;
    int         dk_has_cache;
    struct spdk_nvme_ctrlr *dk_ctrlr;
    uint64_t    dk_ctrlr_flags;
#define dk_bufalign dk_base.d_buf_align
    pthread_t   dk_pthread;
    struct      pfs_spdk_driver_poller dk_driver_poller;
    void        *dk_poller_handle;
    struct spdk_thread *dk_spdk_thread;
    struct spdk_io_channel *dk_ioch;
    int         dk_stop;
    int         dk_jobs;

    pfs_spdk_iocb_t *dk_incoming __rte_aligned(RTE_CACHE_LINE_SIZE);
    pthread_mutex_t dk_work_mutex;
    pfs_futex_event_t dk_event;
    int         dk_running;
    char        dk_path[PFS_MAX_PBDLEN+1];
} pfs_spdk_dev_t;

struct pfs_spdk_iocb {
    union {
        SLIST_ENTRY(pfs_spdk_iocb) cb_free;
        pfs_spdk_iocb_t *cb_next;
    };
    pfs_devio_t             *cb_pfs_io;
    void                    *cb_dma_buf;
    pfs_spdk_dev_t          *cb_dev;
    struct pfs_spdk_ioq     *cb_ioq;
    spdk_msg_fn             cb_io_op;
    spdk_msg_fn             cb_io_done;
    struct spdk_bdev_io_wait_entry cb_bdev_io_wait;
};

typedef struct pfs_spdk_ioq {
    /* must be first member */
    pfs_ioq_t   dkq_ioq;
#define dkq_destroy     dkq_ioq.ioq_destroy

    int         dkq_inflight_count;
    int         dkq_complete_count;
    TAILQ_HEAD(, pfs_devio) dkq_inflight_queue;
    TAILQ_HEAD(, pfs_devio) dkq_complete_queue;

    pfs_spdk_iocb_t *dkq_done_q __rte_aligned(RTE_CACHE_LINE_SIZE);
    pfs_event_t dkq_done_ev;
} pfs_spdk_ioq_t;

static const int64_t g_iodepth = 128;
static int64_t FLAGS_pfs_spdk_driver_poll_delay;
PFS_OPTION_REG2(pfs_spdk_driver_poll_delay, FLAGS_pfs_spdk_driver_poll_delay,
	OPT_LONG, 0, OPT_LONG);
static int64_t FLAGS_pfs_waitio_timeout_sec;
PFS_OPTION_REG2(pfs_waitio_timeout_sec, FLAGS_pfs_waitio_timeout_sec,
	OPT_LONG, 10, OPT_LONG);
static int FLAGS_pfs_spdk_driver_error_interval;
PFS_OPTION_REG2(pfs_spdk_driver_error_interval, FLAGS_pfs_spdk_driver_error_interval,
	OPT_INT, 1, OPT_INT);
static int FLAGS_pfs_spdk_driver_auto_dma;
PFS_OPTION_REG2(pfs_spdk_driver_auto_dma, FLAGS_pfs_spdk_driver_auto_dma,
	OPT_INT, 1, OPT_INT);

#define PFS_MAX_CACHED_SPDK_IOCB        128
static __thread SLIST_HEAD(, pfs_spdk_iocb) tls_free_iocb = SLIST_HEAD_INITIALIZER(tls_free_iocb);
static __thread int tls_free_iocb_num = 0;

#define error_time_interval {FLAGS_pfs_spdk_driver_error_interval, 0}

static void pfs_spdk_dev_io_pread(void *arg);
static void pfs_spdk_dev_io_pwrite(void *arg);
static void pfs_spdk_dev_io_trim(void *arg);
static void pfs_spdk_dev_io_flush(void *arg);
static void pfs_spdk_dev_io_fini_pread(void *iocb);
static void pfs_spdk_dev_io_fini_pwrite(void *iocb);
static void pfs_spdk_dev_io_fini_trim(void *iocb);
static void pfs_spdk_dev_io_fini_flush(void *iocb);
static void pfs_spdk_dev_pull_iocb(pfs_spdk_dev_t *dkdev);
static void pfs_spdk_dev_try_requests(pfs_spdk_dev_t *dkdev);

static pfs_spdk_iocb_t *
pfs_spdk_dev_alloc_iocb(void)
{
    pfs_spdk_iocb_t *iocb = NULL;
    void *p = NULL;
    int err = -1;

    /* try to get it from local cache */
    iocb = SLIST_FIRST(&tls_free_iocb);
    if (iocb != NULL) {
        SLIST_REMOVE_HEAD(&tls_free_iocb, cb_free);
        tls_free_iocb_num--;
    } else {
        /* allocate it by malloc from global heap */
        err = pfs_mem_memalign(&p, PFS_CACHELINE_SIZE, sizeof(*iocb),
                M_SPDK_IOCB);
        if (err) {
            pfs_etrace("create iocb failed, %s\n", strerror(err));
            abort();
        }
        iocb = (pfs_spdk_iocb_t *)p;
    }
    memset(iocb, 0, sizeof(*iocb));
    return iocb;
}

static void
pfs_spdk_dev_free_iocb(pfs_spdk_iocb_t *iocb)
{
    if (tls_free_iocb_num < PFS_MAX_CACHED_SPDK_IOCB) {
        SLIST_INSERT_HEAD(&tls_free_iocb, iocb, cb_free);
        tls_free_iocb_num++;
    } else {
        pfs_mem_free(iocb, M_SPDK_IOCB);
    }
}

static void
pfs_spdk_dev_destroy_ioq(pfs_ioq_t *ioq)
{
    pfs_spdk_ioq_t *dkioq = (pfs_spdk_ioq_t *)ioq;
    int err;

    PFS_ASSERT(dkioq->dkq_inflight_count == 0);
    PFS_ASSERT(TAILQ_EMPTY(&dkioq->dkq_inflight_queue));

    PFS_ASSERT(dkioq->dkq_complete_count == 0);
    PFS_ASSERT(TAILQ_EMPTY(&dkioq->dkq_complete_queue));

    pfs_event_destroy(&dkioq->dkq_done_ev);
    pfs_mem_free(dkioq, M_SPDK_DEV_IOQ);
}

static pfs_ioq_t *
pfs_spdk_dev_create_ioq(pfs_dev_t *dev)
{
    pfs_spdk_ioq_t *dkioq = NULL;
    void *p = NULL;
    size_t alloc_size = roundup(sizeof(*dkioq), 64);
    int err;

    err = pfs_mem_memalign(&p, PFS_CACHELINE_SIZE, alloc_size,
		M_SPDK_DEV_IOQ);
    if (err) {
        pfs_etrace("create disk ioq failed: %d, %s\n", err, strerror(err));
        return NULL;
    }
    memset(p, 0, sizeof(*dkioq));
    dkioq = (pfs_spdk_ioq_t *)p;
    dkioq->dkq_destroy = pfs_spdk_dev_destroy_ioq;
    dkioq->dkq_inflight_count = 0;
    dkioq->dkq_complete_count = 0;
    TAILQ_INIT(&dkioq->dkq_inflight_queue);
    TAILQ_INIT(&dkioq->dkq_complete_queue);
    pfs_event_init(&dkioq->dkq_done_ev);
    return (pfs_ioq_t *)dkioq;
}

static bool
pfs_spdk_dev_need_throttle(pfs_dev_t *dev, pfs_ioq_t *ioq)
{
    pfs_spdk_ioq_t *dkioq = (pfs_spdk_ioq_t *)ioq;
    return (dkioq->dkq_inflight_count >= g_iodepth);
}

static void
bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
    void *event_ctx)
{
    pfs_etrace("Unsupported spdk bdev event: type %d\n", type);
    return;
}

static void
bdev_find_cpuset(pfs_spdk_dev_t *dkdev)
{
    pfs_dev_t *dev = &dkdev->dk_base;
    int cpuid = -1;

    CPU_ZERO(&dkdev->dk_cpuset);
    cpuid = pfs_spdk_get_devcpu_bind(dev->d_devname);
    if (cpuid != -1) {
        CPU_SET(cpuid, &dkdev->dk_cpuset);
        //dev->d_mem_socket_id = pfs_cpuset_socket_id(&dkdev->dk_cpuset);
    }
}

static unsigned 
bdev_poll(void *arg)
{
    int count = 100;
    pfs_spdk_thread_guard guard;

    pfs_spdk_dev_t *dkdev = (pfs_spdk_dev_t *)arg;
    pfs_dev_t *dev = &dkdev->dk_base;
    struct spdk_thread *spdk_thread = dkdev->dk_spdk_thread;
    spdk_set_thread(spdk_thread);

    do {
        if (count-- == 0)
            break;
        pfs_spdk_dev_pull_iocb(dkdev);
    } while (spdk_thread_poll(spdk_thread, 64, 0));

    return 0;
}

static void *
bdev_thread_msg_loop(void *arg)
{
    pfs_spdk_dev_t *dkdev = (pfs_spdk_dev_t *)arg;
    pfs_dev_t *dev = &dkdev->dk_base;
    struct spdk_thread *spdk_thread = NULL;
    const char *thread_name = NULL;
    int err;

    spdk_thread = dkdev->dk_spdk_thread;
    spdk_set_thread(spdk_thread);
    thread_name = spdk_thread_get_name(spdk_thread);
    pthread_setname_np(pthread_self(), thread_name);

    if (CPU_COUNT(&dkdev->dk_cpuset)) {
        err = pthread_setaffinity_np(pthread_self(), sizeof(dkdev->dk_cpuset),
             &dkdev->dk_cpuset);
        if (err)
            pfs_etrace("pthread_setaffinity_np failed, %s", strerror(err));
        else {
            char *cpuset_str = pfs_cpuset_to_string(&dkdev->dk_cpuset);
            pfs_etrace("thread %s binds to cpus: [ %s ]", thread_name, cpuset_str);
            free(cpuset_str);
        }
    }

    struct timespec timeout = {0, FLAGS_pfs_spdk_driver_poll_delay * 1000};
    if (FLAGS_pfs_spdk_driver_poll_delay) {
        pthread_mutex_lock(&dkdev->dk_work_mutex);
        __atomic_store_n(&dkdev->dk_running, 1, __ATOMIC_RELAXED);
    }
    while (!dkdev->dk_stop) {
        pfs_spdk_dev_pull_iocb(dkdev);
        while (dkdev->dk_jobs != 0) {
            spdk_thread_poll(spdk_thread, 64, 0);
            pfs_spdk_dev_pull_iocb(dkdev);
        }
        if (!FLAGS_pfs_spdk_driver_poll_delay) {
            continue;
        }
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        pfs_timespecadd(&ts, &timeout, &ts);
        __atomic_store_n(&dkdev->dk_running, 0, __ATOMIC_RELAXED);
        pthread_mutex_unlock(&dkdev->dk_work_mutex);
        pfs_futex_event_timedwait(&dkdev->dk_event, &ts);
        pthread_mutex_lock(&dkdev->dk_work_mutex);
        __atomic_store_n(&dkdev->dk_running, 1, __ATOMIC_RELAXED);
    }
 
    while (dkdev->dk_jobs != 0) {
        spdk_thread_poll(spdk_thread, 0, 0);
        pfs_spdk_dev_pull_iocb(dkdev);
    }
    __atomic_store_n(&dkdev->dk_running, 0, __ATOMIC_RELAXED);

    spdk_set_thread(NULL);
    if (FLAGS_pfs_spdk_driver_poll_delay)
        pthread_mutex_unlock(&dkdev->dk_work_mutex);
    return NULL;
}

/*
 * return:
 *    failure: < 0
 *    sucess : 0
 */
static int
pfs_spdk_dev_open(pfs_dev_t *dev)
{
    pfs_spdk_thread_guard guard;
    static size_t page_size = sysconf(_SC_PAGESIZE);
    pfs_spdk_dev_t *dkdev = (pfs_spdk_dev_t *)dev;
    struct spdk_thread *spdk_thread = NULL;
    char thread_name[128] = {0};
    char product_name[128] = {0};
    int err = 0;

    snprintf(thread_name, sizeof(thread_name), "pfs-dev-%s", dev->d_devname);
 
    /*
     * pfs_spdk_setup should be called as soon as possible in
     * application's main thread, put it here is just easy
     * for pfs tools.
     */
    if ((err = pfs_spdk_setup())) {
        pfs_etrace("can not init spdk env");
        return -EIO;
    }
 
    dkdev->dk_stop = 0;
    dkdev->dk_jobs = 0;
    pthread_mutex_init(&dkdev->dk_work_mutex, NULL);
    pfs_futex_event_init(&dkdev->dk_event);

    spdk_thread = spdk_thread_create(thread_name, NULL);
    spdk_set_thread(spdk_thread);
    err = spdk_bdev_open_ext(dev->d_devname, dev_writable(dev),
                             bdev_event_cb, dkdev, &dkdev->dk_desc);
    if (err) {
        pfs_etrace("can not open spdk device %s, %s\n", dev->d_devname,
                   strerror(-err));
err_exit:
        pfs_spdk_gc_thread(spdk_thread);
        pthread_mutex_destroy(&dkdev->dk_work_mutex);
        pfs_futex_event_destroy(&dkdev->dk_event);
        return err < 0 ? err : -err;
    }
    dkdev->dk_bdev = spdk_bdev_desc_get_bdev(dkdev->dk_desc);
    dkdev->dk_ioch = spdk_bdev_get_io_channel(dkdev->dk_desc);
    if (dkdev->dk_ioch == NULL) {
        pfs_etrace("can not get io channel of spdk device: %s\n",
            dev->d_devname);
        spdk_bdev_close(dkdev->dk_desc);
        err = ENOMEM;
        goto err_exit;
    }
    dkdev->dk_ctrlr = bdev_nvme_get_ctrlr(dkdev->dk_bdev);
    if (dkdev->dk_ctrlr) { // is nvme device
        dkdev->dk_ctrlr_flags = spdk_nvme_ctrlr_get_flags(dkdev->dk_ctrlr);
        const struct spdk_nvme_ctrlr_data *cdata;
        cdata = spdk_nvme_ctrlr_get_data(dkdev->dk_ctrlr);
        snprintf(product_name, sizeof(product_name), "%-20.20s (%-20.20s)",
                cdata->mn, cdata->sn);
    } else {
        snprintf(product_name, sizeof(product_name), "%s",
                spdk_bdev_get_name(dkdev->dk_bdev));
    }
    strncpy(dkdev->dk_path, dev->d_devname, sizeof(dkdev->dk_path));
    dkdev->dk_path[sizeof(dkdev->dk_path)-1] = 0;
    dkdev->dk_block_num = spdk_bdev_get_num_blocks(dkdev->dk_bdev);
    dkdev->dk_block_size = spdk_bdev_get_block_size(dkdev->dk_bdev);
    dkdev->dk_unit_size = spdk_bdev_get_write_unit_size(dkdev->dk_bdev);
    dkdev->dk_sect_size = dkdev->dk_unit_size * dkdev->dk_block_size;
    dkdev->dk_has_cache = spdk_bdev_has_write_cache(dkdev->dk_bdev);
    dkdev->dk_size = dkdev->dk_block_num * dkdev->dk_block_size;
    dkdev->dk_bufalign = spdk_bdev_get_buf_align(dkdev->dk_bdev);
    if (dkdev->dk_bufalign < page_size)
        dkdev->dk_bufalign = page_size;
    dev->d_cap = DEV_CAP_RD | DEV_CAP_WR | DEV_CAP_FLUSH | DEV_CAP_TRIM;
    if (dkdev->dk_ctrlr_flags & SPDK_NVME_CTRLR_SGL_SUPPORTED) {
        dev->d_cap |= DEV_CAP_SGL;
        if (dkdev->dk_ctrlr_flags & SPDK_NVME_CTRLR_SGL_REQUIRES_DWORD_ALIGNMENT) {
           dev->d_cap |= DEV_CAP_SGL_DW;
        }
    }
    // SPDK unconditionally supports WRITE_ZEROS.
    // It ensures that all specified blocks will be zeroed out.
    // If a block device doesn't natively support a write zeroes command,
    // the bdev layer emulates it using write commands.                                                                 
    // yfxu@
    dev->d_cap |= DEV_CAP_ZERO;
    dev->d_write_unit = dkdev->dk_sect_size; // copy into base dev
    PFS_ASSERT(RTE_IS_POWER_OF_2(dev->d_write_unit));

    pfs_itrace("open spdk device: '%s', product: '%s', block_num: %ld, "
               "block_size: %d, write_unit_size: %d, has_cache: %d, sgl: %s, buf_align:%d\n",
               dev->d_devname, product_name, dkdev->dk_block_num, dkdev->dk_block_size,
               dkdev->dk_unit_size, dkdev->dk_has_cache,
               (dev->d_cap & DEV_CAP_SGL) ? "yes":"no",
               dkdev->dk_bufalign);

    bdev_find_cpuset(dkdev);
    dkdev->dk_spdk_thread = spdk_thread;
    pfs_spdk_get_driver_poller(&dkdev->dk_driver_poller);
    if (dkdev->dk_driver_poller.register_callback) {
        dkdev->dk_poller_handle =
            (*dkdev->dk_driver_poller.register_callback)(bdev_poll, dkdev);
        if (dkdev->dk_poller_handle == NULL) {
            pfs_etrace("can not register callback");
            err = ENOMEM;
        }
    } else {
        err = pthread_create(&dkdev->dk_pthread, NULL, bdev_thread_msg_loop, dkdev);
        if (err) {
            pfs_etrace("can not create spdk poller pthread");
        }
    }

    if (err) {
        spdk_put_io_channel(dkdev->dk_ioch);
        spdk_bdev_close(dkdev->dk_desc);
        goto err_exit;
    }

    return 0;
}

static int
pfs_spdk_dev_close(pfs_dev_t *dev)
{
    pfs_spdk_thread_guard guard;
    pfs_spdk_dev_t *dkdev = (pfs_spdk_dev_t *)dev;

    if (dkdev->dk_desc == NULL)
        return 0;
    
    dkdev->dk_stop = 1;
    if (dkdev->dk_poller_handle) {
        (*dkdev->dk_driver_poller.remove_callback)(dkdev->dk_poller_handle);
    } else {
        pfs_futex_event_set(&dkdev->dk_event);
        pthread_join(dkdev->dk_pthread, NULL);
    }

    spdk_set_thread(dkdev->dk_spdk_thread);
    spdk_put_io_channel(dkdev->dk_ioch);
    spdk_bdev_close(dkdev->dk_desc);
    pfs_spdk_gc_thread(dkdev->dk_spdk_thread);
    dkdev->dk_spdk_thread = NULL;

    pfs_futex_event_destroy(&dkdev->dk_event);
    pthread_mutex_destroy(&dkdev->dk_work_mutex);
    return 0;
}

static int
pfs_spdk_dev_reopen(pfs_dev_t *dev)
{
    pfs_spdk_dev_close(dev);
    return pfs_spdk_dev_open(dev);
}

static int
pfs_spdk_dev_info(pfs_dev_t *dev, struct pbdinfo *pi)
{
    pfs_spdk_dev_t *dkdev = (pfs_spdk_dev_t *)dev;
    size_t size = dkdev->dk_block_num * dkdev->dk_block_size;

    pi->pi_pbdno = 0;
    pi->pi_unitsize = (4UL << 20);
    pi->pi_chunksize = (10ULL << 30);
    pi->pi_disksize = (size / pi->pi_chunksize) * pi->pi_chunksize;
    pi->pi_rwtype = 1; // FIXME

    pfs_itrace("%s get pi_pbdno %u, pi_rwtype %d, pi_unitsize %" PRIu64 ", "
               "pi_chunksize %" PRIu64 ", pi_disksize %" PRIu64 "\n",
               __func__, pi->pi_pbdno, pi->pi_rwtype,
               pi->pi_unitsize, pi->pi_chunksize, pi->pi_disksize);
    pfs_itrace("%s waste size: %zu\n", __func__, size - pi->pi_disksize);
    return 0;
}

static int
pfs_spdk_dev_reload(pfs_dev_t *dev)
{
    return 0;
}

static inline bool
pfs_spdk_dev_dio_aligned(pfs_spdk_dev_t *dkdev, uint64_t val)
{
    return (val & (dkdev->dk_sect_size-1)) == 0;
}

static inline void
pfs_spdk_dev_enq_inflight_io(pfs_spdk_ioq_t *dkioq, pfs_devio_t *io)
{
    TAILQ_INSERT_TAIL(&dkioq->dkq_inflight_queue, io, io_next);
    dkioq->dkq_inflight_count++;
}

static inline void
pfs_spdk_dev_deq_inflight_io(pfs_spdk_ioq_t *dkioq, pfs_devio_t *io)
{
    TAILQ_REMOVE(&dkioq->dkq_inflight_queue, io, io_next);
    dkioq->dkq_inflight_count--;
}

static inline void
pfs_spdk_dev_enq_complete_io(pfs_spdk_ioq_t *dkioq, pfs_devio_t *io)
{
    TAILQ_INSERT_TAIL(&dkioq->dkq_complete_queue, io, io_next);
    dkioq->dkq_complete_count++;
}

static inline void
pfs_spdk_dev_deq_complete_io(pfs_spdk_ioq_t *dkioq, pfs_devio_t *io)
{
    TAILQ_REMOVE(&dkioq->dkq_complete_queue, io, io_next);
    dkioq->dkq_complete_count--;
}

static void
pfs_spdk_dev_io_done(struct spdk_bdev_io *bdev_io,
    bool success, void *cb_arg)                                                  
{
    pfs_spdk_iocb_t *iocb = (pfs_spdk_iocb_t *)cb_arg;
    pfs_spdk_iocb_t *tmp = NULL;
    pfs_devio_t *io = iocb->cb_pfs_io;
    pfs_spdk_ioq_t *dkioq = iocb->cb_ioq;

    PFS_ASSERT(io->io_error == PFSDEV_IO_DFTERR);
    io->io_error = success ? 0 : -EIO;
    io->io_private = nullptr;

    iocb->cb_dev->dk_jobs--;
    tmp = __atomic_load_n(&dkioq->dkq_done_q, __ATOMIC_RELAXED);
    for (;;) {
        iocb->cb_next = tmp;
        if (__atomic_compare_exchange_n(&dkioq->dkq_done_q, &tmp, iocb,
                false, __ATOMIC_RELEASE, __ATOMIC_RELAXED)) {
            break;
        }
    }

    /* This works like WIN32 event, we don't repeatly set semaphore */
    pfs_event_set(&dkioq->dkq_done_ev);
    spdk_bdev_free_io(bdev_io);
}

static int
pfs_spdk_dev_io_prep_pread(pfs_spdk_dev_t *dkdev, pfs_devio_t *io,
    pfs_spdk_iocb_t *iocb)
{
    static struct timeval last;
    pfs_dev_t *dev = &dkdev->dk_base;

    PFS_ASSERT(pfs_spdk_dev_dio_aligned(dkdev, io->io_bda));
    PFS_ASSERT(pfs_spdk_dev_dio_aligned(dkdev, io->io_len));

    if (FLAGS_pfs_spdk_driver_auto_dma &&
        !(io->io_flags & IO_DMABUF)) {
        iocb->cb_dma_buf = pfs_iomem_alloc(io->io_len, dev->d_mem_socket_id);
        if (iocb->cb_dma_buf == NULL) {
            struct timeval tv = error_time_interval;
            if (pfs_ratecheck(&last, &tv)) {
                pfs_etrace("can not allocate dma mem:%s", __func__);
            }
            return -ENOBUFS;
        }
    } else {
        iocb->cb_dma_buf = NULL;
    }
    iocb->cb_io_op = pfs_spdk_dev_io_pread;
    iocb->cb_io_done = pfs_spdk_dev_io_fini_pread;
    return 0;
}

static void
pfs_spdk_dev_io_pread(void *arg)
{
    pfs_spdk_iocb_t *iocb = (pfs_spdk_iocb_t *)arg;
    pfs_spdk_dev_t *dkdev;
    pfs_devio_t *io;
    int rc;

    dkdev = iocb->cb_dev;
    io = iocb->cb_pfs_io;
    if (iocb->cb_dma_buf) {
        rc = spdk_bdev_read(dkdev->dk_desc, dkdev->dk_ioch, iocb->cb_dma_buf,
            io->io_bda, io->io_len, pfs_spdk_dev_io_done, iocb);
    } else {
        rc = spdk_bdev_readv(dkdev->dk_desc, dkdev->dk_ioch, io->io_iov,
            io->io_iovcnt, io->io_bda, io->io_len, pfs_spdk_dev_io_done, iocb);
    }
    if (rc == ENOMEM) {
        iocb->cb_bdev_io_wait.bdev = dkdev->dk_bdev;
        iocb->cb_bdev_io_wait.cb_fn = pfs_spdk_dev_io_pread;
        iocb->cb_bdev_io_wait.cb_arg = iocb;
        spdk_bdev_queue_io_wait(dkdev->dk_bdev, dkdev->dk_ioch,
                                &iocb->cb_bdev_io_wait);
    } else if (rc) {
        pfs_etrace("%s error while reading from bdev: %d\n",
            spdk_strerror(-rc), rc);
        if (iocb->cb_dma_buf) {
            pfs_iomem_free(iocb->cb_dma_buf);
        }
        abort();
    }
}

static void
pfs_spdk_dev_io_fini_pread(void *arg)
{
    pfs_spdk_iocb_t *iocb = (pfs_spdk_iocb_t *)arg;
    pfs_devio_t *io = iocb->cb_pfs_io;
    pfs_spdk_ioq_t *dkioq = (pfs_spdk_ioq_t *)io->io_queue;
    if (io->io_error == 0 && !(io->io_flags & IO_DMABUF) && iocb->cb_dma_buf) {
        size_t remain = pfs_copy_from_buf_to_iovec(io->io_iov, io->io_iovcnt, iocb->cb_dma_buf, io->io_len);
        if (remain) {
            pfs_etrace("internal error, not enough iovec, %ld bytes remaining", remain);
            abort();
        }
    }
    pfs_spdk_dev_deq_inflight_io(dkioq, io);
    pfs_spdk_dev_enq_complete_io(dkioq, io);

    if (iocb->cb_dma_buf)
        pfs_iomem_free(iocb->cb_dma_buf);
    pfs_spdk_dev_free_iocb(iocb);
}

static int
pfs_spdk_dev_io_prep_pwrite(pfs_spdk_dev_t *dkdev, pfs_devio_t *io,
    pfs_spdk_iocb_t *iocb)
{
    static struct timeval last;
    pfs_dev_t *dev = &dkdev->dk_base;
    int rc;

    PFS_ASSERT(pfs_spdk_dev_dio_aligned(dkdev, io->io_bda));
    PFS_ASSERT(pfs_spdk_dev_dio_aligned(dkdev, io->io_len));

    if (FLAGS_pfs_spdk_driver_auto_dma &&
        !(io->io_flags & IO_DMABUF) && !(io->io_flags & IO_ZERO)) {
        iocb->cb_dma_buf = pfs_iomem_alloc(io->io_len, dev->d_mem_socket_id);
        if (iocb->cb_dma_buf == NULL) {
            struct timeval tv = error_time_interval;
            if (pfs_ratecheck(&last, &tv)) {
                pfs_etrace("can not allocate dma mem:%s", __func__);
            }
            return -ENOBUFS;
        }
        size_t remain = pfs_copy_from_iovec_to_buf(iocb->cb_dma_buf, io->io_iov, io->io_iovcnt, io->io_len);
        if (remain) {
            pfs_etrace("internal error, not enough iovec, %ld bytes remaining", remain);
            abort();
        }
    } else {
        iocb->cb_dma_buf = NULL;
    }
    iocb->cb_io_op = pfs_spdk_dev_io_pwrite;
    iocb->cb_io_done = pfs_spdk_dev_io_fini_pwrite;
    return 0;
}

static void
pfs_spdk_dev_io_pwrite(void *arg)
{
    pfs_spdk_iocb_t *iocb = (pfs_spdk_iocb_t *)arg;
    pfs_spdk_dev_t *dkdev;
    pfs_devio_t *io;
    int rc;

    dkdev = iocb->cb_dev;
    io = iocb->cb_pfs_io;
    if (io->io_flags & IO_ZERO) {
        rc = spdk_bdev_write_zeroes(dkdev->dk_desc, dkdev->dk_ioch,
            io->io_bda, io->io_len, pfs_spdk_dev_io_done, iocb);
    } else if (iocb->cb_dma_buf) {
        rc = spdk_bdev_write(dkdev->dk_desc, dkdev->dk_ioch, iocb->cb_dma_buf,
            io->io_bda, io->io_len, pfs_spdk_dev_io_done, iocb);
    } else {
        rc = spdk_bdev_writev(dkdev->dk_desc, dkdev->dk_ioch, io->io_iov,
            io->io_iovcnt, io->io_bda, io->io_len, pfs_spdk_dev_io_done, iocb);
    }
    if (rc == ENOMEM) {
        iocb->cb_bdev_io_wait.bdev = dkdev->dk_bdev;
        iocb->cb_bdev_io_wait.cb_fn = pfs_spdk_dev_io_pwrite;
        iocb->cb_bdev_io_wait.cb_arg = iocb;
        spdk_bdev_queue_io_wait(dkdev->dk_bdev, dkdev->dk_ioch,
                                &iocb->cb_bdev_io_wait);
    } else if (rc) {
        pfs_etrace("%s error while writting to bdev: %d, ioflags=%x",
            spdk_strerror(-rc), rc, io->io_flags);
        if (iocb->cb_dma_buf)
            pfs_iomem_free(iocb->cb_dma_buf);
        abort();
    }
}

static void
pfs_spdk_dev_io_fini_pwrite(void *arg)
{
    pfs_spdk_iocb_t *iocb = (pfs_spdk_iocb_t *)arg;
    pfs_devio_t *io = iocb->cb_pfs_io;
    pfs_spdk_ioq_t *dkioq = (pfs_spdk_ioq_t *)io->io_queue;
    pfs_spdk_dev_deq_inflight_io(dkioq, io);
    pfs_spdk_dev_enq_complete_io(dkioq, io);

    if (iocb->cb_dma_buf)
        pfs_iomem_free(iocb->cb_dma_buf);
    pfs_spdk_dev_free_iocb(iocb);
}

static int
pfs_spdk_dev_io_prep_trim(pfs_spdk_dev_t *dkdev, pfs_devio_t *io,
    pfs_spdk_iocb_t *iocb)
{
    int rc;

    PFS_ASSERT(pfs_spdk_dev_dio_aligned(dkdev, io->io_bda));
    PFS_ASSERT(pfs_spdk_dev_dio_aligned(dkdev, io->io_len));

    iocb->cb_io_op = pfs_spdk_dev_io_trim;
    iocb->cb_io_done = pfs_spdk_dev_io_fini_trim;
    return 0;
}

static void
pfs_spdk_dev_io_trim(void *arg)
{
    pfs_spdk_iocb_t *iocb = (pfs_spdk_iocb_t *)arg;
    pfs_spdk_dev_t *dkdev;
    pfs_devio_t *io;
    int rc;

    dkdev = iocb->cb_dev;
    io = iocb->cb_pfs_io;
    rc = spdk_bdev_unmap(dkdev->dk_desc, dkdev->dk_ioch, io->io_bda,
            io->io_len, pfs_spdk_dev_io_done, iocb);
     if (rc == ENOMEM) {
        iocb->cb_bdev_io_wait.bdev = dkdev->dk_bdev;
        iocb->cb_bdev_io_wait.cb_fn = pfs_spdk_dev_io_trim;
        iocb->cb_bdev_io_wait.cb_arg = iocb;
        spdk_bdev_queue_io_wait(dkdev->dk_bdev, dkdev->dk_ioch,
                                &iocb->cb_bdev_io_wait);
    } else if (rc) {
        pfs_etrace("%s error while trimming bdev: %d\n",
            spdk_strerror(-rc), rc);
        abort();
    }
}

static void
pfs_spdk_dev_io_fini_trim(void *arg)
{
    pfs_spdk_iocb_t *iocb = (pfs_spdk_iocb_t *)arg;
    pfs_devio_t *io = iocb->cb_pfs_io;
    pfs_spdk_ioq_t *dkioq = (pfs_spdk_ioq_t *)io->io_queue;
    pfs_spdk_dev_deq_inflight_io(dkioq, io);
    pfs_spdk_dev_enq_complete_io(dkioq, io);
    pfs_spdk_dev_free_iocb(iocb);
}

static int
pfs_spdk_dev_io_prep_flush(pfs_spdk_dev_t *dkdev, pfs_devio_t *io,
    pfs_spdk_iocb_t *iocb)
{
    int rc;

    PFS_ASSERT(pfs_spdk_dev_dio_aligned(dkdev, io->io_bda));
    PFS_ASSERT(pfs_spdk_dev_dio_aligned(dkdev, io->io_len));

    iocb->cb_io_op = pfs_spdk_dev_io_flush;
    iocb->cb_io_done = pfs_spdk_dev_io_fini_flush;
    return 0;
}

static void
pfs_spdk_dev_io_flush(void *arg)
{
    pfs_spdk_iocb_t *iocb = (pfs_spdk_iocb_t *)arg;
    pfs_spdk_dev_t *dkdev;
    int rc;

    dkdev = iocb->cb_dev;
    rc = spdk_bdev_flush(dkdev->dk_desc, dkdev->dk_ioch, 0, dkdev->dk_size,
            pfs_spdk_dev_io_done, iocb);
    if (rc == ENOMEM) {
        iocb->cb_bdev_io_wait.bdev = dkdev->dk_bdev;
        iocb->cb_bdev_io_wait.cb_fn = pfs_spdk_dev_io_flush;
        iocb->cb_bdev_io_wait.cb_arg = iocb;
        spdk_bdev_queue_io_wait(dkdev->dk_bdev, dkdev->dk_ioch,
                                &iocb->cb_bdev_io_wait);
    } else if (rc) {
        pfs_etrace("%s error while flushing bdev: %d\n", 
            spdk_strerror(-rc), rc);
        abort();
    }
}

static void
pfs_spdk_dev_io_fini_flush(void *arg)
{
    pfs_spdk_iocb_t *iocb = (pfs_spdk_iocb_t *)arg;
    pfs_devio_t *io = iocb->cb_pfs_io;
    pfs_spdk_ioq_t *dkioq = (pfs_spdk_ioq_t *)io->io_queue;
    pfs_spdk_dev_deq_inflight_io(dkioq, io);
    pfs_spdk_dev_enq_complete_io(dkioq, io);
    pfs_spdk_dev_free_iocb(iocb);
}

static void
pfs_spdk_dev_send_iocb(pfs_spdk_dev_t *dkdev, pfs_spdk_iocb_t *iocb)
{
    pfs_spdk_iocb_t *head;
    int count;

    head = __atomic_load_n(&dkdev->dk_incoming, __ATOMIC_RELAXED);
    for (;;) {
        iocb->cb_next = head;
        if (__atomic_compare_exchange_n(&dkdev->dk_incoming, &head, iocb,
                true, __ATOMIC_RELEASE, __ATOMIC_RELAXED)) {
            break;
        }
        rte_pause();
    }

    if (dkdev->dk_poller_handle == NULL) {
        if (FLAGS_pfs_spdk_driver_poll_delay != 0)
            pfs_futex_event_set(&dkdev->dk_event);
    } else {
        dkdev->dk_driver_poller.notify_callback(dkdev->dk_poller_handle);
    }
}

static void
pfs_spdk_dev_pull_iocb(pfs_spdk_dev_t *dkdev)
{
    pfs_spdk_iocb_t *prev, *curr, *next = NULL;

    curr = __atomic_load_n(&dkdev->dk_incoming, __ATOMIC_RELAXED);
    if (curr == NULL)
        return;
    curr = __atomic_exchange_n(&dkdev->dk_incoming, NULL, __ATOMIC_ACQUIRE);
    /* reverse the list */
    if (curr != NULL) {
        for (;;) {
            prev = curr->cb_next; 
            curr->cb_next = next;
            if (prev == NULL)
                break;
            next = curr;
            curr = prev;
        }
    }

    while (curr != NULL) {
        dkdev->dk_jobs++;
        next = curr->cb_next;
        rte_prefetch1(next);
        curr->cb_io_op(curr);
        curr = next;
    }
}

static int
pfs_spdk_dev_submit_io(pfs_dev_t *dev, pfs_ioq_t *ioq, pfs_devio_t *io)
{   
    pfs_spdk_dev_t *dkdev = (pfs_spdk_dev_t *)dev;
    pfs_spdk_ioq_t *dkioq = (pfs_spdk_ioq_t *)ioq;
    pfs_spdk_iocb_t *iocb = nullptr;
    spdk_msg_fn fn = nullptr;
    int err = 0, count = 0;

    iocb = pfs_spdk_dev_alloc_iocb();
    if (iocb == NULL) {
        return -ENOBUFS;
    }
    iocb->cb_pfs_io = io;
    iocb->cb_ioq = dkioq;
    iocb->cb_dev = dkdev;
    io->io_private = iocb;
    io->io_error = PFSDEV_IO_DFTERR;
    pfs_spdk_dev_enq_inflight_io(dkioq, io);

    switch (io->io_op) {
    case PFSDEV_REQ_RD:
        err = pfs_spdk_dev_io_prep_pread(dkdev, io, iocb);
        if (err)
            goto fail;
        break;
    case PFSDEV_REQ_WR:
        err = pfs_spdk_dev_io_prep_pwrite(dkdev, io, iocb);
        if (err)
            goto fail;
        break;
    case PFSDEV_REQ_TRIM:
        err = pfs_spdk_dev_io_prep_trim(dkdev, io, iocb);
        if (err)
            goto fail;
        break;
    case PFSDEV_REQ_FLUSH:
        err = pfs_spdk_dev_io_prep_flush(dkdev, io, iocb);
        if (err)
            goto fail;
        break;

    default:
        err = EINVAL;
        pfs_etrace("invalid io task! op: %d, bufp: %p, len: %zu, bda%lu\n",
            io->io_op, io->io_iov[0].iov_base, io->io_iov[0].iov_len, io->io_bda);
        PFS_ASSERT("unsupported io type" == NULL);
    }

    pfs_spdk_dev_send_iocb(dkdev, iocb);
    if (err) {
fail:
        /* io submit failure */
        pfs_spdk_dev_deq_inflight_io(dkioq, io);
        io->io_private = nullptr;
        io->io_error = err;
        pfs_spdk_dev_free_iocb(iocb);
        return err;
    }

    return 0;
}

static pfs_devio_t *
pfs_spdk_dev_wait_io(pfs_dev_t *dev, pfs_ioq_t *ioq, pfs_devio_t *io)
{
    pfs_spdk_thread_guard guard;
    struct pfs_spdk_iocb *iocb, *next;
    pfs_spdk_dev_t *dkdev = (pfs_spdk_dev_t *)dev;
    pfs_spdk_ioq_t *dkioq = (pfs_spdk_ioq_t *)ioq;
    pfs_devio_t *nio = nullptr;

    while ((dkioq->dkq_inflight_count | dkioq->dkq_complete_count)) {
        TAILQ_FOREACH(nio, &dkioq->dkq_complete_queue, io_next) {
            if (io == nullptr || nio == io)
                break;
        }
        if (nio == nullptr) {
            if (!dkioq->dkq_inflight_count) {
                pfs_etrace("inflight is empty\n");
                break;
            }

            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            struct timespec timeout = {FLAGS_pfs_waitio_timeout_sec, 0};
            pfs_timespecadd(&ts, &timeout, &ts);
            int err = pfs_event_timedwait(&dkioq->dkq_done_ev, &ts);
            if (err) {
                pfs_fatal("wait io time runing out, err:%d\n", err);
            }
            for (iocb = __atomic_exchange_n(&dkioq->dkq_done_q, NULL,
                    __ATOMIC_ACQUIRE); iocb; iocb = next) {
                next = iocb->cb_next;
                iocb->cb_io_done(iocb);
            }
        } else {
            pfs_spdk_dev_deq_complete_io(dkioq, nio);
            break;
        }
    }
    return nio;
}

static int
pfs_spdk_dev_has_cache(pfs_dev_t *dev)
{
    pfs_spdk_dev_t *dkdev = (pfs_spdk_dev_t *)dev;
    return dkdev->dk_has_cache;
}

void
pfsdev_exit_thread_spdk_drv(void)
{
    pfs_spdk_iocb *iocb;

    while ((iocb = SLIST_FIRST(&tls_free_iocb))) {
        SLIST_REMOVE_HEAD(&tls_free_iocb, cb_free);
        pfs_mem_free(iocb, M_SPDK_IOCB);
    }
}

struct pfs_devops pfs_spdk_dev_ops = {
    .dop_name           = "spdk",
    .dop_type           = PFS_DEV_SPDK,
    .dop_size           = sizeof(pfs_spdk_dev_t),
    .dop_memtag         = M_SPDK_DEV,
    .dop_open           = pfs_spdk_dev_open,
    .dop_reopen         = pfs_spdk_dev_reopen,
    .dop_close          = pfs_spdk_dev_close,
    .dop_info           = pfs_spdk_dev_info,
    .dop_reload         = pfs_spdk_dev_reload,
    .dop_create_ioq     = pfs_spdk_dev_create_ioq,
    .dop_need_throttle  = pfs_spdk_dev_need_throttle,
    .dop_submit_io      = pfs_spdk_dev_submit_io,
    .dop_wait_io        = pfs_spdk_dev_wait_io,
    .dop_has_cache      = pfs_spdk_dev_has_cache
};
