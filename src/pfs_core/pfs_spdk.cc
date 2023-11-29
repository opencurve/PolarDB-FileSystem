/* vim: set ts=4 sw=4 expandtab: */

/*
 *  Copyright (c) 2020 NetEase Inc.
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

/**
 * Project : curve
 * Date : 2022/06/07
 * Author: XuYifeng
 */

#include "pfs_spdk.h"
#include "pfs_trace.h"
#include "pfs_memory.h"
#include "pfs_util.h"
#include "pfs_option.h"

#include "lib/kvec.h"
#include "lib/kstring.h"

#include <ctype.h>
#include <semaphore.h>
#include <stdlib.h>
#include <string.h>

#include <spdk/init.h>
#include <spdk/env.h>
#include <spdk/string.h>
#include <spdk/log.h>
#include <spdk/util.h>
#include <spdk/json.h>
#include <spdk/rpc.h>

#include <sys/user.h>   // For PAGE_SIZE and PAGE_MASK
#include <sys/param.h>  // For roundup

#include "pfs_errno_wrapper.h"

static char* FLAGS_spdk_name;
PFS_OPTION_REG2(spdk_name, FLAGS_spdk_name, OPT_STR, "", OPT_CSTR);

static char* FLAGS_spdk_core_mask;
PFS_OPTION_REG2(spdk_core_mask, FLAGS_spdk_core_mask, OPT_STR, "", OPT_CSTR);

static int FLAGS_spdk_shm_id = -1;
PFS_OPTION_REG2(spdk_shm_id, FLAGS_spdk_shm_id, OPT_INT, -1, OPT_INT);

static int FLAGS_spdk_mem_channel = -1;
PFS_OPTION_REG2(spdk_mem_channel, FLAGS_spdk_mem_channel, OPT_INT, -1, OPT_INT);

static int FLAGS_spdk_main_core = -1;
PFS_OPTION_REG2(spdk_main_core, FLAGS_spdk_main_core, OPT_INT, -1, OPT_INT);

static int FLAGS_spdk_mem_size = -1;
PFS_OPTION_REG2(spdk_mem_size, FLAGS_spdk_mem_size, OPT_INT, -1, OPT_INT);

static int FLAGS_spdk_no_pci = 0;
PFS_OPTION_REG2(spdk_no_pci, FLAGS_spdk_no_pci, OPT_INT, 0, OPT_INT);

static int FLAGS_spdk_hugepage_single_segments = 0;
PFS_OPTION_REG2(spdk_hugepage_single_segments, FLAGS_spdk_hugepage_single_segments,
        OPT_INT, 0, OPT_INT);

static int FLAGS_spdk_unlink_hugepage = 0;
PFS_OPTION_REG2(spdk_unlink_hugepage, FLAGS_spdk_unlink_hugepage,
    OPT_INT, 0, OPT_INT);

static char *FLAGS_spdk_hugedir;
PFS_OPTION_REG2(spdk_hugedir, FLAGS_spdk_hugedir, OPT_STR, "", OPT_CSTR);

static char *FLAGS_spdk_pci_blocked;
PFS_OPTION_REG2(spdk_pci_blocked, FLAGS_spdk_pci_blocked, OPT_STR, "", OPT_CSTR);

static char *FLAGS_spdk_pci_allowed;
PFS_OPTION_REG2(spdk_pci_allowed, FLAGS_spdk_pci_allowed, OPT_STR, "", OPT_CSTR);

static char *FLAGS_spdk_iova_mode;
PFS_OPTION_REG2(spdk_iova_mode, FLAGS_spdk_iova_mode, OPT_STR, "va", OPT_CSTR);

static uint64_t FLAGS_spdk_base_virtaddr;
PFS_OPTION_REG2(spdk_base_virtaddr, FLAGS_spdk_base_virtaddr, OPT_LONG, 0, OPT_LONG);

static char *FLAGS_spdk_env_context;
PFS_OPTION_REG2(spdk_env_context, FLAGS_spdk_env_context, OPT_STR, "--in-memory", OPT_CSTR);

static char *FLAGS_spdk_json_config_file;
PFS_OPTION_REG2(spdk_json_config_file, FLAGS_spdk_json_config_file, OPT_STR, "", OPT_CSTR);

static char *FLAGS_spdk_rpc_address;
PFS_OPTION_REG2(spdk_rpc_address, FLAGS_spdk_rpc_address, OPT_STR,  "", OPT_CSTR);

static char *FLAGS_spdk_log_flags;
PFS_OPTION_REG2(spdk_log_flags, FLAGS_spdk_log_flags, OPT_STR,  "", OPT_CSTR);
//PFS_OPTION_REG2(spdk_log_flags, FLAGS_spdk_log_flags, OPT_STR,  "bdev,thread,nvme", OPT_CSTR);

static int FLAGS_spdk_log_level = SPDK_LOG_INFO;
PFS_OPTION_REG2(spdk_log_level, FLAGS_spdk_log_level, OPT_INT, SPDK_LOG_INFO, OPT_INT);

static int FLAGS_spdk_log_print_level = SPDK_LOG_INFO;
PFS_OPTION_REG2(spdk_log_print_level, FLAGS_spdk_log_print_level, OPT_INT, SPDK_LOG_INFO, OPT_INT);

static char *FLAGS_spdk_nvme_controller;
PFS_OPTION_REG2(spdk_nvme_controller, FLAGS_spdk_nvme_controller, OPT_STR, "", OPT_CSTR);

static int FLAGS_spdk_delete_temp_json_file;
PFS_OPTION_REG2(spdk_delete_temp_json_file, FLAGS_spdk_delete_temp_json_file, OPT_INT, 1, OPT_INT);

static char *g_spdk_temp_config_file;
static pthread_mutex_t g_init_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_rpc_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_rpc_cond = PTHREAD_COND_INITIALIZER;
static bool g_rpc_stop = false;
static pthread_mutex_t g_gc_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_gc_cond = PTHREAD_COND_INITIALIZER;
static bool g_gc_stop = false;
struct spdk_thread_node {
    TAILQ_ENTRY(spdk_thread_node) link;
    struct spdk_thread *thread;
};

static TAILQ_HEAD(,spdk_thread_node) g_gc_list =
    TAILQ_HEAD_INITIALIZER(g_gc_list);

static bool g_init_stop = false;
static pthread_t g_init_thread_id = 0;
static pthread_t g_rpc_thread_id = 0;
static pthread_t g_gc_thread_id = 0;
static bool g_spdk_env_initialized = false;
static struct pfs_spdk_driver_poller spdk_driver_poller = { NULL, NULL, NULL };

struct devcpu {
	char *devname;
	int cpuid;
	TAILQ_ENTRY(devcpu) link;
};

static TAILQ_HEAD(,devcpu) g_dev_list =
	TAILQ_HEAD_INITIALIZER(g_dev_list);

static pthread_mutex_t g_devcpu_mtx = PTHREAD_MUTEX_INITIALIZER;

void
pfs_spdk_set_devcpu_bind(const char *devname, int cpuid)
{
    struct devcpu *d;

    pthread_mutex_lock(&g_devcpu_mtx);
    TAILQ_FOREACH(d, &g_dev_list, link) {
        if (strcmp(d->devname, devname) == 0) {
            d->cpuid = cpuid;
            pthread_mutex_unlock(&g_devcpu_mtx);
            return;
        }
    }
    d = (struct devcpu *)malloc(sizeof(*d));
    d->devname = strdup(devname);
    d->cpuid = cpuid;	
    TAILQ_INSERT_TAIL(&g_dev_list, d, link);
    pthread_mutex_unlock(&g_devcpu_mtx);
}

int
pfs_spdk_get_devcpu_bind(const char *devname)
{
    struct devcpu *d;
    int cpuid = -1;
    pthread_mutex_lock(&g_devcpu_mtx);
    TAILQ_FOREACH(d, &g_dev_list, link) {
        if (strcmp(d->devname, devname) == 0) {
            cpuid = d->cpuid;
            break;
        }
    }
    pthread_mutex_unlock(&g_devcpu_mtx);
    return cpuid;
}

static char *
safe_strdup(const char *p)
{
    if (p == NULL)
        return strdup("");
    return strdup(p);
}

void
pfs_spdk_set_driver_poller(const struct pfs_spdk_driver_poller *poller)
{
    spdk_driver_poller = *poller;
}

void
pfs_spdk_get_driver_poller(struct pfs_spdk_driver_poller *poller)
{
    *poller = spdk_driver_poller;
}

static void
parse_pci_address(struct spdk_env_opts *opts)
{
    char *s = NULL;
    char *cp = NULL, *t = NULL;
    struct spdk_pci_addr **pa = NULL;

    opts->pci_blocked = NULL;
    opts->pci_allowed = NULL;
    if (!pfs_empty_string(FLAGS_spdk_pci_blocked)) {
        s = safe_strdup(FLAGS_spdk_pci_blocked);
        pa = &opts->pci_blocked;
    } else {
        s = safe_strdup(FLAGS_spdk_pci_allowed);
        pa = &opts->pci_allowed;
    }
    *pa = NULL;
    if (!strlen(s)) {
        free(s);
        return;
    }

    cp = s;
    opts->num_pci_addr = 0;
    while ((t = strsep(&cp, " ,"))) {
        struct spdk_pci_addr addr;
        if (spdk_pci_addr_parse(&addr, t)) {
            pfs_etrace("can not parse pci address: %s\n", t);
        } else {
            *pa = (spdk_pci_addr *)realloc(*pa,
                sizeof(spdk_pci_addr) * (1 +  opts->num_pci_addr));
            (*pa)[opts->num_pci_addr++] = addr;
        }
    }

    free(s);
}

static void
set_spdk_opts_from_flags(struct spdk_env_opts *opts)
{
    if (!pfs_empty_string(FLAGS_spdk_name)) {
        opts->name = FLAGS_spdk_name;
    }
    if (!pfs_empty_string(FLAGS_spdk_core_mask)) {
        opts->core_mask = FLAGS_spdk_core_mask;
    }
    opts->shm_id = FLAGS_spdk_shm_id;
    opts->mem_channel = FLAGS_spdk_mem_channel;
    opts->main_core = FLAGS_spdk_main_core;
    opts->mem_size = FLAGS_spdk_mem_size;
    opts->no_pci = FLAGS_spdk_no_pci;
    opts->hugepage_single_segments = FLAGS_spdk_hugepage_single_segments;
    opts->unlink_hugepage = FLAGS_spdk_unlink_hugepage;
    opts->num_pci_addr = 0;
    if (!pfs_empty_string(FLAGS_spdk_hugedir)) {
        opts->hugedir = FLAGS_spdk_hugedir;
    }
    parse_pci_address(opts);
    if (!pfs_empty_string(FLAGS_spdk_iova_mode)) {
        opts->iova_mode = FLAGS_spdk_iova_mode;
    }
    if (FLAGS_spdk_base_virtaddr)
        opts->base_virtaddr = FLAGS_spdk_base_virtaddr;
    if (!pfs_empty_string(FLAGS_spdk_env_context)) {
        opts->env_context = FLAGS_spdk_env_context;
    }
}

/*
 * Generate json config file base on FLAGS_spdk_nvme_controller
 *
 * Return:
 *    failure:  -1
 *    nothing:   0
 *    generated: 1
 */
static int
pfs_generate_json_file(void)
{
    char temp[128];
    int fd;

    if (pfs_empty_string(FLAGS_spdk_nvme_controller))
        return 0;

    strcpy(temp, "/tmp/pfs_spdk_json_config_XXXXXX");
    fd = mkstemp(temp);
    if (fd == -1) {
        pfs_etrace("can not create temp file");
        return -1;
    }

    char *s = NULL;
    int n = asprintf(&s,
R"foo({
    "subsystems":
    [
        {
            "subsystem": "bdev",
            "config":
            [
                {
                    "method": "bdev_set_options",
                    "params": {
                        "bdev_io_pool_size": 65535,
                        "bdev_io_cache_size": 2048
                    }
                },
                {
                    "method": "bdev_nvme_attach_controller",
                    "params":
                    {
                        "trtype": "PCIe",
                        "name":"%s",
                        "traddr":"%s"
                    }
                }
            ]
        }
    ]
})foo",
    FLAGS_spdk_nvme_controller, FLAGS_spdk_nvme_controller);
    int rc = write(fd, s, n); 
    if (rc == -1)
        pfs_etrace("cannot write file %s, %s", temp, strerror(errno));
    close(fd);
    free(s);
    pfs_itrace("generated json config file: %s", temp);
    if (rc == -1) {
        return -1;
    }
    free(g_spdk_temp_config_file);
    g_spdk_temp_config_file = strdup(temp); 
    return 1;
}

struct subsys_init_param {
    int result;
    bool done;
};

static void
pfs_spdk_init_subsys_done(int rc, void *cb_arg)
{
    struct subsys_init_param *param = (struct subsys_init_param *)cb_arg;
    param->result = rc;
    param->done = true;
    if (FLAGS_spdk_delete_temp_json_file) {
        unlink(g_spdk_temp_config_file);
    }
    free(g_spdk_temp_config_file);
    g_spdk_temp_config_file = NULL;
}

static void
pfs_spdk_init_subsys_start(void *arg)
{
    struct subsys_init_param *param = (struct subsys_init_param *) arg;
    char *json_file;

    param->result = 0;
    param->done = false;
    json_file = g_spdk_temp_config_file;
    if (pfs_empty_string(json_file)) {
        if (pfs_empty_string(FLAGS_spdk_json_config_file)) {
            pfs_etrace("json config file is not set!");
        }
        else
            json_file = FLAGS_spdk_json_config_file;
        pfs_itrace("json config file: %s", json_file);
    }

    spdk_subsystem_init_from_json_config(
        json_file,
        SPDK_DEFAULT_RPC_ADDR,
        pfs_spdk_init_subsys_done, param, true);
}

// Update the time point to wait
// @param ts in-out
static void
pfs_spdk_calc_timeout(struct spdk_thread *spdk_thread, uint64_t polltime, struct timespec *ts)
{
    uint64_t timeout, now;

    if (spdk_thread_has_active_pollers(spdk_thread)) {
        return;
    }

    timeout = spdk_thread_next_poller_expiration(spdk_thread);
    now = spdk_get_ticks();

    if (timeout == 0) {
        // no next poller, use polltime instead
        timeout = now + (polltime * spdk_get_ticks_hz()) / SPDK_SEC_TO_NSEC;
    }

    if (timeout > now) {
        // if timeout > now, add the delta to ts
        timeout = ((timeout - now) * SPDK_SEC_TO_NSEC) / spdk_get_ticks_hz() +
              ts->tv_sec * SPDK_SEC_TO_NSEC + ts->tv_nsec;

        ts->tv_sec  = timeout / SPDK_SEC_TO_NSEC;
        ts->tv_nsec = timeout % SPDK_SEC_TO_NSEC;
    }
}

void
pfs_spdk_gc_thread(struct spdk_thread *spdk_thread)
{
    spdk_thread_exit(spdk_thread);
    if (spdk_get_thread() == spdk_thread)
        spdk_set_thread(NULL);

    struct spdk_thread_node *node =
        (struct spdk_thread_node *)malloc(sizeof(*node));
    node->thread = spdk_thread;
    // kill spdk thread in gc thread context
    pthread_mutex_lock(&g_gc_mutex);
    TAILQ_INSERT_TAIL(&g_gc_list, node, link);
    pthread_cond_broadcast(&g_gc_cond);
    pthread_mutex_unlock(&g_gc_mutex);
}

void
pfs_spdk_teardown_thread(struct spdk_thread *spdk_thread)
{
    // kill spdk thread in current thread context
    spdk_thread_exit(spdk_thread);
    while (!spdk_thread_is_exited(spdk_thread)) {
        spdk_thread_poll(spdk_thread, 0, 0);
    }
    spdk_thread_destroy(spdk_thread);
    spdk_set_thread(NULL);
}

static void *
rpc_service(void *arg)
{
    struct spdk_thread *spdk_thread = NULL;

    pthread_setname_np(pthread_self(), "pfs_spdk_rpc");

    spdk_thread = spdk_thread_create("rpc service", NULL);
    spdk_set_thread(spdk_thread);
    if (!pfs_empty_string(FLAGS_spdk_rpc_address)) {
        if (spdk_rpc_initialize(FLAGS_spdk_rpc_address)) {
            pfs_etrace("can not init spdk rpc server at : %s",
                       FLAGS_spdk_rpc_address);

            pfs_spdk_teardown_thread(spdk_thread);
            return NULL;
        } else {
            spdk_rpc_set_state(SPDK_RPC_RUNTIME);
            pfs_itrace("init spdk rpc server at : %s",
                       FLAGS_spdk_rpc_address);
        }
    }

    struct timespec ts, interval = { 0, 100000000 };
    pthread_mutex_lock(&g_rpc_mutex);
    while (!g_rpc_stop) {
        while (spdk_thread_poll(spdk_thread, 0, 0))
            ;
        clock_gettime(CLOCK_REALTIME, &ts);
        pfs_timespecadd(&ts, &interval, &ts);
        pthread_cond_timedwait(&g_rpc_cond, &g_rpc_mutex, &ts);
    }
    pthread_mutex_unlock(&g_rpc_mutex);

    spdk_rpc_finish();
    pfs_spdk_teardown_thread(spdk_thread);

    return NULL;
}

// spdk-thread gc service
static void *
gc_service(void *arg)
{
    struct spdk_thread *spdk_thread = NULL;

    pthread_setname_np(pthread_self(), "spdk_thread_gc");
    spdk_thread = spdk_thread_create("gc service", NULL);

    pthread_mutex_lock(&g_gc_mutex);
    for (;;) {
        if (TAILQ_EMPTY(&g_gc_list) && g_gc_stop)
            break;

        // poll my spdk thread
        spdk_set_thread(spdk_thread);
        spdk_thread_poll(spdk_thread, 0, 0);
        // poll discarded spdk threads
        for (auto node = TAILQ_FIRST(&g_gc_list); node != NULL;) {
            struct spdk_thread *tmp = node->thread;
            spdk_set_thread(tmp);
            while (spdk_thread_poll(tmp, 0, 0))
                ;
            if (spdk_thread_is_exited(tmp)) {
                auto next = TAILQ_NEXT(node, link);
                TAILQ_REMOVE(&g_gc_list, node, link);
                const char *name = spdk_thread_get_name(tmp);
                if (name)
                    pfs_itrace("spdk thread '%s' is garbage collected", name);
                else
                    pfs_itrace("spdk thread %p is garbage collected", tmp);
                spdk_thread_destroy(tmp);
                spdk_set_thread(NULL);
                free(node);
                node = next;
            } else {
                node = TAILQ_NEXT(node, link);
            }
        }
        spdk_set_thread(spdk_thread);
        // Figure out how long to sleep. 
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        // sleep 0.1 seconds
        pfs_spdk_calc_timeout(spdk_thread, 100000000ULL, &ts);
        pthread_cond_timedwait(&g_gc_cond, &g_gc_mutex, &ts);
    }
    pthread_mutex_unlock(&g_gc_mutex);

    // Kill my spdk thread
    spdk_set_thread(spdk_thread);
    spdk_thread_exit(spdk_thread);
    while (!spdk_thread_is_exited(spdk_thread)) {
        spdk_thread_poll(spdk_thread, 0, 0);
    }
    spdk_thread_destroy(spdk_thread);

    return NULL;
}

static int
pfs_spdk_schedule_thread(struct spdk_thread *spdk_thread)
{
    return 0;
}

struct init_env_param {
    int result;
    sem_t sem;

    init_env_param() {
        result = -1;
        sem_init(&sem, 0, 0);
    }

    ~init_env_param() {
        sem_destroy(&sem);
    }

    void wait() {
        while(sem_wait(&sem))
            ;
    }

    void signal() {
        sem_post(&sem);
    }

    void set_result(int res) {
        result = res;
    }
};

static void
pfs_spdk_subsys_fini_done(void *cb_arg)
{
    *(bool *)cb_arg = true;
}

static void *
pfs_spdk_init_env(void *arg)
{
    struct spdk_env_opts opts;
    struct spdk_thread  *spdk_thread = NULL;
    int                 rc = 0;
    struct init_env_param *param = (struct init_env_param *)arg;
    struct timespec     ts;

    param->set_result(-1);
    if (0) {
out:
        param->signal(); 
        return NULL;
    }
    if (pfs_generate_json_file() < 0) {
        goto out;
    }

    memset(&opts, 0, sizeof(opts));
    spdk_env_opts_init(&opts);
    set_spdk_opts_from_flags(&opts);

    if (spdk_env_init(&opts) < 0) {
        pfs_etrace("Unable to initialize SPDK env\n");
        goto out;
    }

    // [ Important.
    // please don't remove following code. DPDK may only bind process to
    // one cpu, the code fix the problem.
    spdk_unaffinitize_thread();
    // end Important ]

    if (!pfs_empty_string(FLAGS_spdk_log_flags)) {
        // duplicate string
        char *log_flags = safe_strdup(FLAGS_spdk_log_flags);
        char *tok = strtok(log_flags, ",");
        do {
            rc = spdk_log_set_flag(tok);
            if (rc < 0) {
                pfs_etrace("unknown spdk log flag %s\n", tok);
                free(log_flags);
                goto out;
            }
        } while ((tok = strtok(NULL, ",")) != NULL);
        free(log_flags);
    }

    spdk_thread_lib_init(pfs_spdk_schedule_thread, 0);

    spdk_thread = spdk_thread_create("init", NULL);
    if (spdk_thread == NULL) {
        pfs_etrace("Failed to create initialization thread\n");
        spdk_env_fini();
        goto out;
    }
    spdk_set_thread(spdk_thread);

    // init spdk subsys
    struct subsys_init_param subsys_param;
    subsys_param.result = 0;
    subsys_param.done = false;

    pfs_itrace("Initializing spdk subsystem...");
    spdk_thread_send_msg(spdk_thread, pfs_spdk_init_subsys_start,
                         &subsys_param);
    do {
        spdk_thread_poll(spdk_thread, 0, 0);
    } while (!subsys_param.done);

    param->set_result(subsys_param.result);
    if (subsys_param.result) { 
        pfs_itrace("Initialize spdk subsystem failed, result is %d",
                   subsys_param.result);
        pfs_spdk_teardown_thread(spdk_thread);

        param->signal();
        return 0;
    }

    pfs_itrace("Initialize spdk subsystem success");

    // spdk is inited, signal caller
    param->signal();

    // spdk admin thread should keep running
    while (!g_init_stop) {
        while (spdk_thread_poll(spdk_thread, 0, 0))
            ;
        usleep(500);
    }

    // shutdown spdk subsys
    bool fini_done = false;
    spdk_subsystem_fini(pfs_spdk_subsys_fini_done, &fini_done);
    while (!fini_done) {
        spdk_thread_poll(spdk_thread, 0, 0);
    }
    pfs_spdk_teardown_thread(spdk_thread);
    pfs_itrace("spdk subsystem is shutdown now");
    return 0;
}

static int
pfs_spdk_init_rpc_thread(void)
{
    int rc;

    pfs_itrace("Starting spdk rpc service thread");
    rc = pthread_create(&g_rpc_thread_id, NULL, rpc_service, NULL);
    if (rc) {
        fprintf(stderr, "can not create spdk rpc service thread\n");
    }
    return rc;
}

static int
pfs_spdk_init_gc_thread(void)
{
    int rc;

    pfs_itrace("Starting spdk thread gc service thread");
    rc = pthread_create(&g_gc_thread_id, NULL, gc_service, NULL);
    if (rc) {
        fprintf(stderr, "can not create spdk thread gc service thread\n");
    }
    return rc;
}

static void
pfs_spdk_dump_devices(void)
{
    pfs_itrace("Found devices:\n");
    for (auto bdev = spdk_bdev_first(); bdev; bdev = spdk_bdev_next(bdev)) {
        char *cpuset_str = NULL;
        cpu_set_t cpuset;
        if (pfs_get_dev_local_cpus(bdev, &cpuset) == 0)
            cpuset_str = pfs_cpuset_to_string(&cpuset);
        pfs_itrace("\tName: %s, Size: %ld, BlockSize: %d, PhyBlockSize: %d, OptimalIoBoundary: %d, WriteUnit: %d, WriteZero: %d, BufAlign: %ld, Local CPUs: %s",
             spdk_bdev_get_name(bdev),
             spdk_bdev_get_num_blocks(bdev) * spdk_bdev_get_block_size(bdev),
             spdk_bdev_get_block_size(bdev),
             spdk_bdev_get_physical_block_size(bdev),  
             spdk_bdev_get_optimal_io_boundary(bdev),
             spdk_bdev_get_write_unit_size(bdev),
             spdk_bdev_io_type_supported(bdev, SPDK_BDEV_IO_TYPE_WRITE_ZEROES),
             spdk_bdev_get_buf_align(bdev),
             cpuset_str);
        free(cpuset_str);
    }
}

int
pfs_spdk_setup(void)
{
    pfs_spdk_thread_guard guard;

    spdk_log_set_level((spdk_log_level)FLAGS_spdk_log_level);
    spdk_log_set_print_level((spdk_log_level)FLAGS_spdk_log_print_level);

    //lock initialization procedure
    pthread_mutex_lock(&g_init_mutex);
    if (g_spdk_env_initialized) {
        pthread_mutex_unlock(&g_init_mutex);
        return 0;
    }

    g_init_stop = false;
    g_rpc_stop = false;
    g_gc_stop = false;

    struct init_env_param param;
    // init spdk asynchornously
    if (pthread_create(&g_init_thread_id, NULL, pfs_spdk_init_env, &param)) {
        pfs_etrace("can not create spdk_init_env thread");
        pthread_mutex_unlock(&g_init_mutex);
        return -1;
    }
    param.wait();
    if (param.result == -1) {
        pfs_etrace("failed to initialize spdk\n");
        pthread_mutex_unlock(&g_init_mutex);
        return -1;
    }

    pfs_spdk_init_rpc_thread();
    pfs_spdk_init_gc_thread();

    g_spdk_env_initialized = true;
    pthread_mutex_unlock(&g_init_mutex);

    pfs_spdk_dump_devices();
    return 0;
}

static void
pfs_spdk_shutdown_init_thread(void)
{
    pfs_itrace("Join spdk admin service thread");
    g_init_stop = true;
    pthread_join(g_init_thread_id, NULL);
}

static void
pfs_spdk_shutdown_rpc_thread(void)
{
    int rc;

    pfs_itrace("Join spdk rpc service thread");
    pthread_mutex_lock(&g_rpc_mutex);
    g_rpc_stop = true;
    pthread_cond_broadcast(&g_rpc_cond);
    pthread_mutex_unlock(&g_rpc_mutex);

    rc = pthread_join(g_rpc_thread_id, NULL);
    if (rc)
        pfs_etrace("can not join spdk rpc service thread, %s\n", strerror(rc));
}

static void
pfs_spdk_shutdown_gc_thread(void)
{
    int rc;

    pfs_itrace("Join spdk thread gc service thread");
    pthread_mutex_lock(&g_gc_mutex);
    g_gc_stop = true;
    pthread_cond_broadcast(&g_gc_cond);
    pthread_mutex_unlock(&g_gc_mutex);

    rc = pthread_join(g_gc_thread_id, NULL);
    if (rc)
        pfs_etrace("can not join spdk rpc service thread, %s\n", strerror(rc));
}

void
pfs_spdk_cleanup(void)
{
    pfs_spdk_thread_guard guard;
    int rc;

    pthread_mutex_lock(&g_init_mutex);
    if (!g_spdk_env_initialized) {
        pthread_mutex_unlock(&g_init_mutex);
        return;
    }
    pfs_itrace("Cleaning up spdk...");
    pfs_spdk_shutdown_rpc_thread();
    pfs_spdk_shutdown_init_thread();
    pfs_spdk_shutdown_gc_thread();

    spdk_thread_lib_fini();
    spdk_env_fini();
    spdk_log_close();

    g_spdk_env_initialized = 0;

    pthread_mutex_unlock(&g_init_mutex);
    pfs_itrace("Cleaning up spdk done");
}

/* functions for mkfs */
void pfs_spdk_conf_set_blocked_pci(const char *s)
{
    free(FLAGS_spdk_pci_blocked);
    FLAGS_spdk_pci_blocked = strdup(s);
}

void pfs_spdk_conf_set_allowed_pci(const char *s)
{
    free( FLAGS_spdk_pci_allowed);
    FLAGS_spdk_pci_allowed = strdup(s);
}

void pfs_spdk_conf_set_json_config_file(const char *s)
{
    free(FLAGS_spdk_json_config_file);
    FLAGS_spdk_json_config_file = strdup(s);
}

void pfs_spdk_conf_set_name(const char *s)
{
    free(FLAGS_spdk_name);
    FLAGS_spdk_name = strdup(s);
}

void pfs_spdk_conf_set_env_context(const char *s)
{
    free(FLAGS_spdk_env_context);
    FLAGS_spdk_env_context = strdup(s);
}

void pfs_spdk_conf_set_controller(const char *s)
{
    free(FLAGS_spdk_nvme_controller);
    FLAGS_spdk_nvme_controller = strdup(s);
}

int
pfs_get_dev_local_cpus(struct spdk_bdev *bdev, cpu_set_t *set)
{
    char* pci_addr;

    CPU_ZERO(set);
    pci_addr = pfs_get_dev_pci_address(bdev);
    if (pci_addr == NULL)
        return -1;
    if (!strlen(pci_addr)) {
        free(pci_addr);
        return -1;
    }
    int rc = pfs_get_pci_local_cpus(pci_addr, set);
    free(pci_addr);
    return rc;
}

static int
json_write_cb(void *cb_ctx, const void *data, size_t size)
{
    FILE *f = (FILE*) cb_ctx;
    size_t rc;

    rc = fwrite(data, 1, size, f);
    return rc == size ? 0 : -1;
}

// return memory must be freed by caller
char *
pfs_get_dev_pci_address(struct spdk_bdev *bdev)
{
    kvec_t(spdk_json_val) array;
    struct spdk_json_write_ctx *w;
    char *address = NULL, *json = NULL, *end = NULL;
    size_t json_size = 0;
    ssize_t values_cnt = 0, rc = 0;
    struct spdk_json_val *nvme = NULL, *o = NULL, *v = NULL;
    FILE *f = open_memstream(&json, &json_size);

    kv_init(array);

    if (0) {
err:
        kv_destroy(array);
        free(json);
        return NULL;
    }

    w = spdk_json_write_begin(json_write_cb, f, SPDK_JSON_WRITE_FLAG_FORMATTED);
    spdk_json_write_object_begin(w);
    spdk_bdev_dump_info_json(bdev, w);
    spdk_json_write_object_end(w);
    spdk_json_write_end(w);
    fclose(f);
    f = NULL;

    rc = spdk_json_parse(json, json_size, NULL, 0, (void **)&end,
            SPDK_JSON_PARSE_FLAG_ALLOW_COMMENTS);
    if (rc < 0) {
        pfs_etrace("Parsing JSON configuration failed (%zd)\n", rc);
        goto err;
    }

    kv_resize(spdk_json_val, array, rc);
    rc = spdk_json_parse(json, json_size, array.a, kv_max(array),
             (void **)&end, SPDK_JSON_PARSE_FLAG_ALLOW_COMMENTS);
    if (rc < 0) {
        pfs_etrace("Parsing JSON configuration failed (%zd)\n", rc);
        goto err;
    }
    kv_size(array) = rc;

    rc = spdk_json_find_array(array.a, "nvme", NULL, &nvme);
    if (rc) {
        pfs_etrace("No 'nvme' key in JSON.\n");
        goto err;
    }

    for (o = spdk_json_array_first(nvme); o; o = spdk_json_next(o)) {
        rc = spdk_json_find_string(o, "pci_address", NULL, &v);
        if (rc == 0) {
            char *s = NULL;
            rc = spdk_json_decode_string(v, &s);
            if (rc == 0) {
                address = s;
                break;
            }
        }
    }

    kv_destroy(array);
    free(json);
    return address;
}

int
pfs_get_pci_local_cpus(const char *pci_addr, cpu_set_t *set)
{
    char sys_path[1024];
    size_t size = 0;
    char *line = NULL;
    kvec_t(uint32_t) local_cpus;
    uint32_t mask;
    int widx;
    bool found = false;

    if (!strlen(pci_addr))
        return -1;

    snprintf(sys_path, sizeof(sys_path), "/sys/bus/pci/devices/%s/local_cpus", pci_addr);
    FILE *fp = fopen(sys_path, "r");
    if (fp == NULL) {
        return -1;
    }

    if (getline(&line, &size, fp) == -1) {
        fclose(fp);
        return -1;
    }
    fclose(fp);

    kv_init(local_cpus);
    char *t, *p = line, *e = NULL;
    while ((t = strsep(&p, ","))) {
        mask = (uint32_t)strtol(t, &e, 16);
        if (*e != '\0' && *e != '\n') {
            pfs_etrace("can not pass cpu list\n");
            kv_destroy(local_cpus);
            return -1;
        }
        *(kv_pushp(uint32_t, local_cpus)) = mask;
    }

    widx = 0;
    while (kv_size(local_cpus)) {
        mask = kv_pop(local_cpus);
        for (int i = 0; i < 32; ++i) {
            if (mask & (1 << i)) {
                int cpu = (widx * 32) + i;
                CPU_SET(cpu, set);
                found = true;
            }
        }
        widx++;
    }

    kv_destroy(local_cpus);
    if (found)
        return 0;
    return -1;
}

char *
pfs_cpuset_to_string(const cpu_set_t *mask)
{
    int i = 0, j = 0;
    char buf[64];
    kstring_t s;

    bzero(&s, sizeof(s));
    for (i = 0; i < CPU_SETSIZE;) {
        if (CPU_ISSET(i, mask)) {
            int run = 0;
            for (j = i + 1; j < CPU_SETSIZE; j++) {
                if (CPU_ISSET(j, mask)) run++;
                else break;
            }
            if (!run)
                sprintf(buf, "%d,", i);
            else if (run == 1) {
                sprintf(buf, "%d,%d,", i, i + 1);
                i++;
            } else {
                sprintf(buf, "%d-%d,", i, i + run);
                i += run;
            }
            kputs(buf, &s);
            i = j;
        } else {
            i++;
        }
    }
    if (ks_len(&s)) {
        s.s[s.l-1] = '\0';
        s.l--; // remove last ','
    }
    return ks_release(&s);
}

int
pfs_parse_set(const char *input, cpu_set_t *set)
{
    unsigned idx;
    const char *str = input;
    char *end = NULL;
    unsigned min, max;

    CPU_ZERO(set);

    while (isblank(*str))
        str++;

    /* only digit or left bracket is qualify for start point */
    if ((!isdigit(*str) && *str != '(') || *str == '\0')
        return -1;

    /* process single number or single range of number */
    if (*str != '(') {
        errno = 0;
        idx = strtoul(str, &end, 10);
        if (errno || end == NULL || idx >= CPU_SETSIZE)
            return -1;
        else {
            while (isblank(*end))
                end++;

            min = idx;
            max = idx;
            if (*end == '-') {
                /* process single <number>-<number> */
                end++;
                while (isblank(*end))
                    end++;
                if (!isdigit(*end))
                    return -1;

                errno = 0;
                idx = strtoul(end, &end, 10);
                if (errno || end == NULL || idx >= CPU_SETSIZE)
                    return -1;
                max = idx;
                while (isblank(*end))
                    end++;
                if (*end != ',' && *end != '\0')
                    return -1;
            }

            if (*end != ',' && *end != '\0' &&
                *end != '@')
                return -1;

            for (idx = RTE_MIN(min, max);
                 idx <= RTE_MAX(min, max); idx++)
                CPU_SET(idx, set);

            return end - input;
        }
    }

    /* process set within bracket */
    str++;
    while (isblank(*str))
        str++;
    if (*str == '\0')
        return -1;

    min = RTE_MAX_LCORE;
    do {

        /* go ahead to the first digit */
        while (isblank(*str))
            str++;
        if (!isdigit(*str))
            return -1;

        /* get the digit value */
        errno = 0;
        idx = strtoul(str, &end, 10);
        if (errno || end == NULL || idx >= CPU_SETSIZE)
            return -1;

        /* go ahead to separator '-',',' and ')' */
        while (isblank(*end))
            end++;
        if (*end == '-') {
            if (min == RTE_MAX_LCORE)
                min = idx;
            else /* avoid continuous '-' */
                return -1;
        } else if ((*end == ',') || (*end == ')')) {
            max = idx;
            if (min == RTE_MAX_LCORE)
                min = idx;
            for (idx = RTE_MIN(min, max);
                 idx <= RTE_MAX(min, max); idx++)
                CPU_SET(idx, set);

            min = RTE_MAX_LCORE;
        } else
            return -1;

        str = end + 1;
    } while (*end != '\0' && *end != ')');

    /*
     * to avoid failure that tail blank makes end character check fail
     * in eal_parse_lcores( )
     */
    while (isblank(*str))
        str++;

    return str - input;
}

static unsigned
pfs_cpu_socket_id(unsigned lcore_id)
{
#define NUMA_NODE_PATH "/sys/devices/system/node" 
    unsigned socket;

    for (socket = 0; socket < RTE_MAX_NUMA_NODES; socket++) {
        char path[PATH_MAX];

        snprintf(path, sizeof(path), "%s/node%u/cpu%u", NUMA_NODE_PATH,
                socket, lcore_id);
        if (access(path, F_OK) == 0)
            return socket;
    }
    return 0;
}

int
pfs_cpuset_socket_id(cpu_set_t *cpusetp)
{
    unsigned cpu = 0;
    int socket_id = SOCKET_ID_ANY;
    int sid;

    if (cpusetp == NULL)
        return SOCKET_ID_ANY;

    do {
        if (!CPU_ISSET(cpu, cpusetp))
            continue;

        if (socket_id == SOCKET_ID_ANY)
            socket_id = pfs_cpu_socket_id(cpu);

        sid = pfs_cpu_socket_id(cpu);
        if (socket_id != sid) {
            socket_id = SOCKET_ID_ANY;
            break;
        }

    } while (++cpu < CPU_SETSIZE);

    return socket_id;
}

static int
__pfs_is_spdk_mem(const void *p, size_t size)
{
    const char *cp = (const char *)p;
    size_t left = size;
    while (left > 0) {
        size_t tmp = left;
        if (spdk_vtophys(cp, &tmp) == SPDK_VTOPHYS_ERROR)
            return 0;
        cp += tmp;
        left -= tmp;
    }

    return 1;
}

static int
__pfs_is_spdk_memv(const struct iovec *iov, int iovcnt)
{
    for (int i = 0; i < iovcnt; ++i) {
        if (!__pfs_is_spdk_mem(iov[i].iov_base, iov[i].iov_len))
            return 0;
    }

    return 1;
}

extern "C" int
pfs_is_spdk_mem(const void *p, size_t size)
{
    return __pfs_is_spdk_mem(p, size);
}

extern "C" int
pfs_is_spdk_memv(const struct iovec *iov, int iovcnt)
{
    return __pfs_is_spdk_memv(iov, iovcnt);
}

static inline bool                                                              
_is_page_aligned(uint64_t address, uint64_t page_size)                          
{
    return (address & (page_size - 1)) == 0;
}

/*
 * A simple function to verify if an io vector is PRP aligned.
 * return: 1 is aligned
 *         0 not aligned
 */
int
pfs_iov_is_prp_aligned(const struct iovec *iov, int iovcnt, off_t off)
{
    uintptr_t addr;
    size_t len, total_len = 0;
    int i;

    if (off % 512)
        return 0;

    if (iovcnt == 0)
        return 0;

    if (!__pfs_is_spdk_memv(iov, iovcnt)) // Is not spdk memory
        return 0;

    addr = (uintptr_t)iov[0].iov_base;
    if (iovcnt == 1) {
        // check if dword aligned and size is times of sector size
        return (addr & 3) == 0 && (iov[0].iov_len % 512) == 0;
    }
    addr += iov[0].iov_len;
    if (!_is_page_aligned(addr, PAGE_SIZE)) {
        return 0;
    }
    total_len += iov[0].iov_len;

    for (i = 1; i < iovcnt - 1; ++i) {
        // middle page must be page aligned and size is times of page
        addr = (uintptr_t)iov[i].iov_base;
        if (!_is_page_aligned(addr, PAGE_SIZE))
            return 0;
        len = iov[i].iov_len;
        if (len % PAGE_SIZE)
            return 0;
        total_len += len;
    }

    addr = (uintptr_t)iov[i].iov_base;
    if (!_is_page_aligned(addr, PAGE_SIZE))
        return 0;

    len = iov[i].iov_len;
    if (len & 3)
        return 0;
    total_len += len;
    if (total_len % 512)
        return 0;
    return 1;
}

int pfs_is_prp_aligned(const void *addr, size_t len, off_t off)
{
    struct iovec iov;

    iov.iov_base = (void *)addr;
    iov.iov_len = len;
    return pfs_iov_is_prp_aligned(&iov, 1, off);
}

/*
 * check if an io vector is nvme SGL aligned,
 * We assume SGL requires dword alignment.
 */
int
pfs_iov_is_sgl_aligned(const struct iovec *iov, int iovcnt, off_t off)
{
    uintptr_t addr = 0;
    size_t len = 0, total = 0;
    int i = 0;

    if (off % 512) 
        return 0;

    if (iovcnt == 0)
        return 0;

    if (!__pfs_is_spdk_memv(iov, iovcnt)) // Is not spdk memory
        return 0;

    for (i = 0; i < iovcnt; ++i) {
        addr = (uintptr_t)iov[i].iov_base;
        if (addr & 3)
            return 0;
        len = iov[i].iov_len;
        if (len & 3)
            return 0;
        total += len;
    }
    if (total % 512)
        return 0;

    return 1;
}

int pfs_is_sgl_aligned(const void *addr, size_t len, off_t off)
{
    struct iovec iov;

    iov.iov_base = (void *)addr;
    iov.iov_len = len;
    return pfs_iov_is_sgl_aligned(&iov, 1, off);
}
