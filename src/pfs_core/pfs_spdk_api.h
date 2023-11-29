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

#ifndef _PFS_SPDK_API_H
#define _PFS_SPDK_API_H

#ifdef __cplusplus
extern "C" {
#endif

int pfs_spdk_setup(void);
void pfs_spdk_cleanup(void);

struct pfs_spdk_driver_poller {
    void* (*register_callback)(unsigned (*cb)(void *), void *arg);
    void (*notify_callback)(void *handle);
    void (*remove_callback)(void *handle);
};

void pfs_spdk_set_driver_poller(const struct pfs_spdk_driver_poller *);
void pfs_spdk_get_driver_poller(struct pfs_spdk_driver_poller *);
void pfs_spdk_set_devcpu_bind(const char *devname, int cpuid);
int pfs_spdk_get_devcpu_bind(const char *devname);

#ifdef __cplusplus
}
#endif

#endif
