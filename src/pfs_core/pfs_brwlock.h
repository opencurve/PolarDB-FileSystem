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
 * Author: XuYifeng
 */

#ifndef PFS_BRWLOCK_H
#define PFS_BRWLOCK_H

//
// A big rwlock to avoid cache line ping-pong between many threads
//
typedef struct pfs_brwlock *pfs_brwlock_t;

int	pfs_brwlock_init(pfs_brwlock_t *);
void	pfs_brwlock_destroy(pfs_brwlock_t *);
int	pfs_brwlock_tryrdlock(pfs_brwlock_t *);
int	pfs_brwlock_rdlock(pfs_brwlock_t *);
int	pfs_brwlock_trywrlock(pfs_brwlock_t *);
int	pfs_brwlock_wrlock(pfs_brwlock_t *);
int	pfs_brwlock_unlock(pfs_brwlock_t *);

#endif // PFS_BRWLOCK_H
