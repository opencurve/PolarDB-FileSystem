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

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/param.h>

#include "pfs_brwlock.h"

#define leaf_count 32
#define leaf_mask  31

#ifndef PFS_CACHELINE_SIZE
#define PFS_CACHELINE_SIZE 64
#endif

struct pfs_brwlock {
	pthread_rwlock_t	*leaves[leaf_count];
	pthread_t		owner;
};

static int next_thread_id;
static pthread_mutex_t id_lock = PTHREAD_MUTEX_INITIALIZER;
 
static __thread int my_thread_id = -1;
static inline unsigned int
get_thread_id(void)
{
	if (my_thread_id == -1) {
		pthread_mutex_lock(&id_lock);
		my_thread_id = next_thread_id++;
		pthread_mutex_unlock(&id_lock);
		if (my_thread_id == -1) /* overflow */
			my_thread_id = 0;
	}
	return (unsigned int)my_thread_id;
}

static inline int
get_leaf_index(void)
{
	return (int)(get_thread_id() & leaf_mask);
}

int
pfs_brwlock_init(pfs_brwlock_t *rwlock)
{
	struct pfs_brwlock *lck;
	const int align = 64;
	size_t alloc_size = 0;
	int i;

	alloc_size = roundup(sizeof(struct pfs_brwlock), align);
	lck = (struct pfs_brwlock *)aligned_alloc(align, alloc_size);
	if (lck == NULL) {
		return ENOMEM;
	}
	for (i = 0; i < leaf_count; ++i) {
		void *ptr;
		alloc_size = roundup(sizeof(pthread_rwlock_t), align);
		if (!(ptr = aligned_alloc(align, alloc_size))) {
			int err;
			err = errno;
			while (--i >= 0) {
				pthread_rwlock_destroy(lck->leaves[i]);
			}
			free(lck);
			return (err);
		}
		lck->leaves[i] = (pthread_rwlock_t *)ptr;
		pthread_rwlock_init(lck->leaves[i], NULL);
	}
	lck->owner = 0;
	*rwlock = lck;
	return (0);
}

void
pfs_brwlock_destroy(pfs_brwlock_t *rwlock)
{
	struct pfs_brwlock *lck = *rwlock;
	int i;

	for (i = 0; i < leaf_count; ++i) {
		pthread_rwlock_destroy(lck->leaves[i]);
		free(lck->leaves[i]);
	}
	free(lck);
	*rwlock = NULL;
}

int
pfs_brwlock_tryrdlock(pfs_brwlock_t *rwlock)
{
	struct pfs_brwlock *lck = *rwlock;
	pthread_rwlock_t *leaf = lck->leaves[get_leaf_index()];

	return pthread_rwlock_tryrdlock(leaf);
}

int
pfs_brwlock_rdlock(pfs_brwlock_t *rwlock)
{
	struct pfs_brwlock *lck = *rwlock;
	pthread_rwlock_t *leaf = lck->leaves[get_leaf_index()];
	return pthread_rwlock_rdlock(leaf);
}

int
pfs_brwlock_wrlock(pfs_brwlock_t *rwlock)
{
	struct pfs_brwlock *lck = *rwlock;
	int i, err;
	
	for (i = 0; i < leaf_count; ++i) {
		err = pthread_rwlock_wrlock(lck->leaves[i]);
		if (err != 0) {
			while (--i >= 0) {
				pthread_rwlock_unlock(lck->leaves[i]);
			}
			return (err);
		}
	}
	lck->owner = pthread_self();
	return (0);
}

int
pfs_brwlock_trywrlock(pfs_brwlock_t *rwlock)
{
	struct pfs_brwlock *lck = *rwlock;
	int i, err;
	
	for (i = 0; i < leaf_count; ++i) {
		err = pthread_rwlock_trywrlock(lck->leaves[i]);
		if (err != 0) {
			while (--i >= 0) {
				pthread_rwlock_unlock(lck->leaves[i]);
			}
			return (err);
		}
	}
	lck->owner = pthread_self();
	return (0);
}

int
pfs_brwlock_unlock(pfs_brwlock_t *rwlock)
{
	struct pfs_brwlock *lck = *rwlock;
	pthread_rwlock_t *leaf;
	int i;

	if (lck->owner != 0) {
		if (lck->owner == pthread_self()) {
			lck->owner = 0;
			for (i = 0; i < leaf_count; ++i)
				pthread_rwlock_unlock(lck->leaves[i]);
			return (0);
		}
		return (EPERM);
	}

	leaf = lck->leaves[get_leaf_index()];
	return pthread_rwlock_unlock(leaf);
}
