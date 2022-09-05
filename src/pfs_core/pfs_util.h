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

#ifndef	_PFS_UTIL_H_
#define	_PFS_UTIL_H_

#include <sys/types.h>
#include <sys/uio.h>
#include <stdint.h>
#include <stdarg.h>

#define GOLDEN_RATIO_32 0x61C88647
#define GOLDEN_RATIO_64 0x61C8864680B583EBull

static inline uint32_t hash_32(uint64_t val, unsigned int bits)
{                                                                               
	return val * GOLDEN_RATIO_32 >> (32 - bits);
}       

static inline uint32_t hash_64(uint64_t val, unsigned int bits)
{
	return val * GOLDEN_RATIO_64 >> (64 - bits);
}

uint32_t 	crc32c(uint32_t crc, const void *buf, size_t size);
uint64_t	roundup_power2(uint64_t val);
int		strncpy_safe(char *dst, const char *src, size_t n);
uint32_t	crc32c_compute(const void *buf, size_t size, size_t offset);
uint64_t	gettimeofday_us();

#define	DATA_SET_ATTR(set)	 	\
       	__attribute__((used)) 		\
	__attribute__((section(#set)))

#define	DATA_SET_DECL(type, set)	\
	extern struct type *__start_##set[], *__stop_##set[];

#define	DATA_SET_FOREACH(var, set)	\
	for (var = __start_##set; var < __stop_##set; var++)

typedef struct oidvect {
	uint64_t	*ov_buf;
	size_t		ov_size;
	int		ov_next;
	int32_t		*ov_holeoff_buf;
} oidvect_t;
void 	oidvect_init(oidvect_t *ov);
int 	oidvect_push(oidvect_t *ov, uint64_t val, int32_t holeoff);
uint64_t oidvect_pop(oidvect_t *ov);
void 	oidvect_fini(oidvect_t *ov);
static inline int
oidvect_begin(const oidvect_t *ov)
{
	return 0;
}

static inline int
oidvect_end(const oidvect_t *ov)
{
	return ov->ov_next;
}

static inline uint64_t
oidvect_get(const oidvect_t *ov, int indx)
{
	return ov->ov_buf[indx];
}

static inline int32_t
oidvect_get_holeoff(const oidvect_t *ov, int indx)
{
	return ov->ov_holeoff_buf[indx];
}

typedef struct pfs_printer {
	void	*pr_dest;
	int	(*pr_func)(void *dest, const char *fmt, va_list ap);
} pfs_printer_t;
int	pfs_printf(pfs_printer_t *pr, const char *fmt, ...);

int pfs_ratecheck(struct timeval *lasttime, const struct timeval *mininterval);

#define pfs_arraysize(a)	(sizeof(a) / sizeof(a[0]))

static inline void
forward_iovec_iter(struct iovec **itp, int *iovcnt, int len)
{
	struct iovec *it = *itp;

	while (len > 0) {
		if (it->iov_len <= len) {
			it->iov_base = ((char *)it->iov_base) + it->iov_len;
			len -= it->iov_len;
			*iovcnt = *iovcnt - 1;
			it++;
		} else {
			it->iov_base = ((char *)it->iov_base) + len;
			it->iov_len -= len;
			len = 0;
			break;
		}
	}
	*itp = it;
}

static inline size_t
iovec_bytes(const struct iovec *iov, int cnt)
{
    size_t total = 0;
    while (cnt-- > 0) {
        total += iov[cnt].iov_len;
    }
    return total;
}

void pfs_copy_from_buf_to_iovec(struct iovec *iovec, const void *_buf, size_t len);
void pfs_copy_from_iovec_to_buf(void *_buf, const struct iovec *iovec, size_t len);
void pfs_reset_iovcnt(struct iovec *iovec, size_t len, int *iovcnt, bool reset_iov);

size_t pfs_getpagesize();
#endif
