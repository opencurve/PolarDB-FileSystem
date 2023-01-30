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

#ifndef PFS_TRACE_FUNC_H
#define PFS_TRACE_FUNC_H

#include <stdarg.h>

#ifdef __cplusplus
extern "C" {
#endif

enum {
	PFS_TRACE_FATAL,
	PFS_TRACE_ERROR,
	PFS_TRACE_WARN,
	PFS_TRACE_INFO,
	PFS_TRACE_DBG,
	PFS_TRACE_DEBUG	= PFS_TRACE_DBG,
	PFS_TRACE_VERB
};

void    pfs_vtrace(int level, const char *file, const char *func, int line,
	const char *fmt, ...) __attribute__ ((format (printf, 5, 6)));

typedef void (*pfs_trace_func_t)(int level, const char *filename,
	const char *func, int line, const char *fmt, va_list ap);
                                                                                
void	pfs_set_trace_func(pfs_trace_func_t func);
pfs_trace_func_t pfs_get_trace_func();
int	pfs_set_trace_level(int);
int	pfs_get_trace_level();

#ifdef __cplusplus
}
#endif

#endif // PFS_TRACE_FUNC_H
