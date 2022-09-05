/*
 * Copyright (c) 2022, Netease Inc
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

#ifndef	_PFS_IOMEM_H_
#define	_PFS_IOMEM_H_ 

#include <sys/types.h>

void *pfs_iomem_alloc(size_t size);
void  pfs_iomem_free(void *buf);

#endif
