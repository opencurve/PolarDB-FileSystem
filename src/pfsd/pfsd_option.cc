/*
 * Copyright (c) 2023 Netease Inc
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

/* Author: Xu Yifeng */

#include "pfsd_option.h"
#include "pfsd_common.h"

#include <string.h>

pfsd_option_t g_pfsd_option;

extern "C"
void pfsd_option_init(struct pfsd_option *opt)
{
	bzero(opt, sizeof(*opt));
	opt->o_pollers = 2;
	opt->o_workers = 20;
	opt->o_usleep = 1;
	strncpy(opt->o_shm_dir, PFSD_SHM_PATH, sizeof g_pfsd_option.o_shm_dir);
	opt->o_daemon = 0;
	opt->o_server_id = 0;
	opt->o_auto_increase_epoch = 0;
}

extern "C"
void pfsd_option_fini(struct pfsd_option *opt)
{
	/* nothing to do */
}

static void __attribute__((constructor))
init_default_value()
{
	pfsd_option_init(&g_pfsd_option);
}
