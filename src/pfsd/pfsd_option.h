/*
 * Copyright (c) 2023 Netease inc.
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

#ifndef _PFSD_OPTION_H_
#define _PFSD_OPTION_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct pfsd_option {
	int o_pollers;
	/* Worker threads, same as num of channels */
	int o_workers;
	/* Worker thread usleep interval in us */
	int o_usleep;
	/* pbdname like 1-1 */
	char o_pbdname[64];
	/* shm directory */
	char o_shm_dir[1024];
	/* daemon mode */
	int o_daemon;
	/* auto increase epoch when mount which write mode */
	int o_auto_increase_epoch;
	/* server id, for Postgresql? */
	int o_server_id;
} pfsd_option_t;

void pfsd_option_init(struct pfsd_option *opt);
void pfsd_option_fini(struct pfsd_option *opt);

extern pfsd_option_t g_pfsd_option;

#ifdef __cplusplus
}
#endif
#endif
