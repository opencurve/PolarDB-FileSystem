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

#include <iostream>
#include <string>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "pfs_testenv.h"
#include "pfs_spdk.h"
#include "pfs_option.h"

using namespace std;

DEFINE_string(cluster, "", "cluster name");
DEFINE_int32(host_id, 1, "hosit id");
DEFINE_string(pbd_name, "", "pbdname name");                       
DEFINE_string(spdk_nvme_controller, "", "nvme controller");                       

/*
 * usage:
 * ./pfs_unittest -cluster=spdk -pbd_name=0000:3e:00:0n1 -spdk_nvme_controller=0000:3e:00:0
 */

int main(int argc, char **argv)
{
	gflags::ParseCommandLineFlags(&argc, &argv, true);

	int hostid = FLAGS_host_id;
	string cluster = FLAGS_cluster;
	string pbdname = FLAGS_pbd_name;

	if (cluster.empty()) {
		cerr << "cluster is empty";
		return 1;
	}
	if (pbdname.empty()) {
		cerr << "pbd_name is empty";
		return 1;
	}

	pfs_option_set("spdk_nvme_controller", FLAGS_spdk_nvme_controller.c_str());

	g_testenv = dynamic_cast<PFSTestEnv *>(
			::testing::AddGlobalTestEnvironment(
				new PFSTestEnv(cluster, pbdname, hostid)
				)
			);
	::testing::InitGoogleTest(&argc, argv);

	if (pfs_spdk_setup()) {
		cerr << "can not init spdk";
		return 1;
	}
	return RUN_ALL_TESTS();
}
