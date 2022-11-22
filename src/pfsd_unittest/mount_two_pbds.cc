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


#include <sys/types.h>
#include <string>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "pfsd_sdk.h"
#include "pfsd_shm.h"

#define READ_SIZE (1 * 1024)

int test_file(const char *cluster, int host_id, std::string pbd_path, std::string pbd) {
    int r = 0, fd = pfsd_open((pbd_path + "hello.txt").data(), O_RDWR|O_CREAT, 0);
    printf("hello.txt: open fd %d\n", fd);
    if (fd < 0) {
        printf("hello.txt: open failed %d\n", errno);
	return -1;
    }

    ssize_t wbytes = pfsd_pwrite(fd, "abcdefghijklmnopqrstuvwxyz", 26, 0);
    printf("hello.txt: write %ld errno %d\n", wbytes, errno);

    char buf[READ_SIZE] = "";
    ssize_t bytes = pfsd_read(fd, buf, READ_SIZE);
    if (bytes > 0)
        printf("read %.*s\n", int(bytes), buf);
    else
        printf("read error %d, %d\n", int(bytes), errno);
    return fd;
}

int main(int argc, char* argv[]) {
    const char* cluster = NULL;
    std::string pbd1, pbd1_path;
    std::string pbd2, pbd2_path;

    if (argc < 4) {
	printf("usage: %s cluster pbd1 pbd2\n", argv[0]);
	return 1;
    }
    cluster = argv[1];
    pbd1 = argv[2];
    pbd2 = argv[3];

    pbd1_path = "/" + pbd1 + "/";
    pbd2_path = "/" + pbd2 + "/";

    int flags = PFS_RDWR | MNTFLG_PAXOS_BYFORCE;
    int host_id = 1;

    pfsd_set_mode(PFSD_SDK_THREADS);

    printf("mounting %s\n", pbd1.c_str());
    int r = pfsd_mount(cluster, pbd1.c_str(), host_id, flags);
    if (r != 0) {
        printf("mount failed : %s %d\n", pbd1.c_str(), errno);
        return -1;
    }
    printf("result %d\n", r);

    printf("mounting %s\n", pbd2.c_str());
    r = pfsd_mount(cluster, pbd2.c_str(), host_id, flags);
    if (r != 0) {
        printf("mount failed : %s %d\n", pbd2.c_str(), errno);
        return -1;
    }
    printf("mounting %s\n", pbd2.c_str());

    int fd1 = test_file(cluster, host_id, pbd1_path, pbd1);
    int fd2 = test_file(cluster, host_id, pbd2_path, pbd2);

    pfsd_umount(pbd1.c_str());
    pfsd_umount(pbd2.c_str());
    ssize_t wbytes = pfsd_pwrite(fd1, "abcdefghijklmnopqrstuvwxyz", 26, 0);
    printf("write after umount: %ld, errno:%d\n", wbytes, errno);
    pfsd_close(fd1);
    pfsd_close(fd2);


    return 0;
}

