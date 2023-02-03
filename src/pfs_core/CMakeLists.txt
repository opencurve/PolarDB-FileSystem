# Copyright (c) 2023, NetEase Inc.
# Copyright (c) 2017-2021, Alibaba Group Holding Limited
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(pfs SHARED
    devio_disk.cc
    devio_curve.cc
    pfs_admin.cc
    pfs_alloc.cc
    pfs_api.cc
    pfs_blkio.cc
    pfs_command.cc
    pfs_config.cc
    pfs_devio.cc
    pfs_devstat.cc
    pfs_dir.cc
    pfs_file.cc
    pfs_inode.cc
    pfs_log.cc
    pfs_memory.cc
    pfs_meta.cc
    pfs_mount.cc
    pfs_namecache.cc
    pfs_namei.cc
    pfs_option.cc
    pfs_paxos.cc
    pfs_stat.cc
    pfs_stat_file_type.cc
    pfs_tls.cc
    pfs_trace.cc
    pfs_tx.cc
    pfs_util.cc
    pfs_version.cc
    pfs_chunk.cc
    pfs_avl.cc
)

target_include_directories(pfs
    PUBLIC
      ${CMAKE_CURRENT_SOURCE_DIR}
      ${CMAKE_CURRENT_SOURCE_DIR}/lib
      ${CMAKE_CURRENT_SOURCE_DIR}/../trace/include
)

set_target_properties(pfs PROPERTIES
    VERSION 2.1.5
    SOVERSION 2
)

target_link_directories(pfs PRIVATE ${CURVE_LIB_DIR})

target_link_libraries(pfs
   PRIVATE
      pthread
      aio
      rt
      curve_sdk
)
target_link_options(pfs PRIVATE "-Wl,--no-as-needed")

install(TARGETS pfs LIBRARY DESTINATION "${PKG_DIR}/lib")
