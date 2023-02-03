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

include_directories(
    ${CMAKE_SOURCE_DIR}/src/pfsd/
    ${CMAKE_SOURCE_DIR}/src/pfs_sdk/
    ${CMAKE_SOURCE_DIR}/src/pfs_core/
    ${CMAKE_SOURCE_DIR}/src/trace/include/
)

add_library(pfsd_cli SHARED
    ../pfsd/pfsd_common.cc
    ../pfsd/pfsd_chnl.cc
    ../pfsd/pfsd_chnl_shm.cc
    ../pfsd/pfsd_memory.cc
    ../pfsd/pfsd_shm.cc
    pfsd_sdk_file.cc
    pfsd_sdk_mount.cc
    pfsd_sdk.cc
    pfsd_sdk_log.cc
)

target_compile_definitions(pfsd_cli PUBLIC PFSD_CLIENT)
target_link_libraries(pfsd_cli rt pthread)

# pfsd shm tool
add_executable(pfsd_shm_tool
    pfsd_shm_tool.cc
)
target_link_libraries(pfsd_shm_tool pfsd_cli)

install(TARGETS pfsd_cli ARCHIVE DESTINATION "${PKG_DIR}/lib")
install(TARGETS pfsd_shm_tool DESTINATION "${PKG_DIR}/bin")

