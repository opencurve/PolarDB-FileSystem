# Copyright (c) 2023, Netease Inc.
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

add_library(pfsd_server SHARED
    pfsd.cc
    pfsd_svr.cc
    pfsd_chnl.cc
    pfsd_chnl_shm.cc
    pfsd_common.cc
    pfsd_memory.cc
    pfsd_option.cc
    pfsd_shm.cc
    pfsd_worker.cc
)

# Set public definitions used by ourself and pfsdaemon
target_compile_definitions(pfsd_server PUBLIC PFSD_SERVER)

# Include current source directory, other directories are inherited
# from libpfs target
target_include_directories(pfsd_server PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(pfsd_server PUBLIC pfs pthread)

add_executable(pfsdaemon
    pfsd_main.cc
)

target_link_libraries(pfsdaemon
    pfsd_server
    libzlog.a
    pthread
)

install(TARGETS pfsdaemon DESTINATION "${PKG_DIR}/bin")
install(TARGETS pfsd_server LIBRARY DESTINATION "${PKG_DIR}/lib")

