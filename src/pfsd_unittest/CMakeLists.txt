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
    ${PROJECT_SOURCE_DIR}/src/pfs_tools
    ${PROJECT_SOURCE_DIR}/src/pfs_core/
	${PROJECT_SOURCE_DIR}/src/pfsd/
	${PROJECT_SOURCE_DIR}/src/pfs_sdk/
    ${PROJECT_SOURCE_DIR}/src/trace/include/
)

link_directories(
    ${LIBRARY_OUTPUT_PATH}
)

add_executable(
	pfsd_unittest
	pfsd_unittest.cc
	pfsd_filetest.cc
	pfsd_testenv.cc
)

add_definitions(-D__STDC_FORMAT_MACROS)

set(CXXFLAGS
    -Werror=no-literal-suffix
    -g
    -O0
)

if(${CMAKE_HOST_SYSTEM_PROCESSOR} STREQUAL "x86_64")
    set(CXXFLAGS ${CXXFLAGS}  -march=native)
endif()

target_link_libraries(pfsd_unittest
    gtest
    -Wl,--start-group
    -Wl,--no-as-needed
    pfs
    pfsd
    pthread
    -Wl,--end-group
)

add_dependencies(pfs-tools
    pfs
    pfsd_cli
)

add_executable(
	sdk_demo
	sdk_demo.cc
)

target_link_libraries(sdk_demo
    gtest
    -Wl,--start-group
    -Wl,--no-as-needed
    pfs
    pfsd_cli
    pthread
    -Wl,--end-group
)

add_executable(
	mount_two_pbds
	mount_two_pbds.cc
)
target_link_libraries(mount_two_pbds
    gtest
    -Wl,--start-group
    -Wl,--no-as-needed
    pfs
    pfsd_cli
    pthread
    -Wl,--end-group
)



