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
    ${PROJECT_SOURCE_DIR}/src/trace/include/
)

add_executable(
	pfs_unittest
	pfs_unittest.cc
	pfs_filetest.cc
	pfs_testenv.cc
)

add_definitions(-D__STDC_FORMAT_MACROS)

set(CXXFLAGS
    -Werror=no-literal-suffix
    -g
    -O0
)

target_link_libraries(pfs_unittest
    pfs
    gtest
    gflags
    pthread
)
