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
    ${PROJECT_SOURCE_DIR}/src/pfsd/
    ${PROJECT_SOURCE_DIR}/src/pfs_sdk/
    ${PROJECT_SOURCE_DIR}/src/pfs_core/
    ${PROJECT_SOURCE_DIR}/src/trace/include/
)

link_directories(
    ${LIBRARY_OUTPUT_PATH}
)

SET(SRC_LIST_PFSD_CLI ../pfsd/pfsd_common.cc
                      ../pfsd/pfsd_chnl.cc
                      ../pfsd/pfsd_chnl_shm.cc
                      ../pfsd/pfsd_memory.cc
                      ../pfsd/pfsd_shm.cc
                      pfsd_sdk_file.cc
                      pfsd_sdk_mount.cc
                      pfsd_sdk.cc
                      pfsd_sdk_log.cc
    )
# objects
add_library(tmp_object_lib OBJECT ${SRC_LIST_PFSD_CLI})
target_compile_definitions(tmp_object_lib PUBLIC PFSD_CLIENT)
set_property(TARGET tmp_object_lib PROPERTY POSITION_INDEPENDENT_CODE ON)

# lib pfsd_cli
add_library(pfsd_cli STATIC
    $<TARGET_OBJECTS:tmp_object_lib>
)
add_dependencies(pfsd_cli
    tmp_object_lib
)

# lib pfsd_cli
add_library(pfsd_cli_test SHARED
    $<TARGET_OBJECTS:tmp_object_lib>
)
add_dependencies(pfsd_cli_test
    tmp_object_lib
)
set_property(TARGET pfsd_cli PROPERTY POSITION_INDEPENDENT_CODE ON)
set_property(TARGET pfsd_cli_test PROPERTY POSITION_INDEPENDENT_CODE ON)

target_link_libraries(pfsd_cli; rt;pthread;dl)

# pfsd shm tool
add_executable(pfsd_shm_tool
    pfsd_shm_tool.cc
    )

add_dependencies(pfsd_shm_tool
    pfsd_cli
)

target_link_libraries(pfsd_shm_tool pfsd_cli rt pthread)

SET_TARGET_PROPERTIES(pfsd_cli PROPERTIES LINKER_LANGUAGE CXX)
SET_TARGET_PROPERTIES(pfsd_shm_tool PROPERTIES LINKER_LANGUAGE CXX)

install(TARGETS pfsd_cli ARCHIVE DESTINATION "${PKG_DIR}/lib")
install(TARGETS pfsd_cli_test LIBRARY DESTINATION "${PKG_DIR}/lib")
install(TARGETS pfsd_shm_tool DESTINATION "${PKG_DIR}/bin")

