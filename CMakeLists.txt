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

cmake_minimum_required (VERSION 3.13)
project(pfsd LANGUAGES CXX C VERSION 2.1.5)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(CMakeLists-config)

set(CURVE_LIB_DIR /usr/lib/curve2 CACHE STRING "curve lib dir")
set(PKG_DIR "${CMAKE_INSTALL_PREFIX}/polarstore/pfsd")

list(APPEND CMAKE_INSTALL_RPATH ${CURVE_LIB_DIR})
list(APPEND CMAKE_INSTALL_RPATH ${PKG_DIR}/lib)

set(COMMON_FLAGS
 "-fno-strict-aliasing \
  -Wall \
  -Wextra \
  -Werror \
  -Wno-deprecated-declarations \
  -Wno-format-truncation \
  -Wno-unused-parameter \
  -Wno-missing-field-initializers"
)

set(CXX_FLAGS
 "-Wno-sign-compare \
  -Wno-unused-function"
)

string(APPEND CMAKE_C_FLAGS " ${COMMON_FLAGS}")
string(APPEND CMAKE_CXX_FLAGS " ${COMMON_FLAGS} ${CXX_FLAGS}")
set(CMAKE_CXX_STANDARD 11)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

execute_process(COMMAND mkdir -p ${PROJECT_SOURCE_DIR}/include)
execute_process(COMMAND cp ${PROJECT_SOURCE_DIR}/src/pfs_sdk/pfsd_cli.h ${PROJECT_SOURCE_DIR}/include/)

add_subdirectory(src)

set(CPACK_PACKAGE_NAME pfsd)
set(CPACK_PACKAGE_VENDOR "Netease")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Polar filesystem")
set(CPACK_PACKAGE_INSTALL_DIRECTORY pfsd)
set(CPACK_PACKAGE_VERSION_MAJOR ${pfsd_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${pfsd_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${pfsd_VERSION_PATCH})
set(CPACK_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}-${RAW_GIT_DESC}-${pfsd_VERSION_MAJOR}.${pfsd_VERSION_MINOR}.${pfsd_VERSION_PATCH}-${CMAKE_SYSTEM_PROCESSOR}")
set(CPACK_VERBATIM_VARIABLES TRUE)
set(CPACK_MONOLITHIC_INSTALL TRUE)
set(CPACK_PACKAGE_CONTACT "yfxu@")
set(CPACK_GENERATOR "RPM;DEB")
set(CPACK_RPM_PACKAGE_AUTOREQ FALSE)
file(GLOB pfsd_HEADERS
     ${PROJECT_SOURCE_DIR}/src/pfs_core/pfs_api.h
     ${PROJECT_SOURCE_DIR}/src/pfs_core/pfs_trace_func.h
     ${PROJECT_SOURCE_DIR}/src/pfs_core/pfs_option_api.h
     ${PROJECT_SOURCE_DIR}/src/pfsd/pfsd_api.h
     ${PROJECT_SOURCE_DIR}/src/pfsd/pfsd_option.h
     ${PROJECT_SOURCE_DIR}/src/pfs_sdk/pfsd_cli.h
)

file(GLOB deploy_FILES
     "${PROJECT_SOURCE_DIR}/deploy_scripts/*"
)

install(
  FILES
	${pfsd_HEADERS}
  DESTINATION "${PKG_DIR}/include")
install(
  FILES
	conf/pfsd_logger.conf
  DESTINATION "${PKG_DIR}/conf")
install(
  FILES
	${deploy_FILES}
  DESTINATION "${PKG_DIR}/bin"
  PERMISSIONS  OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
)
  
install(
  FILES
	pfs_init
  DESTINATION "/etc/init.d/"
  RENAME pfs_init
)

install(
  FILES
	etc/polarfs.conf
  DESTINATION "/etc/"
)

set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA
    "${CMAKE_CURRENT_SOURCE_DIR}/package/deb/preinst;${CMAKE_CURRENT_SOURCE_DIR}/package/deb/postrm")
set(CPACK_RPM_PRE_INSTALL_SCRIPT_FILE "${CMAKE_CURRENT_SOURCE_DIR}/package/rpm/preinst")
set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE "${CMAKE_CURRENT_SOURCE_DIR}/package/rpm/postinst")
include(CPack)
