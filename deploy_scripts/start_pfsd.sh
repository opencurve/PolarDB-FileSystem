#! /bin/bash

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

mkdir -p /var/run/pfs
mkdir -p /var/run/pfsd
mkdir -p /dev/shm/pfsd

chmod 777 /var/run/pfs
chmod 777 /var/run/pfsd
chmod 777 /dev/shm/pfsd

BASE_DIR=$(cd "$(dirname "$0")"; pwd)
CONF_FILE=$BASE_DIR/../conf

# check if pfdameon exist
pbdname=$2
exist_command="ps -ef | grep pfsdaemon |grep -w '$pbdname' | wc -l"
exist=$(eval $exist_command)
if [ $exist -ge 1 ]; then
    echo "$pbdname already exist"
    exit 1
fi

ulimit -c unlimited

nohup ${BASE_DIR}/../bin/pfsdaemon $* -c ${CONF_FILE}/pfsd_logger.conf -f 1>/dev/null 2>&1 &

sleep 1

# check if start success 
exist=$(eval $exist_command)
if [ $exist -eq 0 ]; then
    echo "pfsdaemon $pfsdname start failed"
    exit 1
fi

echo "pfsdaemon $pbdname start success"
