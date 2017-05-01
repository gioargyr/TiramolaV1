#!/bin/bash

#
#   Copyright 2008-2009 LinkedIn, Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

base_dir=$(dirname $0)/..

$base_dir/bin/run-class.sh -Dlog4j.configuration=file:/opt/voldemort-0.81/config/euca_config/config/log4j.properties -Dlog4j.debug voldemort.client.rebalance.RebalanceCLI $@