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

# Type of Garbage Collector to use
JVM_GC_TYPE="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC"

# Tuning options for the above garbage collector
JVM_GC_OPTS="-XX:CMSInitiatingOccupancyFraction=70"

# JVM GC activity logging settings ($LOG_DIR set in the ctl script)
JVM_GC_LOG="-XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/var/log/gc.log"

if [ $# -gt 1 ];
then
	echo 'USAGE: bin/voldemort-server.sh [voldemort_home]'
	exit 1
fi

base_dir=$(dirname $0)/..

for file in $base_dir/dist/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/contrib/hadoop-store-builder/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

CLASSPATH=$CLASSPATH:$base_dir/dist/resources

if [ -z "$VOLD_OPTS" ]; then
  VOLD_OPTS="-Xmx6G -server -Dcom.sun.management.jmxremote"
fi

java -Dlog4j.configuration=file:/opt/voldemort-0.81/src/java/log4j.properties -Dlog4j.debug $VOLD_OPTS $JVM_GC_TYPE $JVM_GC_OPTS $JVM_GC_LOG -cp $CLASSPATH voldemort.server.VoldemortServer $@