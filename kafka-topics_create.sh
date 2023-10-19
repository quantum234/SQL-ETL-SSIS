#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#display help usage
if [ "$1" == "-h" ] 
then
    echo "Usage: 'basename $0' number-of-partitions number-of-replica-factor retention-days zookeepernodes"
    exit 0
fi

#check if input arguments number match to 4
ARGS=4 # Script requires 4 arguments
E_BADARGS=128 # Wrong number of arguments passed to script

if [ $# -ne "$ARGS" ]
then
	echo "Usage: 'basename $0' number-of-partitions number-of-replica-factor retention-days zookeepernodes"
	exit $E_BADARGS
fi


PartitionsNumber=$1
ReplicationFactor=$2
RetentionMs=$(($3*24*60*60*1000))
ZooKeeperServer=$4

# Input arguments validation 
# Check the Number of Partitions is a positive integer or not
if [[ $PartitionsNumber =~ ^[\-0-9]+$ ]] && (( $PartitionsNumber > 0)) 
then
	echo "Number of Partitions: $PartitionsNumber"
else
	echo "Number of Partitions $PartitionsNumber is not a positive integer"
	exit $E_BADARGS
fi

# Check the Number of Replication Factor is a positive integer or not
if [[ $ReplicationFactor =~ ^[\-0-9]+$ ]] && (( $ReplicationFactor > 0)) 
then
	echo "Number of Replication Factor: $ReplicationFactor"
else
	echo "Number of Replication Factor $ReplicationFactor is not a positive integer"
	exit $E_BADARGS
fi

# Check the Retention Time Milliseconds is a positive integer or not
if [[ $RetentionMs =~ ^[\-0-9]+$ ]] && (( $RetentionMs > 0)) 
then
	echo "Retention Time Milliseconds: $RetentionMs"
else
	echo "Retention Time Milliseconds $RetentionMs is not a positive integer"
	exit $E_BADARGS
fi

echo "ZooKeeper Nodes: $ZooKeeperServer"


# every '&&' means: choose to run the second command only if the first exited successfully
kafka-topics --create --if-not-exists --topic ops-event-offerServed --partitions $PartitionsNumber --replication-factor $ReplicationFactor --config retention.ms=$RetentionMs  --zookeeper $ZooKeeperServer &&

kafka-topics --create --if-not-exists --topic ops-event-clientEvent --partitions $PartitionsNumber --replication-factor $ReplicationFactor --config retention.ms=$RetentionMs --zookeeper $ZooKeeperServer &&

kafka-topics --create --if-not-exists --topic ops-metrics-count --partitions $PartitionsNumber --replication-factor $ReplicationFactor --config retention.ms=86400000 --zookeeper $ZooKeeperServer &&

kafka-topics --create --if-not-exists --topic ops-cmd-dataReplay --partitions 1 --replication-factor 1 --config retention.ms=86400000 --zookeeper $ZooKeeperServer &&

kafka-topics --create --if-not-exists --topic ops-event-dataReplayStatus --partitions 1 --replication-factor 1 --config retention.ms=86400000 --zookeeper $ZooKeeperServer 

# check if the last above command exit successfully (0) or not (1)
if [ $? -eq 0 ]
then
	echo "Topics Created Successfully"
	exit 0
else
	echo "Error During Topic Creation"
	exit 1
fi
