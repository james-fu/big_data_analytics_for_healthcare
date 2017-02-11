#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Setup rng-tools to improve virtual machine entropy performance.
# The poor entropy performance will cause kerberos provisioning failed.
su -c 'rpm -Uvh http://download.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-5.noarch.rpm'
yum -y --nogpg install rng-tools haveged
sudo /usr/sbin/haveged --run=0
sudo /usr/sbin/rngd --no-tpm=1 --rng-device=/dev/urandom

# Install puppet modules
puppet apply /bootcamp/docker/puppet-modules.pp

mkdir -p /data/{1,2}

sysctl kernel.hostname=`hostname -f`

# Unmount device /etc/hosts and replace it by a shared hosts file
echo -e "`hostname -i`\t`hostname -f`" >> /bootcamp/docker/hosts
umount /etc/hosts
mv /etc/hosts /etc/hosts.bak
ln -s /bootcamp/docker/hosts /etc/hosts

# Prepare puppet configuration file
mkdir -p /etc/puppet/hieradata
cp /bootcamp/docker/puppet/hiera.yaml /etc/puppet
cp -r /bootcamp/docker/puppet/hieradata/bigtop/ /etc/puppet/hieradata/
cat > /etc/puppet/hieradata/site.yaml << EOF
bigtop::hadoop_head_node: $1
hadoop::hadoop_storage_dirs: [/data/1, /data/2]
bigtop::bigtop_repo_uri: $2
hadoop_cluster_node::cluster_components: $3
bigtop::jdk_package_name: $4
EOF

cat > /etc/profile.d/hadoop.sh << EOF
export HADOOP_CONF_DIR=/etc/hadoop/conf/
export HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce/
export HIVE_HOME=/usr/lib/hive/
export PIG_HOME=/usr/lib/pig/
export FLUME_HOME=/usr/lib/flume/
export SQOOP_HOME=/usr/lib/sqoop/
export HIVE_CONF_DIR=/etc/hive/conf/
export JAVA_HOME=/usr/lib/jvm/java-openjdk/
EOF


