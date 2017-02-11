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

# Install puppet agent
yum -y install http://yum.puppetlabs.com/puppetlabs-release-el-6.noarch.rpm
yum -y install curl nss puppet sudo unzip

# Setup rng-tools to improve virtual machine entropy performance.
# The poor entropy performance will cause kerberos provisioning failed.
yum -y install rng-tools
sed -i.bak 's/EXTRAOPTIONS=\"\"/EXTRAOPTIONS=\"-r \/dev\/urandom\"/' /etc/sysconfig/rngd
service rngd start

# Install puppet modules
puppet apply /bootcamp/docker/puppet-modules.pp

# Install Scala
wget http://www.scala-lang.org/files/archive/scala-2.10.5.tgz
tar xvf scala-2.10.5.tgz
mv scala-2.10.5 /usr/lib
ln -s /usr/lib/scala-2.10.5 /usr/lib/scala
ln -s /usr/lib/scala/bin/scala /usr/local/bin/scala
echo export GROOVY_HOME=/usr/lib/bigtop-groovy >> .bashrc

mkdir -p /data/{1,2}
