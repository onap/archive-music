#
# -------------------------------------------------------------------------
#   Copyright (c) 2017 AT&T Intellectual Property
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
# -------------------------------------------------------------------------
# In this example we are building a docker bridge network(music-net) for all
# the containers
# Then we connect the host bridge network(bridge) to the internal network(music-net) 
# 
#
#
CASS_IMG=nexus3.onap.org:10001/onap/music/cassandra_music:latest
TOMCAT_IMG=nexus3.onap.org:10001/library/tomcat:8.0
ZK_IMG=nexus3.onap.org:10001/library/zookeeper:3.4
MUSIC_IMG=nexus3.onap.org:10001/onap/music/music:latest
WORK_DIR=${PWD}
CASS_USERNAME=cassandra1
CASS_PASSWORD=cassandra1

if [ "$1" = "start" ]; then

# Create Volume for mapping war file and tomcat
docker volume create music-vol;

# Create a network for all the containers to run in.
docker network create music-net;

# Start Cassandra
docker run -d --rm --name music-db --network music-net \
-p "7000:7000" -p "7001:7001" -p "7199:7199" -p "9042:9042" -p "9160:9160" \
-e CASSUSER=${CASS_USERNAME} \
-e CASSPASS=${CASS_PASSWORD} \
${CASS_IMG};

# Start Music war
docker run -d --rm --name music-war \
-v music-vol:/app \
${MUSIC_IMG};

# Start Zookeeper
docker run -d --rm --name music-zk --network music-net 
-p "2181:2181" -p "2888:2888" -p "3888:3888" \
${ZK_IMG};

# Delay for Cassandra
sleep 20;

# Start Up tomcat - Needs to have properties,logs dir and war file volume mapped.
docker run -d --rm --name music-tomcat --network music-net -p "8080:8080" \
-v music-vol:/usr/local/tomcat/webapps \
-v ${WORK_DIR}/properties:/opt/app/music/etc:ro \
-v ${WORK_DIR}/logs:/opt/app/music/logs \
${TOMCAT_IMG};

# Connect tomcat to host bridge network so that its port can be seen. 
docker network connect bridge music-tomcat;

fi


# Shutdown and clean up. 
if [ "$1" = "stop" ]; then
docker stop music-war;
docker stop music-db;
docker stop music-zk;
docker stop music-tomcat;
docker network rm music-net;
sleep 5;
docker volume rm music-vol;
fi
