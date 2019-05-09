#
# ============LICENSE_START==========================================
# org.onap.music
# ===================================================================
#  Copyright (c) 2019 AT&T Intellectual Property
# ===================================================================
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
# ============LICENSE_END=============================================
# ====================================================================

FROM openjdk:8
LABEL purpose="Springboot for MUSIC"
RUN apt update && apt install -y netcat telnet vim vim-common
RUN groupadd --gid 1000 music && useradd --gid 1000 --uid 1000 music
RUN mkdir -p /opt/app/music/logs/MUSIC 
COPY MUSIC.jar /opt/app/music
COPY startup.sh /opt/app/music
RUN mkdir -p /opt/app/music/logs && \
    chown -R music:music /opt/app/music/ && \
    chmod 755 /opt/app/music/startup.sh
USER music:music
WORKDIR /opt/app/music
CMD ["/opt/app/music/startup.sh"]
