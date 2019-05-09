#!/bin/bash
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

echo "Running startup script to get password from certman"
PWFILE=/opt/app/aafcertman/.password
LOGFILE=/opt/app/music/logs/MUSIC/music-sb.log
echo "PWFILE=${PWFILE}" >> $LOGFILE
if [ -f $PWFILE ]; then
echo "Found ${PWFILE}" >> $LOGFILE
PASSWORD=$(cat ${PWFILE})
echo "#### Using Password from ${PWFILE} for Certs" >> ${LOGFILE}
else
PASSWORD=changeit
echo "#### Using Default Password for Certs" >> ${LOGFILE}
fi

java -jar MUSIC.jar --server.ssl.key-store-password="${PASSWORD}" --aaf_password="enc:${PASSWORD}" 2>&1 | tee ${LOGFILE}
