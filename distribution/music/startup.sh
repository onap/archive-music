#!/bin/bash

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
