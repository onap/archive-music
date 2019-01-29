if [ "${1}" ]; then

docker build -t dockercentral.it.att.com:5100/com.att.music/music-sb:${1} .
docker tag dockercentral.it.att.com:5100/com.att.music/music-sb:${1} dockercentral.it.att.com:5100/com.att.music/music-sb:latest
docker push dockercentral.it.att.com:5100/com.att.music/music-sb:latest
docker push dockercentral.it.att.com:5100/com.att.music/music-sb:${1}
else
echo "Missing version"

fi
