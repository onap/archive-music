FROM openjdk:8
LABEL purpose="Springboot for MUSIC"
RUN apt update && apt install -y netcat telnet vim vim-common
RUN groupadd --gid 1000 music && useradd --gid 1000 --uid 1000 music
RUN mkdir -p /opt/app/music 
COPY MUSIC.jar /opt/app/music
RUN mkdir -p /opt/app/music/logs && \
    chown -R music:music /opt/app/music/
USER music:music
WORKDIR /opt/app/music
CMD ["/bin/bash","-c","java -jar MUSIC.jar >> /opt/app/music/logs/music-sb.log 2>&1"]
