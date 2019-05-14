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
