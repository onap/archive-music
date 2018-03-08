### Docker Setup for Single instance of MUSIC

<p>Please update the <b>properties/music.properties</b> file to fit your env.<br/>
Update the start.sh file.<br/>
The beginning of the <b>start.sh</b> file contains various variables.<br/></p>

CASS_IMG - Cassandra Image<br/>
TOMCAT_IMG - Tomcat Image<br/>
ZK_IMG - Zookeeper Image<br/>
MUSIC_IMG - Music Image containing the MUSIC war file.<br/>
WORK_DIR -  Default to PWD.<br/>
CASS_USERNAME - Username for Cassandra - should match cassandra.user in music.properties 
file<br/>
CASS_PASSWORD - Password for Cassandra - should match cassandra.password in music.properties.<br/>

MUSIC Logs will be saved in logs/MUSIC after start of tomcat.<br/> 

