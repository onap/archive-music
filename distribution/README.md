# Docker Setup
---------------

```bash
# Start the Container
docker run -d --name cassandra_music -v $PWD/data:/var/lib/cassandra cassandra_music:3.0
# Load cql script into DB
docker run -it --link cassandra1:cassandra -v $PWD/music.cql:/music.cql cassandra_music1:3.0 cqlsh -u cassandra -p cassandra cassandra1 -f music.cql
# Start cqlsh
docker run -it --link cassandra_music:cassandra cassandra_music:3.0 cqlsh -u cassandra -p cassandra cassandra1

```
