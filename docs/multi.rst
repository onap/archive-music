===========================
Multi-site or Local Cluster
===========================
Follow the instructions for local MUSIC installation on all the machines/VMs/hosts (referred to as a node) on which you want MUSIC installed. However, Cassandra and Zookeeper needs to be configured to run as multi-node installations (instructions below) before running them.

Cassandra:
----------
In the cassandra.yaml file which is present in the cassa_install/conf directory in each node, set the following parameters:
cassandra.yaml::

    cluster_name: ‘name of cluster’
    #...
    num_tokens: 256
    #...
    seed_provider:
     - class_name: org.apache.cassandra.locator.SimpleSeedProvider
     parameters:
      - seeds:  "<public ip of first seed>, <public ip of second seed>, etc"
    #...
    listen_address: private ip of VM
    #...
    broadcast_address: public ip of VM
    #...
    endpoint_snitch: GossipingPropertyFileSnitch
    #...
    rpc_address: <private ip>
    #...
    phi_convict_threshold: 12

- In the cassandra-rackdc.properties file, assign data center and rack names as needed if required ( This is for multi data center install).
- Once this is done on all three nodes, you can run cassandra on each of the nodes through the cassandra bin folder with this command::     

    ./cassandra

- In the cassandra bin folder, if you run the following it will tell you the state of the cluster::       

    ./nodetool status

- To access cassandra, one any of the nodes you can run the following and then perform CQL queries.::

    ./cqlsh <private ip>

Extra Cassandra information for Authentication:
-----------------------------------------------
To create first user in Cassandra

1. Edit conf/Cassandra.yaml file::

    authenticator: PasswordAuthenticator
    authorizer: CassandraAuthorizer


2. Restart Cassandra
3. Login to cqlsh with default credentials::

    cqlsh -u cassandra -p cassandra

4. To change default user create new user with the following command.::

    CREATE USER new_user WITH PASSWORD ‘new_password’ SUPERUSER;

5. Change password for default user ‘Cassandra’ so that no one will be able to login::
   
    ALTER USER cassandra WITH PASSWORD ‘SomeLongRandomStringNoonewillthinkof’;

6. Provide the new user credentials to Music. Update music.properties file and uncomment or add the following::

    cassandra.user=<new_user>
    cassandra.password=<new_password>

To access keyspace through cqlsh, login with credentials that are passed to MUSIC while creating the keyspace.



MUSIC
Create a music.properties file and place it in /opt/app/music/etc at each node. Here is a sample of the file: 
cassandra.yaml::

    my.id=0
    all.ids=0
    my.public.ip=localhost
    all.public.ips=localhost
    #######################################
    # Optional current values are defaults
    #######################################
    # If using docker this would point to the specific docker name.
    #zookeeper.host=localhost
    #cassandra.host=localhost
    #music.ip=localhost
    #debug=true
    #music.rest.ip=localhost
    #lock.lease.period=6000
    # Cassandra Login - Do not user cassandra/cassandra
    cassandra.user=cassandra1
    cassandra.password=cassandra1
    # AAF Endpoint
    #aaf.endpoint.url=<aaf url>

- Build the MUSIC.war (see `Build Music`_) and place it within the webapps folder of the tomcat installation.
- Start tomcat and you should now have MUSIC running.

For Logging create a dir /opt/app/music/logs. When MUSIC/Tomcat starts a MUSIC dir with various logs will be created.

Build Music
^^^^^^^^^^^
Documentation will be updated to show that. Code can be downloaded from Music Gerrit. 
To build you will need to ensure you update your settings with the ONAP settings.xml 
(Workspace and Development Tools)

Once you have done that run the following:

::

    # If you installed settings.xml in your ./m2 folder
    mvn clean package
    # If you placed the settings.xml elsewhere:
    mvn clean package -s /path/to/settings.xml

After it is built you will find the MUSIC.war in the ./target folder. 

There is a folder called postman that contains a postman collection for testing with postman. 

Continue with `Authentication <./automation.rst>`_
  