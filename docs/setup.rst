Setup for Developing MUSIC
==========================

.. toctree::
   :maxdepth: 1

   Single-Site Install <single>
   Multi-Site Install <multi>
   Authentication <authentication>

MUSIC is to be installed in a single Dir on a vm. 


The main MUSIC dir should be::

    /opt/app/music
    # These also need to be set up
    /opt/app/music/etc
    /opt/app/music/logs

When installing, Cassandra should also be installed here.::

    /opt/app/music/apache-cassandra-n.n.n


You could also create links from install dirs to a common name ie\:::

    ln -s /opt/app/music/apache-cassandra-n.n.n cassandra

Cassandra has data dirs.::
    
    # For cassandra it should be (This is the default) 
    /opt/app/music/cassandra/data    


Continue by selecting the link to the setup you are doing.

.. toctree::
   :maxdepth: 1

   Single-Site Install <single>
   Multi-Site Install <multi>
   Authentication <authentication>
