package org.onap.music.unittests;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.CassaLockStore;
import org.onap.music.main.MusicCore;

import com.datastax.driver.core.ResultSet;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Ignore
public class TestMusicCore {
	
	static PreparedQueryObject testObject;
    static MusicDataStore dataStore;
    String keyspace = "MusicCoreUnitTestKp";
    String table = "SampleTable";

    @BeforeClass
    public static void init() {
    	System.out.println("TestMusicCore Init");
        try {
            MusicDataStoreHandle.mDstoreHandle = CassandraCQL.connectToEmbeddedCassandra();
            MusicCore.mLockHandle = new CassaLockStore(MusicDataStoreHandle.mDstoreHandle);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @AfterClass
    public static void close() throws MusicServiceException, MusicQueryException {
    	System.out.println("After class TestMusicCore");
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(CassandraCQL.dropKeyspace);
        MusicCore.eventualPut(testObject);
        MusicDataStoreHandle.mDstoreHandle.close();
    }

    @Test
    public void Test1_createKeyspace() throws MusicServiceException, MusicQueryException {

        Map<String,Object> replicationInfo = new HashMap<String, Object>();
        replicationInfo.put("'class'", "'SimpleStrategy'");
        replicationInfo.put("'replication_factor'", 1);
        
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(
          "CREATE KEYSPACE " + keyspace + " WITH REPLICATION = " + replicationInfo.toString().replaceAll("=", ":"));
        MusicCore.nonKeyRelatedPut(queryObject, "eventual");
        
        
        //check with the system table in cassandra
        queryObject = new PreparedQueryObject();
        String systemQuery = "SELECT keyspace_name FROM system_schema.keyspaces where keyspace_name='"+keyspace.toLowerCase()+"';";
        queryObject.appendQueryString(systemQuery);
        ResultSet rs = dataStore.executeOneConsistencyGet(queryObject);
        assert rs.all().size()> 0;
    }
    
    @Test
    public void Test1_createTable() throws MusicServiceException, MusicQueryException {
    		PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(
          "CREATE TABLE " + keyspace + "." + table + " (name text PRIMARY KEY, count varint);");
        MusicCore.createTable(keyspace, table, queryObject, "eventual");              

        
        //check with the system table in cassandra
        queryObject = new PreparedQueryObject();
        String systemQuery = "SELECT table_name FROM system_schema.tables where keyspace_name='"+keyspace.toLowerCase()+"' and table_name='"+table.toLowerCase()+"';";
        queryObject.appendQueryString(systemQuery);
        ResultSet rs = dataStore.executeOneConsistencyGet(queryObject);
        assert rs.all().size()> 0;
    }


}
