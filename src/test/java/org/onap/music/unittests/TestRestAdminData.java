/*
 * ============LICENSE_START========================================== 
 * org.onap.music
 * =================================================================== 
 * Copyright (c) 2017 AT&T * Intellectual Property 
 * ===================================================================
 * Modifications Copyright (c) 2019 IBM
 * ===================================================================
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 * 
 * ============LICENSE_END=============================================
 * ====================================================================
 */

package org.onap.music.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
//import static org.onap.music.main.MusicCore.mLockHandle;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mindrot.jbcrypt.BCrypt;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import org.onap.music.conductor.conditionals.JsonConditional;
import org.onap.music.conductor.conditionals.RestMusicConditionalAPI;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.CassaKeyspaceObject;
import org.onap.music.datastore.jsonobjects.CassaTableObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonKeySpace;
import org.onap.music.datastore.jsonobjects.JsonLeasedLock;
import org.onap.music.datastore.jsonobjects.JsonOnboard;
import org.onap.music.datastore.jsonobjects.JsonSelect;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.zookeeper.MusicLockingService;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.rest.RestMusicAdminAPI;
import org.onap.music.rest.RestMusicBmAPI;
import org.onap.music.rest.RestMusicDataAPI;
import org.onap.music.rest.RestMusicHealthCheckAPI;
import org.onap.music.rest.RestMusicLocksAPI;
import org.onap.music.rest.RestMusicTestAPI;
import org.onap.music.rest.RestMusicVersionAPI;
import org.onap.music.service.impl.MusicZKCore;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.sun.jersey.core.util.Base64;
import com.sun.jersey.core.util.MultivaluedMapImpl;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(SpringJUnit4ClassRunner.class)
public class TestRestAdminData {

    RestMusicDataAPI data = new RestMusicDataAPI();
    RestMusicAdminAPI admin = new RestMusicAdminAPI();
    RestMusicLocksAPI lock = new RestMusicLocksAPI(); 
    static PreparedQueryObject testObject;
    static TestingServer zkServer;

    @Mock
    HttpServletResponse http;

    @Mock
    UriInfo info;

    //* cjc out 

    
    @InjectMocks
      private MusicCore mCore;
    
    static MusicLockingService mLockHandle;
    //*/
    
    static String appName = "TestApp";
    static String userId = "TestUser";
    static String password = "TestPassword";
    static String adminName = "username";
    static String adminPassword = "password";
    static String adminAuthData = adminName +":"+adminPassword;
    static String wrongAdminAuthData = adminName+"123"+":"+adminPassword;
    static String authData = userId+":"+password;
    static String wrongAuthData = userId+":"+"pass";
    static String authorization = new String(Base64.encode(authData.getBytes()));
    static String wrongAuthorization = new String(Base64.encode(wrongAuthData.getBytes()));
    static String adminAuthorization = new String(Base64.encode(adminAuthData.getBytes()));
    static String worngAdminAuthorization = new String(Base64.encode(wrongAdminAuthData.getBytes()));
    
    static boolean isAAF = false;
    static UUID uuid = UUID.fromString("abc66ccc-d857-4e90-b1e5-df98a3d40ce6");
    static String keyspaceName = "testCassa";
    static String tableName = "employees";
    static String tableNameConditional = "Conductor";
    static String xLatestVersion = "X-latestVersion";
    static String onboardUUID = null;
    static String lockId = null;
    static String lockName = "testCassa.employees.sample3";

    @BeforeClass
    public static void init() throws Exception {
       try {
            MusicDataStoreHandle.mDstoreHandle = CassandraCQL.connectToEmbeddedCassandra();
            zkServer = new TestingServer(2181, new File("/tmp/zk"));
            mLockHandle = MusicZKCore.getLockingServiceHandle();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("DROP KEYSPACE IF EXISTS " + keyspaceName);
        MusicCore.eventualPut(testObject);
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("DROP KEYSPACE IF EXISTS admin");
        MusicCore.eventualPut(testObject);
        MusicDataStoreHandle.mDstoreHandle.close();
        mLockHandle.close();
        zkServer.stop();
    }

    @Test
    public void Test1_createKeyspace() throws Exception {
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("CREATE KEYSPACE admin WITH REPLICATION = "
                        + "{'class' : 'SimpleStrategy' , "
                        + "'replication_factor': 1} AND DURABLE_WRITES = true");
        MusicCore.eventualPut(testObject);
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(
                        "CREATE TABLE admin.keyspace_master (" + "  uuid uuid, keyspace_name text,"
                                        + "  application_name text, is_api boolean,"
                                        + "  password text, username text,"
                                        + "  is_aaf boolean, PRIMARY KEY (uuid)\n" + ");");
        MusicCore.eventualPut(testObject);

        testObject = new PreparedQueryObject();
        testObject.appendQueryString(
                        "INSERT INTO admin.keyspace_master (uuid, keyspace_name, application_name, is_api, "
                                        + "password, username, is_aaf) VALUES (?,?,?,?,?,?,?)");
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(),
                        MusicUtil.DEFAULTKEYSPACENAME));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), "True"));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), BCrypt.hashpw(password, BCrypt.gensalt())));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));
        MusicCore.eventualPut(testObject);

        testObject = new PreparedQueryObject();
        testObject.appendQueryString(
                        "INSERT INTO admin.keyspace_master (uuid, keyspace_name, application_name, is_api, "
                                        + "password, username, is_aaf) VALUES (?,?,?,?,?,?,?)");
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.uuid(),
                        UUID.fromString("bbc66ccc-d857-4e90-b1e5-df98a3d40de6")));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(),
                        MusicUtil.DEFAULTKEYSPACENAME));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), "TestApp1"));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), "True"));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), BCrypt.hashpw(password, BCrypt.gensalt())));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), "TestUser1"));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));
        MusicCore.eventualPut(testObject);

        testObject = new PreparedQueryObject();
        testObject.appendQueryString(
                        "select uuid from admin.keyspace_master where application_name = ? allow filtering");
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        ResultSet rs = MusicCore.get(testObject);
        List<Row> rows = rs.all();
        if (rows.size() > 0) {
            System.out.println("#######UUID is:" + rows.get(0).getUUID("uuid"));
        }
        PreparedQueryObject pQuery = new PreparedQueryObject();
        String consistency = MusicUtil.EVENTUAL;
        pQuery.appendQueryString("CREATE TABLE IF NOT EXISTS admin.locks ( lock_id text PRIMARY KEY, ctime text)");
        try {
            ResultType result = MusicCore.nonKeyRelatedPut(pQuery, consistency);
        } catch (MusicServiceException e1) {
            e1.printStackTrace();
        }
    }

    @Test
    public void Test2_createKeyspace() throws Exception {
        CassaKeyspaceObject jsonKeyspace = new CassaKeyspaceObject();
        Map<String, Object> replicationInfo = new HashMap<>();
        replicationInfo.put("class", "SimpleStrategy");
        replicationInfo.put("replication_factor", 1);
        jsonKeyspace.setDurabilityOfWrites(true);
        jsonKeyspace.setKeyspaceName(keyspaceName);
        jsonKeyspace.setReplicationInfo(replicationInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createKeySpace("1", "1", "1", null,authorization, appName,  jsonKeyspace, keyspaceName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(200,response.getStatus());
        //assertEquals(400,response.getStatus());
    }

    @Test
    public void Test2_createKeyspace_wrongConsistency() throws Exception {
        CassaKeyspaceObject jsonKeyspace = new CassaKeyspaceObject();
        Map<String, Object> replicationInfo = new HashMap<>();
        replicationInfo.put("class", "SimpleStrategy");
        replicationInfo.put("replication_factor", 1);
        jsonKeyspace.setDurabilityOfWrites(true);
        jsonKeyspace.setKeyspaceName(keyspaceName);
        jsonKeyspace.setReplicationInfo(replicationInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createKeySpace("1", "1", "1", null,authorization, appName,  jsonKeyspace, keyspaceName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(400,response.getStatus());
    }

    @Test
    public void Test2_createKeyspace1() throws Exception {
        CassaKeyspaceObject jsonKeyspace = new CassaKeyspaceObject();
        Map<String, Object> replicationInfo = new HashMap<>();
        replicationInfo.put("class", "SimpleStrategy");
        replicationInfo.put("replication_factor", 1);
        jsonKeyspace.setDurabilityOfWrites(true);
        jsonKeyspace.setKeyspaceName(keyspaceName);
        jsonKeyspace.setReplicationInfo(replicationInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createKeySpace("1", "1", "1", null,authorization, appName,  jsonKeyspace, "keyspaceName");
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(200,response.getStatus());
        //assertEquals(400,response.getStatus());
    }

    @Test
    public void Test2_createKeyspace0() throws Exception {
        CassaKeyspaceObject jsonKeyspace = new CassaKeyspaceObject();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> replicationInfo = new HashMap<>();
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createKeySpace("1", "1", "1", null, authorization,appName, jsonKeyspace, keyspaceName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(400,response.getStatus());
    }
//MusicCore.autheticateUser
    @Test
    public void Test2_createKeyspace01() throws Exception {
        CassaKeyspaceObject jsonKeyspace = new CassaKeyspaceObject();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> replicationInfo = new HashMap<>();
        String appName1 = "test";
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createKeySpace("1", "1", "1", null,authorization, appName1, jsonKeyspace, keyspaceName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(401,response.getStatus());
        //assertEquals(400,response.getStatus());
    }

    @Test
    public void Test3_createKeyspace1() throws Exception {
        CassaKeyspaceObject jsonKeyspace = new CassaKeyspaceObject();
        Map<String, Object> replicationInfo = new HashMap<>();
        replicationInfo.put("class", "SimpleStrategy");
        replicationInfo.put("replication_factor", 1);
        jsonKeyspace.setDurabilityOfWrites(true);
        jsonKeyspace.setKeyspaceName("TestApp1");
        jsonKeyspace.setReplicationInfo(replicationInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createKeySpace("1", "1", "1", null,authorization, "TestApp1",
                 jsonKeyspace, keyspaceName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(401,response.getStatus());
        //assertEquals(400,response.getStatus());
    }

    @Test
    public void Test2_createKeyspaceEmptyAuth() throws Exception {
  
        //MockitoAnnotations.initMocks(this);
        CassaKeyspaceObject jsonKeyspace = new CassaKeyspaceObject();
        Map<String, Object> replicationInfo = new HashMap<>();
        replicationInfo.put("class", "SimpleStrategy");
        replicationInfo.put("replication_factor", 1);
        jsonKeyspace.setDurabilityOfWrites(true);
        jsonKeyspace.setKeyspaceName(keyspaceName);
        jsonKeyspace.setReplicationInfo(replicationInfo);
        //Map<String, Object> m1= new HashMap<>() ;
        //Mockito.when(CachingUtil.verifyOnboarding("x","y","x")).thenReturn(m1);
        //Mockito.when(CachingUtil.verifyOnboarding(appNamex,userId,password).thenReturn(m1));
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        String authDatax = ":"+password;
        String authorizationx = new String(Base64.encode(authDatax.getBytes()));
        try {
        Response response = data.createKeySpace("1", "1", "1", null,authorizationx, appName,  jsonKeyspace, keyspaceName);
        //System.out.println("#######status is " + response.getStatus());
        //System.out.println("Entity" + response.getEntity());
        //assertNotEquals(200,response.getStatus());
        } catch (RuntimeException e ) {
          System.out.println("#######status is runtime exception= " + e);
        }
    }
    
    @Test
    public void Test3_createTable() throws Exception {
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableName);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",appName, authorization, 
                        jsonTable, keyspaceName, tableName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(200, response.getStatus());
        //assertEquals(401, response.getStatus());
    }

    @Test
    public void Test3_createTable_wrongKeyspace() throws Exception {
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name)");
        jsonTable.setKeyspaceName("keyspaceName12");
        jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableName);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",appName, authorization, 
                        jsonTable, keyspaceName, tableName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(400, response.getStatus());
        //assertEquals(401, response.getStatus());
    }

    @Test
    public void Test3_createTableClusterOrderBad() throws Exception {
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name,emp_salary)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name,emp_salary");
        jsonTable.setClusteringOrder("ASC");
        jsonTable.setTableName(tableName);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",appName, authorization, 
                        jsonTable, keyspaceName, tableName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertNotEquals(200, response.getStatus());
    }
     
    @Test
    public void Test3_createTable_withPropertiesNotNull() throws Exception {
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name)");
        Map<String, Object> properties = new HashMap<>();
        properties.put("comment","Testing prperties not null");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        String tableName_prop=tableName+"_Prop";
        jsonTable.setTableName(tableName_prop);
        jsonTable.setFields(fields);
        jsonTable.setProperties(properties);
        
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",appName, authorization, 
                        jsonTable, keyspaceName, tableName_prop);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(200, response.getStatus());
        //assertEquals(401, response.getStatus());
    }
    
    @Test
    public void Test3_createTable_duplicateTable() throws Exception {
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        String tableNameDup=tableName+"X";
        jsonTable.setTableName(tableNameDup);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, keyspaceName, tableNameDup);
        System.out.println("#######status for 1st time " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        
        Response response0 = data.createTable("1", "1", "1",
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                jsonTable, keyspaceName, tableNameDup);
        // 400 is the duplicate status found in response
        // Music 113 duplicate testing 
        //import static org.junit.Assert.assertNotEquals;
        System.out.println("#######status for 2nd time " + response0.getStatus());
        System.out.println("Entity" + response0.getEntity());
        
        assertFalse("Duplicate table not created for "+tableNameDup, 200==response0.getStatus());

    }

    // Improper Auth
    @Test
    public void Test3_createTable1() throws Exception {
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableName);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, wrongAuthorization,
                        jsonTable, keyspaceName, tableName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(401, response.getStatus());
    }

    // Improper keyspace
    @Test
    public void Test3_createTable3() throws Exception {
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableName);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, "wrong", tableName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(401, response.getStatus());
    }
    
    @Test
    public void Test3_createTable3_with_samePartition_clusteringKeys() throws Exception {
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name, emp_name)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPartitionKey("emp_name");
        jsonTable.setClusteringKey("emp_name");
        jsonTable.setTableName(tableName);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, keyspaceName, tableName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(400, response.getStatus());
        
    }

    @Test
    public void Test3_createTable3_with_Partition_clusteringKeys() throws Exception {
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPartitionKey("emp_name");
        jsonTable.setClusteringKey("uuid");
        jsonTable.setTableName(tableName);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, keyspaceName, "tableName1");
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(200, response.getStatus());
        //assertEquals(401, response.getStatus());
    }

    // Improper parenthesis in key field
    @Test
    public void Test3_createTable_badParantesis() throws Exception {
        String tableNameC ="testTable0";
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name),emp_id)");
        fields.put("emp_id", "varint");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_id Desc");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        //assertEquals(400, response.getStatus());
        assertTrue(200 != response.getStatus());
    }
    

    // good clustering key
    @Test
    public void Test3_createTable_1_clusterKey_good() throws Exception {
        String tableNameC ="testTableC1";
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "((emp_name),emp_salary)");
        jsonTable.setKeyspaceName(keyspaceName);
       // jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_salary ASC");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(200, response.getStatus());
        //assertEquals(401, response.getStatus());
    }

    // bad partition key=clustering key
    @Test
    public void Test3_createTable_2_clusterKey_bad() throws Exception {
        String tableNameC ="testTableC2";
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "((emp_name),emp_name)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");  // "PRIMARY KEY" overrides if primaryKey present
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_salary ASC");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertTrue(200 !=response.getStatus());
    }

    // good composite partition key,clustering key
    @Test
    public void Test3_createTable_3_partition_clusterKey_good() throws Exception {
        String tableNameC ="testTableC3";
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "varint");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "((emp_name,emp_id),emp_salary)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_salary ASC");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(200, response.getStatus());
        //assertEquals(401, response.getStatus());
    }

    // bad - not all cols in order by of composite partition key,clustering key
    @Test
    public void Test3_createTable_4_clusteringOrder_bad() throws Exception {
        String tableNameC ="testTableC4";
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "varint");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "((emp_name),emp_id,emp_salary)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_salary ASC");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertTrue(200 != response.getStatus());
    }

    // bad - wrong cols in order by of composite partition key,clustering key
    @Test
    public void Test3_createTable_5_clusteringOrder_bad() throws Exception {
        String tableNameC ="testTableC5";
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "varint");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "((uuid,emp_name),emp_id,emp_salary)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_idx desc, emp_salary ASC");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertTrue(200 != response.getStatus());
    }
    
    // bad - wrong cols in order by of composite partition key,clustering key
    @Test
    public void Test3_createTable_6_clusteringOrder_bad() throws Exception {
        String tableNameC ="testTableC6";
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "varint");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "((uuid,emp_name),emp_id,emp_salary)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("((uuid,emp_name),emp_id,emp_salary)"); // overridden by
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_id desc, emp_salary ASC,uuid desc");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertTrue(200 != response.getStatus());
    }


    @Test
    public void Test3_createTableIndex_1() throws Exception {
        String tableNameC ="testTableCinx";
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "((emp_name),emp_salary)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_salary ASC");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        MultivaluedMap<String, String> rowParams = Mockito.mock(MultivaluedMap.class);
        Mockito.when(info.getQueryParameters()).thenReturn(rowParams);
        Mockito.when(rowParams.getFirst("index_name")).thenReturn("my_index");
        response = data.createIndex("1", "1", "1",
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                 keyspaceName, tableNameC,"uuid",info);
        assertEquals(200, response.getStatus());
        //assertEquals(400, response.getStatus());
    }

    @Test
    public void Test3_createTableIndex_authorizationWrong() throws Exception {
        String tableNameC ="testTableCinx";
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "((emp_name),emp_salary)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_salary ASC");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, null,
                        jsonTable, keyspaceName, tableNameC);
        MultivaluedMap<String, String> rowParams = Mockito.mock(MultivaluedMap.class);
        Mockito.when(info.getQueryParameters()).thenReturn(rowParams);
        Mockito.when(rowParams.getFirst("index_name")).thenReturn("my_index");
        response = data.createIndex("1", "1", "1",
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, wrongAuthorization,
                 keyspaceName, tableNameC,"uuid",info);
        assertEquals(401, response.getStatus());
    }

    @Test
    public void Test3_createTableIndex_badindexname() throws Exception {
        String tableNameC ="testTableCinx";
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "((emp_name),emp_salary)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_salary ASC");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        MultivaluedMap<String, String> rowParams = Mockito.mock(MultivaluedMap.class);
        Mockito.when(info.getQueryParameters()).thenReturn(rowParams);
        Mockito.when(rowParams.getFirst("index_name")).thenReturn("my index");
        response = data.createIndex("1", "1", "1",
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                 keyspaceName, tableNameC,"uuid",info);
        assertEquals(400, response.getStatus());
        //assertEquals(401, response.getStatus());
    }

    @Test
    public void Test3_createTableIndex_wrongindex() throws Exception {
        String tableNameC ="testTableCinx";
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "((emp_name),emp_salary)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_salary ASC");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        MultivaluedMap<String, String> rowParams = Mockito.mock(MultivaluedMap.class);
        Mockito.when(info.getQueryParameters()).thenReturn(rowParams);
        Mockito.when(rowParams.getFirst("index_name")).thenReturn("my_index");
        response = data.createIndex("1", "1", "1",
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                 keyspaceName, tableNameC,"id",info);
        assertEquals(400, response.getStatus());
        //assertEquals(401, response.getStatus());
    }

    /*
     * @Test public void Test4_insertIntoTable() throws Exception { JsonInsert
     * jsonInsert = new JsonInsert(); Map<String, String> consistencyInfo = new
     * HashMap<>(); Map<String, Object> values = new HashMap<>(); values.put("uuid",
     * "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6"); values.put("emp_name", "testName");
     * values.put("emp_salary", 500); consistencyInfo.put("type", "eventual");
     * jsonInsert.setConsistencyInfo(consistencyInfo);
     * jsonInsert.setKeyspaceName(keyspaceName); jsonInsert.setTableName(tableName);
     * jsonInsert.setValues(values);
     * Mockito.doNothing().when(http).addHeader(xLatestVersion,
     * MusicUtil.getVersion()); Response response = data.insertIntoTable("1", "1",
     * "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
     * jsonInsert, keyspaceName, tableName); assertEquals(200,
     * response.getStatus()); }
     */

    @Ignore
    public void Test4_insertIntoTable_wrongConsistency() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "testName");
        values.put("emp_salary", 500);
        consistencyInfo.put("type", "eventual123");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.insertIntoTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, authorization, jsonInsert, keyspaceName, tableName);
        assertEquals(400, response.getStatus());
    }

    @Ignore
    public void Test4_insertIntoTable2() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "test1");
        values.put("emp_salary", 1500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.insertIntoTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonInsert, keyspaceName, tableName);
        assertEquals(200, response.getStatus());
    }

    // Auth Error
   @Ignore
    public void Test4_insertIntoTable3() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "test1");
        values.put("emp_salary", 1500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.insertIntoTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, wrongAuthorization,
                        jsonInsert, keyspaceName, tableName);
        assertEquals(401, response.getStatus());
    }

    // Table wrong
    @Ignore
    public void Test4_insertIntoTable4() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "test1");
        values.put("emp_salary", 1500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.insertIntoTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonInsert, keyspaceName, "wrong");
        assertEquals(400, response.getStatus());
    }
    
    @Ignore
    public void Test4_insertIntoTable5() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("id", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "test1");
        values.put("emp_salary", 1500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.insertIntoTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonInsert, keyspaceName, tableName);
        assertEquals(400, response.getStatus());
    }

    @Ignore
    public void Test4_insertIntoTable6() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_salary", 1500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.insertIntoTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonInsert, keyspaceName, tableName);
        assertEquals(400, response.getStatus());
    }
    
    @Ignore
    public void Test4_insertIntoTable7() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "test2");
        values.put("emp_salary", 1500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        jsonInsert.setTtl("1000");
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.insertIntoTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonInsert, keyspaceName, tableName);
        assertEquals(200, response.getStatus());
    }

    @Ignore
    public void Test4_insertIntoTable8() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "test3");
        values.put("emp_salary", 1500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        jsonInsert.setTimestamp("15000");
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.insertIntoTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonInsert, keyspaceName, tableName);
        assertEquals(200, response.getStatus());
    }
    
    @Ignore
    public void Test4_insertIntoTable9() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "test4");
        values.put("emp_salary", 1500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        jsonInsert.setTtl("1000");
        jsonInsert.setTimestamp("15000");
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.insertIntoTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonInsert, keyspaceName, tableName);
        assertEquals(200, response.getStatus());
    }

    /*
     * @Test public void Test4_insertIntoTable10() throws Exception { JsonInsert
     * jsonInsert = new JsonInsert(); Map<String, String> consistencyInfo = new
     * HashMap<>(); Map<String, Object> values = new HashMap<>(); values.put("uuid",
     * "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6"); values.put("emp_name", "test5");
     * values.put("emp_salary", 1500); consistencyInfo.put("type", "critical");
     * jsonInsert.setConsistencyInfo(consistencyInfo);
     * jsonInsert.setKeyspaceName(keyspaceName); jsonInsert.setTableName(tableName);
     * jsonInsert.setValues(values); jsonInsert.setTtl("1000");
     * jsonInsert.setTimestamp("15000");
     * Mockito.doNothing().when(http).addHeader(xLatestVersion,
     * MusicUtil.getVersion()); Response response = data.insertIntoTable("1", "1",
     * "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
     * jsonInsert, keyspaceName, tableName); assertEquals(400,
     * response.getStatus()); }
     */

    /*
     * @Test public void Test4_insertIntoTable11() throws Exception { JsonInsert
     * jsonInsert = new JsonInsert(); Map<String, String> consistencyInfo = new
     * HashMap<>(); Map<String, Object> values = new HashMap<>(); values.put("uuid",
     * "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6"); values.put("emp_name", "test6");
     * values.put("emp_salary", 1500); consistencyInfo.put("type",
     * "atomic_delete_lock"); jsonInsert.setConsistencyInfo(consistencyInfo);
     * jsonInsert.setKeyspaceName(keyspaceName); jsonInsert.setTableName(tableName);
     * jsonInsert.setValues(values); jsonInsert.setTtl("1000");
     * jsonInsert.setTimestamp("15000");
     * Mockito.doNothing().when(http).addHeader(xLatestVersion,
     * MusicUtil.getVersion()); Response response = data.insertIntoTable("1", "1",
     * "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
     * jsonInsert, keyspaceName, tableName); //TODO 200 assertEquals(400,
     * response.getStatus()); }
     */

   @Ignore
    public void Test4_insertIntoTable12() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "test7");
        values.put("emp_salary", 1500);
        consistencyInfo.put("type", "atomic");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        jsonInsert.setTtl("1000");
        jsonInsert.setTimestamp("15000");
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.insertIntoTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonInsert, keyspaceName, tableName);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test5_updateTable() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("emp_name", "testName");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "atomic");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(200, response.getStatus());
        //assertEquals(401, response.getStatus());
    }

    @Test
    public void Test5_updateTable_wrongTablename() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("emp_name", "testName");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "atomic");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName("tableName123");
        jsonUpdate.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, "tableName123", info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test5_updateTable_wrongConsistency() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("emp_name", "testName");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "eventual123");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    // need mock code to create error for MusicCore methods
    @Test
    public void Test5_updateTableAuthE() throws Exception {
      MockitoAnnotations.initMocks(this);
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("emp_name", "testName");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "atomic");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(200, response.getStatus());
        //assertEquals(401, response.getStatus());
    }

    @Test
    public void Test5_updateTableAuthException1() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("emp_name", "testName");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "atomic");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        String authDatax = ":";//+password;
        String authorizationx = new String(Base64.encode(authDatax.getBytes()));
        try {
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorizationx, jsonUpdate, keyspaceName, tableName, info);
              assertEquals(200, response.getStatus());
        } catch(RuntimeException e) {
           System.out.println("Update table Runtime exception="+e);
        }
    }

    @Test
    public void Test5_updateTableAuthEmpty() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("emp_name", "testName");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "atomic");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        String authDatax =":"+password;
        String authorizationx = new String(Base64.encode(authDatax.getBytes()));
        String appNamex="xx";
        try {
            // Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
               Response response = data.updateTable("1", "1", "1", "", appNamex,
                authorizationx, jsonUpdate, keyspaceName, tableName, info);
              assertEquals(200, response.getStatus());
        } catch(RuntimeException e) {
           System.out.println("Update table Runtime exception="+e);
        }
    }

    @Test
    public void Test5_updateTable_wrongauth() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("emp_name", "testName");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "atomic");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                wrongAuthorization, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(401, response.getStatus());
    }

    @Test
    public void Test5_updateTable_invalidColumn() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("id", "testName");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "atomic");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test5_updateTable_ttl() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("emp_name", "testName8");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "atomic");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        jsonUpdate.setTtl("1000");
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test5_updateTable_timsetamp() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("emp_name", "testName9");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "atomic");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        jsonUpdate.setTimestamp("15000");
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test5_updateTable_ttl_timestamp() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("emp_name", "testName10");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "atomic");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        jsonUpdate.setTtl("1000");
        jsonUpdate.setTimestamp("15000");
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test5_updateTable_rowIdEmpty() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        //row.add("emp_name", "testName3");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "atomic");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        jsonUpdate.setTtl("1000");
        jsonUpdate.setTimestamp("15000");
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test5_updateTable_conditions() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        Map<String, Object> conditions =  new HashMap<>();
        conditions.put("emp_name","testName3");
        row.add("emp_name", "testName3");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "atomic");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        jsonUpdate.setConditions(conditions);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test5_updateTable_eventual() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("emp_name", "testName");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "eventual");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test5_updateTable_critical() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("emp_name", "testName");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "critical");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test5_updateTable_atomic_delete_lock() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("emp_name", "testName");
        values.put("emp_salary", 2500);
        consistencyInfo.put("type", "atomic_delete_lock");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test6_select() throws Exception {
        JsonSelect jsonSelect = new JsonSelect();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testName");
        consistencyInfo.put("type", "atomic");
        jsonSelect.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.select("1", "1", "1","abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                        appName, authorization, keyspaceName, tableName, info);
        HashMap<String,HashMap<String,Object>> map = (HashMap<String, HashMap<String, Object>>) response.getEntity();
        HashMap<String, Object> result = map.get("result");
        //assertEquals("2500", ((HashMap<String,Object>) result.get("row 0")).get("emp_salary").toString());
    }

    @Test
    public void Test6_select_withException() throws Exception {
        JsonSelect jsonSelect = new JsonSelect();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testName");
        consistencyInfo.put("type", "atomic");
        jsonSelect.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        RestMusicDataAPI spyData = Mockito.spy(RestMusicDataAPI.class);
        Mockito.doThrow(MusicServiceException.class).when(spyData).selectSpecificQuery("v2", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password, keyspaceName, tableName, info, -1);
        Response response = spyData.select("1", "1", "1","abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                        appName, authorization, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test6_select_nodata() throws Exception {
        JsonSelect jsonSelect = new JsonSelect();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testName12");
        consistencyInfo.put("type", "atomic");
        jsonSelect.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.select("1", "1", "1","abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                        appName, authorization, keyspaceName, tableName, info);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test6_selectCritical() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testName");
        consistencyInfo.put("type", "atomic");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.selectCritical("1", "1", "1","abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                        appName, authorization, jsonInsert, keyspaceName, tableName,info);
        HashMap<String,HashMap<String,Object>> map = (HashMap<String, HashMap<String, Object>>) response.getEntity();
        HashMap<String, Object> result = map.get("result");
        //assertEquals("2500", ((HashMap<String,Object>) result.get("row 0")).get("emp_salary").toString());
    }

    @Test
    public void Test6_selectCritical_wrongAuthorization() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testName");
        consistencyInfo.put("type", "atomic");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.selectCritical("1", "1", "1","abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                        appName, wrongAuthorization, jsonInsert, keyspaceName, tableName,info);
        /*HashMap<String,HashMap<String,Object>> map = (HashMap<String, HashMap<String, Object>>) response.getEntity();
        HashMap<String, Object> result = map.get("result");
        assertEquals("2500", ((HashMap<String,Object>) result.get("row 0")).get("emp_salary").toString());*/
        assertEquals(401, response.getStatus());
    }

    @Test
    public void Test6_selectCritical_without_lockID() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testName");
        consistencyInfo.put("type", "critical");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.selectCritical("1", "1", "1","abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                        appName, authorization, jsonInsert, keyspaceName, tableName,info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test6_selectCritical_with_atomic_delete_lock() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testName");
        consistencyInfo.put("type", "atomic_delete_lock");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.selectCritical("1", "1", "1","abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                        appName, authorization, jsonInsert, keyspaceName, tableName,info);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test6_selectCritical_with_nodata() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testName12");
        consistencyInfo.put("type", "atomic_delete_lock");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.selectCritical("1", "1", "1","abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                        appName, authorization, jsonInsert, keyspaceName, tableName,info);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test6_select_all() throws Exception {
        JsonSelect jsonSelect = new JsonSelect();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();;
        consistencyInfo.put("type", "atomic");
        jsonSelect.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.select("1", "1", "1","abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                        appName, authorization, keyspaceName, tableName, info);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test6_select_all_wrongAuthorization() throws Exception {
        JsonSelect jsonSelect = new JsonSelect();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();;
        consistencyInfo.put("type", "atomic");
        jsonSelect.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.select("1", "1", "1","abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                        appName, wrongAuthorization, keyspaceName, tableName, info);
        assertEquals(401, response.getStatus());
    }

    @Test
    public void Test6_deleteFromTable() throws Exception {
        JsonDelete jsonDelete = new JsonDelete();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "test1");
        consistencyInfo.put("type", "atomic");
        jsonDelete.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.deleteFromTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonDelete, keyspaceName, tableName, info);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test6_deleteFromTable_wrongAuthorization() throws Exception {
        JsonDelete jsonDelete = new JsonDelete();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "test1");
        consistencyInfo.put("type", "atomic");
        jsonDelete.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.deleteFromTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, wrongAuthorization,
                        jsonDelete, keyspaceName, tableName, info);
        assertEquals(401, response.getStatus());
    }

    // Values
    /*
     * @Test public void Test6_deleteFromTable1() throws Exception { JsonDelete
     * jsonDelete = new JsonDelete(); Map<String, String> consistencyInfo = new
     * HashMap<>(); MultivaluedMap<String, String> row = new MultivaluedMapImpl();
     * consistencyInfo.put("type", "atomic");
     * jsonDelete.setConsistencyInfo(consistencyInfo);
     * Mockito.doNothing().when(http).addHeader(xLatestVersion,
     * MusicUtil.getVersion());
     * Mockito.when(info.getQueryParameters()).thenReturn(row); Response response =
     * data.deleteFromTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
     * appName, authorization, jsonDelete, keyspaceName, tableName, info);
     * assertEquals(400, response.getStatus()); }
     */

    // delObj
    @Test
    public void Test6_deleteFromTable2() throws Exception {
        JsonDelete jsonDelete = new JsonDelete();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "test1");
        consistencyInfo.put("type", "atomic");
        jsonDelete.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.deleteFromTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        null, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test6_deleteFromTable_columns() throws Exception {
        JsonDelete jsonDelete = new JsonDelete();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "test1");
        consistencyInfo.put("type", "atomic");
        jsonDelete.setConsistencyInfo(consistencyInfo);
        ArrayList<String> columns = new ArrayList<>();
        columns.add("uuid");
        jsonDelete.setColumns(columns);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.deleteFromTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonDelete, keyspaceName, tableName, info);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test6_deleteFromTable_conditions() throws Exception {
        JsonDelete jsonDelete = new JsonDelete();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> conditions =  new HashMap<>();
        conditions.put("emp_name","testName3");
        row.add("emp_name", "test1");
        consistencyInfo.put("type", "atomic");
        jsonDelete.setConsistencyInfo(consistencyInfo);
        ArrayList<String> columns = new ArrayList<>();
        jsonDelete.setConditions(conditions);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.deleteFromTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonDelete, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test6_deleteFromTable_eventual() throws Exception {
        JsonDelete jsonDelete = new JsonDelete();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "test2");
        consistencyInfo.put("type", "eventual");
        jsonDelete.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.deleteFromTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonDelete, keyspaceName, tableName, info);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test6_deleteFromTable_wrongConsistency() throws Exception {
        JsonDelete jsonDelete = new JsonDelete();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "test2");
        consistencyInfo.put("type", "eventual123");
        jsonDelete.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.deleteFromTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonDelete, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test6_deleteFromTable_critical() throws Exception {
        JsonDelete jsonDelete = new JsonDelete();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "test2");
        consistencyInfo.put("type", "critical");
        jsonDelete.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.deleteFromTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonDelete, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test6_deleteFromTable_atomic_delete_lock() throws Exception {
        JsonDelete jsonDelete = new JsonDelete();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "test3");
        consistencyInfo.put("type", "atomic_delete_lock");
        jsonDelete.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.deleteFromTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonDelete, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test7_dropTable() throws Exception {
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        consistencyInfo.put("type", "atomic");
        jsonTable.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.dropTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                         keyspaceName, tableName);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test7_dropTable_wrongAuthorization() throws Exception {
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        consistencyInfo.put("type", "atomic");
        jsonTable.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.dropTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, wrongAuthorization,
                         keyspaceName, tableName);
        assertEquals(401, response.getStatus());
    }

    
    /*
     * @Test public void Test8_deleteKeyspace() throws Exception { JsonKeySpace
     * jsonKeyspace = new JsonKeySpace(); Map<String, String> consistencyInfo = new
     * HashMap<>(); Map<String, Object> replicationInfo = new HashMap<>();
     * consistencyInfo.put("type", "eventual"); replicationInfo.put("class",
     * "SimpleStrategy"); replicationInfo.put("replication_factor", 1);
     * jsonKeyspace.setConsistencyInfo(consistencyInfo);
     * jsonKeyspace.setDurabilityOfWrites(true);
     * jsonKeyspace.setKeyspaceName("TestApp1");
     * jsonKeyspace.setReplicationInfo(replicationInfo);
     * Mockito.doNothing().when(http).addHeader(xLatestVersion,
     * MusicUtil.getVersion()); Response response = data.dropKeySpace("1", "1", "1",
     * "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", authorization,appName, keyspaceName);
     * assertEquals(200, response.getStatus()); }
     */
    
    @Test
    public void Test8_deleteKeyspace1() throws Exception {
        CassaKeyspaceObject jsonKeyspace = new CassaKeyspaceObject();
        Map<String, Object> replicationInfo = new HashMap<>();
        replicationInfo.put("class", "SimpleStrategy");
        replicationInfo.put("replication_factor", 1);
        jsonKeyspace.setDurabilityOfWrites(true);
        jsonKeyspace.setKeyspaceName("TestApp1");
        jsonKeyspace.setReplicationInfo(replicationInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.dropKeySpace("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                authorization,appName,jsonKeyspace,"keyspaceName");
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test8_deleteKeyspace2() throws Exception {
        CassaKeyspaceObject jsonKeyspace = new CassaKeyspaceObject();
        Map<String, Object> replicationInfo = new HashMap<>();
        replicationInfo.put("class", "SimpleStrategy");
        replicationInfo.put("replication_factor", 1);
        jsonKeyspace.setDurabilityOfWrites(true);
        jsonKeyspace.setKeyspaceName("TestApp1");
        jsonKeyspace.setReplicationInfo(replicationInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.dropKeySpace("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                wrongAuthorization, appName, jsonKeyspace, keyspaceName);
        assertEquals(401, response.getStatus());
    }

    /*
     * @Test public void Test6_onboard() throws Exception { JsonOnboard jsonOnboard
     * = new JsonOnboard(); jsonOnboard.setAppname("TestApp2");
     * jsonOnboard.setIsAAF("false"); jsonOnboard.setUserId("TestUser2");
     * jsonOnboard.setPassword("TestPassword2");
     * 
     * @SuppressWarnings("unchecked") Map<String, Object> resultMap = (Map<String,
     * Object>)
     * admin.onboardAppWithMusic(jsonOnboard,adminAuthorization).getEntity();
     * resultMap.containsKey("success"); onboardUUID =
     * resultMap.get("Generated AID").toString();
     * assertEquals("Your application TestApp2 has been onboarded with MUSIC.",
     * resultMap.get("Success")); }
     */

    @Test
    public void Test6_onboard_duplicate() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser2");
        jsonOnboard.setPassword("TestPassword2");
        Response response = admin.onboardAppWithMusic(jsonOnboard,adminAuthorization);
        assertEquals(204, response.getStatus());
    }

    // Missing appname
    @Test
    public void Test6_onboard1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser2");
        jsonOnboard.setPassword("TestPassword2");
        Map<String, Object> resultMap = (Map<String, Object>) admin.onboardAppWithMusic(jsonOnboard,adminAuthorization).getEntity();
//        assertTrue(resultMap.containsKey("error"));
        //System.out.println("--->" + resultMap.toString());
        //assertEquals("Unauthorized: Please check the request parameters. Some of the required values appName(ns), userId, password, isAAF are missing.", resultMap.get("Exception"));
    }


    @Test
    public void Test7_onboardSearch() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization).getEntity();
        resultMap.containsKey("success");
        assertEquals(null, resultMap.get(onboardUUID));
    }

    // Missing appname
    @Test
    public void Test7_onboardSearch1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization).getEntity();
        System.out.println("--->" + resultMap.toString());
        resultMap.containsKey("success");
        assertEquals(null, resultMap.get(onboardUUID));
    }
    
    @Test
    public void Test7_onboardSearch_empty() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        Response response =  admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization);
      //  assertEquals(400, response.getStatus());
    }

    @Test
    public void Test7_onboardSearch_invalidAid() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid("abc66ccc-d857-4e90-b1e5-df98a3d40ce6");
        Response response = admin.getOnboardedInfoSearch(jsonOnboard,adminAuthorization);
       // assertEquals(400, response.getStatus());
    }

    @Test
    public void Test8_onboardUpdate() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser3");
        jsonOnboard.setPassword("TestPassword3");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.updateOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        System.out.println("--->" + resultMap.toString());
        resultMap.containsKey("success");
        assertNotNull(resultMap);
    }

    // Aid null
    @Test
    public void Test8_onboardUpdate1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser3");
        jsonOnboard.setPassword("TestPassword3");
        Map<String, Object> resultMap = (Map<String, Object>) admin.updateOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        System.out.println("--->" + resultMap.toString());
        assertNotNull(resultMap);
    }

    // Appname not null
    @Test
    public void Test8_onboardUpdate2() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser3");
        jsonOnboard.setPassword("TestPassword3");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.updateOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        assertNotNull(resultMap);
    }

    // All null
    @Test
    public void Test8_onboardUpdate3() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.updateOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        assertNotNull(resultMap);
    }

    @Test
    public void Test9_onboardDelete() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.deleteOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        resultMap.containsKey("success");
        assertNotNull(resultMap);
    }

    @Test
    public void Test9_onboardDelete1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        Map<String, Object> resultMap = (Map<String, Object>) admin.deleteOnboardApp(jsonOnboard,adminAuthorization).getEntity();
        assertNotNull(resultMap);
    }

    @Test
    public void Test3_createLockReference() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.createLockReference(lockName,"1","1",authorization, null, appName).getEntity();
       //TODO Success
        assertEquals(ResultType.FAILURE, resultMap.get("status"));
    }

    @Test
    public void Test3_createLockReference_invalidLock() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        if (lock.createLockReference("lockName","1","1",authorization, null, appName).getEntity() == null) {
            System.err.println("yo");
            System.exit(-1);
        }
        Map<String, Object> resultMap = (Map<String, Object>) lock.createLockReference("lockName","1","1",authorization, null, appName).getEntity();
        assertEquals(ResultType.FAILURE, resultMap.get("status"));
    }

    @Test
    public void Test3_createLockReference_invalidAuthorization() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.createLockReference(lockName,"1","1",wrongAuthorization, null, appName).getEntity();
        assertEquals(ResultType.FAILURE, resultMap.get("status"));
    }

    @Test
    public void Test4_accquireLock() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.accquireLock(lockName,"1","1",authorization, null, appName).getEntity();
        //TODO Sucess
        assertEquals(ResultType.FAILURE, resultMap.get("status"));
    }

    /*
     * @Test public void Test4_accquireLock_wrongAuthorization() throws Exception {
     * Mockito.doNothing().when(http).addHeader(xLatestVersion,
     * MusicUtil.getVersion()); Map<String, Object> resultMap = (Map<String,
     * Object>) lock.accquireLock(Mockito.anyString(),"1","1",wrongAuthorization,
     * null, appName).getEntity(); assertEquals(ResultType.FAILURE,
     * resultMap.get("status")); }
     */

    /*
     * @Test public void Test5_accquireLockwithLease() throws Exception {
     * Mockito.doNothing().when(http).addHeader(xLatestVersion,
     * MusicUtil.getVersion()); JsonLeasedLock leasedLock = new JsonLeasedLock();
     * leasedLock.setLeasePeriod(1000l); Map<String, Object> resultMap =
     * (Map<String, Object>)
     * lock.accquireLockWithLease(leasedLock,lockId,"1","1",authorization, null,
     * appName).getEntity(); assertEquals(ResultType.SUCCESS,
     * resultMap.get("status")); }
     */

    @Test
    public void Test5_accquireLockwithLease_invalidLock() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        JsonLeasedLock leasedLock = new JsonLeasedLock();
        leasedLock.setLeasePeriod(1000l);
        Map<String, Object> resultMap = (Map<String, Object>) lock.accquireLockWithLease(leasedLock,"lockId","1","1",authorization, null, appName).getEntity();
        assertEquals(ResultType.FAILURE, resultMap.get("status"));
    }

   
    /*
     * @Test public void Test5_currentLockHolder() throws Exception {
     * Mockito.doNothing().when(http).addHeader(xLatestVersion,
     * MusicUtil.getVersion()); Map<String, Object> resultMap = (Map<String,
     * Object>) lock.currentLockHolder(lockName,"1","1",authorization, null,
     * appName).getEntity(); assertEquals(ResultType.SUCCESS,
     * resultMap.get("status")); }
     */

    @Test
    public void Test5_currentLockHolder_invalidLock() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.currentLockHolder("lockName","1","1",authorization, null, appName).getEntity();
        assertEquals(ResultType.FAILURE, resultMap.get("status"));
    }

    @Test
    public void Test5_currentLockHolder_wrongAuthorization() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.currentLockHolder(lockName,"1","1",wrongAuthorization, null, appName).getEntity();
        assertEquals(ResultType.FAILURE, resultMap.get("status"));
    }

    @Ignore
    public void Test6_currentLockState() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.currentLockState(lockName,"1","1",authorization, null, appName).getEntity();
        //TODO Success
        assertEquals(ResultType.FAILURE, resultMap.get("status"));
    }

    @Test
    public void Test6_currentLockState_invalidLock() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.currentLockState("lockName","1","1",authorization, null, appName).getEntity();
        assertEquals(ResultType.FAILURE, resultMap.get("status"));
    }

    @Test
    public void Test6_currentLockState_wrongAuthorization() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.currentLockState(lockName,"1","1",wrongAuthorization, null, appName).getEntity();
        assertEquals(ResultType.FAILURE, resultMap.get("status"));
    }

    /*
     * @Test public void Test7_unLock() throws Exception {
     * Mockito.doNothing().when(http).addHeader(xLatestVersion,
     * MusicUtil.getVersion()); Map<String, Object> resultMap = (Map<String,
     * Object>) lock.unLock(lockId,"1","1",authorization, null,
     * appName).getEntity(); assertEquals(ResultType.SUCCESS,
     * resultMap.get("status")); }
     */

    /*
     * @Test public void Test7_unLock_invalidLock() throws Exception {
     * Mockito.doNothing().when(http).addHeader(xLatestVersion,
     * MusicUtil.getVersion()); Map<String, Object> resultMap = (Map<String,
     * Object>) lock.unLock("lockId","1","1",authorization, null,
     * appName).getEntity(); assertEquals(ResultType.FAILURE,
     * resultMap.get("status")); }
     */
    /*
     * @Test public void Test7_unLock_wrongAUthorization() throws Exception {
     * Mockito.doNothing().when(http).addHeader(xLatestVersion,
     * MusicUtil.getVersion()); Map<String, Object> resultMap = (Map<String,
     * Object>) lock.unLock(lockId,"1","1",wrongAuthorization, null,
     * appName).getEntity(); assertEquals(ResultType.FAILURE,
     * resultMap.get("status")); }
     */

    @Test
    public void Test8_delete() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.deleteLock(lockName,"1","1", null,authorization, appName).getEntity();
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
    }

    @Test
    public void Test8_delete_invalidLock() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.deleteLock("lockName","1","1", null,authorization, appName).getEntity();
        assertEquals(ResultType.FAILURE, resultMap.get("status"));
    }

    @Test
    public void Test8_delete_wrongAuthorization() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.deleteLock(lockName,"1","1", null,wrongAuthorization, appName).getEntity();
        assertEquals(ResultType.FAILURE, resultMap.get("status"));
    }
    // Version api
    @Test
    public void Test1_version( ) {
        RestMusicVersionAPI versionapi = new RestMusicVersionAPI();
        HttpServletResponse servletResponse = Mockito.mock(HttpServletResponse.class);
        Map<String, Object> resultMap = versionapi.version(servletResponse);
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
    }

    //Music Test Api
    @Test
    public void Test2_testAPI() {
        RestMusicTestAPI musicTest = new RestMusicTestAPI();
        HttpServletResponse servletResponse = Mockito.mock(HttpServletResponse.class);
        Map<String, HashMap<String, String>> resultMap = musicTest.simpleTests(servletResponse);
        assertNotNull(resultMap);
    }

    //Music Health Check
    @Test
    public void Test3_HealthCheck_cassandra() {
        String consistency = "ONE";
        RestMusicHealthCheckAPI healthCheck = new RestMusicHealthCheckAPI();
        HttpServletResponse servletResponse = Mockito.mock(HttpServletResponse.class);
        Response response = healthCheck.cassandraStatus(servletResponse, consistency);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test3_HealthCheck_cassandra_cosistencyQuorum() {
        String consistency = "QUORUM";
        RestMusicHealthCheckAPI healthCheck = new RestMusicHealthCheckAPI();
        HttpServletResponse servletResponse = Mockito.mock(HttpServletResponse.class);
        Response response = healthCheck.cassandraStatus(servletResponse, consistency);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test3_HealthCheck_zookeeper() {
        RestMusicHealthCheckAPI healthCheck = new RestMusicHealthCheckAPI();
        HttpServletResponse servletResponse = Mockito.mock(HttpServletResponse.class);
        Response response = healthCheck.ZKStatus(servletResponse);
        assertEquals(200, response.getStatus());
    }

    @Ignore
    public void Test4_pureZKcreate() throws Exception {
        RestMusicBmAPI bmApi = new RestMusicBmAPI();
        bmApi.pureZkCreate("sample");
    }

    @Ignore
    public void Test4_pureZKUpdate() throws Exception {
        RestMusicBmAPI bmApi = new RestMusicBmAPI();
        bmApi.pureZkCreate("sample1");
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "testName_create");
        values.put("emp_salary", 500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        bmApi.pureZkUpdate(jsonInsert, "sampleNode1");
    }

    @Ignore
    public void Test4_pureZKGet() throws Exception {
        RestMusicBmAPI bmApi = new RestMusicBmAPI();
        bmApi.pureZkGet("sample");
    }

    /*
     * @Test public void Test5_ZKAtomicPut_atomic() throws Exception {
     * RestMusicBmAPI bmApi = new RestMusicBmAPI(); JsonInsert jsonInsert = new
     * JsonInsert(); Map<String, String> consistencyInfo = new HashMap<>();
     * Map<String, Object> values = new HashMap<>(); values.put("uuid",
     * "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6"); values.put("emp_name",
     * "testName_create"); values.put("emp_salary", 1500);
     * consistencyInfo.put("type", "atomic");
     * jsonInsert.setConsistencyInfo(consistencyInfo);
     * jsonInsert.setKeyspaceName(keyspaceName); jsonInsert.setTableName(tableName);
     * jsonInsert.setValues(values); bmApi.pureZkAtomicPut(jsonInsert, lockName,
     * "sampleNode1"); }
     */
    /*
     * @Test public void Test5_ZKAtomicPut_atomic_with_delete() throws Exception {
     * RestMusicBmAPI bmApi = new RestMusicBmAPI(); JsonInsert jsonInsert = new
     * JsonInsert(); Map<String, String> consistencyInfo = new HashMap<>();
     * Map<String, Object> values = new HashMap<>(); values.put("uuid",
     * "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6"); values.put("emp_name",
     * "testName_create"); values.put("emp_salary", 1500);
     * consistencyInfo.put("type", "atomic_delete_lock");
     * jsonInsert.setConsistencyInfo(consistencyInfo);
     * jsonInsert.setKeyspaceName(keyspaceName); jsonInsert.setTableName(tableName);
     * jsonInsert.setValues(values); bmApi.pureZkAtomicPut(jsonInsert, lockName,
     * "sampleNode1"); }
     */

    /*
     * @Test public void Test5_ZKAtomicGet_atomic() throws Exception {
     * RestMusicBmAPI bmApi = new RestMusicBmAPI(); JsonInsert jsonInsert = new
     * JsonInsert(); Map<String, String> consistencyInfo = new HashMap<>();
     * Map<String, Object> values = new HashMap<>(); values.put("uuid",
     * "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6"); values.put("emp_name",
     * "testName_create"); values.put("emp_salary", 1500);
     * consistencyInfo.put("type", "atomic_delete_lock");
     * jsonInsert.setConsistencyInfo(consistencyInfo);
     * jsonInsert.setKeyspaceName(keyspaceName); jsonInsert.setTableName(tableName);
     * jsonInsert.setValues(values); bmApi.pureZkAtomicGet(jsonInsert, lockName,
     * "sampleNode1"); }
     */

    /*
     * @Test public void Test5_ZKAtomicGet_atomic_with_delete() throws Exception {
     * RestMusicBmAPI bmApi = new RestMusicBmAPI(); JsonInsert jsonInsert = new
     * JsonInsert(); Map<String, String> consistencyInfo = new HashMap<>();
     * Map<String, Object> values = new HashMap<>(); values.put("uuid",
     * "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6"); values.put("emp_name",
     * "testName_create"); values.put("emp_salary", 1500);
     * consistencyInfo.put("type", "atomic_delete_lock");
     * jsonInsert.setConsistencyInfo(consistencyInfo);
     * jsonInsert.setKeyspaceName(keyspaceName); jsonInsert.setTableName(tableName);
     * jsonInsert.setValues(values); bmApi.pureZkAtomicGet(jsonInsert, lockName,
     * "sampleNode1"); }
     */

    @Test
    public void Test5_updateCassa() throws Exception {
        RestMusicBmAPI bmApi = new RestMusicBmAPI();
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("emp_salary", 1500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testName_create");
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        //bmApi.updateTableCassa(jsonInsert, keyspaceName, tableName, info);
    }

    // RestMusicConditional
    @Test
    public void Test5_createTable_conditional() throws Exception {
        CassaTableObject jsonTable = new CassaTableObject();
        Map<String, String> fields = new HashMap<>();
        fields.put("id", "text");
        fields.put("plans", "Map<text,text>");
        fields.put("PRIMARY KEY", "(id)");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("id");
        jsonTable.setTableName(tableNameConditional);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",appName, authorization,
                        jsonTable, keyspaceName, tableNameConditional);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(200, response.getStatus());
        //assertEquals(401, response.getStatus());
    }

    @Test
    public void Test6_insertConditional() throws Exception {
        RestMusicConditionalAPI conditionalApi = new RestMusicConditionalAPI();
        JsonConditional json = new JsonConditional();
        json.setPrimaryKey("id");
        json.setPrimaryKeyValue("123|abc|port");
        json.setCasscadeColumnName("plans");
        Map<String, Object> tableValues =  new HashMap<>();
        tableValues.put("id", "123|abc|port");
        json.setTableValues(tableValues);
        Map<String, Object> columnData =  new HashMap<>();
        Map<String, String> column =  new HashMap<>();
        column.put("created", "time");
        columnData.put("key", "P2");
        columnData.put("value", column);
        json.setCasscadeColumnData(columnData);
        Map<String, String> cond = new HashMap<>();
        Map<String, String> cond1 = new HashMap<>();
        Map<String, Map<String, String>> conditions = new HashMap<String, Map<String, String>>();
        cond.put("status", "under-spin-up");
        cond1.put("status", "parked");
        conditions.put("exists", cond);
        conditions.put("nonexists", cond1);
        json.setConditions(conditions);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = conditionalApi.insertConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, authorization, keyspaceName, tableNameConditional, json);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test6_insertConditional_primaryKey_null() throws Exception {
        RestMusicConditionalAPI conditionalApi = new RestMusicConditionalAPI();
        JsonConditional json = new JsonConditional();
        json.setPrimaryKeyValue("123|abc|port");
        json.setCasscadeColumnName("plans");
        Map<String, Object> tableValues =  new HashMap<>();
        tableValues.put("id", "123|abc|port");
        json.setTableValues(tableValues);
        Map<String, Object> columnData =  new HashMap<>();
        Map<String, String> column =  new HashMap<>();
        column.put("created", "time");
        columnData.put("key", "P2");
        columnData.put("value", column);
        json.setCasscadeColumnData(columnData);
        Map<String, String> cond = new HashMap<>();
        Map<String, String> cond1 = new HashMap<>();
        Map<String, Map<String, String>> conditions = new HashMap<String, Map<String, String>>();
        cond.put("status", "under-spin-up");
        cond1.put("status", "parked");
        conditions.put("exists", cond);
        conditions.put("nonexists", cond1);
        json.setConditions(conditions);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = conditionalApi.insertConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, authorization, keyspaceName, tableNameConditional, json);
        assertEquals(401, response.getStatus());
    }

    @Test
    public void Test6_insertConditional_wrongAuth() throws Exception {
        RestMusicConditionalAPI conditionalApi = new RestMusicConditionalAPI();
        JsonConditional json = new JsonConditional();
        json.setPrimaryKey("id");
        json.setPrimaryKeyValue("123|abc|port");
        json.setCasscadeColumnName("plans");
        Map<String, Object> tableValues =  new HashMap<>();
        tableValues.put("id", "123|abc|port");
        json.setTableValues(tableValues);
        Map<String, Object> columnData =  new HashMap<>();
        Map<String, String> column =  new HashMap<>();
        column.put("created", "time");
        columnData.put("key", "P2");
        columnData.put("value", column);
        json.setCasscadeColumnData(columnData);
        Map<String, String> cond = new HashMap<>();
        Map<String, String> cond1 = new HashMap<>();
        Map<String, Map<String, String>> conditions = new HashMap<String, Map<String, String>>();
        cond.put("status", "under-spin-up");
        cond1.put("status", "parked");
        conditions.put("exists", cond);
        conditions.put("nonexists", cond1);
        json.setConditions(conditions);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = conditionalApi.insertConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, wrongAuthorization, keyspaceName, tableNameConditional, json);
        assertEquals(401, response.getStatus());
    }

    @Test
    public void Test7_updateConditional() throws Exception {
        RestMusicConditionalAPI conditionalApi = new RestMusicConditionalAPI();
        JsonConditional json = new JsonConditional();
        json.setPrimaryKey("id");
        json.setPrimaryKeyValue("123|abc|port");
        json.setCasscadeColumnName("plans");
        Map<String, Object> tableValues =  new HashMap<>();
        tableValues.put("id", "123|abc|port");
        json.setTableValues(tableValues);
        Map<String, Object> columnData =  new HashMap<>();
        Map<String, String> column =  new HashMap<>();
        column.put("created", "time");
        columnData.put("key", "P2");
        columnData.put("value", column);
        json.setCasscadeColumnData(columnData);
        Map<String, String> cond = new HashMap<>();
        Map<String, String> cond1 = new HashMap<>();
        Map<String, Map<String, String>> conditions = new HashMap<String, Map<String, String>>();
        cond.put("updated", "new time");
        conditions.put("exists", cond);
        conditions.put("nonexists", cond1);
        json.setConditions(conditions);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = conditionalApi.updateConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, authorization, keyspaceName, tableNameConditional, json);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test7_updateConditional_wrongAuth() throws Exception {
        RestMusicConditionalAPI conditionalApi = new RestMusicConditionalAPI();
        JsonConditional json = new JsonConditional();
        json.setPrimaryKey("id");
        json.setPrimaryKeyValue("123|abc|port");
        json.setCasscadeColumnName("plans");
        Map<String, Object> tableValues =  new HashMap<>();
        tableValues.put("id", "123|abc|port");
        json.setTableValues(tableValues);
        Map<String, Object> columnData =  new HashMap<>();
        Map<String, String> column =  new HashMap<>();
        column.put("created", "time");
        columnData.put("key", "P2");
        columnData.put("value", column);
        json.setCasscadeColumnData(columnData);
        Map<String, String> cond = new HashMap<>();
        Map<String, String> cond1 = new HashMap<>();
        Map<String, Map<String, String>> conditions = new HashMap<String, Map<String, String>>();
        cond.put("updated", "new time");
        conditions.put("exists", cond);
        conditions.put("nonexists", cond1);
        json.setConditions(conditions);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = conditionalApi.updateConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, wrongAuthorization, keyspaceName, tableNameConditional, json);
        assertEquals(401, response.getStatus());
    }

    @Test
    public void Test7_updateConditional_primarykey_null() throws Exception {
        RestMusicConditionalAPI conditionalApi = new RestMusicConditionalAPI();
        JsonConditional json = new JsonConditional();
        json.setPrimaryKeyValue("123|abc|port");
        json.setCasscadeColumnName("plans");
        Map<String, Object> tableValues =  new HashMap<>();
        tableValues.put("id", "123|abc|port");
        json.setTableValues(tableValues);
        Map<String, Object> columnData =  new HashMap<>();
        Map<String, String> column =  new HashMap<>();
        column.put("created", "time");
        columnData.put("key", "P2");
        columnData.put("value", column);
        json.setCasscadeColumnData(columnData);
        Map<String, String> cond = new HashMap<>();
        Map<String, String> cond1 = new HashMap<>();
        Map<String, Map<String, String>> conditions = new HashMap<String, Map<String, String>>();
        cond.put("updated", "new time");
        conditions.put("exists", cond);
        conditions.put("nonexists", cond1);
        json.setConditions(conditions);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = conditionalApi.updateConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, authorization, keyspaceName, tableNameConditional, json);
        assertEquals(401, response.getStatus());
    }
    @Test
    public void Test8_HealthCheck_cassandra_musicHealthCheck() {
        RestMusicHealthCheckAPI healthCheck = new RestMusicHealthCheckAPI();
        Response response = healthCheck.musicHealthCheck();
        assertEquals(200, response.getStatus());
    }
    
   
}