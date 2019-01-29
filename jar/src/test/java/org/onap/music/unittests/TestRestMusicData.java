/*
 * ============LICENSE_START========================================== org.onap.music
 * =================================================================== Copyright (c) 2017 AT&T
 * Intellectual Property ===================================================================
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
import static org.junit.Assert.assertTrue;
import java.io.File;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mindrot.jbcrypt.BCrypt;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonKeySpace;
import org.onap.music.datastore.jsonobjects.JsonOnboard;
import org.onap.music.datastore.jsonobjects.JsonSelect;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.lockingservice.MusicLockingService;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.rest.RestMusicAdminAPI;
import org.onap.music.rest.RestMusicDataAPI;
import org.onap.music.rest.RestMusicLocksAPI;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.sun.jersey.core.util.MultivaluedMapImpl;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(MockitoJUnitRunner.class)
public class TestRestMusicData {

    RestMusicDataAPI data = new RestMusicDataAPI();
    RestMusicAdminAPI admin = new RestMusicAdminAPI();
    RestMusicLocksAPI lock = new RestMusicLocksAPI(); 
    static PreparedQueryObject testObject;
    static TestingServer zkServer;

    @Mock
    HttpServletResponse http;

    @Mock
    UriInfo info;

    static String appName = "TestApp";
    static String userId = "TestUser";
    static String password = "TestPassword";
    static boolean isAAF = false;
    static UUID uuid = UUID.fromString("abc66ccc-d857-4e90-b1e5-df98a3d40ce6");
    static String keyspaceName = "testCassa";
    static String tableName = "employees";
    static String xLatestVersion = "X-latestVersion";
    static String onboardUUID = null;
    static String lockId = null;
    static String lockName = "testCassa.employees.sample3";

    @BeforeClass
    public static void init() throws Exception {
        try {
            MusicCore.mDstoreHandle = CassandraCQL.connectToEmbeddedCassandra();
            zkServer = new TestingServer(2181, new File("/tmp/zk"));
            MusicCore.mLockHandle = new MusicLockingService();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        System.out.println("After class");
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("DROP KEYSPACE IF EXISTS " + keyspaceName);
        MusicCore.eventualPut(testObject);
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("DROP KEYSPACE IF EXISTS admin");
        MusicCore.eventualPut(testObject);
        MusicCore.mDstoreHandle.close();
        MusicCore.mLockHandle.getzkLockHandle().close();
        MusicCore.mLockHandle.close();
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
    }

    @Test
    public void Test2_createKeyspace() throws Exception {
        JsonKeySpace jsonKeyspace = new JsonKeySpace();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> replicationInfo = new HashMap<>();
        consistencyInfo.put("type", "eventual");
        replicationInfo.put("class", "SimpleStrategy");
        replicationInfo.put("replication_factor", 1);
        jsonKeyspace.setConsistencyInfo(consistencyInfo);
        jsonKeyspace.setDurabilityOfWrites("true");
        jsonKeyspace.setKeyspaceName(keyspaceName);
        jsonKeyspace.setReplicationInfo(replicationInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createKeySpace("1", "1", "1", null, appName, userId,
                        password, jsonKeyspace, keyspaceName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(200,response.getStatus());
    }

    @Test
    public void Test2_createKeyspace0() throws Exception {
        JsonKeySpace jsonKeyspace = new JsonKeySpace();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> replicationInfo = new HashMap<>();
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createKeySpace("1", "1", "1", null, appName, userId,
                        password, jsonKeyspace, keyspaceName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(400,response.getStatus());
    }
//MusicCore.autheticateUser
    @Test
    public void Test2_createKeyspace01() throws Exception {
        JsonKeySpace jsonKeyspace = new JsonKeySpace();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> replicationInfo = new HashMap<>();
        String appName1 = "test";
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createKeySpace("1", "1", "1", null, appName1, userId,
                        password, jsonKeyspace, keyspaceName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(401,response.getStatus());
    }

    @Test
    public void Test3_createKeyspace1() throws Exception {
        JsonKeySpace jsonKeyspace = new JsonKeySpace();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> replicationInfo = new HashMap<>();
        consistencyInfo.put("type", "eventual");
        replicationInfo.put("class", "SimpleStrategy");
        replicationInfo.put("replication_factor", 1);
        jsonKeyspace.setConsistencyInfo(consistencyInfo);
        jsonKeyspace.setDurabilityOfWrites("true");
        jsonKeyspace.setKeyspaceName("TestApp1");
        jsonKeyspace.setReplicationInfo(replicationInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createKeySpace("1", "1", "1", null, "TestApp1",
                        "TestUser1", password, jsonKeyspace, keyspaceName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(400,response.getStatus());
    }

    @Test
    public void Test3_createTable() throws Exception {
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name)");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableName);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
                        jsonTable, keyspaceName, tableName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(200, response.getStatus());
    }

    // Improper Auth
    @Test
    public void Test3_createTable1() throws Exception {
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name)");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableName);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, "wrong",
                        jsonTable, keyspaceName, tableName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(401, response.getStatus());
    }

    // Improper keyspace
    @Test
    public void Test3_createTable2() throws Exception {
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name)");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableName);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
                        jsonTable, "wrong", tableName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(401, response.getStatus());
    }



    @Test
    public void Test4_insertIntoTable() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "testName");
        values.put("emp_salary", 500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.insertIntoTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, userId, password, jsonInsert, keyspaceName, tableName);
        assertEquals(200, response.getStatus());
    }

    @Test
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
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
                        jsonInsert, keyspaceName, tableName);
        assertEquals(200, response.getStatus());
    }

    // Auth Error
    @Test
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
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, "wrong",
                        jsonInsert, keyspaceName, tableName);
        assertEquals(401, response.getStatus());
    }

    // Table wrong
    @Test
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
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
                        jsonInsert, keyspaceName, "wrong");
        assertEquals(400, response.getStatus());
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
                userId, password, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(200, response.getStatus());
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
                        appName, userId, password, keyspaceName, tableName, info);
        HashMap<String,HashMap<String,Object>> map = (HashMap<String, HashMap<String, Object>>) response.getEntity();
        HashMap<String, Object> result = map.get("result");
        assertEquals("2500", ((HashMap<String,Object>) result.get("row 0")).get("emp_salary").toString());
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
                        appName, userId, password, jsonInsert, keyspaceName, tableName,info);
        HashMap<String,HashMap<String,Object>> map = (HashMap<String, HashMap<String, Object>>) response.getEntity();
        HashMap<String, Object> result = map.get("result");
        assertEquals("2500", ((HashMap<String,Object>) result.get("row 0")).get("emp_salary").toString());
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
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
                        jsonDelete, keyspaceName, tableName, info);
        assertEquals(200, response.getStatus());
    }

    // Values
    @Test
    public void Test6_deleteFromTable1() throws Exception {
        JsonDelete jsonDelete = new JsonDelete();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        consistencyInfo.put("type", "atomic");
        jsonDelete.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = data.deleteFromTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
                        jsonDelete, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

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
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
                        null, keyspaceName, tableName, info);
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
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
                         keyspaceName, tableName);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void Test8_deleteKeyspace() throws Exception {
        JsonKeySpace jsonKeyspace = new JsonKeySpace();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> replicationInfo = new HashMap<>();
        consistencyInfo.put("type", "eventual");
        replicationInfo.put("class", "SimpleStrategy");
        replicationInfo.put("replication_factor", 1);
        jsonKeyspace.setConsistencyInfo(consistencyInfo);
        jsonKeyspace.setDurabilityOfWrites("true");
        jsonKeyspace.setKeyspaceName("TestApp1");
        jsonKeyspace.setReplicationInfo(replicationInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.dropKeySpace("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, userId, password, keyspaceName);
        assertEquals(200, response.getStatus());
    }
    
    @Test
    public void Test8_deleteKeyspace2() throws Exception {
        JsonKeySpace jsonKeyspace = new JsonKeySpace();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> replicationInfo = new HashMap<>();
        consistencyInfo.put("type", "eventual");
        replicationInfo.put("class", "SimpleStrategy");
        replicationInfo.put("replication_factor", 1);
        jsonKeyspace.setConsistencyInfo(consistencyInfo);
        jsonKeyspace.setDurabilityOfWrites("true");
        jsonKeyspace.setKeyspaceName("TestApp1");
        jsonKeyspace.setReplicationInfo(replicationInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.dropKeySpace("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, userId, "wrong", keyspaceName);
        assertEquals(401, response.getStatus());
    }

    @Test
    public void Test8_deleteKeyspace3() throws Exception {
        JsonKeySpace jsonKeyspace = new JsonKeySpace();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> replicationInfo = new HashMap<>();
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = data.dropKeySpace("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, userId, password, keyspaceName);
        assertEquals(400, response.getStatus());
    }

    
    
    @Test
    public void Test6_onboard() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser2");
        jsonOnboard.setPassword("TestPassword2");
        Map<String, Object> resultMap = (Map<String, Object>) admin.onboardAppWithMusic(jsonOnboard).getEntity();
        resultMap.containsKey("success");
        onboardUUID = resultMap.get("Generated AID").toString();
        assertEquals("Your application TestApp2 has been onboarded with MUSIC.", resultMap.get("Success"));
    }
    // Missing appname
    @Test
    public void Test6_onboard1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser2");
        jsonOnboard.setPassword("TestPassword2");
        Map<String, Object> resultMap = (Map<String, Object>) admin.onboardAppWithMusic(jsonOnboard).getEntity();
        resultMap.containsKey("success");
        System.out.println("--->" + resultMap.toString());
        assertEquals("Unauthorized: Please check the request parameters. Some of the required values appName(ns), userId, password, isAAF are missing.", resultMap.get("Exception"));
    }

    
    @Test
    public void Test7_onboardSearch() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.getOnboardedInfoSearch(jsonOnboard).getEntity();
        resultMap.containsKey("success");
        assertEquals(MusicUtil.DEFAULTKEYSPACENAME, resultMap.get(onboardUUID));

    }

    // Missing appname
    @Test
    public void Test7_onboardSearch1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.getOnboardedInfoSearch(jsonOnboard).getEntity();
        System.out.println("--->" + resultMap.toString());
        resultMap.containsKey("success");
        assertEquals(MusicUtil.DEFAULTKEYSPACENAME, resultMap.get(onboardUUID));

    }

    @Test
    public void Test8_onboardUpdate() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser3");
        jsonOnboard.setPassword("TestPassword3");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.updateOnboardApp(jsonOnboard).getEntity();
        System.out.println("--->" + resultMap.toString());
        resultMap.containsKey("success");
        assertEquals("Your application has been updated successfully", resultMap.get("Success"));
    }

    // Aid null
    @Test
    public void Test8_onboardUpdate1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser3");
        jsonOnboard.setPassword("TestPassword3");
        Map<String, Object> resultMap = (Map<String, Object>) admin.updateOnboardApp(jsonOnboard).getEntity();
        System.out.println("--->" + resultMap.toString());
        resultMap.containsKey("success");
        assertEquals("Please make sure Aid is present", resultMap.get("Exception"));
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
        Map<String, Object> resultMap = (Map<String, Object>) admin.updateOnboardApp(jsonOnboard).getEntity();
        resultMap.containsKey("success");
        System.out.println("--->" + resultMap.toString());
        assertEquals("Application TestApp2 has already been onboarded. Please contact admin.", resultMap.get("Exception"));
    }

    // All null
    @Test
    public void Test8_onboardUpdate3() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.updateOnboardApp(jsonOnboard).getEntity();
        assertTrue(resultMap.containsKey("Exception") );
    }

    @Test
    public void Test9_onboardDelete() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = (Map<String, Object>) admin.deleteOnboardApp(jsonOnboard).getEntity();
        resultMap.containsKey("success");
        assertEquals("Your application has been deleted successfully", resultMap.get("Success"));
    }

    @Test
    public void Test9_onboardDelete1() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        Map<String, Object> resultMap = (Map<String, Object>) admin.deleteOnboardApp(jsonOnboard).getEntity();
        assertTrue(resultMap.containsKey("Exception"));
    }

    @Test
    public void Test3_createLockReference() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.createLockReference(lockName,"1","1", null, appName, userId, password).getEntity();
        @SuppressWarnings("unchecked")
        Map<String, Object> resultMap1 = (Map<String, Object>) resultMap.get("lock");
        lockId = (String) resultMap1.get("lock");
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
    }

    @Test
    public void Test4_accquireLock() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.accquireLock(lockId,"1","1", null, appName, userId, password).getEntity();
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
    }

    @Test
    public void Test5_currentLockHolder() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.currentLockHolder(lockName,"1","1", null, appName, userId, password).getEntity();
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
    }

    @Test
    public void Test7_unLock() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.unLock(lockId,"1","1", null, appName, userId, password).getEntity();
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
    }

    @Test
    public void Test8_delete() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = (Map<String, Object>) lock.deleteLock(lockName,"1","1", null, appName, userId, password).getEntity();
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
    }
}