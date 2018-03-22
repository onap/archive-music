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
import static org.onap.music.main.MusicCore.mLockHandle;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
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
import org.onap.music.main.CachingUtil;
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
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), password));
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
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), password));
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
        Map<String, Object> resultMap = data.createKeySpace("1", "1", "1", null, appName, userId,
                        password, jsonKeyspace, keyspaceName, http);
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
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
        Map<String, Object> resultMap = data.createKeySpace("1", "1", "1", null, "TestApp1",
                        "TestUser1", password, jsonKeyspace, keyspaceName, http);
        System.out.println("#######status is " + resultMap.get("Exception"));
        assertEquals("Keyspace testcassa already exists",
                resultMap.get("error"));;
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
        Map<String, Object> resultMap = data.createTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
                        jsonTable, keyspaceName, tableName, http);
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
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
        Map<String, Object> resultMap = data.insertIntoTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, userId, password, jsonInsert, keyspaceName, tableName, http);
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
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
        Map<String, Object> resultMap = data.insertIntoTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
                        jsonInsert, keyspaceName, tableName, http);
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
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
        Map<String, Object> resultMap = data.updateTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                userId, password, jsonUpdate, keyspaceName, tableName, info, http);
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
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
        Map<String, Object> resultMap = data.select("1", "1", "1",
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password, keyspaceName, tableName, info, http);
        assertEquals("2500", ((HashMap<String,HashMap<String,Object>>) resultMap.get("result")).get("row 0").get("emp_salary").toString());
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
        Map<String, Object> resultMap = data.selectCritical("1", "1", "1",
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password, jsonInsert, keyspaceName, tableName,
                info, http);
        assertEquals("2500", ((HashMap<String,HashMap<String,Object>>) resultMap.get("result")).get("row 0").get("emp_salary").toString());
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
        Map<String, Object> resultMap = data.deleteFromTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
                        jsonDelete, keyspaceName, tableName, info, http);
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
    }

    @Test
    public void Test7_dropTable() throws Exception {
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        consistencyInfo.put("type", "atomic");
        jsonTable.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = data.dropTable("1", "1", "1",
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
                         keyspaceName, tableName, http);
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
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
        Map<String, Object> resultMap = data.dropKeySpace("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, userId, password, keyspaceName, http);
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
    }
    
    @Test
    public void Test6_onboard() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setUserId("TestUser2");
        jsonOnboard.setPassword("TestPassword2");
        Map<String, Object> resultMap = admin.onboardAppWithMusic(jsonOnboard, http);
        resultMap.containsKey("success");
        onboardUUID = resultMap.get("Generated AID").toString();
        assertEquals("Your application TestApp2 has been onboarded with MUSIC.", resultMap.get("Success"));
    }

    @Test
    public void Test7_onboardSearch() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setIsAAF("false");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = admin.getOnboardedInfoSearch(jsonOnboard, http);
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
        Map<String, Object> resultMap = admin.updateOnboardApp(jsonOnboard, http);
        resultMap.containsKey("success");
        assertEquals("Your application has been updated successfully", resultMap.get("Success"));
    }

    @Test
    public void Test9_onboardDelete() throws Exception {
        JsonOnboard jsonOnboard = new JsonOnboard();
        jsonOnboard.setAppname("TestApp2");
        jsonOnboard.setAid(onboardUUID);
        Map<String, Object> resultMap = admin.deleteOnboardApp(jsonOnboard, http);
        resultMap.containsKey("success");
        assertEquals("Your application has been deleted successfully", resultMap.get("Success"));
    }

    @Test
    public void Test3_createLockReference() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = lock.createLockReference(lockName, null, appName, userId, password, http);
        @SuppressWarnings("unchecked")
        Map<String, Object> resultMap1 = (Map<String, Object>) resultMap.get("lock");
        lockId = (String) resultMap1.get("lock");
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
    }

    @Test
    public void Test4_accquireLock() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = lock.accquireLock(lockId, null, appName, userId, password, http);
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
    }

    @Test
    public void Test5_currentLockHolder() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = lock.currentLockHolder(lockName, null, appName, userId, password, http);
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
    }

    @Test
    public void Test7_unLock() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = lock.unLock(lockId, null, appName, userId, password, http);
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
    }

    @Test
    public void Test8_delete() throws Exception {
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Map<String, Object> resultMap = lock.deleteLock(lockName, null, appName, userId, password, http);
        assertEquals(ResultType.SUCCESS, resultMap.get("status"));
    }
}