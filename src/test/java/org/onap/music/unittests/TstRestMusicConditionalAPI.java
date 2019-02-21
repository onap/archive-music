/*
 * ============LICENSE_START========================================== org.onap.music
 * =================================================================== Copyright (c) 2017 AT&T Intellectual Property
 * =================================================================== Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * 
 * ============LICENSE_END=============================================
 * ====================================================================
 */

package org.onap.music.unittests;

import static org.junit.Assert.assertEquals;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mindrot.jbcrypt.BCrypt;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.onap.music.conductor.conditionals.JsonConditional;
import org.onap.music.conductor.conditionals.RestMusicConditionalAPI;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonKeySpace;
import org.onap.music.datastore.jsonobjects.JsonSelect;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.rest.RestMusicDataAPI;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.sun.jersey.core.util.Base64;
import com.sun.jersey.core.util.MultivaluedMapImpl;

@RunWith(MockitoJUnitRunner.class)
public class TstRestMusicConditionalAPI {

    RestMusicDataAPI data = new RestMusicDataAPI();
    RestMusicConditionalAPI cond = new RestMusicConditionalAPI();
    static PreparedQueryObject testObject;

    @Mock
    HttpServletResponse http;

    @Mock
    UriInfo info;

    static String appName = "TestApp";
    static String userId = "TestUser";
    static String password = "TestPassword";
    static String authData = userId + ":" + password;
    static String wrongAuthData = userId + ":" + "pass";
    static String authorization = new String(Base64.encode(authData.getBytes()));
    static String wrongAuthorization = new String(Base64.encode(wrongAuthData.getBytes()));
    static boolean isAAF = false;
    static UUID uuid = UUID.fromString("abc66ccc-d857-4e90-b1e5-df98a3d40ce6");
    static String keyspaceName = "testcassa";
    static String tableName = "employees";
    static String xLatestVersion = "X-latestVersion";
    static String onboardUUID = null;

    @BeforeClass
    public static void init() throws Exception {
        System.out.println("Testing RestMusicConditional class");
        try {
            createKeyspace();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Unable to initialize before TestRestMusicData test class. " + e.getMessage());
        }
    }

    @After
    public void afterEachTest() throws MusicServiceException {
        clearAllTablesFromKeyspace();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("DROP KEYSPACE IF EXISTS " + keyspaceName);
        MusicCore.eventualPut(testObject);
    }

    @Test
    public void test_insertIntoTable() throws Exception {
        System.out.println("Testing conditional insert into table");
        createTable();

        JsonConditional jsonCond = new JsonConditional();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("id", "test_id");
        consistencyInfo.put("type", "eventual");
        HashMap<String, Object> cascadeData = new HashMap<>();
        HashMap<String, String> cascadeValue = new HashMap<>();
        cascadeValue.put("created", "hello");
        cascadeValue.put("updated", "world");
        cascadeData.put("key", "p1");
        cascadeData.put("value", cascadeValue);
        HashMap<String, Map<String, String>> condition = new HashMap<>();
        HashMap<String, String> exists = new HashMap<>();
        exists.put("status", "parked");
        HashMap<String, String> nonexists = new HashMap<>();
        nonexists.put("status", "underway");
        condition.put("exists", exists);
        condition.put("nonexists", nonexists);

        jsonCond.setPrimaryKey("id");
        jsonCond.setPrimaryKeyValue("testname");
        jsonCond.setCasscadeColumnName("plans");
        jsonCond.setTableValues(values);
        jsonCond.setCasscadeColumnData(cascadeData);
        jsonCond.setConditions(condition);

        Response response = cond.insertConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, keyspaceName, tableName, jsonCond);

        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

        assertEquals(200, response.getStatus());
    }
    /*
     * @Test public void test4_insertIntoTable2() throws Exception { System.out.println("Testing insert into table #2");
     * createTable(); JsonInsert jsonInsert = new JsonInsert(); Map<String, String> consistencyInfo = new HashMap<>();
     * Map<String, Object> values = new HashMap<>(); values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
     * values.put("emp_name", "test1"); values.put("emp_salary", 1500); consistencyInfo.put("type", "eventual");
     * jsonInsert.setConsistencyInfo(consistencyInfo); jsonInsert.setKeyspaceName(keyspaceName);
     * jsonInsert.setTableName(tableName); jsonInsert.setValues(values); Response response = data.insertIntoTable("1",
     * "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization, jsonInsert, keyspaceName, tableName);
     * System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
     * 
     * assertEquals(200, response.getStatus()); }
     * 
     * // Auth Error
     * 
     * @Test public void test4_insertIntoTable3() throws Exception {
     * System.out.println("Testing insert into table with bad credentials"); createTable(); JsonInsert jsonInsert = new
     * JsonInsert(); Map<String, String> consistencyInfo = new HashMap<>(); Map<String, Object> values = new
     * HashMap<>(); values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6"); values.put("emp_name", "test1");
     * values.put("emp_salary", 1500); consistencyInfo.put("type", "eventual");
     * jsonInsert.setConsistencyInfo(consistencyInfo); jsonInsert.setKeyspaceName(keyspaceName);
     * jsonInsert.setTableName(tableName); jsonInsert.setValues(values); Response response = data.insertIntoTable("1",
     * "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, wrongAuthorization, jsonInsert, keyspaceName,
     * tableName); System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
     * 
     * assertEquals(401, response.getStatus()); }
     * 
     * // Table wrong
     * 
     * @Test public void test4_insertIntoTable4() throws Exception {
     * System.out.println("Testing insert into wrong table"); createTable(); JsonInsert jsonInsert = new JsonInsert();
     * Map<String, String> consistencyInfo = new HashMap<>(); Map<String, Object> values = new HashMap<>();
     * values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6"); values.put("emp_name", "test1");
     * values.put("emp_salary", 1500); consistencyInfo.put("type", "eventual");
     * jsonInsert.setConsistencyInfo(consistencyInfo); jsonInsert.setKeyspaceName(keyspaceName);
     * jsonInsert.setTableName(tableName); jsonInsert.setValues(values); Response response = data.insertIntoTable("1",
     * "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization, jsonInsert, keyspaceName, "wrong");
     * System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
     * 
     * assertEquals(400, response.getStatus()); }
     */

    @Test
    public void test5_updateTable() throws Exception {
        System.out.println("Testing conditional update table");
        createAndInsertIntoTable();

        Map<String, String> consistencyInfo = new HashMap<>();
        consistencyInfo.put("type", "eventual");

        JsonConditional jsonCond = new JsonConditional();
        Map<String, Object> values = new HashMap<>();
        values.put("id", "test_id");
        HashMap<String, Object> cascadeData = new HashMap<>();
        HashMap<String, String> cascadeValue = new HashMap<>();
        cascadeValue.put("created", "hello");
        cascadeValue.put("updated", "world");
        cascadeData.put("key", "p1");
        cascadeData.put("value", cascadeValue);

        jsonCond.setPrimaryKey("id");
        jsonCond.setPrimaryKeyValue("test_id");
        jsonCond.setCasscadeColumnName("plans");
        jsonCond.setTableValues(values);
        jsonCond.setCasscadeColumnData(cascadeData);

        Response response = cond.updateConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, keyspaceName, tableName, jsonCond);
        System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());

        assertEquals(200, response.getStatus());
    }
    /*
     * // need mock code to create error for MusicCore methods
     * 
     * @Test public void test5_updateTableAuthE() throws Exception { System.out.println("Testing update table #2");
     * createTable(); //MockitoAnnotations.initMocks(this); JsonUpdate jsonUpdate = new JsonUpdate(); Map<String,
     * String> consistencyInfo = new HashMap<>(); MultivaluedMap<String, String> row = new MultivaluedMapImpl();
     * Map<String, Object> values = new HashMap<>(); row.add("emp_name", "testname"); values.put("emp_salary", 2500);
     * consistencyInfo.put("type", "atomic"); jsonUpdate.setConsistencyInfo(consistencyInfo);
     * jsonUpdate.setKeyspaceName(keyspaceName); jsonUpdate.setTableName(tableName); jsonUpdate.setValues(values); //add
     * ttl & timestamp //Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
     * Mockito.when(info.getQueryParameters()).thenReturn(row); //Map<String, Object> m1= new HashMap<>() ;
     * //Mockito.when(MusicCore.autheticateUser(appName,userId,password,keyspaceName,
     * "abc66ccc-d857-4e90-b1e5-df98a3d40ce6","updateTable")).thenReturn(m1); Response response = data.updateTable("1",
     * "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization, jsonUpdate, keyspaceName, tableName,
     * info); System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
     * 
     * assertEquals(200, response.getStatus()); }
     * 
     * @Ignore
     * 
     * @Test public void test5_updateTableAuthException1() throws Exception {
     * System.out.println("Testing update table authentication error"); createTable(); JsonUpdate jsonUpdate = new
     * JsonUpdate(); Map<String, String> consistencyInfo = new HashMap<>(); MultivaluedMap<String, String> row = new
     * MultivaluedMapImpl(); Map<String, Object> values = new HashMap<>(); row.add("emp_name", "testname");
     * values.put("emp_salary", 2500); consistencyInfo.put("type", "atomic");
     * jsonUpdate.setConsistencyInfo(consistencyInfo); jsonUpdate.setKeyspaceName(keyspaceName);
     * jsonUpdate.setTableName(tableName); jsonUpdate.setValues(values);
     * 
     * Mockito.when(info.getQueryParameters()).thenReturn(row); String authDatax = ":"; String authorizationx = new
     * String(Base64.encode(authDatax.getBytes())); Response response = data.updateTable("1", "1", "1",
     * "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorizationx, jsonUpdate, keyspaceName, tableName, info);
     * System.out.println("Status: " + response.getStatus() + ". Entity " + response.getEntity());
     * 
     * assertEquals(401, response.getStatus()); }
     * 
     * @Ignore
     * 
     * @Test public void test5_updateTableAuthEmpty() throws Exception {
     * System.out.println("Testing update table without authentication"); createTable();
     * 
     * JsonUpdate jsonUpdate = new JsonUpdate(); Map<String, String> consistencyInfo = new HashMap<>();
     * MultivaluedMap<String, String> row = new MultivaluedMapImpl(); Map<String, Object> values = new HashMap<>();
     * row.add("emp_name", "testname"); values.put("emp_salary", 2500); consistencyInfo.put("type", "atomic");
     * jsonUpdate.setConsistencyInfo(consistencyInfo); jsonUpdate.setKeyspaceName(keyspaceName);
     * jsonUpdate.setTableName(tableName); jsonUpdate.setValues(values);
     * 
     * Mockito.when(info.getQueryParameters()).thenReturn(row); String authDatax =":"+password; String authorizationx =
     * new String(Base64.encode(authDatax.getBytes())); String appNamex="xx"; Response response = data.updateTable("1",
     * "1", "1", "", appNamex, authorizationx, jsonUpdate, keyspaceName, tableName, info); System.out.println("Status: "
     * + response.getStatus() + ". Entity " + response.getEntity());
     * 
     * assertEquals(401, response.getStatus()); }
     * 
     */

    private static void createKeyspace() throws Exception {
        // shouldn't really be doing this here, but create keyspace is currently turned off
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(CassandraCQL.createKeySpace);
        MusicCore.eventualPut(query);

        boolean isAAF = false;
        String hashedpwd = BCrypt.hashpw(password, BCrypt.gensalt());
        query = new PreparedQueryObject();
        query.appendQueryString("INSERT into admin.keyspace_master (uuid, keyspace_name, application_name, is_api, "
                + "password, username, is_aaf) values (?,?,?,?,?,?,?)");
        query.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
        query.addValue(MusicUtil.convertToActualDataType(DataType.text(), keyspaceName));
        query.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        query.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), "True"));
        query.addValue(MusicUtil.convertToActualDataType(DataType.text(), hashedpwd));
        query.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
        query.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));
        CachingUtil.updateMusicCache(keyspaceName, appName);
        CachingUtil.updateMusicValidateCache(appName, userId, hashedpwd);
        MusicCore.eventualPut(query);
    }

    private void clearAllTablesFromKeyspace() throws MusicServiceException {
        ArrayList<String> tableNames = new ArrayList<>();
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '" + keyspaceName + "';");
        ResultSet rs = MusicCore.get(query);
        for (Row row : rs) {
            tableNames.add(row.getString("table_name"));
        }
        for (String table : tableNames) {
            query = new PreparedQueryObject();
            query.appendQueryString("DROP TABLE " + keyspaceName + "." + table);
            MusicCore.eventualPut(query);
        }
    }

    /**
     * Create a table {@link tableName} in {@link keyspaceName}
     * 
     * @throws Exception
     */
    private void createTable() throws Exception {
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("id", "text");
        fields.put("plans", "map<text,text>");
        fields.put("PRIMARY KEY", "(id)");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("id");
        jsonTable.setTableName(tableName);
        jsonTable.setFields(fields);
        Response response = data.createTable("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonTable, keyspaceName, tableName);
    }

    /**
     * Create table {@link createTable} and insert into said table
     * 
     * @throws Exception
     */
    private void createAndInsertIntoTable() throws Exception {
        createTable();

        Map<String, String> consistencyInfo = new HashMap<>();
        consistencyInfo.put("type", "eventual");
        JsonConditional jsonCond = new JsonConditional();
        Map<String, Object> values = new HashMap<>();
        values.put("id", "test_id");
        HashMap<String, Object> cascadeData = new HashMap<>();
        HashMap<String, String> cascadeValue = new HashMap<>();
        cascadeValue.put("created", "hello");
        cascadeValue.put("updated", "world");
        cascadeData.put("key", "p1");
        cascadeData.put("value", cascadeValue);
        HashMap<String, Map<String, String>> condition = new HashMap<>();
        HashMap<String, String> exists = new HashMap<>();
        exists.put("status", "parked");
        HashMap<String, String> nonexists = new HashMap<>();
        nonexists.put("status", "underway");
        condition.put("exists", exists);
        condition.put("nonexists", nonexists);

        jsonCond.setPrimaryKey("id");
        jsonCond.setPrimaryKeyValue("test_id");
        jsonCond.setCasscadeColumnName("plans");
        jsonCond.setTableValues(values);
        jsonCond.setCasscadeColumnData(cascadeData);
        jsonCond.setConditions(condition);

        Response response = cond.insertConditional("1", "1", "1", "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, keyspaceName, tableName, jsonCond);
    }
}
