/*******************************************************************************
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 AT&T Intellectual Property
 * ===================================================================
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 * ============LICENSE_END=============================================
 * ====================================================================
 *******************************************************************************/
package org.onap.music.datastore;

import static org.junit.Assert.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicUtil;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.WriteType;

@RunWith(MockitoJUnitRunner.class)
public class MusicDataStoreTest {
    
    MusicDataStore dataStore;
    
    @Mock
    Session session;
    
    @Mock
    Cluster cluster;
    
    @Before
    public void before() {
        CodecRegistry cr = Mockito.mock(CodecRegistry.class);
        Configuration config = Mockito.mock(Configuration.class);
        Mockito.when(cluster.getConfiguration()).thenReturn(config);
        Mockito.when(config.getCodecRegistry()).thenReturn(cr);
        dataStore = new MusicDataStore(cluster, session);
    }

    @Test
    public void testMusicDataStore() {
        //MusicDataStore mds = new MusicDataStore();
    }
    
    @Test
    public void testMusicDataStoreClusterSession() {
        Session session = Mockito.mock(Session.class);
        Cluster cluster = Mockito.mock(Cluster.class);
        
        CodecRegistry cr = Mockito.mock(CodecRegistry.class);
        Configuration config = Mockito.mock(Configuration.class);
        Mockito.when(cluster.getConfiguration()).thenReturn(config);
        Mockito.when(config.getCodecRegistry()).thenReturn(cr);
        
        
        MusicDataStore mds = new MusicDataStore(cluster, session);
        assertEquals(session, mds.getSession());
        assertEquals(cluster, mds.getCluster());
    }

    @Test
    public void testSession() {
        Session session = Mockito.mock(Session.class);
        dataStore.setSession(session);
        assertEquals(session, dataStore.getSession());
    }

    @Test
    public void testCluster() {
        Cluster cluster = Mockito.mock(Cluster.class);
        CodecRegistry cr = Mockito.mock(CodecRegistry.class);
        Configuration config = Mockito.mock(Configuration.class);
        Mockito.when(cluster.getConfiguration()).thenReturn(config);
        Mockito.when(config.getCodecRegistry()).thenReturn(cr);
        
        dataStore.setCluster(cluster);
        assertEquals(cluster, dataStore.getCluster());
    }

    @Test
    public void testClose() {
        dataStore.close();
        Mockito.verify(session).close();
    }

    @Test
    public void testReturnColumnDataType() {
        Metadata meta = Mockito.mock(Metadata.class);
        Mockito.when(cluster.getMetadata()).thenReturn(meta);
        KeyspaceMetadata ksmd = Mockito.mock(KeyspaceMetadata.class);
        Mockito.when(meta.getKeyspace("keyspace")).thenReturn(ksmd);
        TableMetadata tmd = Mockito.mock(TableMetadata.class);
        Mockito.when(ksmd.getTable("table")).thenReturn(tmd);
        ColumnMetadata cmd = Mockito.mock(ColumnMetadata.class);
        Mockito.when(tmd.getColumn("columnName")).thenReturn(cmd);
        Mockito.when(cmd.getType()).thenReturn(com.datastax.driver.core.DataType.text());
            
        com.datastax.driver.core.DataType dt = dataStore.returnColumnDataType("keyspace", "table", "columnName");
        assertEquals(com.datastax.driver.core.DataType.text(), dt);
    }

    @Test
    public void testReturnColumnMetadata() {
        Metadata meta = Mockito.mock(Metadata.class);
        Mockito.when(cluster.getMetadata()).thenReturn(meta);
        KeyspaceMetadata ksmd = Mockito.mock(KeyspaceMetadata.class);
        Mockito.when(meta.getKeyspace("keyspace")).thenReturn(ksmd);
        TableMetadata tmd = Mockito.mock(TableMetadata.class);
        Mockito.when(ksmd.getTable("tableName")).thenReturn(tmd);
        
        dataStore.returnColumnMetadata("keyspace", "tableName");
        assertEquals(tmd, dataStore.returnColumnMetadata("keyspace", "tableName"));
    }

    @Test
    public void testReturnKeyspaceMetadata() {
        Metadata meta = Mockito.mock(Metadata.class);
        Mockito.when(cluster.getMetadata()).thenReturn(meta);
        KeyspaceMetadata ksmd = Mockito.mock(KeyspaceMetadata.class);
        Mockito.when(meta.getKeyspace("keyspace")).thenReturn(ksmd);
        
        assertEquals(ksmd, dataStore.returnKeyspaceMetadata("keyspace"));
    }

    @Test
    public void testGetColValue() {
        Row row = Mockito.mock(Row.class);
        Mockito.when(row.getString("columnName")).thenReturn("value");
        UUID uuid = UUID.randomUUID();
        Mockito.when(row.getUUID("columnName")).thenReturn(uuid);
        Mockito.when(row.getVarint("columnName")).thenReturn(BigInteger.ONE);
        Mockito.when(row.getLong("columnName")).thenReturn((long) 117);
        Mockito.when(row.getInt("columnName")).thenReturn(5);
        Mockito.when(row.getFloat("columnName")).thenReturn(Float.MAX_VALUE);
        Mockito.when(row.getDouble("columnName")).thenReturn(Double.valueOf("2.5"));
        Mockito.when(row.getBool("columnName")).thenReturn(true);
        Mockito.when(row.getMap("columnName", String.class, String.class)).thenReturn(new HashMap<String, String>());
        Mockito.when(row.getList("columnName", String.class)).thenReturn(new ArrayList<String>());
        
        
        assertEquals("value", dataStore.getColValue(row, "columnName", DataType.varchar()));
        assertEquals(uuid, dataStore.getColValue(row, "columnName", DataType.uuid()));
        assertEquals(BigInteger.ONE, dataStore.getColValue(row, "columnName", DataType.varint()));
        assertEquals((long) 117, dataStore.getColValue(row, "columnName", DataType.bigint()));
        assertEquals(5, dataStore.getColValue(row, "columnName", DataType.cint()));
        assertEquals(Float.MAX_VALUE, dataStore.getColValue(row, "columnName", DataType.cfloat()));
        assertEquals(2.5, dataStore.getColValue(row, "columnName", DataType.cdouble()));
        assertEquals(true, dataStore.getColValue(row, "columnName", DataType.cboolean()));
        assertEquals(0, ((Map<String, String>) dataStore.getColValue(row, "columnName",
                DataType.map(DataType.varchar(), DataType.varchar()))).size());
        assertEquals(0,
                ((List<String>) dataStore.getColValue(row, "columnName", DataType.list(DataType.varchar()))).size());
    }

    @Test
    public void testGetBlobValue() {
        Row row = Mockito.mock(Row.class);
        Mockito.when(row.getBytes("col")).thenReturn(ByteBuffer.allocate(16));
        
        byte[] byteArray = dataStore.getBlobValue(row, "col", DataType.blob());
        assertEquals(16, byteArray.length);
    }

    @Test
    public void testDoesRowSatisfyCondition() throws Exception {
        Row row = Mockito.mock(Row.class);
        ColumnDefinitions cd = Mockito.mock(ColumnDefinitions.class);
        Mockito.when(row.getColumnDefinitions()).thenReturn(cd);
        Mockito.when(cd.getType("col1")).thenReturn(DataType.varchar());

        Map<String, Object> condition = new HashMap<>();
        condition.put("col1",  "val1");
        
        Mockito.when(row.getString("col1")).thenReturn("val1");
        
        assertTrue(dataStore.doesRowSatisfyCondition(row, condition));
        
        condition.put("col1",  "val2");
        assertFalse(dataStore.doesRowSatisfyCondition(row, condition));
    }

    @Test
    public void testMarshalData() {
        ResultSet results = Mockito.mock(ResultSet.class);
        Row row = Mockito.mock(Row.class);
        Mockito.when(row.getString("colName")).thenReturn("rowValue");
        //mock for (Row row: results)
        Iterator mockIterator = Mockito.mock(Iterator.class);
        //Mockito.doCallRealMethod().when(results).forEach(Mockito.any(Consumer.class));
        Mockito.when(results.iterator()).thenReturn(mockIterator);
        Mockito.when(mockIterator.hasNext()).thenReturn(true, false);
        Mockito.when(mockIterator.next()).thenReturn(row);
        
        ColumnDefinitions cd = Mockito.mock(ColumnDefinitions.class);
        Mockito.when(row.getColumnDefinitions()).thenReturn(cd);
        //for (Definition: colDefinitions)
        Iterator mockIterator2 = Mockito.mock(Iterator.class);
        //Mockito.doCallRealMethod().when(cd).forEach(Mockito.any(Consumer.class));
        Mockito.when(cd.iterator()).thenReturn(mockIterator2);
        Mockito.when(mockIterator2.hasNext()).thenReturn(true, false);
        Definition def = Mockito.mock(Definition.class);
        Mockito.when(mockIterator2.next()).thenReturn(def);
        Mockito.when(def.getType()).thenReturn(DataType.varchar());
        Mockito.when(def.getName()).thenReturn("colName");
        
        Map<String, HashMap<String, Object>> data = dataStore.marshalData(results); 
        System.out.println("Marshalled data: " + data);
        
        assertTrue(data.containsKey("row 0"));
        assertEquals("rowValue", data.get("row 0").get("colName"));
    }

    private ArgumentCaptor<SimpleStatement> sessionExecuteResponse() {
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(session.execute(Mockito.any(Statement.class))).thenReturn(rs);
        
        ArgumentCaptor<SimpleStatement> argument = ArgumentCaptor.forClass(SimpleStatement.class);
        return argument;
    }

    @Test
    public void testExecutePutPreparedQueryObjectString() throws Exception {
        ArgumentCaptor<SimpleStatement> argument = sessionExecuteResponse();
        String queryString = "INSERT INTO cycling.cyclist_name (lastname, firstname) VALUES (?,?);";
        String lastName = "KRUIKSWIJK";
        String firstName = "Steven";
        
        PreparedQueryObject query = new PreparedQueryObject(queryString, lastName, firstName);
        dataStore.executePut(query, MusicUtil.CRITICAL);

        Mockito.verify(session).execute(argument.capture());
        assertEquals(ConsistencyLevel.QUORUM, argument.getValue().getConsistencyLevel());
        assertEquals(queryString, argument.getValue().getQueryString());
        assertEquals(2, argument.getValue().valuesCount());
    }
    
    @Test
    public void testExecutePut_ONE() throws Exception {
        ArgumentCaptor<SimpleStatement> argument = sessionExecuteResponse();
        String queryString = "INSERT INTO cycling.cyclist_name (lastname, firstname) VALUES (?,?);";
        String lastName = "KRUIKSWIJK";
        String firstName = "Steven";
        
        PreparedQueryObject query = new PreparedQueryObject(queryString, lastName, firstName);
        dataStore.executePut(query, MusicUtil.ONE);

        Mockito.verify(session).execute(argument.capture());
        assertEquals(ConsistencyLevel.ONE, argument.getValue().getConsistencyLevel());
        assertEquals(queryString, argument.getValue().getQueryString());
        assertEquals(2, argument.getValue().valuesCount());
    }
    
    @Test
    public void testExecutePut_quorum() throws Exception {
        ArgumentCaptor<SimpleStatement> argument = sessionExecuteResponse();
        String queryString = "INSERT INTO cycling.cyclist_name (lastname, firstname) VALUES (?,?);";
        String lastName = "KRUIKSWIJK";
        String firstName = "Steven";
        
        PreparedQueryObject query = new PreparedQueryObject(queryString, lastName, firstName);
        dataStore.executePut(query, MusicUtil.QUORUM);

        Mockito.verify(session).execute(argument.capture());
        //should be quorum!
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, argument.getValue().getConsistencyLevel());
        assertEquals(queryString, argument.getValue().getQueryString());
        assertEquals(2, argument.getValue().valuesCount());
    }
    
    @Test
    public void testExecutePut_ALL() throws Exception {
        ArgumentCaptor<SimpleStatement> argument = sessionExecuteResponse();
        String queryString = "INSERT INTO cycling.cyclist_name (lastname, firstname) VALUES (?,?);";
        String lastName = "KRUIKSWIJK";
        String firstName = "Steven";
        
        PreparedQueryObject query = new PreparedQueryObject(queryString, lastName, firstName);
        dataStore.executePut(query, MusicUtil.ALL);

        Mockito.verify(session).execute(argument.capture());
        assertEquals(ConsistencyLevel.ALL, argument.getValue().getConsistencyLevel());
        assertEquals(queryString, argument.getValue().getQueryString());
        assertEquals(2, argument.getValue().valuesCount());
    }
    
    @Test(expected = MusicQueryException.class)
    public void testExecutePut_BadQueryObj() throws Exception {
        String queryString = "INSERT INTO cycling.cyclist_name (lastname, firstname) VALUES (?,?);";
        String lastName = "KRUIKSWIJK";
        String firstName = "Steven";
        
        //Provide extra value here, middle initial
        PreparedQueryObject query = new PreparedQueryObject(queryString, lastName, firstName, "P");
        try {
            dataStore.executePut(query, MusicUtil.CRITICAL);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw e;
        }

        fail("Should have throw error");
    }

    @Test
    public void testExecutePutPreparedQueryObjectStringLong() throws Exception {
        ArgumentCaptor<SimpleStatement> argument = sessionExecuteResponse();
        String queryString = "INSERT INTO cycling.cyclist_name (lastname, firstname) VALUES ('KRUIKSWIJK','Steven');";

        
        PreparedQueryObject query = new PreparedQueryObject(queryString);
        dataStore.executePut(query, MusicUtil.EVENTUAL, 10);
        
        Mockito.verify(session).execute(argument.capture());
        assertEquals(ConsistencyLevel.ONE, argument.getValue().getConsistencyLevel());
        assertEquals(queryString, argument.getValue().getQueryString());
    }

    @Test
    public void testExecuteGet() throws Exception {
        ArgumentCaptor<SimpleStatement> argument = sessionExecuteResponse();

        PreparedQueryObject query = new PreparedQueryObject("SELECT * FROM KEYSPACE.TABLE");
        
        dataStore.executeGet(query, MusicUtil.ONE);
        
        Mockito.verify(session).execute(argument.capture());
        assertEquals(ConsistencyLevel.ONE, argument.getValue().getConsistencyLevel());
        assertEquals("SELECT * FROM KEYSPACE.TABLE", argument.getValue().getQueryString());
    }
    
    @Test (expected = MusicQueryException.class)
    public void testExecuteGet_badQuery() throws Exception {
        ArgumentCaptor<SimpleStatement> argument = sessionExecuteResponse();

        PreparedQueryObject query = new PreparedQueryObject("SELECT * FROM KEYSPACE.TABLE", "broken");
        
        dataStore.executeGet(query, MusicUtil.ONE);
        
        Mockito.verify(session).execute(argument.capture());
        assertEquals(ConsistencyLevel.ONE, argument.getValue().getConsistencyLevel());
        assertEquals("SELECT * FROM KEYSPACE.TABLE", argument.getValue().getQueryString());
    }

    @Test
    public void testExecuteOneConsistencyGet() throws Exception {
        ArgumentCaptor<SimpleStatement> argument = sessionExecuteResponse();
        PreparedQueryObject query = new PreparedQueryObject("SELECT * FROM KEYSPACE.TABLE");
        
        dataStore.executeOneConsistencyGet(query);
        
        
        Mockito.verify(session).execute(argument.capture());
        assertEquals(ConsistencyLevel.ONE, argument.getValue().getConsistencyLevel());
        assertEquals("SELECT * FROM KEYSPACE.TABLE", argument.getValue().getQueryString());
    }

    @Test
    public void testExecuteLocalQuorumConsistencyGet() throws Exception {
        ArgumentCaptor<SimpleStatement> argument = sessionExecuteResponse();
        PreparedQueryObject query = new PreparedQueryObject("SELECT * FROM KEYSPACE.TABLE");
        
        dataStore.executeLocalQuorumConsistencyGet(query);
        
        Mockito.verify(session).execute(argument.capture());
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, argument.getValue().getConsistencyLevel());
        assertEquals("SELECT * FROM KEYSPACE.TABLE", argument.getValue().getQueryString());
    }

    @Test
    public void testExecuteQuorumConsistencyGet() throws Exception {
        ArgumentCaptor<SimpleStatement> argument = sessionExecuteResponse();
        PreparedQueryObject query = new PreparedQueryObject("SELECT * FROM KEYSPACE.TABLE");
        
        dataStore.executeQuorumConsistencyGet(query);
        
        Mockito.verify(session).execute(argument.capture());
        assertEquals(ConsistencyLevel.QUORUM, argument.getValue().getConsistencyLevel());
        assertEquals("SELECT * FROM KEYSPACE.TABLE", argument.getValue().getQueryString());
    }

    
    @Test
    public void testExecutePut() {
        Mockito.when(session.execute(Mockito.any(SimpleStatement.class)))
                .thenThrow(new WriteTimeoutException(ConsistencyLevel.QUORUM, WriteType.CAS, 1, 3));
        
        try {
            dataStore.executePut(new PreparedQueryObject("Test query"), "critical");
        } catch (MusicServiceException e) {
            return;
        } catch (MusicQueryException e) {
            // should never reach here
            fail();
        }
        fail();
    }
}
