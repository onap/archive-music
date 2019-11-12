/*******************************************************************************
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2018 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2019 IBM.
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

package org.onap.music.datastore.jsonobjects;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.onap.music.datastore.jsonobjects.JsonUpdate.RowIdentifier;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;

public class JsonUpdateTest {
    
    JsonUpdate ju = null;
    
    @Before
    public void init() {
        ju = new JsonUpdate();
    }
    

    @Test
    public void testGetConditions() {
        Map<String,Object>  mapSo = new HashMap<>();
        mapSo.put("key1","one");
        mapSo.put("key2","two");
        ju.setConditions(mapSo);
        assertEquals("one",ju.getConditions().get("key1"));
    }

    @Test
    public void testGetRow_specification() {
        Map<String,Object>  mapSo = new HashMap<>();
        mapSo.put("key1","one");
        mapSo.put("key2","two");
        ju.setRow_specification(mapSo);
        assertEquals("one",ju.getRow_specification().get("key1"));
    }

    @Test
    public void testGetKeyspaceName() {
        String keyspace = "keyspace";
        ju.setKeyspaceName(keyspace);
        assertEquals(keyspace,ju.getKeyspaceName());
    }

    @Test
    public void testGetTableName() {
        String table = "table";
        ju.setTableName(table);
        assertEquals(table,ju.getTableName());
   }

    @Test
    public void testGetConsistencyInfo() {
        Map<String, String> mapSs = new HashMap<>();
        mapSs.put("k1", "one");
        ju.setConsistencyInfo(mapSs);
        assertEquals("one",ju.getConsistencyInfo().get("k1"));
    }

    @Test
    public void testGetTtl() {
        ju.setTtl("2000");
        assertEquals("2000",ju.getTtl());
    }

    @Test
    public void testGetTimestamp() {
        ju.setTimestamp("20:00");
        assertEquals("20:00",ju.getTimestamp());

    }

    @Test
    public void testGetValues() {
        Map<String,Object> cons = new HashMap<>();
        cons.put("val1","one");
        cons.put("val2","two");
        ju.setValues(cons);
        assertEquals("one",ju.getValues().get("val1"));
    }
    
    @Test
    public void testSerialize() {
        assertTrue(ju.serialize() instanceof byte[]);
    }

    @Test
    public void testRowIdString() {
        ju.setRowIdString("testing");
        assertEquals("testing", ju.getRowIdString());
    }

    @Test
    public void testPrimaryKeyValue() {
        ju.setPrimarKeyValue("primeKey");
        assertEquals("primeKey", ju.getPrimarKeyValue());
    }

    @Test
    public void testGenUpdatePreparedQueryObj() throws Exception {
        JsonUpdate ju = Mockito.spy(JsonUpdate.class);
        MultivaluedMap<String, String> rowParams = Mockito.mock(MultivaluedMap.class);
 
        ju.setKeyspaceName("keyspace");
        ju.setTableName("table");
        ju.setPrimarKeyValue("primaryKeyValue");
        Map<String, String> consistencyInfo = new HashMap<>();
        consistencyInfo.put("type", "critical");
        ju.setConsistencyInfo(consistencyInfo);
        Map<String, Object> values = new HashMap<>();
        values.put("col1", "val1");
        ju.setValues(values);
        
        TableMetadata tmd = Mockito.mock(TableMetadata.class);
        Mockito.doReturn(tmd).when(ju).returnColumnMetadata(Mockito.anyString(), Mockito.anyString());
        ColumnMetadata cmd = Mockito.mock(ColumnMetadata.class);
        Mockito.when(tmd.getColumn("col1")).thenReturn(cmd);
        List<ColumnMetadata> colList = new ArrayList<>();
        colList.add(cmd);
        Mockito.when(tmd.getPrimaryKey()).thenReturn(colList);
        Mockito.when(cmd.getType()).thenReturn(DataType.varchar());
        
        RowIdentifier rowId = Mockito.mock(RowIdentifier.class);
        Mockito.doReturn(rowId).when(ju).getRowIdentifier(Mockito.anyString(), Mockito.anyString(), Mockito.any(),
                Mockito.any());
        
        Mockito.when(rowId.getRowIdString()).thenReturn("col1");
        Mockito.when(rowId.getPrimaryKeyValue()).thenReturn("val1");

        
        assertEquals("UPDATE keyspace.table  SET vector_ts=?,col1= ? WHERE col1;",
                ju.genUpdatePreparedQueryObj(rowParams).getQuery());
    }
}
