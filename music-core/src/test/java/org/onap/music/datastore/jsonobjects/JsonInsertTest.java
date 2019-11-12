/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
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
 */

package org.onap.music.datastore.jsonobjects;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
//import org.mockito.internal.util.reflection.Whitebox;
import com.datastax.driver.core.TableMetadata;


public class JsonInsertTest {
    
    JsonInsert ji = new JsonInsert();

    @Test
    public void testGetKeyspaceName() {
        ji.setKeyspaceName("keyspace");
        assertEquals("keyspace",ji.getKeyspaceName());
    }

    @Test
    public void testGetTableName() {
        ji.setTableName("table");
        assertEquals("table",ji.getTableName());
    }

    @Test
    public void testGetConsistencyInfo() {
        Map<String,String> cons = new HashMap<>();
        cons.put("test","true");
        ji.setConsistencyInfo(cons);
        assertEquals("true",ji.getConsistencyInfo().get("test"));
    }

    @Test
    public void testGetTtl() {
        ji.setTtl("ttl");
        assertEquals("ttl",ji.getTtl());
    }

    @Test
    public void testGetTimestamp() {
        ji.setTimestamp("10:30");
        assertEquals("10:30",ji.getTimestamp());
    }

    @Test
    public void testGetValues() {
        Map<String,Object> cons = new HashMap<>();
        cons.put("val1","one");
        cons.put("val2","two");
        ji.setValues(cons);
        assertEquals("one",ji.getValues().get("val1"));
    }

    @Test
    public void testGetRowSpecification() {
        Map<String,Object> cons = new HashMap<>();
        cons.put("val1","one");
        cons.put("val2","two");
        ji.setRowSpecification(cons);
        assertEquals("two",ji.getRowSpecification().get("val2"));
    }

    @Test
    public void testSerialize() {
        Map<String,Object> cons = new HashMap<>();
        cons.put("val1","one");
        cons.put("val2","two");
        ji.setTimestamp("10:30");
        ji.setRowSpecification(cons);
        byte[] test1 = ji.serialize();
        byte[] ji1 = SerializationUtils.serialize(ji);
        assertArrayEquals(ji1,test1);
    }

    @Test
    public void testObjectMap()
    {
        Map<String, byte[]> map = new HashMap<>();
        ji.setObjectMap(map);
        assertEquals(map, ji.getObjectMap());
    }
    
    @Test
    public void testPrimaryKey() {
        ji.setPrimaryKeyVal("primKey");
        assertEquals("primKey", ji.getPrimaryKeyVal());
    }
    
    @Test
    public void testGenInsertPreparedQueryObj() throws Exception {
        ji.setKeyspaceName("keyspace");
        ji.setTableName("table");
        ji.setPrimaryKeyVal("value");
        Map<String,Object> rowSpec = new HashMap<>();
        rowSpec.put("val1","one");
        rowSpec.put("val2","two");
        ji.setRowSpecification(rowSpec);
        Map<String,Object> vals = new HashMap<>();
        vals.put("val1","one");
        vals.put("val2","two");
        ji.setValues(vals);
        
        Map<String,String> cons = new HashMap<>();
        cons.put("type","quorum");
        ji.setConsistencyInfo(cons);
        
        MusicDataStore mds = Mockito.mock(MusicDataStore.class);
        Session session = Mockito.mock(Session.class);
        Mockito.when(mds.getSession()).thenReturn(session);
        MusicDataStoreHandle mdsh = Mockito.mock(MusicDataStoreHandle.class);
        FieldSetter.setField(mdsh, mdsh.getClass().getDeclaredField("mDstoreHandle"), mds);
        TableMetadata tableMeta = Mockito.mock(TableMetadata.class);
        Mockito.when(mds.returnColumnMetadata(Mockito.anyString(), Mockito.anyString()))
            .thenReturn(tableMeta);
        
        ColumnMetadata cmd = Mockito.mock(ColumnMetadata.class);
        List<ColumnMetadata> listcmd = new ArrayList<>();
        listcmd.add(cmd);
        Mockito.when(tableMeta.getPrimaryKey()).thenReturn(listcmd);
        Mockito.when(cmd.getName()).thenReturn("val1");
        Mockito.when(tableMeta.getColumn("val1")).thenReturn(cmd);
        Mockito.when(tableMeta.getColumn("val2")).thenReturn(cmd);
        Mockito.when(cmd.getType()).thenReturn(DataType.text());
        
        PreparedQueryObject query = ji.genInsertPreparedQueryObj();
        System.out.println(query.getQuery());
        System.out.println(query.getValues());


        assertEquals("INSERT INTO keyspace.table (vector_ts,val2,val1) VALUES (?,?,?);", query.getQuery());
        assertTrue(query.getValues().containsAll(vals.values()));
    }

}
