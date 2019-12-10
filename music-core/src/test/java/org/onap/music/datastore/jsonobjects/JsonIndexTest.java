/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
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

import static org.junit.Assert.*;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.onap.music.datastore.PreparedQueryObject;

public class JsonIndexTest {

    JsonIndex ji = null;
    
    
    @Before
    public void init() {
        ji = new JsonIndex(null, null, null, null);
    }
                    
                
    
    @Test
    public void testKeyspace() {
        ji.setKeyspaceName("keyspaceName");
        assertEquals("keyspaceName", ji.getKeyspaceName());
    }
    
    @Test
    public void testIndexName() {
        ji.setIndexName("indexName");
        assertEquals("indexName", ji.getIndexName());
    }
    
    @Test
    public void testFieldName() {
        ji.setFieldName("field");
        assertEquals("field", ji.getFieldName());
    }
    
    @Test
    public void testTableName() {
        ji.setTableName("table");
        assertEquals("table", ji.getTableName());
    }
    
    @Test
    public void testCreateIndexQuery() {
        JsonIndex ji2 = new JsonIndex("index", "keyspace", "table", "field");
        PreparedQueryObject query = ji2.genCreateIndexQuery();
        assertEquals("Create index if not exists index on keyspace.table (field);", query.getQuery());
    }
}
