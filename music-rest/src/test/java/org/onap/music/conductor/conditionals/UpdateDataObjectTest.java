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

package org.onap.music.conductor.conditionals;

import static org.junit.Assert.assertEquals;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.onap.music.datastore.PreparedQueryObject;

public class UpdateDataObjectTest {

    UpdateDataObject updateDataObject;
    Map<String, PreparedQueryObject> queryBank; 
    Map<String, String> cascadeColumnValues;
    
    @Before
    public void setup() {
        updateDataObject = new UpdateDataObject();
        queryBank = Mockito.mock(Map.class);
        cascadeColumnValues = Mockito.mock(Map.class);
    }
    
    @Test
    public void testSetQueryBank() {
        updateDataObject.setQueryBank(queryBank);
        assertEquals(queryBank, updateDataObject.getQueryBank());
    }
    
    @Test
    public void testSetKeyspace() {
        updateDataObject.setKeyspace("keyspace");
        assertEquals("keyspace", updateDataObject.getKeyspace());
    }
    
    @Test
    public void testSetTableName() {
        updateDataObject.setTableName("table");
        assertEquals("table", updateDataObject.getTableName());
    }
    
    @Test
    public void testSetPrimaryKey() {
        updateDataObject.setPrimaryKey("primarykey");
        assertEquals("primarykey", updateDataObject.getPrimaryKey());
    }
    
    @Test
    public void testSetPrimaryKeyValue() {
        updateDataObject.setPrimaryKeyValue("primarykeyvalue");
        assertEquals("primarykeyvalue", updateDataObject.getPrimaryKeyValue());
    }
    
    @Test
    public void testSetPlanId() {
        updateDataObject.setPlanId("planid");
        assertEquals("planid", updateDataObject.getPlanId());
    }
    
    @Test
    public void testSetCascadeColumnName() {
        updateDataObject.setCascadeColumnName("columnname");
        assertEquals("columnname", updateDataObject.getCascadeColumnName());
    }
    
    @Test
    public void testSetCascadeColumnValues() {
        updateDataObject.setCascadeColumnValues(cascadeColumnValues);
        assertEquals(cascadeColumnValues, updateDataObject.getCascadeColumnValues());
    }
    
    @Test
    public void testSetLockId() {
        updateDataObject.setLockId("lockid");
        assertEquals("lockid", updateDataObject.getLockId());
    }
}
