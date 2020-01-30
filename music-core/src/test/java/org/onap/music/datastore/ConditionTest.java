/*******************************************************************************
 * ============LICENSE_START========================================== org.onap.music
 * =================================================================== Copyright (c) 2019 AT&T
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
 *******************************************************************************/

package org.onap.music.datastore;

import org.onap.music.datastore.Condition;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicServiceException;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.Spy;

public class ConditionTest {

    @Spy
    private Condition condition;
    private Map<String, Object> conditions;
    private PreparedQueryObject selectQueryForTheRow;

    @Before
    public void setup() {
        conditions = Mockito.mock(Map.class);
        selectQueryForTheRow = Mockito.mock(PreparedQueryObject.class);
        condition = spy(new Condition(conditions, selectQueryForTheRow));
    }

    @Test
    public void testCondition() throws Exception {
        ResultSet rs = Mockito.mock(ResultSet.class);
        Row row = Mockito.mock(Row.class);
        MusicDataStore dsHandle = Mockito.mock(MusicDataStore.class);
        Mockito.when(rs.one()).thenReturn(row);
        Mockito.doReturn(rs).when(condition).quorumGet(Mockito.any());
        boolean result = false;
        Mockito.when(dsHandle.doesRowSatisfyCondition(Mockito.any(), Mockito.any())).thenReturn(true);
        Mockito.doReturn(dsHandle).when(condition).getDSHandle();
        result = condition.testCondition();
        assertEquals(true, result);
    }
}
