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

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import java.io.OutputStream;
import java.util.Iterator;
import javax.ws.rs.core.MultivaluedMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.onap.music.datastore.jsonobjects.JsonSelect;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

public class FeedReturnStreamingOutputTest {

    
    @Before
    public void setup() {
    
    }

    @Test
    public void testSelectStream() throws Exception {
        MusicDataStore ds = Mockito.mock(MusicDataStore.class);
        SimpleStatement statement = Mockito.mock(SimpleStatement.class);
        FeedReturnStreamingOutput outputStreamSpy = Mockito.spy(FeedReturnStreamingOutput.class);
        doReturn(ds).when(outputStreamSpy).getMusicDataStore();
        doReturn(statement).when(outputStreamSpy).getSimpleStatement(Mockito.any());
        Session session = Mockito.mock(Session.class);
        Mockito.when(ds.getSession()).thenReturn(session);
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(session.execute(Mockito.any(SimpleStatement.class))).thenReturn(rs);
        OutputStream output = Mockito.mock(OutputStream.class);
        doNothing().when(output).write(Mockito.any());
        doNothing().when(output).flush();
        Row row = Mockito.mock(Row.class);
        ColumnDefinitions colInfo = Mockito.mock(ColumnDefinitions.class);
        Iterator<Definition> iterator = Mockito.mock(Iterator.class);
        Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        Definition def1 = Mockito.mock(Definition.class);
        Definition def2 = Mockito.mock(Definition.class);
        Mockito.when(def1.getName()).thenReturn("def1");
        Mockito.when(def2.getName()).thenReturn("def2");
        Mockito.when(def1.getType()).thenReturn(DataType.blob());
        Mockito.when(def2.getType()).thenReturn(DataType.text());
        Mockito.when(iterator.next()).thenReturn(def1).thenReturn(def2);
        Mockito.when(colInfo.iterator()).thenReturn(iterator);
        Mockito.when(row.getColumnDefinitions()).thenReturn(colInfo);
        Mockito.when(ds.getBlobValue(Mockito.any(), Mockito.anyString(), Mockito.any())).thenReturn("def1".getBytes());
        Mockito.when(ds.getColValue(Mockito.any(), Mockito.anyString(), Mockito.any())).thenReturn("def2");
        Iterator<Row> rowIterator = Mockito.mock(Iterator.class);
        Mockito.when(rs.iterator()).thenReturn(rowIterator);
        Mockito.when(rowIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(rowIterator.next()).thenReturn(row).thenReturn(row).thenReturn(row);
        Mockito.when(rs.isFullyFetched()).thenReturn(true);
        Mockito.when(rs.isExhausted()).thenReturn(false);
        outputStreamSpy.write(output);
    }
    
}
