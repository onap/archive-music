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

import java.io.IOException;
import java.io.OutputStream;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.StreamingOutput;
import org.json.JSONObject;
import org.onap.music.datastore.MusicDataStore;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonSelect;
import org.onap.music.main.MusicUtil;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.ColumnDefinitions.Definition;

public class FeedReturnStreamingOutput implements StreamingOutput {

    JsonSelect jsonInsertObj;
    MultivaluedMap<String, String> rowParams;

    public FeedReturnStreamingOutput(JsonSelect jsonInsertObj, MultivaluedMap<String, String> rowParams) {
        this.jsonInsertObj = jsonInsertObj;
        this.rowParams = rowParams;
    }

    @Override
    public void write(OutputStream output) throws IOException, WebApplicationException {
        PreparedQueryObject queryObject;
        ResultSet results = null;
        MusicDataStore ds = new MusicDataStore();

        try {
            queryObject = jsonInsertObj.genSelectQuery(rowParams);
            
            SimpleStatement statement = new SimpleStatement(queryObject.getQuery(), queryObject.getValues().toArray());
            statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
            statement.setFetchSize(MusicUtil.getCassandraFetchSize());
            
            results = ds.getSession().execute(statement);

            output.write("[".getBytes());
            for (Row row : results) {
                ColumnDefinitions colInfo = row.getColumnDefinitions();
                JSONObject json = new JSONObject();
                for (Definition definition : colInfo) {
                    if (!(("vector_ts").equals(definition.getName()))) {
                        if (definition.getType().toString().toLowerCase().contains("blob")) {
                            json.put(definition.getName(),
                                    ds.getBlobValue(row, definition.getName(), definition.getType()));
                        } else {
                            json.put(definition.getName(),
                                    ds.getColValue(row, definition.getName(), definition.getType()));
                        }
                    }
                }
                output.write(json.toString().getBytes()); 
                if (results.getAvailableWithoutFetching() == 0) {
                    if (!results.isFullyFetched()) {
                        results.fetchMoreResults();
                    }
                    output.flush();
                }

                if (!results.isExhausted()) {
                    output.write(",".getBytes());
                }
            }
            output.write("]".getBytes());
            output.flush();
            
        } catch (Exception e) {
            
        }

    }
}
