/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 *  Modifications Copyright (C) 2019 IBM 
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;

import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicUtil;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonSelect implements Serializable {
    private Map<String, String> consistencyInfo;
    private String keyspaceName;
    private String tableName;
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(JsonSelect.class);



    public Map<String, String> getConsistencyInfo() {
        return consistencyInfo;
    }

    public void setConsistencyInfo(Map<String, String> consistencyInfo) {
        this.consistencyInfo = consistencyInfo;
    }
    
    public String getKeyspaceName() {
        return keyspaceName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public byte[] serialize() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
        } catch (IOException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), e);
        }
        return bos.toByteArray();
    }
    
    /**
     * genSelectQuery
     * 
     * @return
     * @throws MusicQueryException 
     */
    public PreparedQueryObject genSelectQuery(MultivaluedMap<String, String> rowParams) throws MusicQueryException {
        
        if((this.getKeyspaceName() == null || this.getKeyspaceName().isEmpty()) 
                || (this.getTableName() == null || this.getTableName().isEmpty())){
            throw new MusicQueryException("one or more path parameters are not set, please check and try again",
                     Status.BAD_REQUEST.getStatusCode());
        }
        EELFLoggerDelegate.mdcPut("keyspace", "( " + this.getKeyspaceName() + " ) ");
        PreparedQueryObject queryObject = new PreparedQueryObject();

        if (rowParams.isEmpty()) { // select all
            queryObject.appendQueryString("SELECT *  FROM " + this.getKeyspaceName() + "." + this.getTableName() + ";");
        } else {
            int limit = -1; // do not limit the number of results
            try {
                queryObject = selectSpecificQuery(this.getKeyspaceName(), this.getTableName(), rowParams, limit);
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger, ex, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN,
                    ErrorTypes.GENERALSERVICEERROR, ex);
                
                throw new MusicQueryException(ex.getMessage(), Status.BAD_REQUEST.getStatusCode());
            }
        }

        return queryObject;
    }
    
    public PreparedQueryObject selectSpecificQuery(String keyspace,
            String tablename, MultivaluedMap<String, String> rowParams, int limit)
            throws MusicServiceException {
            PreparedQueryObject queryObject = new PreparedQueryObject();
            StringBuilder rowIdString = getRowIdentifier(keyspace, 
                tablename,rowParams,queryObject).rowIdString;
            queryObject.appendQueryString(
                "SELECT *  FROM " + keyspace + "." + tablename + " WHERE " + rowIdString);
            if (limit != -1) {
                queryObject.appendQueryString(" LIMIT " + limit);
            }
            queryObject.appendQueryString(";");
            return queryObject;
    }
    
    private class RowIdentifier {
        public String primarKeyValue;
        public StringBuilder rowIdString;
        @SuppressWarnings("unused")
        public PreparedQueryObject queryObject; // the string with all the row
                                                // identifiers separated by AND

        public RowIdentifier(String primaryKeyValue, StringBuilder rowIdString,
                        PreparedQueryObject queryObject) {
            this.primarKeyValue = primaryKeyValue;
            this.rowIdString = rowIdString;
            this.queryObject = queryObject;
        }
    }
    
    /**
    *
    * @param keyspace
    * @param tablename
    * @param rowParams
    * @param queryObject
    * @return
    * @throws MusicServiceException
    */
   private RowIdentifier getRowIdentifier(String keyspace, String tablename,
       MultivaluedMap<String, String> rowParams, PreparedQueryObject queryObject)
       throws MusicServiceException {
       StringBuilder rowSpec = new StringBuilder();
       int counter = 0;
       TableMetadata tableInfo = MusicDataStoreHandle.returnColumnMetadata(keyspace, tablename);
       if (tableInfo == null) {
           logger.error(EELFLoggerDelegate.errorLogger,
               "Table information not found. Please check input for table name= "
               + keyspace + "." + tablename);
           throw new MusicServiceException(
               "Table information not found. Please check input for table name= "
               + keyspace + "." + tablename);
       }
       StringBuilder primaryKey = new StringBuilder();
       for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()) {
           String keyName = entry.getKey();
           List<String> valueList = entry.getValue();
           String indValue = valueList.get(0);
           DataType colType = null;
           Object formattedValue = null;
           try {
               colType = tableInfo.getColumn(entry.getKey()).getType();
               formattedValue = MusicUtil.convertToActualDataType(colType, indValue);
           } catch (Exception e) {
               logger.error(EELFLoggerDelegate.errorLogger,e);
           }
           if(tableInfo.getPrimaryKey().get(0).getName().equals(entry.getKey())) {
               primaryKey.append(indValue);
           }
           rowSpec.append(keyName + "= ?");
           queryObject.addValue(formattedValue);
           if (counter != rowParams.size() - 1) {
               rowSpec.append(" AND ");
           }
           counter = counter + 1;
       }
       return new RowIdentifier(primaryKey.toString(), rowSpec, queryObject);
    }

}
