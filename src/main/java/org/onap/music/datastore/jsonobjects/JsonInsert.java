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
import java.nio.ByteBuffer;
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
import org.onap.music.main.ReturnType;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "InsertTable", description = "Json model for table vlaues insert")
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonInsert implements Serializable {
    private static final long serialVersionUID = 1L;
    private String keyspaceName;
    private String tableName;
    private transient Map<String, Object> values;
    private String ttl;
    private String timestamp;
    private transient Map<String, Object> rowSpecification;
    private Map<String, String> consistencyInfo;
    private Map<String, byte[]> objectMap;
    private String primaryKeyVal;
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(JsonInsert.class);

    @ApiModelProperty(value = "objectMap",hidden = true)
    public Map<String, byte[]> getObjectMap() {
        return objectMap;
    }
    
    public void setObjectMap(Map<String, byte[]> objectMap) {
        this.objectMap = objectMap;
    }
    
    @ApiModelProperty(value = "keyspace")
    public String getKeyspaceName() {
        return keyspaceName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    @ApiModelProperty(value = "Table name")
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @ApiModelProperty(value = "Consistency level", allowableValues = "eventual,critical,atomic")
    public Map<String, String> getConsistencyInfo() {
        return consistencyInfo;
    }

    public void setConsistencyInfo(Map<String, String> consistencyInfo) {
        this.consistencyInfo = consistencyInfo;
    }

    @ApiModelProperty(value = "Columns and tables support an optional "
        + "expiration period called TTL (time-to-live) in seconds.",
        notes="TTL precision is one second, which is calculated by the coordinator "
        + "node. When using TTL, ensure that all nodes in the cluster have synchronized clocks.",allowEmptyValue = true)
    public String getTtl() {
        return ttl;
    }

    public void setTtl(String ttl) {
        this.ttl = ttl;
    }

    @ApiModelProperty(value = "Time stamp (epoch_in_microseconds)",
        notes = "Marks inserted data (write time) with TIMESTAMP. "
        + "Enter the time since epoch (January 1, 1970) in microseconds."
        + "By default, the actual time of write is used.", allowEmptyValue = true)
    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @ApiModelProperty(value = "Json Object of key/values", notes="Where key is the column name and value is the data value for that column.",
        example = "{'emp_id': 'df98a3d40cd6','emp_name': 'john',"
        + "'emp_salary': 50,'address':{'street' : '1 Some way','city' : 'New York'}}")
    public Map<String, Object> getValues() {
        return values;
    }

    public void setValues(Map<String, Object> values) {
        this.values = values;
    }

    @ApiModelProperty(value = "Information for selecting specific rows for insert",hidden = true)
    public Map<String, Object> getRowSpecification() {
        return rowSpecification;
    }

    public void setRowSpecification(Map<String, Object> rowSpecification) {
        this.rowSpecification = rowSpecification;
    }
    
    public String getPrimaryKeyVal() {
        return primaryKeyVal;
    }

    public void setPrimaryKeyVal(String primaryKeyVal) {
        this.primaryKeyVal = primaryKeyVal;
    }

    public byte[] serialize() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
        } catch (IOException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e, AppMessages.IOERROR, ErrorSeverity.ERROR, ErrorTypes.DATAERROR);
        }
        return bos.toByteArray();
    }
    
    /**
     * Generate TableInsertQuery
     * @return
     * @throws MusicQueryException
     */
    public PreparedQueryObject genInsertPreparedQueryObj() throws MusicQueryException {
        if (logger.isDebugEnabled()) {
            logger.debug("Coming inside genTableInsertQuery method " + this.getKeyspaceName());
            logger.debug("Coming inside genTableInsertQuery method " + this.getTableName());
        }

        PreparedQueryObject queryObject = new PreparedQueryObject();
        TableMetadata tableInfo = null;
        try {
            tableInfo = MusicDataStoreHandle.returnColumnMetadata(this.getKeyspaceName(), this.getTableName());
            if(tableInfo == null) {
                throw new MusicQueryException("Table name doesn't exists. Please check the table name.",
                        Status.BAD_REQUEST.getStatusCode());
            }
        } catch (MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
            throw new MusicQueryException(e.getMessage(),Status.BAD_REQUEST.getStatusCode());
            
        }
        String primaryKeyName = tableInfo.getPrimaryKey().get(0).getName();
        StringBuilder fieldsString = new StringBuilder("(vector_ts,");
        String vectorTs =
                        String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
        StringBuilder valueString = new StringBuilder("(" + "?" + ",");
        queryObject.addValue(vectorTs);
        
        Map<String, Object> valuesMap = this.getValues();
        if (valuesMap==null) {
            throw new MusicQueryException("Nothing to insert. No values provided in request.",
                    Status.BAD_REQUEST.getStatusCode());
        }
        int counter = 0;
        String primaryKey = "";
        for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
            fieldsString.append("" + entry.getKey());
            Object valueObj = entry.getValue();
            if (primaryKeyName.equals(entry.getKey())) {
                primaryKey = entry.getValue() + "";
                primaryKey = primaryKey.replace("'", "''");
            }
            DataType colType = null;
            try {
                colType = tableInfo.getColumn(entry.getKey()).getType();
            } catch(NullPointerException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage() +" Invalid column name : "+entry.getKey
                    (), AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR, ex);
                throw new MusicQueryException("Invalid column name : " + entry.getKey(),
                        Status.BAD_REQUEST.getStatusCode());
            }

            Object formattedValue = null;
            try {
                formattedValue = MusicUtil.convertToActualDataType(colType, valueObj);
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.errorLogger,e);
            }
            valueString.append("?");

            queryObject.addValue(formattedValue);

            if (counter == valuesMap.size() - 1) {
                fieldsString.append(")");
                valueString.append(")");
            } else {
                fieldsString.append(",");
                valueString.append(",");
            }
            counter = counter + 1;
        }

        //blobs..
        Map<String, byte[]> objectMap = this.getObjectMap();
        if(objectMap != null) {
            for (Map.Entry<String, byte[]> entry : objectMap.entrySet()) {
                if(counter > 0) {
                    fieldsString.replace(fieldsString.length()-1, fieldsString.length(), ",");
                    valueString.replace(valueString.length()-1, valueString.length(), ",");
                }
                fieldsString.append("" + entry.getKey());
                byte[] valueObj = entry.getValue();
                if (primaryKeyName.equals(entry.getKey())) {
                    primaryKey = entry.getValue() + "";
                    primaryKey = primaryKey.replace("'", "''");
                }
                DataType colType = tableInfo.getColumn(entry.getKey()).getType();
                ByteBuffer formattedValue = null;
                if(colType.toString().toLowerCase().contains("blob")) {
                    formattedValue = MusicUtil.convertToActualDataType(colType, valueObj);
                }
                valueString.append("?");
                queryObject.addValue(formattedValue);
                counter = counter + 1;
                fieldsString.append(",");
                valueString.append(",");
            } 
        }
        this.setPrimaryKeyVal(primaryKey);
        if(primaryKey == null || primaryKey.length() <= 0) {
            logger.error(EELFLoggerDelegate.errorLogger, "Some required partition key parts are missing: "+primaryKeyName );
            throw new MusicQueryException("Some required partition key parts are missing: " + primaryKeyName,
                    Status.BAD_REQUEST.getStatusCode());
        }

        fieldsString.replace(fieldsString.length()-1, fieldsString.length(), ")");
        valueString.replace(valueString.length()-1, valueString.length(), ")");

        queryObject.appendQueryString("INSERT INTO " + this.getKeyspaceName() + "." + this.getTableName() + " "
                        + fieldsString + " VALUES " + valueString);

        String ttl = this.getTtl();
        String timestamp = this.getTimestamp();

        if ((ttl != null) && (timestamp != null)) {
            logger.info(EELFLoggerDelegate.applicationLogger, "both there");
            queryObject.appendQueryString(" USING TTL ? AND TIMESTAMP ?");
            queryObject.addValue(Integer.parseInt(ttl));
            queryObject.addValue(Long.parseLong(timestamp));
        }

        if ((ttl != null) && (timestamp == null)) {
            logger.info(EELFLoggerDelegate.applicationLogger, "ONLY TTL there");
            queryObject.appendQueryString(" USING TTL ?");
            queryObject.addValue(Integer.parseInt(ttl));
        }

        if ((ttl == null) && (timestamp != null)) {
            logger.info(EELFLoggerDelegate.applicationLogger, "ONLY timestamp there");
            queryObject.appendQueryString(" USING TIMESTAMP ?");
            queryObject.addValue(Long.parseLong(timestamp));
        }

        queryObject.appendQueryString(";");

        String consistency = this.getConsistencyInfo().get("type");
        if(consistency.equalsIgnoreCase(MusicUtil.EVENTUAL) && this.getConsistencyInfo().get("consistency") != null) {
            if(MusicUtil.isValidConsistency(this.getConsistencyInfo().get("consistency"))) {
                queryObject.setConsistency(this.getConsistencyInfo().get("consistency"));
            } else {
                throw new MusicQueryException("Invalid Consistency type", Status.BAD_REQUEST.getStatusCode());
            }
        }
        queryObject.setOperation("insert");

        logger.info("Data insert Query ::::: " + queryObject.getQuery());

        return queryObject;
    }
    
    /**
     * 
     * @param rowParams
     * @return
     * @throws MusicQueryException
     */
    public PreparedQueryObject genSelectCriticalPreparedQueryObj(MultivaluedMap<String, String> rowParams) throws MusicQueryException {
        
        PreparedQueryObject queryObject = new PreparedQueryObject();
        
        if((this.getKeyspaceName() == null || this.getKeyspaceName().isEmpty()) 
                || (this.getTableName() == null || this.getTableName().isEmpty())){
            throw new MusicQueryException("one or more path parameters are not set, please check and try again",
                     Status.BAD_REQUEST.getStatusCode());
        }
        EELFLoggerDelegate.mdcPut("keyspace", "( "+this.getKeyspaceName()+" ) ");
        RowIdentifier rowId = null;
        try {
            rowId = getRowIdentifier(this.getKeyspaceName(), this.getTableName(), rowParams, queryObject);
            this.setPrimaryKeyVal(rowId.primarKeyValue);
        } catch (MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes
                .GENERALSERVICEERROR, ex);
            throw new MusicQueryException(ex.getMessage(), Status.BAD_REQUEST.getStatusCode());
        }
        
        queryObject.appendQueryString(
            "SELECT *  FROM " + this.getKeyspaceName() + "." + this.getTableName() + " WHERE " + rowId.rowIdString + ";");
        
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
