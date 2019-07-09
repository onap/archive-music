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
import java.util.UUID;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;

import org.onap.music.datastore.Condition;
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

@ApiModel(value = "JsonTable", description = "Json model for table update")
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonUpdate implements Serializable {
    private String keyspaceName;
    private String tableName;
    private transient Map<String, Object> values;
    private String ttl;
    private String timestamp;
    private Map<String, String> consistencyInfo;
    private transient Map<String, Object> conditions;
    private transient Map<String, Object> rowSpecification;
    private StringBuilder rowIdString;
    private String primarKeyValue;
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(JsonUpdate.class);

    @ApiModelProperty(value = "Conditions")
    public Map<String, Object> getConditions() {
        return conditions;
    }

    public void setConditions(Map<String, Object> conditions) {
        this.conditions = conditions;
    }

    @ApiModelProperty(value = "Information for selecting sepcific rows")
    public Map<String, Object> getRow_specification() {
        return rowSpecification;
    }

    public void setRow_specification(Map<String, Object> rowSpecification) {
        this.rowSpecification = rowSpecification;
    }


    @ApiModelProperty(value = "Keyspace name")
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

    @ApiModelProperty(value = "Time to live value")
    public String getTtl() {
        return ttl;
    }

    public void setTtl(String ttl) {
        this.ttl = ttl;
    }

    @ApiModelProperty(value = "Time stamp")
    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @ApiModelProperty(value = "Column values")
    public Map<String, Object> getValues() {
        return values;
    }

    public void setValues(Map<String, Object> values) {
        this.values = values;
    }
    
    public StringBuilder getRowIdString() {
        return rowIdString;
    }

    public void setRowIdString(StringBuilder rowIdString) {
        this.rowIdString = rowIdString;
    }

    public String getPrimarKeyValue() {
        return primarKeyValue;
    }

    public void setPrimarKeyValue(String primarKeyValue) {
        this.primarKeyValue = primarKeyValue;
    }

    public byte[] serialize() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
        } catch (IOException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e,AppMessages.IOERROR, ErrorSeverity.ERROR, ErrorTypes.DATAERROR);
        }
        return bos.toByteArray();
    }
    
    /**
     * Generate TableInsertQuery
     * @return
     * @throws MusicQueryException
     */
    public PreparedQueryObject genUpdatePreparedQueryObj(MultivaluedMap<String, String> rowParams) throws MusicQueryException {
        if (logger.isDebugEnabled()) {
            logger.debug("Coming inside genUpdatePreparedQueryObj method " + this.getKeyspaceName());
            logger.debug("Coming inside genUpdatePreparedQueryObj method " + this.getTableName());
        }
        
        PreparedQueryObject queryObject = new PreparedQueryObject();
        
         if((this.getKeyspaceName() == null || this.getKeyspaceName().isEmpty()) || 
                 (this.getTableName() == null || this.getTableName().isEmpty())){
             
             /*return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                 .setError("one or more path parameters are not set, please check and try again")
                 .toMap()).build();*/
             
             throw new MusicQueryException("one or more path parameters are not set, please check and try again",
                     Status.BAD_REQUEST.getStatusCode());
         }
         
         EELFLoggerDelegate.mdcPut("keyspace", "( "+this.getKeyspaceName()+" ) ");
         long startTime = System.currentTimeMillis();
         String operationId = UUID.randomUUID().toString();  // just for infoging purposes.
         String consistency = this.getConsistencyInfo().get("type");

         logger.info(EELFLoggerDelegate.applicationLogger, "--------------Music " + consistency
             + " update-" + operationId + "-------------------------");
         // obtain the field value pairs of the update
         
        Map<String, Object> valuesMap = this.getValues();

        TableMetadata tableInfo;
        
        try {
            tableInfo = MusicDataStoreHandle.returnColumnMetadata(this.getKeyspaceName(), this.getTableName());
        } catch (MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger,e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes
                .GENERALSERVICEERROR, e);
            /*return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();*/
            throw new MusicQueryException(e.getMessage(), Status.BAD_REQUEST.getStatusCode());
        }catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e, AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            throw new MusicQueryException(e.getMessage(), Status.BAD_REQUEST.getStatusCode());
        }
        
        if (tableInfo == null) {
            logger.error(EELFLoggerDelegate.errorLogger,"Table information not found. Please check input for table name= "+this.getTableName(), AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
           
            /*return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                .setError("Table information not found. Please check input for table name= "
                + this.getKeyspaceName() + "." + this.getTableName()).toMap()).build();*/
            
            throw new MusicQueryException("Table information not found. Please check input for table name= "
                    + this.getKeyspaceName() + "." + this.getTableName(), Status.BAD_REQUEST.getStatusCode());
        }
        
        String vectorTs = String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
        StringBuilder fieldValueString = new StringBuilder("vector_ts=?,");
        queryObject.addValue(vectorTs);
        int counter = 0;
        for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
            Object valueObj = entry.getValue();
            DataType colType = null;
            try {
                colType = tableInfo.getColumn(entry.getKey()).getType();
            } catch(NullPointerException ex) {
                logger.error(EELFLoggerDelegate.errorLogger, ex, "Invalid column name : "+entry.getKey(), ex);
                /*return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).
                 * setError("Invalid column name : "+entry.getKey()).toMap()).build();*/
                
                throw new MusicQueryException("Invalid column name : " + entry.getKey(),Status.BAD_REQUEST.getStatusCode());
            }
            Object valueString = null;
            try {
                valueString = MusicUtil.convertToActualDataType(colType, valueObj);
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.errorLogger,e);
            }
            fieldValueString.append(entry.getKey() + "= ?");
            queryObject.addValue(valueString);
            if (counter != valuesMap.size() - 1) {
                fieldValueString.append(",");
            }    
            counter = counter + 1;
        }
        String ttl = this.getTtl();
        String timestamp = this.getTimestamp();

        queryObject.appendQueryString("UPDATE " + this.getKeyspaceName() + "." + this.getTableName() + " ");
        if ((ttl != null) && (timestamp != null)) {
            logger.info("both there");
            queryObject.appendQueryString(" USING TTL ? AND TIMESTAMP ?");
            queryObject.addValue(Integer.parseInt(ttl));
            queryObject.addValue(Long.parseLong(timestamp));
        }

        if ((ttl != null) && (timestamp == null)) {
            logger.info("ONLY TTL there");
            queryObject.appendQueryString(" USING TTL ?");
            queryObject.addValue(Integer.parseInt(ttl));
        }

        if ((ttl == null) && (timestamp != null)) {
            logger.info("ONLY timestamp there");
            queryObject.appendQueryString(" USING TIMESTAMP ?");
            queryObject.addValue(Long.parseLong(timestamp));
        }
        
        // get the row specifier
        RowIdentifier rowId = null;
        try {
            rowId = getRowIdentifier(this.getKeyspaceName(), this.getTableName(), rowParams, queryObject);
            this.setRowIdString(rowId.rowIdString);
            this.setPrimarKeyValue(rowId.primarKeyValue);
            if(rowId == null || rowId.primarKeyValue.isEmpty()) {
                /*return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                        .setError("Mandatory WHERE clause is missing. Please check the input request.").toMap()).build();*/
                
                throw new MusicQueryException("Mandatory WHERE clause is missing. Please check the input request.", 
                        Status.BAD_REQUEST.getStatusCode());
            }
        } catch (MusicQueryException ex) {
             throw new MusicQueryException("Mandatory WHERE clause is missing. Please check the input request.", 
                     Status.BAD_REQUEST.getStatusCode());
            
        }catch (MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes
                .GENERALSERVICEERROR, ex);
            /*return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();*/
            
            throw new MusicQueryException(ex.getMessage(), Status.BAD_REQUEST.getStatusCode());
            
        }
        
        

        queryObject.appendQueryString(
            " SET " + fieldValueString + " WHERE " + rowId.rowIdString + ";");
            
        

        // get the conditional, if any
        Condition conditionInfo;
        if (this.getConditions() == null) {
            conditionInfo = null;
        } else {
            // to avoid parsing repeatedly, just send the select query to obtain row
            PreparedQueryObject selectQuery = new PreparedQueryObject();
            selectQuery.appendQueryString("SELECT *  FROM " + this.getKeyspaceName() + "." + this.getTableName() + " WHERE "
                + rowId.rowIdString + ";");
            selectQuery.addValue(rowId.primarKeyValue);
            conditionInfo = new Condition(this.getConditions(), selectQuery);
        }

        ReturnType operationResult = null;
        long jsonParseCompletionTime = System.currentTimeMillis();

        if(consistency.equalsIgnoreCase(MusicUtil.EVENTUAL) && this.getConsistencyInfo().get("consistency") != null) {
            if(MusicUtil.isValidConsistency(this.getConsistencyInfo().get("consistency"))) {
                queryObject.setConsistency(this.getConsistencyInfo().get("consistency"));
            } else {
                /*return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.SYNTAXERROR)
                        .setError("Invalid Consistency type").toMap()).build();*/
                
                logger.error("Invalid Consistency type");
                throw new MusicQueryException("Invalid Consistency type", Status.BAD_REQUEST.getStatusCode());
            }
        }
        
        queryObject.setOperation("update");
        
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
