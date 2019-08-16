/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2019 Samsung
 * ===================================================================
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

import java.util.List;
import java.util.Map;

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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "JsonTable", description = "Json model for delete")
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonDelete {

    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(JsonDelete.class);
    
    private List<String> columns = null;
    private Map<String, String> consistencyInfo;
    private Map<String, Object> conditions;
    private String ttl;
    private String timestamp;
    private String keyspaceName;
    private String tableName;
    private StringBuilder rowIdString;
    private String primarKeyValue;


    @ApiModelProperty(value = "Conditions")
    public Map<String, Object> getConditions() {
        return conditions;
    }

    public void setConditions(Map<String, Object> conditions) {
        this.conditions = conditions;
    }

    @ApiModelProperty(value = "Consistency level", allowableValues = "eventual,critical,atomic")
    public Map<String, String> getConsistencyInfo() {
        return consistencyInfo;
    }

    public void setConsistencyInfo(Map<String, String> consistencyInfo) {
        this.consistencyInfo = consistencyInfo;
    }

    @ApiModelProperty(value = "Column values")
    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }


    @ApiModelProperty(value = "Time to live information")
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

    
    public PreparedQueryObject genDeletePreparedQueryObj(MultivaluedMap<String, String> rowParams) throws MusicQueryException {
        if (logger.isDebugEnabled()) {
            logger.debug("Coming inside genUpdatePreparedQueryObj method " + this.getKeyspaceName());
            logger.debug("Coming inside genUpdatePreparedQueryObj method " + this.getTableName());
        }
        
        PreparedQueryObject queryObject = new PreparedQueryObject();
        
        if((this.getKeyspaceName() == null || this.getKeyspaceName().isEmpty()) 
                || (this.getTableName() == null || this.getTableName().isEmpty())){

            
            throw new MusicQueryException("one or more path parameters are not set, please check and try again",
                     Status.BAD_REQUEST.getStatusCode());
        }
        
        EELFLoggerDelegate.mdcPut("keyspace", "( "+this.getKeyspaceName()+" ) ");
        
        if(this == null) {
            logger.error(EELFLoggerDelegate.errorLogger,"Required HTTP Request body is missing.", AppMessages.MISSINGDATA  ,ErrorSeverity.WARN, ErrorTypes.DATAERROR);

            throw new MusicQueryException("Required HTTP Request body is missing.",
                     Status.BAD_REQUEST.getStatusCode());
        }
        StringBuilder columnString = new StringBuilder();

        int counter = 0;
        List<String> columnList = this.getColumns();
        if (columnList != null) {
            for (String column : columnList) {
                columnString.append(column);
                if (counter != columnList.size() - 1)
                    columnString.append(",");
                counter = counter + 1;
            }
        }

        // get the row specifier
        RowIdentifier rowId = null;
        try {
            rowId = getRowIdentifier(this.getKeyspaceName(), this.getTableName(), rowParams, queryObject);
            this.setRowIdString(rowId.rowIdString);
            this.setPrimarKeyValue(rowId.primarKeyValue);
            if(rowId == null || rowId.primarKeyValue.isEmpty()) {

                throw new MusicQueryException("Mandatory WHERE clause is missing. Please check the input request.", 
                        Status.BAD_REQUEST.getStatusCode());
            }
        } catch (MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes
                .GENERALSERVICEERROR, ex);
            /*return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();*/
            throw new MusicQueryException(AppMessages.UNKNOWNERROR.toString(), Status.BAD_REQUEST.getStatusCode());
        }
        String rowSpec = rowId.rowIdString.toString();

        if ((columnList != null) && (!rowSpec.isEmpty())) {
            queryObject.appendQueryString("DELETE " + columnString + " FROM " + this.getKeyspaceName() + "."
                            + this.getTableName() + " WHERE " + rowSpec + ";");
        }

        if ((columnList == null) && (!rowSpec.isEmpty())) {
            queryObject.appendQueryString("DELETE FROM " + this.getKeyspaceName() + "." + this.getTableName() + " WHERE "
                            + rowSpec + ";");
        }

        if ((columnList != null) && (rowSpec.isEmpty())) {
            queryObject.appendQueryString(
                            "DELETE " + columnString + " FROM " + this.getKeyspaceName() + "." + rowSpec + ";");
        }
        // get the conditional, if any
        Condition conditionInfo;
        if (this.getConditions() == null) {
            conditionInfo = null;
        } else {
            // to avoid parsing repeatedly, just send the select query to
            // obtain row
            PreparedQueryObject selectQuery = new PreparedQueryObject();
            selectQuery.appendQueryString("SELECT *  FROM " + this.getKeyspaceName() + "." + this.getTableName() + " WHERE "
                + rowId.rowIdString + ";");
            selectQuery.addValue(rowId.primarKeyValue);
            conditionInfo = new Condition(this.getConditions(), selectQuery);
        }

        String consistency = this.getConsistencyInfo().get("type");


        if(consistency.equalsIgnoreCase(MusicUtil.EVENTUAL) && this.getConsistencyInfo().get("consistency")!=null) {
            if(MusicUtil.isValidConsistency(this.getConsistencyInfo().get("consistency"))) {
                queryObject.setConsistency(this.getConsistencyInfo().get("consistency"));
            } else {
                throw new MusicQueryException("Invalid Consistency type", Status.BAD_REQUEST.getStatusCode());
            }
        }
        
        queryObject.setOperation("delete");
        
        return queryObject;
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
   
    private class RowIdentifier {
       private String primarKeyValue;
       private StringBuilder rowIdString;
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
}
