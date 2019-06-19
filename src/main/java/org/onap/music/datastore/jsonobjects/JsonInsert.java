/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 *  Modifications Copyright (C) 2018 IBM.
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
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;

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

}
