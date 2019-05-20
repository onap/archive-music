/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2019 IBM
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
import java.util.Map;

import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicServiceException;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@ApiModel(value = "CassaSelect", description = "Cass Select  Object")
public class CassaSelect {

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(CassaSelect.class);

    private String keyspaceName;
    private String tableName;
    private StringBuilder rowIdString;
    private int limit;

    @ApiModelProperty(value = "Keyspace Name")
    public String getKeyspaceName() {
        return keyspaceName;
    }

    public CassaSelect setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
        return this;
    }

    @ApiModelProperty(value = "Table Name")
    public String getTableName() {
        return tableName;
    }

    public CassaSelect setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    @ApiModelProperty(value = "Row id Value")
    public StringBuilder getRowIdString() {
        return rowIdString;
    }

    public CassaSelect setRowIdString(StringBuilder rowIdString) {
        this.rowIdString = rowIdString;
        return this;
    }

    @ApiModelProperty(value = "Limit value")
    public int getLimit() {
        return limit;
    }

    public CassaSelect setLimit(int limit) {
        this.limit = limit;
        return this;
    }

    private Map<String, String> consistencyInfo;

    public Map<String, String> getConsistencyInfo() {
        return consistencyInfo;
    }

    public CassaSelect setConsistencyInfo(Map<String, String> consistencyInfo) {
        this.consistencyInfo = consistencyInfo;
        return this;
    }

    public byte[] serialize() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
        } catch (IOException e) {
            logger.error("Error while serialize:: ", e);
        }
        return bos.toByteArray();
    }

    /**
     * genSelectQuery
     * 
     * @return
     */
    public PreparedQueryObject genSelectQuery() {
        PreparedQueryObject queryObject = new PreparedQueryObject();

        queryObject.appendQueryString("SELECT *  FROM " + this.getKeyspaceName() + "." + this.getTableName() + ";");

        return queryObject;
    }

    /**
     * genSpecificSelectQuery
     * 
     * @return
     * @throws MusicServiceException
     */
    public PreparedQueryObject genSpecificSelectQuery() throws MusicServiceException {

        PreparedQueryObject queryObject = new PreparedQueryObject();

        queryObject.appendQueryString(
                "SELECT *  FROM " + this.keyspaceName + "." + this.getTableName() + " WHERE " + this.getRowIdString());

        if (this.getLimit() != -1) {
            queryObject.appendQueryString(" LIMIT " + limit);
        }

        queryObject.appendQueryString(";");
        return queryObject;

    }
}
