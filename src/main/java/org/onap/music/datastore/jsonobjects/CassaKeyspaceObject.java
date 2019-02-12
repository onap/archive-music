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

import java.util.Map;

import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.main.MusicUtil;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@ApiModel(value = "CassaKeyspaceObject", description = "Keyspace Object")
public class CassaKeyspaceObject {

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(CassaKeyspaceObject.class);

    private String keyspaceName;
    private Map<String, Object> replicationInfo;
    private Map<String, String> consistencyInfo;
    private boolean durabilityOfWrites;

    @ApiModelProperty(value = "Keyspace name")
    public String getKeyspaceName() {
        return keyspaceName;
    }

    public CassaKeyspaceObject setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
        return this;
    }

    @ApiModelProperty(value = "Replication information")
    public Map<String, Object> getReplicationInfo() {
        return replicationInfo;
    }

    public CassaKeyspaceObject setReplicationInfo(Map<String, Object> replicationInfo) {
        this.replicationInfo = replicationInfo;
        return this;
    }

    @ApiModelProperty(value = "Consistency level", allowableValues = "eventual,critical,atomic")
    public Map<String, String> getConsistencyInfo() {
        return consistencyInfo;
    }

    public CassaKeyspaceObject setConsistencyInfo(Map<String, String> consistencyInfo) {
        this.consistencyInfo = consistencyInfo;
        return this;
    }

    @ApiModelProperty(value = "Durability", allowableValues = "true,false")
    public boolean isDurabilityOfWrites() {
        return durabilityOfWrites;
    }

    public CassaKeyspaceObject setDurabilityOfWrites(boolean durabilityOfWrites) {
        this.durabilityOfWrites = durabilityOfWrites;
        return this;
    }

    /**
     * Will generate query to create Keyspacce.
     */
    public PreparedQueryObject genKeyspaceQuery() {

        if (logger.isDebugEnabled()) {
            logger.debug("Came inside createKeyspace method");
        }

        String keyspaceName = this.getKeyspaceName();
        boolean durabilityOfWrites = this.isDurabilityOfWrites();

        if (logger.isDebugEnabled()) {
            logger.debug("keyspaceName ::" + keyspaceName);
            logger.debug("class :: " + this.getReplicationInfo().get("class"));
            logger.debug("replication_factor :: " + this.getReplicationInfo().get("replication_factor"));
            logger.debug("durabilityOfWrites :: " + durabilityOfWrites);
        }

        PreparedQueryObject queryObject = new PreparedQueryObject();
        long start = System.currentTimeMillis();
        Map<String, Object> replicationInfo = this.getReplicationInfo();
        String repString = null;
        try {
            repString = "{" + MusicUtil.jsonMaptoSqlString(replicationInfo, ",") + "}";
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.MISSINGDATA,
                    ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);

        }
        queryObject.appendQueryString("CREATE KEYSPACE " + keyspaceName + " WITH replication = " + repString);
        if (this.isDurabilityOfWrites()) {
            queryObject.appendQueryString(" AND durable_writes = " + this.isDurabilityOfWrites());
        }

        queryObject.appendQueryString(";");
        long end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Time taken for setting up query in create keyspace:" + (end - start));

        return queryObject;
    }

    /**
     * Will generate Query to drop a keyspace.
     * 
     * @return
     */
    public PreparedQueryObject genDropKeyspaceQuery() {
        if (logger.isDebugEnabled()) {
            logger.debug("Coming inside dropKeyspace method");
        }

        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString("DROP KEYSPACE " + this.getKeyspaceName() + ";");

        return queryObject;
    }

    @Override
    public String toString() {
        return "CassaKeyspaceObject [keyspaceName=" + keyspaceName + ", replicationInfo=" + replicationInfo
                + ", consistencyInfo=" + consistencyInfo + ", durabilityOfWrites=" + durabilityOfWrites + "]";
    }

}
