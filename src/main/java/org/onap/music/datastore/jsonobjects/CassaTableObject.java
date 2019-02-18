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

import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.main.MusicUtil;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "JsonTable", description = "Defines the Json for Creating a new Table.")
@JsonIgnoreProperties(ignoreUnknown = true)
public class CassaTableObject {
	
	private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(CassaTableObject.class);
	
    private String keyspaceName;
    private String tableName;

    private Map<String, String> fields;
    private Map<String, Object> properties;
    private String primaryKey;
    private String sortingKey;
    private String partitionKey;
    private String clusteringKey;
    private String filteringKey;
    private String clusteringOrder;
    private Map<String, String> consistencyInfo;

    @ApiModelProperty(value = "Consistency level", allowableValues = "eventual,critical,atomic")
    public Map<String, String> getConsistencyInfo() {
        return consistencyInfo;
    }

    public void setConsistencyInfo(Map<String, String> consistencyInfo) {
        this.consistencyInfo = consistencyInfo;
    }

    @ApiModelProperty(value = "Properties")
    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    @ApiModelProperty(value = "Fields")
    public Map<String, String> getFields() {
        return fields;
    }

    public void setFields(Map<String, String> fields) {
        this.fields = fields;
    }

    @ApiModelProperty(value = "KeySpace Name")
    public String getKeyspaceName() {
        return keyspaceName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    @ApiModelProperty(value = "Table Name")
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @ApiModelProperty(value = "Sorting Key")
    public String getSortingKey() {
        return sortingKey;
    }

    public void setSortingKey(String sortingKey) {
        this.sortingKey = sortingKey;
    }

    @ApiModelProperty(value = "Clustering Order", notes = "")
    public String getClusteringOrder() {
        return clusteringOrder;
    }

    public void setClusteringOrder(String clusteringOrder) {
        this.clusteringOrder = clusteringOrder;
    }

    @ApiModelProperty(value = "Primary Key")
    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getClusteringKey() {
        return clusteringKey;
    }

    public void setClusteringKey(String clusteringKey) {
        this.clusteringKey = clusteringKey;
    }

    public String getFilteringKey() {
        return filteringKey;
    }

    public void setFilteringKey(String filteringKey) {
        this.filteringKey = filteringKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    
    public PreparedQueryObject genCreateTableQuery() throws MusicQueryException {
    	if (logger.isDebugEnabled()) {
			logger.debug("Coming inside genCreateTableQuery method "+this.getKeyspaceName());
			logger.debug("Coming inside genCreateTableQuery method "+this.getTableName());
		}
    	
        String primaryKey = null;
        String partitionKey = this.getPartitionKey();
        String clusterKey = this.getClusteringKey();
        String filteringKey = this.getFilteringKey();
        if (filteringKey != null) {
            clusterKey = clusterKey + "," + filteringKey;
        }
        primaryKey = this.getPrimaryKey(); // get primaryKey if available

        PreparedQueryObject queryObject = new PreparedQueryObject();
        // first read the information about the table fields
        Map<String, String> fields = this.getFields();
        StringBuilder fieldsString = new StringBuilder("(vector_ts text,");
        int counter = 0;
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            if (entry.getKey().equals("PRIMARY KEY")) {
                primaryKey = entry.getValue(); // replaces primaryKey
                primaryKey = primaryKey.trim();
            } else {
                if (counter == 0)
                    fieldsString.append("" + entry.getKey() + " " + entry.getValue() + "");
                else
                    fieldsString.append("," + entry.getKey() + " " + entry.getValue() + "");
            }

            if (counter != (fields.size() - 1)) {

                counter = counter + 1;
            } else {

                if ((primaryKey != null) && (partitionKey == null)) {
                    primaryKey = primaryKey.trim();
                    int count1 = StringUtils.countMatches(primaryKey, ')');
                    int count2 = StringUtils.countMatches(primaryKey, '(');
                    if (count1 != count2) {
                        /*return response.status(Status.BAD_REQUEST)
                                .entity(new JsonResponse(ResultType.FAILURE).setError(
                                        "Create Table Error: primary key '(' and ')' do not match, primary key="
                                                + primaryKey)
                                        .toMap())
                                .build();*/
                    	
                    	throw new MusicQueryException("Create Table Error: primary key '(' and ')' do not match, primary key="
                                + primaryKey, Status.BAD_REQUEST.getStatusCode());
                    }

                    if (primaryKey.indexOf('(') == -1
                            || (count2 == 1 && (primaryKey.lastIndexOf(')') + 1) == primaryKey.length())) {
                        if (primaryKey.contains(",")) {
                            partitionKey = primaryKey.substring(0, primaryKey.indexOf(','));
                            partitionKey = partitionKey.replaceAll("[\\(]+", "");
                            clusterKey = primaryKey.substring(primaryKey.indexOf(',') + 1); // make sure index
                            clusterKey = clusterKey.replaceAll("[)]+", "");
                        } else {
                            partitionKey = primaryKey;
                            partitionKey = partitionKey.replaceAll("[\\)]+", "");
                            partitionKey = partitionKey.replaceAll("[\\(]+", "");
                            clusterKey = "";
                        }
                    } else { // not null and has ) before the last char
                        partitionKey = primaryKey.substring(0, primaryKey.indexOf(')'));
                        partitionKey = partitionKey.replaceAll("[\\(]+", "");
                        partitionKey = partitionKey.trim();
                        clusterKey = primaryKey.substring(primaryKey.indexOf(')'));
                        clusterKey = clusterKey.replaceAll("[\\(]+", "");
                        clusterKey = clusterKey.replaceAll("[\\)]+", "");
                        clusterKey = clusterKey.trim();
                        if (clusterKey.indexOf(',') == 0)
                            clusterKey = clusterKey.substring(1);
                        clusterKey = clusterKey.trim();
                        if (clusterKey.equals(","))
                            clusterKey = ""; // print error if needed ( ... ),)
                    }

                    if (!(partitionKey.isEmpty() || clusterKey.isEmpty())
                            && (partitionKey.equalsIgnoreCase(clusterKey) || clusterKey.contains(partitionKey)
                                    || partitionKey.contains(clusterKey))) {
                        logger.error("DataAPI createTable partition/cluster key ERROR: partitionKey=" + partitionKey
                                + ", clusterKey=" + clusterKey + " and primary key=" + primaryKey);
                        /*return response.status(Status.BAD_REQUEST)
                                .entity(new JsonResponse(ResultType.FAILURE)
                                        .setError("Create Table primary key error: clusterKey(" + clusterKey
                                                + ") equals/contains/overlaps partitionKey(" + partitionKey
                                                + ")  of" + " primary key=" + primaryKey)
                                        .toMap())
                                .build();*/
                        
                        throw new MusicQueryException("Create Table primary key error: clusterKey(" + clusterKey
                                + ") equals/contains/overlaps partitionKey(" + partitionKey
                                + ")  of" + " primary key=" + primaryKey, Status.BAD_REQUEST.getStatusCode());
                    }

                    if (partitionKey.isEmpty())
                        primaryKey = "";
                    else if (clusterKey.isEmpty())
                        primaryKey = " (" + partitionKey + ")";
                    else
                        primaryKey = " (" + partitionKey + ")," + clusterKey;

                    if (primaryKey != null)
                        fieldsString.append(", PRIMARY KEY (" + primaryKey + " )");

                } // end of length > 0
                else {
                    if (!(partitionKey.isEmpty() || clusterKey.isEmpty())
                            && (partitionKey.equalsIgnoreCase(clusterKey) || clusterKey.contains(partitionKey)
                                    || partitionKey.contains(clusterKey))) {
                        logger.error("DataAPI createTable partition/cluster key ERROR: partitionKey=" + partitionKey
                                + ", clusterKey=" + clusterKey);
                        /*return response.status(Status.BAD_REQUEST)
                                .entity(new JsonResponse(ResultType.FAILURE)
                                        .setError("Create Table primary key error: clusterKey(" + clusterKey
                                                + ") equals/contains/overlaps partitionKey(" + partitionKey + ")")
                                        .toMap())
                                .build();*/
                        
                        throw new MusicQueryException("Create Table primary key error: clusterKey(" + clusterKey
                                + ") equals/contains/overlaps partitionKey(" + partitionKey + ")", Status.BAD_REQUEST.getStatusCode());
                    }

                    if (partitionKey.isEmpty())
                        primaryKey = "";
                    else if (clusterKey.isEmpty())
                        primaryKey = " (" + partitionKey + ")";
                    else
                        primaryKey = " (" + partitionKey + ")," + clusterKey;

                    if (primaryKey != null)
                        fieldsString.append(", PRIMARY KEY (" + primaryKey + " )");
                }
                fieldsString.append(")");

            } // end of last field check

        } // end of for each
            // information about the name-value style properties
        Map<String, Object> propertiesMap = this.getProperties();
        StringBuilder propertiesString = new StringBuilder();
        if (propertiesMap != null) {
            counter = 0;
            for (Map.Entry<String, Object> entry : propertiesMap.entrySet()) {
                Object ot = entry.getValue();
                String value = ot + "";
                if (ot instanceof String) {
                    value = "'" + value + "'";
                } else if (ot instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> otMap = (Map<String, Object>) ot;
                    try {
						value = "{" + MusicUtil.jsonMaptoSqlString(otMap, ",") + "}";
					} catch (Exception e) {
						throw new MusicQueryException("Error while converting josnMap to String", Status.BAD_REQUEST.getStatusCode());
					}
                }

                propertiesString.append(entry.getKey() + "=" + value + "");
                if (counter != propertiesMap.size() - 1)
                    propertiesString.append(" AND ");

                counter = counter + 1;
            }
        }

        String clusteringOrder = this.getClusteringOrder();

        if (clusteringOrder != null && !(clusteringOrder.isEmpty())) {
            String[] arrayClusterOrder = clusteringOrder.split("[,]+");

            for (int i = 0; i < arrayClusterOrder.length; i++) {
                String[] clusterS = arrayClusterOrder[i].trim().split("[ ]+");
                if ((clusterS.length == 2)
                        && (clusterS[1].equalsIgnoreCase("ASC") || clusterS[1].equalsIgnoreCase("DESC"))) {
                    continue;
                } else {
                    /*return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(
                            "createTable/Clustering Order vlaue ERROR: valid clustering order is ASC or DESC or expecting colname  order; please correct clusteringOrder:"
                                    + clusteringOrder + ".")
                            .toMap()).build();*/
                	
                	throw new MusicQueryException("createTable/Clustering Order vlaue ERROR: valid clustering order is ASC or DESC or expecting colname  order; please correct clusteringOrder:"
                            + clusteringOrder + ".", Status.BAD_REQUEST.getStatusCode());
                }
                // add validation for column names in cluster key
            }

            if (!(clusterKey.isEmpty())) {
                clusteringOrder = "CLUSTERING ORDER BY (" + clusteringOrder + ")";
                // cjc check if propertiesString.length() >0 instead propertiesMap
                if (propertiesMap != null) {
                    propertiesString.append(" AND  " + clusteringOrder);
                } else {
                    propertiesString.append(clusteringOrder);
                }
            } else {
                logger.warn("Skipping clustering order=(" + clusteringOrder + ") since clustering key is empty ");
            }
        } // if non empty

        queryObject.appendQueryString("CREATE TABLE " + this.getKeyspaceName() + "." + this.getTableName() + " " + fieldsString);

        if (propertiesString != null && propertiesString.length() > 0)
            queryObject.appendQueryString(" WITH " + propertiesString);
        queryObject.appendQueryString(";");
        
        return queryObject;
    }
    
    /**
     * 
     * @return
     */
    public PreparedQueryObject genCreateShadowLockingTableQuery() {
    	if (logger.isDebugEnabled()) {
			logger.debug("Coming inside genCreateShadowLockingTableQuery method "+this.getKeyspaceName());
			logger.debug("Coming inside genCreateShadowLockingTableQuery method "+this.getTableName());
		}
    	
    	String tableName = "unsyncedKeys_"+this.getTableName();
        String tabQuery = "CREATE TABLE IF NOT EXISTS "+this.getKeyspaceName()+"."+tableName+ " ( key text,PRIMARY KEY (key) );";
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(tabQuery);
        
    	return queryObject;
    }
    
    /**
     * genDropTableQuery
     * @return PreparedQueryObject
     */
    public PreparedQueryObject genDropTableQuery() {
    	if (logger.isDebugEnabled()) {
			logger.debug("Coming inside genDropTableQuery method "+this.getKeyspaceName());
			logger.debug("Coming inside genDropTableQuery method "+this.getTableName());
		}
    	
		PreparedQueryObject query = new PreparedQueryObject();
		query.appendQueryString("DROP TABLE  " + this.getKeyspaceName() + "." + this.getTableName() + ";");
		logger.info("Delete Query ::::: "+query.getQuery());
		
		return query;
    }

}
