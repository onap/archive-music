/*
 * ============LICENSE_START========================================== org.onap.music
 * =================================================================== Copyright (c) 2017 AT&T
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
 */
package org.onap.music.rest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonKeySpace;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicCore.Condition;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.response.jsonobjects.JsonResponse;

import com.att.eelf.configuration.EELFLogger;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Path("/v{version: [0-9]+}/keyspaces")
@Api(value = "Data Api")
public class RestMusicDataAPI {
    /*
     * Header values for Versioning X-minorVersion *** - Used to request or communicate a MINOR
     * version back from the client to the server, and from the server back to the client - This
     * will be the MINOR version requested by the client, or the MINOR version of the last MAJOR
     * version (if not specified by the client on the request) - Contains a single position value
     * (e.g. if the full version is 1.24.5, X-minorVersion = "24") - Is optional for the client on
     * request; however, this header should be provided if the client needs to take advantage of
     * MINOR incremented version functionality - Is mandatory for the server on response
     * 
     *** X-patchVersion *** - Used only to communicate a PATCH version in a response for
     * troubleshooting purposes only, and will not be provided by the client on request - This will
     * be the latest PATCH version of the MINOR requested by the client, or the latest PATCH version
     * of the MAJOR (if not specified by the client on the request) - Contains a single position
     * value (e.g. if the full version is 1.24.5, X-patchVersion = "5") - Is mandatory for the
     * server on response
     *
     *** X-latestVersion *** - Used only to communicate an API's latest version - Is mandatory for the
     * server on response, and shall include the entire version of the API (e.g. if the full version
     * is 1.24.5, X-latestVersion = "1.24.5") - Used in the response to inform clients that they are
     * not using the latest version of the API
     *
     */

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(RestMusicDataAPI.class);
    private static String xLatestVersion = "X-latestVersion";

    private class RowIdentifier {
        public String primarKeyValue;
        public StringBuilder rowIdString;
        @SuppressWarnings("unused")
        public PreparedQueryObject queryObject;// the string with all the row
                                               // identifiers separated by AND

        public RowIdentifier(String primaryKeyValue, StringBuilder rowIdString,
                        PreparedQueryObject queryObject) {
            this.primarKeyValue = primaryKeyValue;
            this.rowIdString = rowIdString;
            this.queryObject = queryObject;
        }
    }

    @SuppressWarnings("unused")
    private String buildVersion(String major, String minor, String patch) {
        if (minor != null) {
            major += "." + minor;
            if (patch != null) {
                major += "." + patch;
            }
        }
        return major;
    }

    /**
     * Create Keyspace REST
     * 
     * @param kspObject
     * @param keyspaceName
     * @return
     * @throws Exception
     */
    @POST
    @Path("/{name}")
    @ApiOperation(value = "Create Keyspace", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> createKeySpace(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam("X-minorVersion") String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam("X-patchVersion") String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam("ns") String ns,
                    @ApiParam(value = "userId",
                                    required = true) @HeaderParam("userId") String userId,
                    @ApiParam(value = "Password",
                                    required = true) @HeaderParam("password") String password,
                    JsonKeySpace kspObject,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("name") String keyspaceName,
                    @Context HttpServletResponse response) {
        Map<String, Object> authMap = CachingUtil.verifyOnboarding(ns, userId, password);
        response.addHeader(xLatestVersion, MusicUtil.getVersion());
        if (!authMap.isEmpty()) {
            return new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap();
        }
        if(kspObject == null || kspObject.getReplicationInfo() == null) {
        	authMap.put(ResultType.EXCEPTION.getResult(), ResultType.BODYMISSING.getResult());
    		return authMap;
    	}

        try {
        	authMap = MusicCore.autheticateUser(ns, userId, password, keyspaceName, aid,
			                "createKeySpace");
		} catch (Exception e) {
			logger.error(EELFLoggerDelegate.applicationLogger,
                        "Exception while authenting the user.");
			return new JsonResponse(ResultType.FAILURE).setError("Unable to authenticate.").toMap();
		}
        String newAid = null;
        if (!authMap.isEmpty()) {
            if (authMap.containsKey("aid")) {
                newAid = (String) authMap.get("aid");
            } else {
                return new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap();
            }
        }

        String consistency = MusicUtil.EVENTUAL;// for now this needs only
                                                // eventual consistency

        PreparedQueryObject queryObject = new PreparedQueryObject();
        long start = System.currentTimeMillis();
        Map<String, Object> replicationInfo = kspObject.getReplicationInfo();
        String repString = null;
        try {
            repString = "{" + MusicUtil.jsonMaptoSqlString(replicationInfo, ",") + "}";
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
        }
        queryObject.appendQueryString(
                        "CREATE KEYSPACE " + keyspaceName + " WITH replication = " + repString);
        if (kspObject.getDurabilityOfWrites() != null) {
            queryObject.appendQueryString(
                            " AND durable_writes = " + kspObject.getDurabilityOfWrites());
        }

        queryObject.appendQueryString(";");
        long end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,
                        "Time taken for setting up query in create keyspace:" + (end - start));

        ResultType result = ResultType.FAILURE;
        try {
            result = MusicCore.nonKeyRelatedPut(queryObject, consistency);
            logger.error(EELFLoggerDelegate.errorLogger, "resulta = " + result);
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
            return new JsonResponse(ResultType.FAILURE)
            		.setError("Couldn't create keyspace. Please make sure all the information is correct.").toMap();
        }

        if (result==ResultType.FAILURE) {
        	logger.info(EELFLoggerDelegate.applicationLogger, "Cannot create keyspace, cleaning up");
        	JsonResponse resultJson = new JsonResponse(ResultType.FAILURE);
            resultJson.setError("Keyspace already exists. Please contact admin.");
            if (authMap.get("uuid").equals("new")) {
                queryObject = new PreparedQueryObject();
                queryObject.appendQueryString(
                                "DELETE FROM admin.keyspace_master where uuid = " + newAid);
                queryObject.appendQueryString(";");
                try {
					MusicCore.nonKeyRelatedPut(queryObject, consistency);
				} catch (MusicServiceException e) {
					logger.error(EELFLoggerDelegate.errorLogger,
							"Error cleaning up createKeyspace. Cannot DELETE uuid. " + e.getMessage());
				}
                return resultJson.toMap();
            } else {
                queryObject = new PreparedQueryObject();
                queryObject.appendQueryString(
                                "UPDATE admin.keyspace_master SET keyspace_name=? where uuid = ?;");
                try {
                	queryObject.addValue(MusicUtil.convertToActualDataType(DataType.text(),
                                MusicUtil.DEFAULTKEYSPACENAME));
					queryObject.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), newAid));
				} catch (Exception e) {
					logger.error(EELFLoggerDelegate.errorLogger,
							"Error cleaning up createKeyspace. Cannot get correct data types" + e.getMessage());
				}
                try {
					 MusicCore.nonKeyRelatedPut(queryObject, consistency);
				} catch (MusicServiceException e) {
					logger.error(EELFLoggerDelegate.errorLogger, "Unable to process operation. Error: "+e.getMessage());
				}
                return resultJson.toMap();
            }
        }
        
        try {
            queryObject = new PreparedQueryObject();
            queryObject.appendQueryString("CREATE ROLE IF NOT EXISTS '" + userId
                            + "' WITH PASSWORD = '" + password + "' AND LOGIN = true;");
            MusicCore.nonKeyRelatedPut(queryObject, consistency);
            queryObject = new PreparedQueryObject();
            queryObject.appendQueryString("GRANT ALL PERMISSIONS on KEYSPACE " + keyspaceName
                                + " to '" + userId + "'");
            queryObject.appendQueryString(";");
            MusicCore.nonKeyRelatedPut(queryObject, consistency);
        } catch (Exception e) {
        	logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.UNKNOWNERROR,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
        }
        
        return new JsonResponse(ResultType.SUCCESS).toMap();
    }

    /**
     * 
     * @param kspObject
     * @param keyspaceName
     * @return
     * @throws Exception
     */
    @DELETE
    @Path("/{name}")
    @ApiOperation(value = "Delete Keyspace", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> dropKeySpace(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam("X-minorVersion") String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam("X-patchVersion") String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam("ns") String ns,
                    @ApiParam(value = "userId",
                                    required = true) @HeaderParam("userId") String userId,
                    @ApiParam(value = "Password",
                                    required = true) @HeaderParam("password") String password,
                    JsonKeySpace kspObject,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("name") String keyspaceName,
                    @Context HttpServletResponse response) throws Exception {
        Map<String, Object> authMap = MusicCore.autheticateUser(ns, userId, password,
                        keyspaceName, aid, "dropKeySpace");
        response.addHeader(xLatestVersion, MusicUtil.getVersion());
        if (authMap.containsKey("aid"))
        	authMap.remove("aid");
        if (!authMap.isEmpty()) {
            return authMap;
        }

        String consistency = MusicUtil.EVENTUAL;// for now this needs only
                                                // eventual
        // consistency
        String appName = CachingUtil.getAppName(keyspaceName);
        String uuid = CachingUtil.getUuidFromMusicCache(keyspaceName);
        PreparedQueryObject pQuery = new PreparedQueryObject();
        pQuery.appendQueryString(
                        "select  count(*) as count from admin.keyspace_master where application_name=? allow filtering;");
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        Row row = MusicCore.get(pQuery).one();
        long count = row.getLong(0);

        if (count == 0) {
            return new JsonResponse(ResultType.FAILURE).setError("Keyspace not found. Please make sure keyspace exists.").toMap();
        } else if (count == 1) {
            pQuery = new PreparedQueryObject();
            pQuery.appendQueryString(
                    "UPDATE admin.keyspace_master SET keyspace_name=? where uuid = ?;");
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(),
                    MusicUtil.DEFAULTKEYSPACENAME));
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
            MusicCore.nonKeyRelatedPut(pQuery, consistency);
        } else {
            pQuery = new PreparedQueryObject();
            pQuery.appendQueryString("delete from admin.keyspace_master where uuid = ?");
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
            MusicCore.nonKeyRelatedPut(pQuery, consistency);
        }

        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString("DROP KEYSPACE " + keyspaceName + ";");
        return new JsonResponse(MusicCore.nonKeyRelatedPut(queryObject, consistency)).toMap();
    }

    /**
     * 
     * @param tableObj
     * @param keyspace
     * @param tablename
     * @return
     * @throws Exception
     */
    @POST
    @Path("/{keyspace}/tables/{tablename}")
    @ApiOperation(value = "Create Table", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> createTable(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam("X-minorVersion") String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam("X-patchVersion") String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam("ns") String ns,
                    @ApiParam(value = "userId",
                                    required = true) @HeaderParam("userId") String userId,
                    @ApiParam(value = "Password",
                                    required = true) @HeaderParam("password") String password,
                    JsonTable tableObj,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",
                                    required = true) @PathParam("tablename") String tablename,
                    @Context HttpServletResponse response) throws Exception {
    	
        Map<String, Object> authMap = MusicCore.autheticateUser(ns, userId, password, keyspace,
                        aid, "createTable");
        response.addHeader(xLatestVersion, MusicUtil.getVersion());
        if (authMap.containsKey("aid"))
        	authMap.remove("aid");
        if (!authMap.isEmpty()) {
            return new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap();
        }
        String consistency = MusicUtil.EVENTUAL;
        // for now this needs only eventual consistency
        PreparedQueryObject queryObject = new PreparedQueryObject();
        // first read the information about the table fields
        Map<String, String> fields = tableObj.getFields();
        StringBuilder fieldsString = new StringBuilder("(vector_ts text,");
        int counter = 0;
        String primaryKey;
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            
            if (entry.getKey().equals("PRIMARY KEY")) {
            	if(! entry.getValue().contains("("))
            		primaryKey = entry.getValue();
            	else {
            		primaryKey = entry.getValue().substring(entry.getValue().indexOf('(') + 1);
            		primaryKey = primaryKey.substring(0, primaryKey.indexOf(')'));
            	}
            	fieldsString.append("" + entry.getKey() + " (" + primaryKey + ")");
            } else
            	fieldsString.append("" + entry.getKey() + " " + entry.getValue() + "");
            if (counter == fields.size() - 1)
                fieldsString.append(")");
            else
                fieldsString.append(",");
            counter = counter + 1;
        }
        // information about the name-value style properties
        Map<String, Object> propertiesMap = tableObj.getProperties();
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
                    value = "{" + MusicUtil.jsonMaptoSqlString(otMap, ",") + "}";
                }

                propertiesString.append(entry.getKey() + "=" + value + "");
                if (counter != propertiesMap.size() - 1)
                    propertiesString.append(" AND ");

                counter = counter + 1;
            }
        }

        queryObject.appendQueryString(
                        "CREATE TABLE " + keyspace + "." + tablename + " " + fieldsString);

        if (propertiesMap != null)
            queryObject.appendQueryString(" WITH " + propertiesString);

        queryObject.appendQueryString(";");
        ResultType result = ResultType.FAILURE;

        try {
            result = MusicCore.nonKeyRelatedPut(queryObject, consistency);
        } catch (MusicServiceException ex) {
        	response.setStatus(400);
            return new JsonResponse(result).toMap();
        }

        return new JsonResponse(result).toMap();
    }

    /**
     * 
     * @param keyspace
     * @param tablename
     * @param fieldName
     * @param info
     * @throws Exception
     */
    @POST
    @Path("/{keyspace}/tables/{tablename}/index/{field}")
    @ApiOperation(value = "Create Index", response = String.class)
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> createIndex(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam("X-minorVersion") String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam("X-patchVersion") String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam("ns") String ns,
                    @ApiParam(value = "userId",
                                    required = true) @HeaderParam("userId") String userId,
                    @ApiParam(value = "Password",
                                    required = true) @HeaderParam("password") String password,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",
                                    required = true) @PathParam("tablename") String tablename,
                    @ApiParam(value = "Field Name",
                                    required = true) @PathParam("field") String fieldName,
                    @Context UriInfo info, @Context HttpServletResponse response) throws Exception {
        Map<String, Object> authMap = MusicCore.autheticateUser(ns, userId, password, keyspace,
                        aid, "createIndex");
        response.addHeader(xLatestVersion, MusicUtil.getVersion());
        if (authMap.containsKey("aid"))
        	authMap.remove("aid");
        if (!authMap.isEmpty())
            return new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap();
        MultivaluedMap<String, String> rowParams = info.getQueryParameters();
        String indexName = "";
        if (rowParams.getFirst("index_name") != null)
            indexName = rowParams.getFirst("index_name");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString("Create index " + indexName + " if not exists on " + keyspace + "."
                        + tablename + " (" + fieldName + ");");
        
        ResultType result = ResultType.FAILURE;
        try {
            result = MusicCore.nonKeyRelatedPut(query, "eventual");
        } catch (MusicServiceException ex) {
            return new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap();
        }

        return new JsonResponse(result).toMap();
    }

    /**
     * 
     * @param insObj
     * @param keyspace
     * @param tablename
     * @return
     * @throws Exception
     */
    @POST
    @Path("/{keyspace}/tables/{tablename}/rows")
    @ApiOperation(value = "Insert Into Table", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> insertIntoTable(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam("X-minorVersion") String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam("X-patchVersion") String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam("ns") String ns,
                    @ApiParam(value = "userId",
                                    required = true) @HeaderParam("userId") String userId,
                    @ApiParam(value = "Password",
                                    required = true) @HeaderParam("password") String password,
                    JsonInsert insObj,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",
                                    required = true) @PathParam("tablename") String tablename,
                    @Context HttpServletResponse response) {
        Map<String, Object> authMap = null;
        try {
        	authMap = MusicCore.autheticateUser(ns, userId, password, keyspace,
                          aid, "insertIntoTable");
        } catch (Exception e) {
          logger.error(EELFLoggerDelegate.errorLogger,e.getMessage());
          return new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap();
        }
        response.addHeader(xLatestVersion, MusicUtil.getVersion());
        if (authMap.containsKey("aid"))
        	authMap.remove("aid");
        if (!authMap.isEmpty()) {
            return new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap();
        }

        Map<String, Object> valuesMap = insObj.getValues();
        PreparedQueryObject queryObject = new PreparedQueryObject();
        TableMetadata tableInfo = null;
		try {
			tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
			if(tableInfo == null) {
				return new JsonResponse(ResultType.FAILURE)
						.setError("Table name doesn't exists. Please check the table name.").toMap();
			}
		} catch (MusicServiceException e) {
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
			return new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap();
		}
        String primaryKeyName = tableInfo.getPrimaryKey().get(0).getName();
        StringBuilder fieldsString = new StringBuilder("(vector_ts,");
        String vectorTs =
                        String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
        StringBuilder valueString = new StringBuilder("(" + "?" + ",");
        queryObject.addValue(vectorTs);
        int counter = 0;
        String primaryKey = "";

        for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
            fieldsString.append("" + entry.getKey());
            Object valueObj = entry.getValue();
            if (primaryKeyName.equals(entry.getKey())) {
                primaryKey = entry.getValue() + "";
                primaryKey = primaryKey.replace("'", "''");
            }

            DataType colType = tableInfo.getColumn(entry.getKey()).getType();

            Object formattedValue = null;
            try {
              formattedValue = MusicUtil.convertToActualDataType(colType, valueObj);
            } catch (Exception e) {
              logger.error(EELFLoggerDelegate.errorLogger,e.getMessage());
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
        
        if(primaryKey == null || primaryKey.length() <= 0) {
        	logger.error(EELFLoggerDelegate.errorLogger, "Some required partition key parts are missing: "+primaryKeyName );
			return new JsonResponse(ResultType.SYNTAXERROR).setError("Some required partition key parts are missing: "+primaryKeyName).toMap();
        }

        queryObject.appendQueryString("INSERT INTO " + keyspace + "." + tablename + " "
                        + fieldsString + " VALUES " + valueString);

        String ttl = insObj.getTtl();
        String timestamp = insObj.getTimestamp();

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

        ReturnType result = null;
        String consistency = insObj.getConsistencyInfo().get("type");
        try {
            if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL)) {
                result = MusicCore.eventualPut(queryObject);
            } else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                String lockId = insObj.getConsistencyInfo().get("lockId");
                result = MusicCore.criticalPut(keyspace, tablename, primaryKey, queryObject, lockId,
                                null);
            } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
                result = MusicCore.atomicPut(keyspace, tablename, primaryKey, queryObject, null);

            }
            else if (consistency.equalsIgnoreCase(MusicUtil.ATOMICDELETELOCK)) {
                result = MusicCore.atomicPutWithDeleteLock(keyspace, tablename, primaryKey, queryObject, null);

            }
        } catch (Exception ex) {
        	logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
            return new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap();
        }
        
        if (result==null) {
        	return new JsonResponse(ResultType.FAILURE).setError("Null result - Please Contact admin").toMap();
        }
        return new JsonResponse(result.getResult()).toMap();
    }

    /**
     * 
     * @param insObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws Exception
     */
    @PUT
    @Path("/{keyspace}/tables/{tablename}/rows")
    @ApiOperation(value = "Update Table", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> updateTable(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam("X-minorVersion") String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam("X-patchVersion") String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam("ns") String ns,
                    @ApiParam(value = "userId",
                                    required = true) @HeaderParam("userId") String userId,
                    @ApiParam(value = "Password",
                                    required = true) @HeaderParam("password") String password,
                    JsonUpdate updateObj,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",
                                    required = true) @PathParam("tablename") String tablename,
                    @Context UriInfo info, @Context HttpServletResponse response) {
        Map<String, Object> authMap;
        try {
        	authMap = MusicCore.autheticateUser(ns, userId, password, keyspace,
                          aid, "updateTable");
        } catch (Exception e) {
          logger.error(EELFLoggerDelegate.errorLogger,e.getMessage());
          return new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap();
        }
        response.addHeader(xLatestVersion, MusicUtil.getVersion());
        if (authMap.containsKey("aid"))
        	authMap.remove("aid");
        if (!authMap.isEmpty()) {
            return new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap();
        }
        long startTime = System.currentTimeMillis();
        String operationId = UUID.randomUUID().toString();// just for infoging
                                                          // purposes.
        String consistency = updateObj.getConsistencyInfo().get("type");
        logger.info(EELFLoggerDelegate.applicationLogger, "--------------Music " + consistency
                        + " update-" + operationId + "-------------------------");
        // obtain the field value pairs of the update

        PreparedQueryObject queryObject = new PreparedQueryObject();
        Map<String, Object> valuesMap = updateObj.getValues();

        TableMetadata tableInfo;
		try {
			tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
		} catch (MusicServiceException e) {
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
			return new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap();
		}
        if (tableInfo == null) {
            return new JsonResponse(ResultType.FAILURE)
                            .setError("Table information not found. Please check input for table name= "
                                            + keyspace + "." + tablename).toMap();
        }
        String vectorTs =
                        String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
        StringBuilder fieldValueString = new StringBuilder("vector_ts=?,");
        queryObject.addValue(vectorTs);
        int counter = 0;
        for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
            Object valueObj = entry.getValue();
            DataType colType = tableInfo.getColumn(entry.getKey()).getType();
            Object valueString = null;
            try {
              valueString = MusicUtil.convertToActualDataType(colType, valueObj);
            } catch (Exception e) {
              logger.error(EELFLoggerDelegate.errorLogger,e.getMessage());
            }
            fieldValueString.append(entry.getKey() + "= ?");
            queryObject.addValue(valueString);
            if (counter != valuesMap.size() - 1)
                fieldValueString.append(",");
            counter = counter + 1;
        }
        String ttl = updateObj.getTtl();
        String timestamp = updateObj.getTimestamp();

        queryObject.appendQueryString("UPDATE " + keyspace + "." + tablename + " ");
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
            rowId = getRowIdentifier(keyspace, tablename, info.getQueryParameters(), queryObject);
            if(rowId == null || rowId.primarKeyValue.isEmpty()) {
            	
            	return new JsonResponse(ResultType.FAILURE)
            			.setError("Mandatory WHERE clause is missing. Please check the input request.").toMap();
            }
        } catch (MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage());
            return new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap();
        }

        queryObject.appendQueryString(
                        " SET " + fieldValueString + " WHERE " + rowId.rowIdString + ";");

        // get the conditional, if any
        Condition conditionInfo;
        if (updateObj.getConditions() == null)
            conditionInfo = null;
        else {// to avoid parsing repeatedly, just send the select query to
              // obtain row
            PreparedQueryObject selectQuery = new PreparedQueryObject();
            selectQuery.appendQueryString("SELECT *  FROM " + keyspace + "." + tablename + " WHERE "
                            + rowId.rowIdString + ";");
            selectQuery.addValue(rowId.primarKeyValue);
            conditionInfo = new MusicCore.Condition(updateObj.getConditions(), selectQuery);
        }

        ReturnType operationResult = null;
        long jsonParseCompletionTime = System.currentTimeMillis();

        if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL))
            operationResult = MusicCore.eventualPut(queryObject);
        else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
            String lockId = updateObj.getConsistencyInfo().get("lockId");
            operationResult = MusicCore.criticalPut(keyspace, tablename, rowId.primarKeyValue,
                            queryObject, lockId, conditionInfo);
        } else if (consistency.equalsIgnoreCase("atomic_delete_lock")) {
            // this function is mainly for the benchmarks
            try {
              operationResult = MusicCore.atomicPutWithDeleteLock(keyspace, tablename,
                              rowId.primarKeyValue, queryObject, conditionInfo);
            } catch (MusicLockingException e) {
                logger.error(EELFLoggerDelegate.errorLogger,e.getMessage());
                return new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap();
            }
        } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
            try {
              operationResult = MusicCore.atomicPut(keyspace, tablename, rowId.primarKeyValue,
                              queryObject, conditionInfo);
            } catch (MusicLockingException e) {
              logger.error(EELFLoggerDelegate.errorLogger,e.getMessage());
              return new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap();
            }
        }
        long actualUpdateCompletionTime = System.currentTimeMillis();

        long endTime = System.currentTimeMillis();
        String timingString = "Time taken in ms for Music " + consistency + " update-" + operationId
                        + ":" + "|total operation time:" + (endTime - startTime)
                        + "|json parsing time:" + (jsonParseCompletionTime - startTime)
                        + "|update time:" + (actualUpdateCompletionTime - jsonParseCompletionTime)
                        + "|";

        if (operationResult != null && operationResult.getTimingInfo() != null) {
            String lockManagementTime = operationResult.getTimingInfo();
            timingString = timingString + lockManagementTime;
        }
        logger.info(EELFLoggerDelegate.applicationLogger, timingString);
        
        if (operationResult==null) {
        	return new JsonResponse(ResultType.FAILURE).setError("Null result - Please Contact admin").toMap();
        }
        return new JsonResponse(operationResult.getResult()).toMap();
    }

    /**
     * 
     * @param delObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws Exception
     */
    @DELETE
    @Path("/{keyspace}/tables/{tablename}/rows")
    @ApiOperation(value = "Delete From table", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> deleteFromTable(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam("X-minorVersion") String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam("X-patchVersion") String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam("ns") String ns,
                    @ApiParam(value = "userId",
                                    required = true) @HeaderParam("userId") String userId,
                    @ApiParam(value = "Password",
                                    required = true) @HeaderParam("password") String password,
                    JsonDelete delObj,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",
                                    required = true) @PathParam("tablename") String tablename,
                    @Context UriInfo info, @Context HttpServletResponse response) {
        Map<String, Object> authMap = null;
		try {
			authMap = MusicCore.autheticateUser(ns, userId, password, keyspace,
			                aid, "deleteFromTable");
		} catch (Exception e) {
			return new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap();
		}
        response.addHeader(xLatestVersion, MusicUtil.getVersion());
        if (authMap.containsKey("aid"))
        	authMap.remove("aid");
        if (!authMap.isEmpty()) {
            return new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap();
        }
        if(delObj == null) {
			return new JsonResponse(ResultType.FAILURE).setError("Required HTTP Request body is missing.").toMap();
		}
        PreparedQueryObject queryObject = new PreparedQueryObject();
        StringBuilder columnString = new StringBuilder();

        int counter = 0;
        ArrayList<String> columnList = delObj.getColumns();
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
            rowId = getRowIdentifier(keyspace, tablename, info.getQueryParameters(), queryObject);
        } catch (MusicServiceException ex) {
            return new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap();
        }
        String rowSpec = rowId.rowIdString.toString();

        if ((columnList != null) && (!rowSpec.isEmpty())) {
            queryObject.appendQueryString("DELETE " + columnString + " FROM " + keyspace + "."
                            + tablename + " WHERE " + rowSpec + ";");
        }

        if ((columnList == null) && (!rowSpec.isEmpty())) {
            queryObject.appendQueryString("DELETE FROM " + keyspace + "." + tablename + " WHERE "
                            + rowSpec + ";");
        }

        if ((columnList != null) && (rowSpec.isEmpty())) {
            queryObject.appendQueryString(
                            "DELETE " + columnString + " FROM " + keyspace + "." + rowSpec + ";");
        }

        // get the conditional, if any
        Condition conditionInfo;
        if (delObj.getConditions() == null)
            conditionInfo = null;
        else {// to avoid parsing repeatedly, just send the select query to
              // obtain row
            PreparedQueryObject selectQuery = new PreparedQueryObject();
            selectQuery.appendQueryString("SELECT *  FROM " + keyspace + "." + tablename + " WHERE "
                            + rowId.rowIdString + ";");
            selectQuery.addValue(rowId.primarKeyValue);
            conditionInfo = new MusicCore.Condition(delObj.getConditions(), selectQuery);
        }

        String consistency = delObj.getConsistencyInfo().get("type");

        ReturnType operationResult = null;
        try {
	        if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL))
	            operationResult = MusicCore.eventualPut(queryObject);
	        else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
	            String lockId = delObj.getConsistencyInfo().get("lockId");
	            operationResult = MusicCore.criticalPut(keyspace, tablename, rowId.primarKeyValue,
	                            queryObject, lockId, conditionInfo);
	        } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
					operationResult = MusicCore.atomicPut(keyspace, tablename, rowId.primarKeyValue,
					                queryObject, conditionInfo);
	        }
	        else if (consistency.equalsIgnoreCase(MusicUtil.ATOMICDELETELOCK)) {
					operationResult = MusicCore.atomicPutWithDeleteLock(keyspace, tablename, rowId.primarKeyValue,
					                queryObject, conditionInfo);
	        }
        } catch (MusicLockingException e) {
			return new JsonResponse(ResultType.FAILURE)
					.setError("Unable to perform Delete operation. Exception from music").toMap();
		}
        if (operationResult==null) {
        	return new JsonResponse(ResultType.FAILURE).toMap();
        }
        return new JsonResponse(operationResult.getResult()).toMap();
    }

    /**
     * 
     * @param tabObj
     * @param keyspace
     * @param tablename
     * @throws Exception
     */
    @DELETE
    @Path("/{keyspace}/tables/{tablename}")
    @ApiOperation(value = "Drop Table", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> dropTable(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam("X-minorVersion") String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam("X-patchVersion") String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam("ns") String ns,
                    @ApiParam(value = "userId",
                                    required = true) @HeaderParam("userId") String userId,
                    @ApiParam(value = "Password",
                                    required = true) @HeaderParam("password") String password,
                    JsonTable tabObj,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",
                                    required = true) @PathParam("tablename") String tablename,
                    @Context HttpServletResponse response) throws Exception {
        Map<String, Object> authMap =
                        MusicCore.autheticateUser(ns, userId, password, keyspace, aid, "dropTable");
        response.addHeader(xLatestVersion, MusicUtil.getVersion());
        if (authMap.containsKey("aid"))
        	authMap.remove("aid");
        if (!authMap.isEmpty()) {
        	return new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap();
        }
        String consistency = "eventual";// for now this needs only eventual
                                        // consistency
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString("DROP TABLE  " + keyspace + "." + tablename + ";");
        try {
            return new JsonResponse(MusicCore.nonKeyRelatedPut(query, consistency)).toMap();
        } catch (MusicServiceException ex) {
            return new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap();
        }

    }

    /**
     * 
     * @param selObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     */
    @PUT
    @Path("/{keyspace}/tables/{tablename}/rows/criticalget")
    @ApiOperation(value = "Select Critical", response = Map.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> selectCritical(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam("X-minorVersion") String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam("X-patchVersion") String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam("ns") String ns,
                    @ApiParam(value = "userId",
                                    required = true) @HeaderParam("userId") String userId,
                    @ApiParam(value = "Password",
                                    required = true) @HeaderParam("password") String password,
                    JsonInsert selObj,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",
                                    required = true) @PathParam("tablename") String tablename,
                    @Context UriInfo info, @Context HttpServletResponse response) throws Exception {
        Map<String, Object> authMap = MusicCore.autheticateUser(ns, userId, password, keyspace,
                        aid, "selectCritical");
        response.addHeader(xLatestVersion, MusicUtil.getVersion());
        if (authMap.containsKey("aid"))
        	authMap.remove("aid");
        if (!authMap.isEmpty()) {
            logger.error("Error while authentication... ");
            return new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap();
        }
        String lockId = selObj.getConsistencyInfo().get("lockId");

        PreparedQueryObject queryObject = new PreparedQueryObject();

        RowIdentifier rowId = null;
        try {
            rowId = getRowIdentifier(keyspace, tablename, info.getQueryParameters(), queryObject);
        } catch (MusicServiceException ex) {
            return new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap();
        }
        queryObject.appendQueryString(
                        "SELECT *  FROM " + keyspace + "." + tablename + " WHERE " + rowId.rowIdString + ";");

        ResultSet results = null;

        String consistency = selObj.getConsistencyInfo().get("type");

        if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
            results = MusicCore.criticalGet(keyspace, tablename, rowId.primarKeyValue, queryObject,
                            lockId);
        } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
            results = MusicCore.atomicGet(keyspace, tablename, rowId.primarKeyValue, queryObject);
        }
        
        else if (consistency.equalsIgnoreCase(MusicUtil.ATOMICDELETELOCK)) {
            results = MusicCore.atomicGetWithDeleteLock(keyspace, tablename, rowId.primarKeyValue, queryObject);
        }

        return new JsonResponse(ResultType.SUCCESS).setDataResult(MusicCore.marshallResults(results)).toMap();
    }

    /**
     * 
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws Exception
     */
    @GET
    @Path("/{keyspace}/tables/{tablename}/rows")
    @ApiOperation(value = "Select All or Select Specific", response = Map.class)
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> select(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam("X-minorVersion") String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam("X-patchVersion") String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam("ns") String ns,
                    @ApiParam(value = "userId",
                                    required = true) @HeaderParam("userId") String userId,
                    @ApiParam(value = "Password",
                                    required = true) @HeaderParam("password") String password,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",
                                    required = true) @PathParam("tablename") String tablename,
                    @Context UriInfo info, @Context HttpServletResponse response) throws Exception {
        Map<String, Object> authMap =
                        MusicCore.autheticateUser(ns, userId, password, keyspace, aid, "select");
        response.addHeader(xLatestVersion, MusicUtil.getVersion());
        if (authMap.containsKey("aid"))
        	authMap.remove("aid");
        if (!authMap.isEmpty()) {
        	logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.AUTHENTICATIONERROR  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
        	return new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap();
        }
        PreparedQueryObject queryObject = new PreparedQueryObject();

        if (info.getQueryParameters().isEmpty())// select all
            queryObject.appendQueryString("SELECT *  FROM " + keyspace + "." + tablename + ";");
        else {
            int limit = -1; // do not limit the number of results
            try {
                queryObject = selectSpecificQuery(version, minorVersion, patchVersion, aid, ns,
                                userId, password, keyspace, tablename, info, limit);
            } catch (MusicServiceException ex) {
                return new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap();
            }
        }

        try {
            ResultSet results = MusicCore.get(queryObject);
            return new JsonResponse(ResultType.SUCCESS).setDataResult(MusicCore.marshallResults(results)).toMap();
        } catch (MusicServiceException ex) {
        	logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.UNKNOWNERROR  ,ErrorSeverity.ERROR, ErrorTypes.MUSICSERVICEERROR);
            return new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap();
        }

    }

    /**
     * 
     * @param keyspace
     * @param tablename
     * @param info
     * @param limit
     * @return
     * @throws MusicServiceException
     */
    public PreparedQueryObject selectSpecificQuery(String version, String minorVersion,
                    String patchVersion, String aid, String ns, String userId, String password,
                    String keyspace, String tablename, UriInfo info, int limit)
                    throws MusicServiceException {

        PreparedQueryObject queryObject = new PreparedQueryObject();
        StringBuilder rowIdString = getRowIdentifier(keyspace, tablename, info.getQueryParameters(),
                        queryObject).rowIdString;

        queryObject.appendQueryString(
                        "SELECT *  FROM " + keyspace + "." + tablename + " WHERE " + rowIdString);

        if (limit != -1) {
            queryObject.appendQueryString(" LIMIT " + limit);
        }

        queryObject.appendQueryString(";");
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
        TableMetadata tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
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
            DataType colType = tableInfo.getColumn(entry.getKey()).getType();
            Object formattedValue = null;
            try {
              formattedValue = MusicUtil.convertToActualDataType(colType, indValue);
            } catch (Exception e) {
              logger.error(EELFLoggerDelegate.errorLogger,e.getMessage());
            }
            primaryKey.append(indValue);
            rowSpec.append(keyName + "= ?");
            queryObject.addValue(formattedValue);
            if (counter != rowParams.size() - 1)
                rowSpec.append(" AND ");
            counter = counter + 1;
        }
        return new RowIdentifier(primaryKey.toString(), rowSpec, queryObject);
    }
}
