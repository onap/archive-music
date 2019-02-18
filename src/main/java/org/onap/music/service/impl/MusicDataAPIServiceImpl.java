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
package org.onap.music.service.impl;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.mindrot.jbcrypt.BCrypt;
import org.onap.music.authentication.MusicAuthentication;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.CassaKeyspaceObject;
import org.onap.music.datastore.jsonobjects.CassaTableObject;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.response.jsonobjects.JsonResponse;
import org.onap.music.service.MusicDataAPIService;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public class MusicDataAPIServiceImpl implements MusicDataAPIService {

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicDataAPIServiceImpl.class);

    @SuppressWarnings("deprecation")
    @Override
    public Response createKeySpace(String version, boolean keyspace_active, String minorVersion, String patchVersion,
            String authorization, String aid, String ns, CassaKeyspaceObject kspObject, String keyspaceName) {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspaceName + " ) ");
            logger.info(EELFLoggerDelegate.applicationLogger, "In Create Keyspace " + keyspaceName);
            if (keyspace_active) {
                logger.info(EELFLoggerDelegate.applicationLogger, "Creating Keyspace " + keyspaceName);
                Map<String, String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
                String userId = userCredentials.get(MusicUtil.USERID);
                String password = userCredentials.get(MusicUtil.PASSWORD);
                Map<String, Object> authMap = CachingUtil.verifyOnboarding(ns, userId, password);
                if (!authMap.isEmpty()) {
                    logger.error(EELFLoggerDelegate.errorLogger, authMap.get("Exception").toString(),
                            AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL, ErrorTypes.AUTHENTICATIONERROR);
                    response.status(Status.UNAUTHORIZED);
                    return response.entity(new JsonResponse(ResultType.FAILURE)
                            .setError(String.valueOf(authMap.get("Exception"))).toMap()).build();
                }
                if (kspObject == null || kspObject.getReplicationInfo() == null) {
                    response.status(Status.BAD_REQUEST);
                    return response.entity(
                            new JsonResponse(ResultType.FAILURE).setError(ResultType.BODYMISSING.getResult()).toMap())
                            .build();
                }

                try {
                    authMap = MusicAuthentication.autheticateUser(ns, userId, password, keyspaceName, aid,
                            "createKeySpace");
                } catch (Exception e) {
                    logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.MISSINGDATA,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
                    response.status(Status.BAD_REQUEST);
                    return response
                            .entity(new JsonResponse(ResultType.FAILURE).setError("Unable to authenticate.").toMap())
                            .build();
                }
                String newAid = null;
                if (!authMap.isEmpty()) {
                    if (authMap.containsKey("aid")) {
                        newAid = (String) authMap.get("aid");
                    } else {
                        logger.error(EELFLoggerDelegate.errorLogger, String.valueOf(authMap.get("Exception")),
                                AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL, ErrorTypes.AUTHENTICATIONERROR);
                        response.status(Status.UNAUTHORIZED);
                        return response.entity(new JsonResponse(ResultType.FAILURE)
                                .setError(String.valueOf(authMap.get("Exception"))).toMap()).build();
                    }
                }

                String consistency = MusicUtil.EVENTUAL;// for now this needs only eventual consistency
                /**
                 * Keyspace creation goes here.
                 */
                ResultType result = ResultType.FAILURE;
                try {
                    kspObject.setKeyspaceName(keyspaceName);
                    result = MusicCore.createKeyspace(kspObject);
                    logger.info(EELFLoggerDelegate.applicationLogger, "result = " + result);
                } catch (MusicServiceException ex) {
                    logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(), AppMessages.UNKNOWNERROR,
                            ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
                    return response.status(Status.BAD_REQUEST)
                            .entity(new JsonResponse(ResultType.FAILURE).setError("err:" + ex.getMessage()).toMap())
                            .build();
                }

                PreparedQueryObject queryObject = new PreparedQueryObject();
                try {
                    queryObject = new PreparedQueryObject();
                    queryObject.appendQueryString("CREATE ROLE IF NOT EXISTS '" + userId + "' WITH PASSWORD = '"
                            + password + "' AND LOGIN = true;");
                    MusicCore.nonKeyRelatedPut(queryObject, consistency);
                    queryObject = new PreparedQueryObject();
                    queryObject.appendQueryString(
                            "GRANT ALL PERMISSIONS on KEYSPACE " + keyspaceName + " to '" + userId + "'");
                    queryObject.appendQueryString(";");
                    MusicCore.nonKeyRelatedPut(queryObject, consistency);
                } catch (Exception e) {
                    logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.UNKNOWNERROR,
                            ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
                }

                try {
                    boolean isAAF = Boolean.valueOf(CachingUtil.isAAFApplication(ns));
                    String hashedpwd = BCrypt.hashpw(password, BCrypt.gensalt());
                    queryObject = new PreparedQueryObject();
                    queryObject.appendQueryString(
                            "INSERT into admin.keyspace_master (uuid, keyspace_name, application_name, is_api, "
                                    + "password, username, is_aaf) values (?,?,?,?,?,?,?)");
                    queryObject.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), newAid));
                    queryObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), keyspaceName));
                    queryObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), ns));
                    queryObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), "True"));
                    queryObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), hashedpwd));
                    queryObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
                    queryObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));
                    CachingUtil.updateMusicCache(keyspaceName, ns);
                    CachingUtil.updateMusicValidateCache(ns, userId, hashedpwd);
                    MusicCore.eventualPut(queryObject);
                } catch (Exception e) {
                    logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.UNKNOWNERROR,
                            ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
                    return response.status(Response.Status.BAD_REQUEST)
                            .entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
                }

                return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS)
                        .setMessage("Keyspace " + keyspaceName + " Created").toMap()).build();
            } else {
                String vError = "Keyspace Creation no longer supported after versions 3.2.x. Contact DBA to create the keyspace.";
                logger.info(EELFLoggerDelegate.applicationLogger, vError);
                logger.error(EELFLoggerDelegate.errorLogger, vError, AppMessages.UNKNOWNERROR, ErrorSeverity.WARN,
                        ErrorTypes.MUSICSERVICEERROR);
                return response.status(Response.Status.BAD_REQUEST)
                        .entity(new JsonResponse(ResultType.FAILURE).setError(vError).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public Response dropKeySpace(String version, boolean keyspace_active, String minorVersion, String patchVersion,
            String authorization, String aid, String ns, CassaKeyspaceObject kspObject, String keyspaceName)
            throws Exception {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspaceName + " ) ");
            logger.info(EELFLoggerDelegate.applicationLogger, "In Drop Keyspace " + keyspaceName);
            if (keyspace_active) {
                Map<String, String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
                String userId = userCredentials.get(MusicUtil.USERID);
                String password = userCredentials.get(MusicUtil.PASSWORD);
                Map<String, Object> authMap = MusicAuthentication.autheticateUser(ns, userId, password, keyspaceName,
                        aid, "dropKeySpace");
                if (authMap.containsKey("aid"))
                    authMap.remove("aid");
                if (!authMap.isEmpty()) {
                    logger.error(EELFLoggerDelegate.errorLogger, authMap.get("Exception").toString(),
                            AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL, ErrorTypes.AUTHENTICATIONERROR);
                    response.status(Status.UNAUTHORIZED);
                    return response.entity(new JsonResponse(ResultType.FAILURE)
                            .setError(String.valueOf(authMap.get("Exception"))).toMap()).build();
                }

                String consistency = MusicUtil.EVENTUAL;// for now this needs only eventual consistency
                String appName = CachingUtil.getAppName(keyspaceName);
                String uuid = CachingUtil.getUuidFromMusicCache(keyspaceName);
                PreparedQueryObject pQuery = new PreparedQueryObject();
                pQuery.appendQueryString(
                        "select  count(*) as count from admin.keyspace_master where application_name=? allow filtering;");
                pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
                Row row = MusicCore.get(pQuery).one();
                long count = row.getLong(0);

                if (count == 0) {
                    logger.error(EELFLoggerDelegate.errorLogger,
                            "Keyspace not found. Please make sure keyspace exists.", AppMessages.INCORRECTDATA,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                            .setError("Keyspace not found. Please make sure keyspace exists.").toMap()).build();
                    // Admin Functions:
                } else if (count == 1) {
                    pQuery = new PreparedQueryObject();
                    pQuery.appendQueryString("UPDATE admin.keyspace_master SET keyspace_name=? where uuid = ?;");
                    pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), MusicUtil.DEFAULTKEYSPACENAME));
                    pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
                    MusicCore.nonKeyRelatedPut(pQuery, consistency);
                } else {
                    pQuery = new PreparedQueryObject();
                    pQuery.appendQueryString("delete from admin.keyspace_master where uuid = ?");
                    pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
                    MusicCore.nonKeyRelatedPut(pQuery, consistency);
                }

                /**
                 * Drop Keyspace goes here
                 */
                kspObject.setKeyspaceName(keyspaceName);
                ResultType result = MusicCore.dropKeyspace(kspObject);
                if (result.equals(ResultType.FAILURE)) {
                    return response.status(Status.BAD_REQUEST).entity(
                            new JsonResponse(result).setError("Error Deleteing Keyspace " + keyspaceName).toMap())
                            .build();
                }
                return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS)
                        .setMessage("Keyspace " + keyspaceName + " Deleted").toMap()).build();
            } else {
                String vError = "Keyspace Droping no longer supported after versions 3.2.x. Contact DBA to drop the keyspace.";
                logger.info(EELFLoggerDelegate.applicationLogger, vError);
                logger.error(EELFLoggerDelegate.errorLogger, vError, AppMessages.UNKNOWNERROR, ErrorSeverity.WARN,
                        ErrorTypes.MUSICSERVICEERROR);
                return response.status(Response.Status.BAD_REQUEST)
                        .entity(new JsonResponse(ResultType.FAILURE).setError(vError).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public Response createTable(String version, String minorVersion, String patchVersion, String authorization,
            String aid, String ns, CassaTableObject cassaTableObject, String keyspace, String tablename) throws Exception {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);
            if (keyspace == null || keyspace.isEmpty() || tablename == null || tablename.isEmpty()) {
                return response.status(Status.BAD_REQUEST)
                        .entity(new JsonResponse(ResultType.FAILURE)
                                .setError("One or more path parameters are not set, please check and try again."
                                        + "Parameter values: keyspace='" + keyspace + "' tablename='" + tablename + "'")
                                .toMap())
                        .build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspace + " ) ");
            Map<String, String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
            String userId = userCredentials.get(MusicUtil.USERID);
            String password = userCredentials.get(MusicUtil.PASSWORD);
            Map<String, Object> authMap = MusicAuthentication.autheticateUser(ns, userId, password, keyspace, aid,
                    "createTable");
            if (authMap.containsKey("aid"))
                authMap.remove("aid");
            if (!authMap.isEmpty()) {
                logger.error(EELFLoggerDelegate.errorLogger, authMap.get("Exception").toString(),
                        AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL, ErrorTypes.AUTHENTICATIONERROR);
                return response.status(Status.UNAUTHORIZED).entity(
                        new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap())
                        .build();
            }
            String consistency = MusicUtil.EVENTUAL;
            // for now this needs only eventual consistency
/*
            String primaryKey = null;
            String partitionKey = tableObj.getPartitionKey();
            String clusterKey = tableObj.getClusteringKey();
            String filteringKey = tableObj.getFilteringKey();
            if (filteringKey != null) {
                clusterKey = clusterKey + "," + filteringKey;
            }
            primaryKey = tableObj.getPrimaryKey(); // get primaryKey if available

            PreparedQueryObject queryObject = new PreparedQueryObject();
            // first read the information about the table fields
            Map<String, String> fields = tableObj.getFields();
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
                            return response.status(Status.BAD_REQUEST)
                                    .entity(new JsonResponse(ResultType.FAILURE).setError(
                                            "Create Table Error: primary key '(' and ')' do not match, primary key="
                                                    + primaryKey)
                                            .toMap())
                                    .build();
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
                            return response.status(Status.BAD_REQUEST)
                                    .entity(new JsonResponse(ResultType.FAILURE)
                                            .setError("Create Table primary key error: clusterKey(" + clusterKey
                                                    + ") equals/contains/overlaps partitionKey(" + partitionKey
                                                    + ")  of" + " primary key=" + primaryKey)
                                            .toMap())
                                    .build();

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
                            return response.status(Status.BAD_REQUEST)
                                    .entity(new JsonResponse(ResultType.FAILURE)
                                            .setError("Create Table primary key error: clusterKey(" + clusterKey
                                                    + ") equals/contains/overlaps partitionKey(" + partitionKey + ")")
                                            .toMap())
                                    .build();
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

            String clusteringOrder = tableObj.getClusteringOrder();

            if (clusteringOrder != null && !(clusteringOrder.isEmpty())) {
                String[] arrayClusterOrder = clusteringOrder.split("[,]+");

                for (int i = 0; i < arrayClusterOrder.length; i++) {
                    String[] clusterS = arrayClusterOrder[i].trim().split("[ ]+");
                    if ((clusterS.length == 2)
                            && (clusterS[1].equalsIgnoreCase("ASC") || clusterS[1].equalsIgnoreCase("DESC"))) {
                        continue;
                    } else {
                        return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(
                                "createTable/Clustering Order vlaue ERROR: valid clustering order is ASC or DESC or expecting colname  order; please correct clusteringOrder:"
                                        + clusteringOrder + ".")
                                .toMap()).build();
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

            queryObject.appendQueryString("CREATE TABLE " + keyspace + "." + tablename + " " + fieldsString);

            if (propertiesString != null && propertiesString.length() > 0)
                queryObject.appendQueryString(" WITH " + propertiesString);
            queryObject.appendQueryString(";");*/
            ResultType result = ResultType.FAILURE;
            try {
                cassaTableObject.setKeyspaceName(keyspace);
                cassaTableObject.setTableName(tablename);
                Map<String, String> consistencyInfo = new HashMap<>();
                consistencyInfo.put("type", consistency);
                cassaTableObject.setConsistencyInfo(consistencyInfo);
                
                result = MusicCore.createTable(cassaTableObject);
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(), AppMessages.UNKNOWNERROR,
                        ErrorSeverity.CRITICAL, ErrorTypes.MUSICSERVICEERROR);
                response.status(Status.BAD_REQUEST);
                return response.status(Status.BAD_REQUEST)
                        .entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
            if (result.equals(ResultType.FAILURE)) {
                return response.status(Status.BAD_REQUEST)
                        .entity(new JsonResponse(result).setError("Error Creating Table " + tablename).toMap()).build();
            }
            return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS)
                    .setMessage("TableName " + tablename.trim() + " Created under keyspace " + keyspace.trim()).toMap())
                    .build();
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public Response dropTable(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, String keyspace, String tablename) throws Exception {

        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);
            if ((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty())) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                        .setError("one or more path parameters are not set, please check and try again").toMap())
                        .build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspace + " ) ");
            Map<String, String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
            String userId = userCredentials.get(MusicUtil.USERID);
            String password = userCredentials.get(MusicUtil.PASSWORD);
            Map<String, Object> authMap = MusicAuthentication.autheticateUser(ns, userId, password, keyspace, aid,
                    "dropTable");
            if (authMap.containsKey("aid"))
                authMap.remove("aid");
            if (!authMap.isEmpty()) {
                logger.error(EELFLoggerDelegate.errorLogger, authMap.get("Exception").toString(),
                        AppMessages.MISSINGINFO, ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
                return response.status(Status.UNAUTHORIZED).entity(
                        new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap())
                        .build();
            }
            String consistency = MusicUtil.EVENTUAL;// for now this needs only eventual consistency
            
            CassaTableObject cassaTableObject = new CassaTableObject();
            cassaTableObject.setKeyspaceName(keyspace);
            cassaTableObject.setTableName(tablename);
            Map<String, String> consistencyInfo = new HashMap<>();
            consistencyInfo.put("type", consistency);
            cassaTableObject.setConsistencyInfo(consistencyInfo);
            
            try {
            	/**
            	 * Drop Table goes here.
            	 */
                return response.status(Status.OK)
                        .entity(new JsonResponse(MusicCore.dropTable(cassaTableObject)).toMap()).build();
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(), AppMessages.MISSINGINFO,
                        ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST)
                        .entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }
}
