/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
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
package org.onap.music.rest;


import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.mindrot.jbcrypt.BCrypt;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JSONObject;
import org.onap.music.datastore.jsonobjects.JsonCallback;
import org.onap.music.datastore.jsonobjects.JsonOnboard;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
//import org.onap.music.main.CacheAccess;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.response.jsonobjects.JsonResponse;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.Base64;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.jcs.JCS;
import org.apache.commons.jcs.access.CacheAccess;

@Path("/v2/admin")
// @Path("/v{version: [0-9]+}/admin")
// @Path("/admin")
@Api(value = "Admin Api", hidden = true)
public class RestMusicAdminAPI {
    private static EELFLoggerDelegate logger =
                    EELFLoggerDelegate.getLogger(RestMusicAdminAPI.class);
    /*
     * API to onboard an application with MUSIC. This is the mandatory first step.
     * 
     */
    @POST
    @Path("/onboardAppWithMusic")
    @ApiOperation(value = "Onboard application", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response onboardAppWithMusic(JsonOnboard jsonObj) throws Exception {
        ResponseBuilder response =
                        Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        Map<String, Object> resultMap = new HashMap<>();
        String appName = jsonObj.getAppname();
        String userId = jsonObj.getUserId();
        String isAAF = jsonObj.getIsAAF();
        String password = jsonObj.getPassword();
        if (appName == null || userId == null || isAAF == null || password == null) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO,
                            ErrorSeverity.CRITICAL, ErrorTypes.AUTHENTICATIONERROR);
            resultMap.put("Exception",
                            "Unauthorized: Please check the request parameters. Some of the required values appName(ns), userId, password, isAAF are missing.");
            return Response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }

        PreparedQueryObject pQuery = new PreparedQueryObject();
        pQuery.appendQueryString(
                        "select uuid from admin.keyspace_master where application_name = ? allow filtering");
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        ResultSet rs = MusicCore.get(pQuery);
        if (!rs.all().isEmpty()) {
            resultMap.put("Exception", "Application " + appName
                            + " has already been onboarded. Please contact admin.");
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        pQuery = new PreparedQueryObject();
        String uuid = CachingUtil.generateUUID();
        pQuery.appendQueryString(
                        "INSERT INTO admin.keyspace_master (uuid, keyspace_name, application_name, is_api, "
                                        + "password, username, is_aaf) VALUES (?,?,?,?,?,?,?)");
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(),
                        MusicUtil.DEFAULTKEYSPACENAME));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), "True"));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), BCrypt.hashpw(password, BCrypt.gensalt())));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));

        String returnStr = MusicCore.eventualPut(pQuery).toString();
        if (returnStr.contains("Failure")) {
            resultMap.put("Exception",
                            "Oops. Something wrong with onboarding process. Please retry later or contact admin.");
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }
        CachingUtil.updateisAAFCache(appName, isAAF);
        resultMap.put("Success", "Your application " + appName + " has been onboarded with MUSIC.");
        resultMap.put("Generated AID", uuid);
        return Response.status(Status.OK).entity(resultMap).build();
    }


    @POST
    @Path("/search")
    @ApiOperation(value = "Search Onboard application", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getOnboardedInfoSearch(JsonOnboard jsonObj) throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        ResponseBuilder response =
                        Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        String appName = jsonObj.getAppname();
        String uuid = jsonObj.getAid();
        String isAAF = jsonObj.getIsAAF();

        if (appName == null && uuid == null && isAAF == null) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO,
                            ErrorSeverity.CRITICAL, ErrorTypes.AUTHENTICATIONERROR);
            resultMap.put("Exception",
                            "Unauthorized: Please check the request parameters. Enter atleast one of the following parameters: appName(ns), aid, isAAF.");
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        PreparedQueryObject pQuery = new PreparedQueryObject();
        String cql = "select uuid, keyspace_name from admin.keyspace_master where ";
        if (appName != null)
            cql = cql + "application_name = ? AND ";
        if (uuid != null)
            cql = cql + "uuid = ? AND ";
        if (isAAF != null)
            cql = cql + "is_aaf = ?";

        if (cql.endsWith("AND "))
            cql = cql.trim().substring(0, cql.length() - 4);
        System.out.println("Query is: " + cql);
        cql = cql + " allow filtering";
        System.out.println("Get OnboardingInfo CQL: " + cql);
        pQuery.appendQueryString(cql);
        if (appName != null)
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        if (uuid != null)
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
        if (isAAF != null)
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(),
                            Boolean.parseBoolean(isAAF)));
        ResultSet rs = MusicCore.get(pQuery);
        Iterator<Row> it = rs.iterator();
        while (it.hasNext()) {
            Row row = (Row) it.next();
            resultMap.put(row.getUUID("uuid").toString(), row.getString("keyspace_name"));
        }
        if (resultMap.isEmpty()) {
            if (uuid != null) {
                resultMap.put("Exception",
                                "Please make sure Aid is correct and application is onboarded.");
                return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
            } else {
                resultMap.put("Exception",
                                "Application is not onboarded. Please make sure all the information is correct.");
                return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
            }
        }
        return Response.status(Status.OK).entity(resultMap).build();
    }


    @DELETE
    @Path("/onboardAppWithMusic")
    @ApiOperation(value = "Delete Onboard application", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteOnboardApp(JsonOnboard jsonObj) throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        ResponseBuilder response =
                        Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        String appName = jsonObj.getAppname();
        String aid = jsonObj.getAid();
        PreparedQueryObject pQuery = new PreparedQueryObject();
        String consistency = MusicUtil.EVENTUAL;;
        if (appName == null && aid == null) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            resultMap.put("Exception", "Please make sure either appName(ns) or Aid is present");
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }
        if (aid != null) {
            pQuery.appendQueryString(
                            "SELECT keyspace_name FROM admin.keyspace_master WHERE uuid = ?");
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(),
                            UUID.fromString(aid)));
            Row row = MusicCore.get(pQuery).one();
            if (row != null) {
                String ks = row.getString("keyspace_name");
                if (!ks.equals(MusicUtil.DEFAULTKEYSPACENAME)) {
                    PreparedQueryObject queryObject = new PreparedQueryObject();
                    queryObject.appendQueryString("DROP KEYSPACE IF EXISTS " + ks + ";");
                    MusicCore.nonKeyRelatedPut(queryObject, consistency);
                }
            }
            pQuery = new PreparedQueryObject();
            pQuery.appendQueryString("delete from admin.keyspace_master where uuid = ? IF EXISTS");
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(),
                            UUID.fromString(aid)));
            ResultType result = MusicCore.nonKeyRelatedPut(pQuery, consistency);
            if (result == ResultType.SUCCESS) {
                resultMap.put("Success", "Your application has been deleted successfully");
            } else {
                resultMap.put("Exception",
                                "Oops. Something went wrong. Please make sure Aid is correct or Application is onboarded");
                logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA,
                                ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
                return Response.status(Status.BAD_REQUEST).entity(resultMap).build();

            }
            return Response.status(Status.OK).entity(resultMap).build();
        }

        pQuery.appendQueryString(
                        "select uuid from admin.keyspace_master where application_name = ? allow filtering");
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        ResultSet rs = MusicCore.get(pQuery);
        List<Row> rows = rs.all();
        String uuid = null;
        if (rows.size() == 0) {
            resultMap.put("Exception",
                            "Application not found. Please make sure Application exists.");
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        } else if (rows.size() == 1) {
            uuid = rows.get(0).getUUID("uuid").toString();
            pQuery = new PreparedQueryObject();
            pQuery.appendQueryString(
                            "SELECT keyspace_name FROM admin.keyspace_master WHERE uuid = ?");
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(),
                            UUID.fromString(uuid)));
            Row row = MusicCore.get(pQuery).one();
            String ks = row.getString("keyspace_name");
            if (!ks.equals(MusicUtil.DEFAULTKEYSPACENAME)) {
                PreparedQueryObject queryObject = new PreparedQueryObject();
                queryObject.appendQueryString("DROP KEYSPACE " + ks + ";");
                MusicCore.nonKeyRelatedPut(queryObject, consistency);
            }

            pQuery = new PreparedQueryObject();
            pQuery.appendQueryString("delete from admin.keyspace_master where uuid = ?");
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(),
                            UUID.fromString(uuid)));
            MusicCore.eventualPut(pQuery);
            resultMap.put("Success", "Your application " + appName + " has been deleted.");
            return Response.status(Status.OK).entity(resultMap).build();
        } else {
            resultMap.put("Failure",
                            "More than one Aid exists for this application, so please provide Aid.");
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MULTIPLERECORDS,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }
    }


    @PUT
    @Path("/onboardAppWithMusic")
    @ApiOperation(value = "Update Onboard application", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateOnboardApp(JsonOnboard jsonObj) throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        ResponseBuilder response =
                        Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        String aid = jsonObj.getAid();
        String appName = jsonObj.getAppname();
        String userId = jsonObj.getUserId();
        String isAAF = jsonObj.getIsAAF();
        String password = jsonObj.getPassword();
        String consistency = "eventual";
        PreparedQueryObject pQuery;

        if (aid == null) {
            resultMap.put("Exception", "Please make sure Aid is present");
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        if (appName == null && userId == null && password == null && isAAF == null) {
            resultMap.put("Exception",
                            "No parameters found to update. Please update atleast one parameter.");
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        if (appName != null) {
            pQuery = new PreparedQueryObject();
            pQuery.appendQueryString(
                            "select uuid from admin.keyspace_master where application_name = ? allow filtering");
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
            ResultSet rs = MusicCore.get(pQuery);
            if (!rs.all().isEmpty()) {
                resultMap.put("Exception", "Application " + appName
                                + " has already been onboarded. Please contact admin.");
                logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.ALREADYEXIST,
                                ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
                return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
            }
        }

        pQuery = new PreparedQueryObject();
        StringBuilder preCql = new StringBuilder("UPDATE admin.keyspace_master SET ");
        if (appName != null)
            preCql.append(" application_name = ?,");
        if (userId != null)
            preCql.append(" username = ?,");
        if (password != null)
            preCql.append(" password = ?,");
        if (isAAF != null)
            preCql.append(" is_aaf = ?,");
        preCql.deleteCharAt(preCql.length() - 1);
        preCql.append(" WHERE uuid = ? IF EXISTS");
        pQuery.appendQueryString(preCql.toString());
        if (appName != null)
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        if (userId != null)
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
        if (password != null)
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), BCrypt.hashpw(password, BCrypt.gensalt())));
        if (isAAF != null)
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));

        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), UUID.fromString(aid)));
        ResultType result = MusicCore.nonKeyRelatedPut(pQuery, consistency);

        if (result == ResultType.SUCCESS) {
            resultMap.put("Success", "Your application has been updated successfully");
        } else {
            resultMap.put("Exception",
                            "Oops. Something went wrong. Please make sure Aid is correct and application is onboarded");
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        return Response.status(Status.OK).entity(resultMap).build();
    }

    Client client = Client.create();
    
    @POST
    @Path("/callbackOps")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
	public String callbackOps(JSONObject inputJsonObj) {
         // trigger response  {"full_table":"admin.race_winners","keyspace":"admin","name":"Siri","operation":"update","table_name":"race_winner","primary_key":"1"}
        try {
		logger.info("Got notification: " + inputJsonObj.getData());
		String dataStr = inputJsonObj.getData();
		String[] dataStrArr = dataStr.substring(1, dataStr.length() - 1).split(",");

		for (String key : dataStrArr) {
			if (key.contains("full_table")) {
				String tableName = key.split(":")[1].substring(1, key.split(":")[1].length() - 1);
				PreparedQueryObject pQuery = new PreparedQueryObject();
		        pQuery.appendQueryString(
		                        "select endpoint, username, password from admin.callback_api where changes = ? allow filtering");
		        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), tableName));
		        ResultSet rs = MusicCore.get(pQuery);
		        Row row = rs.all().get(0);
		        if(row != null) {
		            String endpoint = row.getString("endpoint");
		            String username = row.getString("username");
		            String password = row.getString("password");
		            logger.info("Notifying the changes to endpoint: "+endpoint);
		            WebResource webResource = client.resource(endpoint);
		            String authData = username+":"+password;
		            byte[] plainCredsBytes = authData.getBytes();
		            byte[] base64CredsBytes = Base64.encode(plainCredsBytes);
		            String base64Creds = new String(base64CredsBytes);
		            ClientResponse response = webResource.header("Authorization", "Basic " + base64Creds).accept("application/json")
		                .post(ClientResponse.class, inputJsonObj);
		            if(response.getStatus() != 200){
		                logger.error("Exception while notifying");
		            }
		        }
		        break;
			}
		}
        } catch(Exception e) {
            e.printStackTrace();
            logger.info("Exception...");
        }
		return "Success";
	}

    @POST
    @Path("/addCallback")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addCallback(JsonCallback jsonCallback) throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        ResponseBuilder response =
                        Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        String username = jsonCallback.getApplicationUsername();
        String password = jsonCallback.getApplicationPassword();
        String endpoint = jsonCallback.getApplicationNotificationEndpoint();
        String changes = jsonCallback.getNotifyWhenChangeIn();
        String inserts = jsonCallback.getNotifyWhenInsertsIn();
        String deletes = jsonCallback.getNotifyWhenDeletesIn();
        PreparedQueryObject pQuery = new PreparedQueryObject();
        if (username == null || password == null || endpoint == null || changes == null || inserts == null || deletes == null) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            resultMap.put("Exception",
                            "Please check the request parameters. Some of the required values are missing.");
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }
        String uuid = CachingUtil.generateUUID();
        try {
        pQuery.appendQueryString(
                        "INSERT INTO admin.callback_api (uuid, username, password, endpoint, "
                                        + "changes, inserts, deletes) VALUES (?,?,?,?,?,?,?)");
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), username));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), password));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), endpoint));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), changes));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), inserts));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), deletes));
        MusicCore.eventualPut(pQuery);
        
        Map<String, String> jsonMap = new HashMap<>();
        jsonMap.put("username", username);
        jsonMap.put("password", password);
        jsonMap.put("endpoint", endpoint);
        jsonMap.put("changes", changes);
        jsonMap.put("inserts", inserts);
        jsonMap.put("deletes", deletes);
        
        //callBackCache.put(jsonCallback.getApplicationName(), jsonMap);
        } catch (InvalidQueryException e) {
            logger.error(EELFLoggerDelegate.errorLogger,"Exception callback_api table not configured."+e.getMessage());
            resultMap.put("Exception", "Please make sure admin.callback_api table is configured.");
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }
       return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setMessage("Callback api successfully registered").toMap()).build();
    }
}
