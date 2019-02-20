/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 * Modifications Copyright (C) 2018 IBM.
 * ================================================================================
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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.mindrot.jbcrypt.BCrypt;
import org.onap.music.authentication.MusicAuthentication;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonOnboard;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicServiceException;
//import org.onap.music.main.CacheAccess;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.response.jsonobjects.JsonResponse;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
//import java.util.Base64.Encoder;
//import java.util.Base64.Decoder;

@Path("/v2/admin")
// @Path("/v{version: [0-9]+}/admin")
// @Path("/admin")
@Api(value = "Admin Api", hidden = true)
public class RestMusicAdminAPI {
    private static EELFLoggerDelegate logger =
                    EELFLoggerDelegate.getLogger(RestMusicAdminAPI.class);
    // Set to true in env like ONAP. Where access to creating and dropping keyspaces exist.    
    private static final boolean KEYSPACE_ACTIVE = false;

    /*
     * API to onboard an application with MUSIC. This is the mandatory first step.
     *
     */
    @POST
    @Path("/onboardAppWithMusic")
    @ApiOperation(value = "Onboard application", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response onboardAppWithMusic(JsonOnboard jsonObj,
            @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization) throws Exception {
        logger.info(EELFLoggerDelegate.errorLogger, "oboarding app");
        ResponseBuilder response =
                        Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        Map<String, Object> resultMap = new HashMap<>();
        String appName = jsonObj.getAppname();
        String userId = jsonObj.getUserId();
        String isAAF = jsonObj.getIsAAF();
        String password = jsonObj.getPassword();
        String keyspace_name = jsonObj.getKeyspace();
        try {
            if (!MusicAuthentication.authenticateAdmin(authorization)) {
                logger.error(EELFLoggerDelegate.errorLogger, "Unauthorized: Please check admin username,password and try again", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
                        ErrorTypes.AUTHENTICATIONERROR);
                response.status(Status.UNAUTHORIZED);
                return response
                        .entity(new JsonResponse(ResultType.FAILURE)
                                .setError("Unauthorized: Please check admin username,password and try again").toMap())
                        .build();
            }
        } catch (Exception e) {
        	logger.error(EELFLoggerDelegate.errorLogger, "Unable to authenticate", e);
        	response.status(Status.UNAUTHORIZED);
            return response.entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        if (appName == null || userId == null || isAAF == null || password == null) {
            logger.error(EELFLoggerDelegate.errorLogger, "Unauthorized: Please check the request parameters. Some of the required values appName(ns), userId, password, isAAF are missing.", AppMessages.MISSINGINFO,
                            ErrorSeverity.CRITICAL, ErrorTypes.AUTHENTICATIONERROR);
            resultMap.put("Exception",
                            "Unauthorized: Please check the request parameters. Some of the required values appName(ns), userId, password, isAAF are missing.");
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }

        PreparedQueryObject pQuery = new PreparedQueryObject();
        /*
         * pQuery.appendQueryString(
         * "select uuid from admin.keyspace_master where application_name = ? allow filtering"
         * ); pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(),
         * appName)); ResultSet rs = MusicCore.get(pQuery); if (!rs.all().isEmpty()) {
         * logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.INCORRECTDATA
         * ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
         * response.status(Status.BAD_REQUEST); return response.entity(new
         * JsonResponse(ResultType.FAILURE).setError("Application " + appName +
         * " has already been onboarded. Please contact admin.").toMap()).build(); }
         */
        //pQuery = new PreparedQueryObject();
        String uuid = CachingUtil.generateUUID();
        pQuery.appendQueryString(
                        "INSERT INTO admin.keyspace_master (uuid, keyspace_name, application_name, is_api, "
                                        + "password, username, is_aaf) VALUES (?,?,?,?,?,?,?)");
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(),keyspace_name));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), "True"));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), BCrypt.hashpw(password, BCrypt.gensalt())));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));

        String returnStr = MusicCore.eventualPut(pQuery).toString();
        if (returnStr.contains("Failure")) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
            response.status(Status.BAD_REQUEST);
            return response.entity(new JsonResponse(ResultType.FAILURE).setError("Oops. Something wrong with onboarding process. "
                    + "Please retry later or contact admin.").toMap()).build();
        }
        CachingUtil.updateisAAFCache(appName, isAAF);
        resultMap.put("Success", "Your application " + appName + " has been onboarded with MUSIC.");
        resultMap.put("Generated AID", uuid);
        return response.status(Status.OK).entity(resultMap).build();
    }


    @POST
    @Path("/search")
    @ApiOperation(value = "Search Onboard application", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getOnboardedInfoSearch(JsonOnboard jsonObj,
            @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization) throws Exception {
        ResponseBuilder response = Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        Map<String, Object> resultMap = new HashMap<>();
        String appName = jsonObj.getAppname();
        String uuid = jsonObj.getAid();
        String isAAF = jsonObj.getIsAAF();

        try {
            if (!MusicAuthentication.authenticateAdmin(authorization)) {
                logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
                        ErrorTypes.AUTHENTICATIONERROR);
                response.status(Status.UNAUTHORIZED);
                return response
                        .entity(new JsonResponse(ResultType.FAILURE)
                                .setError("Unauthorized: Please check admin username,password and try again").toMap())
                        .build();
            }
        } catch (Exception e) {
            return response.entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        if (appName == null && uuid == null && isAAF == null) {
            logger.error(EELFLoggerDelegate.errorLogger, "Unauthorized: Please check the request parameters. Enter atleast one of the following parameters: appName(ns), aid, isAAF.", AppMessages.MISSINGINFO,
                            ErrorSeverity.CRITICAL, ErrorTypes.AUTHENTICATIONERROR);
            resultMap.put("Exception",
                            "Unauthorized: Please check the request parameters. Enter atleast one of the following parameters: appName(ns), aid, isAAF.");
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
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
        logger.info("Query in callback is: " + cql);
        cql = cql + " allow filtering";
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
            Row row = it.next();
            resultMap.put(row.getUUID("uuid").toString(), row.getString("keyspace_name"));
        }
        if (resultMap.isEmpty()) {
            if (uuid != null) {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                response.status(Status.BAD_REQUEST);
                return response.entity(new JsonResponse(ResultType.FAILURE).setError("Please make sure Aid is correct and application is onboarded.").toMap()).build();

            } else {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                response.status(Status.BAD_REQUEST);
                return response.entity(new JsonResponse(ResultType.FAILURE).setError("Application is not onboarded. Please make sure all the information is correct.").toMap()).build();
            }
        }
        return response.status(Status.OK).entity(resultMap).build();
    }


    @DELETE
    @Path("/onboardAppWithMusic")
    @ApiOperation(value = "Delete Onboard application", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteOnboardApp(JsonOnboard jsonObj,
            @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization) throws Exception {
        ResponseBuilder response = Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        Map<String, Object> resultMap = new HashMap<>();
        String appName = jsonObj.getAppname();
        String aid = jsonObj.getAid();
        PreparedQueryObject pQuery = new PreparedQueryObject();
        String consistency = MusicUtil.EVENTUAL;;
        try {
            if (!MusicAuthentication.authenticateAdmin(authorization)) {
                logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
                        ErrorTypes.AUTHENTICATIONERROR);
                response.status(Status.UNAUTHORIZED);
                return response
                        .entity(new JsonResponse(ResultType.FAILURE)
                                .setError("Unauthorized: Please check admin username,password and try again").toMap())
                        .build();
            }
        } catch (Exception e) {
            return response.entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        if (appName == null && aid == null) {
            logger.error(EELFLoggerDelegate.errorLogger, "Please make sure either appName(ns) or Aid is present", AppMessages.MISSINGINFO,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            resultMap.put("Exception", "Please make sure either appName(ns) or Aid is present");
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }
        if (aid != null) {
            if ( KEYSPACE_ACTIVE ) {
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
                logger.error(EELFLoggerDelegate.errorLogger, "Oops. Something went wrong. Please make sure Aid is correct or Application is onboarded", AppMessages.INCORRECTDATA,
                                ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
                return response.status(Status.BAD_REQUEST).entity(resultMap).build();

            }
            return response.status(Status.OK).entity(resultMap).build();
        }

        pQuery.appendQueryString(
                        "select uuid from admin.keyspace_master where application_name = ? allow filtering");
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        ResultSet rs = MusicCore.get(pQuery);
        List<Row> rows = rs.all();
        String uuid = null;
        if (rows.isEmpty()) {
            resultMap.put("Exception",
                            "Application not found. Please make sure Application exists.");
            logger.error(EELFLoggerDelegate.errorLogger, "Application not found. Please make sure Application exists.", AppMessages.INCORRECTDATA,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
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
            return response.status(Status.OK).entity(resultMap).build();
        } else {
            resultMap.put("Failure",
                            "More than one Aid exists for this application, so please provide Aid.");
            logger.error(EELFLoggerDelegate.errorLogger, "More than one Aid exists for this application, so please provide Aid.", AppMessages.MULTIPLERECORDS,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }
    }


    @PUT
    @Path("/onboardAppWithMusic")
    @ApiOperation(value = "Update Onboard application", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateOnboardApp(JsonOnboard jsonObj,
            @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization) throws Exception {
        ResponseBuilder response = Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        Map<String, Object> resultMap = new HashMap<>();
        String aid = jsonObj.getAid();
        String appName = jsonObj.getAppname();
        String userId = jsonObj.getUserId();
        String isAAF = jsonObj.getIsAAF();
        String password = jsonObj.getPassword();
        String consistency = "eventual";
        PreparedQueryObject pQuery;
        try {
            if (!MusicAuthentication.authenticateAdmin(authorization)) {
                logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
                        ErrorTypes.AUTHENTICATIONERROR);
                response.status(Status.UNAUTHORIZED);
                return response
                        .entity(new JsonResponse(ResultType.FAILURE)
                                .setError("Unauthorized: Please check admin username,password and try again").toMap())
                        .build();
            }
        } catch (Exception e) {
            return response.entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        if (aid == null) {
            resultMap.put("Exception", "Please make sure Aid is present");
            logger.error(EELFLoggerDelegate.errorLogger, "Please make sure Aid is present", AppMessages.MISSINGDATA,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        if (appName == null && userId == null && password == null && isAAF == null) {
            resultMap.put("Exception",
                            "No parameters found to update. Please update atleast one parameter.");
            logger.error(EELFLoggerDelegate.errorLogger, "No parameters found to update. Please update atleast one parameter.", AppMessages.MISSINGDATA,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
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
                logger.error(EELFLoggerDelegate.errorLogger, "Application " + appName+"has already been onboarded. Please contact admin.", AppMessages.ALREADYEXIST,
                                ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
                return response.status(Status.BAD_REQUEST).entity(resultMap).build();
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
            logger.error(EELFLoggerDelegate.errorLogger, "Oops. Something went wrong. Please make sure Aid is correct and application is onboarded", AppMessages.INCORRECTDATA,
                            ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        return response.status(Status.OK).entity(resultMap).build();
    }

    
    
  //Dashboard related calls
    @GET
    @Path("/getall")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public List<Application> getall(@ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization) throws MusicServiceException{
        List<Application> appList = new ArrayList<>();
        ResponseBuilder response =
                Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString("SELECT *  FROM " + "admin" + "." + "keyspace_master" + ";");
        ResultSet results = MusicCore.get(queryObject);
        for(Row row : results) {
            Application app = new Application();
            app.setApplication_name(row.getString("application_name"));
            app.setIs_aaf(row.getBool("is_aaf"));
            app.setIs_api(row.getBool("is_api"));
            app.setUsername(row.getString("username"));
            app.setKeyspace_name(row.getString("keyspace_name"));
            app.setUuid(row.getUUID("uuid").toString());
            appList.add(app);
        }
        return appList;
        
        //return app;
        
    }
    @DELETE
    @Path("/delete")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public boolean delete(@ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "uuid", required = true) @HeaderParam("uuid") String uuid) throws Exception {
        ResponseBuilder response =
                Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString("delete from admin.keyspace_master where uuid=?");
        queryObject.addValue(MusicUtil.convertToActualDataType(DataType.uuid(),uuid));
        ResultType result;
        try {
         result = MusicCore.nonKeyRelatedPut(queryObject, "eventual");
        }catch(Exception ex) {
            return false;
        }
        return true;
    }
    
    
    @GET
    @Path("/login")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public boolean login(@ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization) throws Exception {
       
        boolean result =  MusicAuthentication.authenticateAdmin(authorization);
        return result;
    }
}
