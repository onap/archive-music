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


import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.codehaus.jackson.map.ObjectMapper;
import org.mindrot.jbcrypt.BCrypt;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JSONCallbackResponse;
import org.onap.music.datastore.jsonobjects.JSONObject;
import org.onap.music.datastore.jsonobjects.JsonCallback;
import org.onap.music.datastore.jsonobjects.JsonNotification;
import org.onap.music.datastore.jsonobjects.JsonNotifyClientResponse;
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
import org.onap.music.main.ReturnType;
import org.onap.music.response.jsonobjects.JsonResponse;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import com.sun.jersey.core.util.Base64;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

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
	ObjectMapper mapper = new ObjectMapper();
	
    @POST
    @Path("/callbackOps")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
	public Response callbackOps(final JSONObject inputJsonObj) {
         // {"keyspace":"conductor","full_table":"conductor.plans","changeValue":{"conductor.plans.status":"Who??","position":"3"},"operation":"update","table_name":"plans","primary_key":"3"}
    	Map<String, Object> resultMap = new HashMap<>();
    	new Thread(new Runnable() {
    	    public void run() {
    	        makeAsyncCall(inputJsonObj);
    	    }
    	}).start();
	    
		return Response.status(Status.OK).entity(resultMap).build();
	}
    
    private Response makeAsyncCall(JSONObject inputJsonObj) {
    	
    	Map<String, Object> resultMap = new HashMap<>();
    	try {
    		logger.info(EELFLoggerDelegate.applicationLogger, "Got notification: " + inputJsonObj.getData());
    		logger.info("Got notification: " + inputJsonObj.getData());
			String dataStr = inputJsonObj.getData();
			JSONCallbackResponse jsonResponse = mapper.readValue(dataStr, JSONCallbackResponse.class);
			String operation = jsonResponse.getOperation();
			Map<String, String> changeValueMap = jsonResponse.getChangeValue();
			String primaryKey = jsonResponse.getPrimary_key();
			String ksTableName = jsonResponse.getFull_table(); //conductor.plans
			if(ksTableName.equals("admin.notification_master")) {
				CachingUtil.updateCallbackNotifyList(new ArrayList<String>());
				return Response.status(Status.OK).entity(resultMap).build();
			}
			List<String> inputUpdateList = jsonResponse.getUpdateList();
			/*String field_value = changeValueMap.get("field_value");
			if(field_value == null)
				field_value = jsonResponse.getFull_table();*/
			String field_value = null;
			List<String> notifiyList = CachingUtil.getCallbackNotifyList();
			if(notifiyList == null || notifiyList.isEmpty()) {
				logger.info("Is cache empty? reconstructing Object from cache..");
				constructJsonCallbackFromCache();
			}
			notifiyList = CachingUtil.getCallbackNotifyList();
			JsonCallback baseRequestObj = null;
			
			if("update".equals(operation)) {
				for(String element: inputUpdateList) {
					baseRequestObj = CachingUtil.getCallBackCache(element);
					if(baseRequestObj != null) {
						logger.info("Found the element that was changed... "+element);
						break;
					}
				}
				
				List<String> updateList = jsonResponse.getUpdateList();
				//logger.info("update list from trigger: "+updateList);
				for(String element : updateList) {
					if(notifiyList.contains(element)) {
						logger.info("Found the notifyOn property: "+element);
						field_value = element;
						break;
					}
				}
				if(baseRequestObj == null || field_value == null) {		
					for(String element: inputUpdateList) {		
						String[] elementArr = element.split(":");		
						String newElement = null;		
						if(elementArr.length >= 2) {		
							newElement = elementArr[0]+":"+elementArr[1];		
				        } 		
						baseRequestObj = CachingUtil.getCallBackCache(newElement);		
						if(baseRequestObj != null) {		
							logger.info("Found the element that was changed... "+newElement);		
							break;		
						}		
					}		
					for(String element : updateList) {		
						String[] elementArr = element.split(":");		
						String newElement = null;		
						if(elementArr.length >= 2) {		
							newElement = elementArr[0]+":"+elementArr[1];		
				        } 		
						if(notifiyList.contains(newElement)) {		
							logger.info("Found the notifyOn property: "+newElement);		
							field_value = newElement;		
							break;		
						}		
					}		
				}
			} else {
				field_value = jsonResponse.getFull_table();
				baseRequestObj = CachingUtil.getCallBackCache(field_value);
			}
			
			if(baseRequestObj == null || field_value == null) {
				resultMap.put("Exception",
	                    "Oops. Something went wrong. Please make sure Callback properties are onboarded.");
				logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA,
	                    ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
				return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
			}
			logger.info(EELFLoggerDelegate.applicationLogger, "Going through list: "+operation+ " && List: "+jsonResponse.getUpdateList());
			
			String key = "admin" + "." + "notification_master" + "." + baseRequestObj.getUuid();
	        String lockId = MusicCore.createLockReference(key);
	        ReturnType lockAcqResult = MusicCore.acquireLock(key, lockId);
	        if(! lockAcqResult.getResult().toString().equals("SUCCESS")) {
	        	logger.error(EELFLoggerDelegate.errorLogger, "Some other node is notifying the caller..: ");
	        }
			
	        logger.info(EELFLoggerDelegate.applicationLogger, operation+ ": Operation :: changeValue: "+changeValueMap);
			if(operation.equals("update")) {
				String notifyWhenChangeIn = baseRequestObj.getNotifyWhenChangeIn(); // conductor.plans.status
				if(field_value.equals(notifyWhenChangeIn)) {
					notifyCallBackAppl(jsonResponse, baseRequestObj);
				}
				
			} else if(operation.equals("delete")) {
				String notifyWhenDeletesIn = baseRequestObj.getNotifyWhenDeletesIn(); // conductor.plans.status
				if(field_value.equals(notifyWhenDeletesIn)) {
					notifyCallBackAppl(jsonResponse, baseRequestObj);
				}
			} else if(operation.equals("insert")) {
				String notifyWhenInsertsIn = baseRequestObj.getNotifyWhenInsertsIn(); // conductor.plans.status
				if(field_value.equals(notifyWhenInsertsIn)) {
					notifyCallBackAppl(jsonResponse, baseRequestObj);
				}
			}
			MusicCore.releaseLock(lockId, true);	
	    } catch(Exception e) {
            e.printStackTrace();
            logger.error(EELFLoggerDelegate.errorLogger, "Exception while notifying...."+e.getMessage());
        }
    	logger.info(EELFLoggerDelegate.applicationLogger, "callback is completed. Notification was sent from Music...");
    	return Response.status(Status.OK).entity(resultMap).build();
    }
    
    private void notifyCallBackAppl(JSONCallbackResponse jsonResponse, JsonCallback baseRequestObj) throws Exception {
    	int notifytimeout = MusicUtil.getNotifyTimeout();
    	int notifyinterval = MusicUtil.getNotifyInterval();
    	String endpoint = baseRequestObj.getApplicationNotificationEndpoint();
        String username = baseRequestObj.getApplicationUsername();
        String password = baseRequestObj.getApplicationPassword();
        JsonNotification jsonNotification = constructJsonNotification(jsonResponse, baseRequestObj);
        jsonNotification.setPassword("************");
        jsonNotification.setOperation_type(jsonResponse.getOperation());
        logger.info(EELFLoggerDelegate.applicationLogger, "Notification Response sending is: "+jsonNotification);
        logger.info("Notification Response sending is: "+jsonNotification);
        jsonNotification.setPassword(baseRequestObj.getApplicationPassword());
        WebResource webResource = client.resource(endpoint);
        String authData = username+":"+password;
        byte[] plainCredsBytes = authData.getBytes();
        byte[] base64CredsBytes = Base64.encode(plainCredsBytes);
        String base64Creds = new String(base64CredsBytes);
        ClientConfig config = new DefaultClientConfig();
    	config.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        ClientResponse response = null;
        WebResource service = null;
        boolean ok = false;
        try { 
        	Client client = Client.create(config);
        	TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager(){
                public X509Certificate[] getAcceptedIssuers(){return null;}
                public void checkClientTrusted(X509Certificate[] certs, String authType){}
                public void checkServerTrusted(X509Certificate[] certs, String authType){}
            }};

            // Install the all-trusting trust manager
            try {
                SSLContext sc = SSLContext.getInstance("TLS");
                sc.init(null, trustAllCerts, new SecureRandom());
                HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
            } catch (Exception e) {
                ;
            }

        	try {
                SSLContext sslcontext = SSLContext.getInstance( "TLS" );
                sslcontext.init( null, null, null );
                Map<String, Object> properties = config.getProperties();
                HTTPSProperties httpsProperties = new HTTPSProperties(
                        new HostnameVerifier() {
                            @Override
                            public boolean verify( String s, SSLSession sslSession ) {
                                return true;
                            }
                        }, sslcontext
                );
                properties.put( HTTPSProperties.PROPERTY_HTTPS_PROPERTIES, httpsProperties );
                HttpsURLConnection.setDefaultHostnameVerifier (new HostnameVerifier() {
					@Override
					public boolean verify(String hostname, SSLSession session) {
						return true;
					}
				});
                Client.create( config );
            }
            catch ( KeyManagementException | NoSuchAlgorithmException e ) {
                throw new RuntimeException( e );
            }
        	
        	service = client.resource(endpoint);

        	response = service.header("Authorization", "Basic " + base64Creds).accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
        			  .post(ClientResponse.class, jsonNotification);
          	
        } catch (Exception chf) {
        	logger.info(EELFLoggerDelegate.applicationLogger, "Is Service down?");
        	logger.info("An Exception occured while notifying. "+chf+ " : "+chf.getMessage() +" ...Retrying for: "+notifytimeout);
        }
        if(response != null && response.getStatus() == 200) ok = true;
        if(!ok) {
        	long now= System.currentTimeMillis();
        	long end = now+notifytimeout;
        	while(! ok) {
        		logger.info(EELFLoggerDelegate.applicationLogger, "retrying since error in notifying callback for "+notifytimeout+"ms");
        		logger.info("retrying since error in notifying callback.. response status: "+ (response == null ? "404" : response.getStatus()));
        		try {
        			ok = true;
        			response = service.header("Authorization", "Basic " + base64Creds).accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
              			  .post(ClientResponse.class, jsonNotification);
        			if(response != null && response.getStatus() == 200) ok = true;
        			else if(System.currentTimeMillis() < end) {
        				try{ Thread.sleep(notifyinterval); } catch(Exception e1) {}
        				ok = false;
        			}
        		}catch (Exception e) {
        			logger.info(EELFLoggerDelegate.applicationLogger, "Retry until "+(end-System.currentTimeMillis()));
        			if(response == null && System.currentTimeMillis() < end) ok = false;
        			else ok = true;
        			try{ Thread.sleep(notifyinterval); } catch(Exception e1) {}
        		}
        	}
        }
        
        if(response == null) {
        	logger.error(EELFLoggerDelegate.errorLogger, "Can NOT notify the caller as caller failed to respond..");
        	return;
        }
        try {
        	JsonNotifyClientResponse responseStr = response.getEntity(JsonNotifyClientResponse.class);
        	logger.info(EELFLoggerDelegate.applicationLogger, "Response from Notified client: "+responseStr);
        	logger.info("Response from Notified client: "+responseStr);
        } catch(Exception e) {
        	logger.info("Exception while reading response from Caller");
        	logger.error("Exception while reading response from Caller");
        	logger.error(EELFLoggerDelegate.errorLogger, "Can NOT notify the caller as caller failed to respond..");
        }
        
        /*ClientResponse response = null;
        try { 
        	response = webResource.header("Authorization", "Basic " + base64Creds).accept("application/json").type("application/json")
            .post(ClientResponse.class, jsonNotification);
        } catch (com.sun.jersey.api.client.ClientHandlerException chf) {
        	boolean ok = false;
        	logger.info(EELFLoggerDelegate.applicationLogger, "Is Service down?");
        	long now= System.currentTimeMillis();
        	long end = now+notifytimeout;
        	while(! ok) {
        		logger.info(EELFLoggerDelegate.applicationLogger, "retrying since error in notifying callback..");
        		try {
        			response = webResource.header("Authorization", "Basic " + base64Creds).accept("application/json").type("application/json")
        	            .post(ClientResponse.class, jsonNotification);
        			if(response.getStatus() == 200) ok = true;
        		}catch (Exception e) {
        			logger.info(EELFLoggerDelegate.applicationLogger, "Retry until "+(end-System.currentTimeMillis()));
        			if(response == null && System.currentTimeMillis() < end) ok = false;
        			else ok = true;
        			try{ Thread.sleep(notifyinterval); } catch(Exception e1) {}
        		}
        	}
        }
        if(response == null) {
        	logger.error(EELFLoggerDelegate.errorLogger, "Can NOT notify the caller as caller failed to respond..");
        	return;
        }
        JsonNotifyClientResponse responseStr = response.getEntity(JsonNotifyClientResponse.class);
        logger.info(EELFLoggerDelegate.applicationLogger, "Response from Notified client: "+responseStr);
        
        if(response.getStatus() != 200){
        	long now= System.currentTimeMillis();
        	long end = now+30000;
        	while(response.getStatus() != 200 && System.currentTimeMillis() < end) {
        		logger.info(EELFLoggerDelegate.applicationLogger, "retrying since error in notifying callback..");
        		response = webResource.header("Authorization", "Basic " + base64Creds).accept("application/json").type("application/json")
        	            .post(ClientResponse.class, jsonNotification);
        	}
        	logger.info(EELFLoggerDelegate.applicationLogger, "Exception while notifying.. "+response.getStatus());
        }*/
    }
    
    private JsonNotification constructJsonNotification(JSONCallbackResponse jsonResponse, JsonCallback baseRequestObj) {
    	
    	JsonNotification jsonNotification = new JsonNotification();
    	try {
	    	jsonNotification.setNotify_field(baseRequestObj.getNotifyOn());
	    	jsonNotification.setEndpoint(baseRequestObj.getApplicationNotificationEndpoint());
	    	jsonNotification.setUsername(baseRequestObj.getApplicationUsername());
	    	jsonNotification.setPassword(baseRequestObj.getApplicationPassword());
	    	String pkValue = jsonResponse.getPrimary_key();
	    	
	    	String[] fullNotifyArr = baseRequestObj.getNotifyOn().split(":");
	    	
	    	String[] tableArr = fullNotifyArr[0].split("\\.");
	    	TableMetadata tableInfo = MusicCore.returnColumnMetadata(tableArr[0], tableArr[1]);
			DataType primaryIdType = tableInfo.getPrimaryKey().get(0).getType();
			String primaryId = tableInfo.getPrimaryKey().get(0).getName();
			
			Map<String, String> responseBodyMap = baseRequestObj.getResponseBody();
			for (Entry<String, String> entry : new HashSet<>(responseBodyMap.entrySet())) {
			    String trimmed = entry.getKey().trim();
			    if (!trimmed.equals(entry.getKey())) {
			    	responseBodyMap.remove(entry.getKey());
			    	responseBodyMap.put(trimmed, entry.getValue());
			    }
			}
			
	    	Set<String> keySet = responseBodyMap.keySet();
	    	Map<String, String> newMap = new HashMap<>();
	    	if(responseBodyMap.size() == 1 && responseBodyMap.containsKey("")) {
	    		jsonNotification.setResponse_body(newMap);
	    		return jsonNotification;
	    	}
	    	logger.info(EELFLoggerDelegate.applicationLogger, "responseBodyMap is not blank: "+responseBodyMap);
	    	String cql = "select *";
	    	/*for(String keys: keySet) {
	    		cql = cql + keys + ",";
	    	}*/
	    	//cql = cql.substring(0, cql.length()-1);
	    	cql = cql + " FROM "+fullNotifyArr[0]+" WHERE "+primaryId+" = ?";
	    	logger.info(EELFLoggerDelegate.applicationLogger, "CQL in constructJsonNotification: "+cql);
	    	PreparedQueryObject pQuery = new PreparedQueryObject();
	    	pQuery.appendQueryString(cql);
	    	pQuery.addValue(MusicUtil.convertToActualDataType(primaryIdType, pkValue));
	    	Row row = MusicCore.get(pQuery).one();
	    	if(row != null) {
	    		ColumnDefinitions colInfo = row.getColumnDefinitions();
	            for (Definition definition : colInfo) {
	                String colName = definition.getName();
	                if(keySet.contains(colName)) {
		                DataType colType = definition.getType();
		                Object valueObj = MusicCore.getDSHandle().getColValue(row, colName, colType);
		                Object valueString = MusicUtil.convertToActualDataType(colType, valueObj);
		                logger.info(colName+" : "+valueString);
		                newMap.put(colName, valueString.toString());
		                keySet.remove(colName);
	                }
	            }
	    	}
	    	if(! keySet.isEmpty()) {
	    		Iterator<String> iterator = keySet.iterator();
	    		while (iterator.hasNext()) {
	    		    String element = iterator.next();
	    		    newMap.put(element,"COLUMN_NOT_FOUND");
	    		}
	    	}
            
	    	if("delete".equals(jsonResponse.getOperation()) || newMap.isEmpty()) {
	    		newMap.put(primaryId, pkValue);
	    	}
	    	jsonNotification.setResponse_body(newMap);
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    	return jsonNotification;
    }
    
    private void constructJsonCallbackFromCache() throws Exception{
    	PreparedQueryObject pQuery = new PreparedQueryObject();
		JsonCallback jsonCallback = null;
		List<String> notifyList = new java.util.ArrayList<>();
		String cql = 
                "select id, endpoint_userid, endpoint_password, notify_to_endpoint, notify_insert_on,"
                + " notify_delete_on, notify_update_on, request, notifyon from admin.notification_master allow filtering";
        pQuery.appendQueryString(cql);
        //pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), fullTable));
        
        ResultSet rs = MusicCore.get(pQuery);
        Iterator<Row> it = rs.iterator();
        while (it.hasNext()) {
            Row row = (Row) it.next();
        	String endpoint = row.getString("notify_to_endpoint");
            String username = row.getString("endpoint_userid");
            ByteBuffer passwordBytes = row.getBytes("endpoint_password");
            String insert = row.getString("notify_insert_on");
            String delete = row.getString("notify_delete_on");
            String update = row.getString("notify_update_on");
            String request = row.getString("request");
            String notifyon = row.getString("notifyon");
            String uuid = row.getUUID("id").toString();
            notifyList.add(notifyon);
            jsonCallback = new JsonCallback();
            jsonCallback.setApplicationNotificationEndpoint(endpoint);
            
            Charset charset = Charset.forName("ISO-8859-1");
            String decodedPwd = charset.decode(passwordBytes).toString();
            jsonCallback.setApplicationPassword(decodedPwd);
            jsonCallback.setApplicationUsername(username);
            jsonCallback.setNotifyOn(notifyon);
            jsonCallback.setNotifyWhenInsertsIn(insert);
            jsonCallback.setNotifyWhenDeletesIn(delete);
            jsonCallback.setNotifyWhenChangeIn(update);
            jsonCallback.setUuid(uuid);
            logger.info(EELFLoggerDelegate.applicationLogger, "From DB. Saved request_body: "+request);
            request = request.substring(1, request.length()-1);           
            String[] keyValuePairs = request.split(",");              
            Map<String,String> responseBody = new HashMap<>();               

            for(String pair : keyValuePairs) {
                String[] entry = pair.split("="); 
                String val = "";
                if(entry.length == 2)
                	val = entry[1];
                responseBody.put(entry[0], val);          
            }
            logger.info(EELFLoggerDelegate.applicationLogger, "After parsing. Saved request_body: "+responseBody);
            jsonCallback.setResponseBody(responseBody);
            logger.info(EELFLoggerDelegate.applicationLogger, "Updating Cache with updateCallBackCache: "+notifyon+ " :::: "+jsonCallback);
            CachingUtil.updateCallBackCache(notifyon, jsonCallback);
        }
        CachingUtil.updateCallbackNotifyList(notifyList);
    }
    
    @POST
    @Path("/onboardCallback")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addCallback(JsonNotification jsonNotification) {
        Map<String, Object> resultMap = new HashMap<>();
        ResponseBuilder response =
                        Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        String username = jsonNotification.getUsername();
        String password = jsonNotification.getPassword();
        String endpoint = jsonNotification.getEndpoint();
        String notify_field = jsonNotification.getNotify_field();
        Map<String, String> responseBody = jsonNotification.getResponse_body();
        String triggerName = jsonNotification.getTriggerName();
        if(triggerName == null || triggerName.length() == 0)
        	triggerName = "MusicTrigger";
        
        /*JsonCallback callBackCache = CachingUtil.getCallBackCache(notify_field);
        if(callBackCache != null) {
        	resultMap.put("Exception", "The notification property has already been onboarded.");
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }*/
        
        String[] allFields = notify_field.split(":");
        String inserts = null;
        String updates = null;
        String deletes = null;
        String tableName = null;
        if(allFields.length >= 2) {
        	inserts = updates = notify_field;
        } else if(allFields.length == 1) {
        	inserts = deletes = notify_field;;
        }
        tableName = allFields[0];
        String cql = "CREATE TRIGGER IF NOT EXISTS musictrigger ON "+tableName+" Using '"+triggerName+"'";
        PreparedQueryObject pQuery = new PreparedQueryObject();
        
        String uuid = CachingUtil.generateUUID();
        try {
	        pQuery.appendQueryString(
	                        "INSERT INTO admin.notification_master (id, endpoint_userid, endpoint_password, notify_to_endpoint, "
	                                        + "notifyon, notify_insert_on, notify_delete_on, notify_update_on, request, current_notifier) VALUES (?,?,?,?,?,?,?,?,?,?)");
	        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
	        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), username));
	        Charset charset = Charset.forName("ISO-8859-1");
            ByteBuffer decodedPwd = charset.encode(password); 
	        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.blob(), decodedPwd.array()));
	        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), endpoint));
	        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), notify_field));
	        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), inserts));
	        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), deletes));
	        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), updates));
	        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), responseBody));
	        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), MusicCore.getMyHostId()));
	        MusicCore.nonKeyRelatedPut(pQuery, MusicUtil.EVENTUAL);
	        JsonCallback jsonCallback = new JsonCallback();
	        jsonCallback.setUuid(uuid);
	        jsonCallback.setApplicationNotificationEndpoint(endpoint);
	        jsonCallback.setApplicationPassword(password);
	        jsonCallback.setApplicationUsername(username);
	        jsonCallback.setNotifyOn(notify_field);
	        jsonCallback.setNotifyWhenChangeIn(updates);
	        jsonCallback.setNotifyWhenDeletesIn(deletes);
	        jsonCallback.setNotifyWhenInsertsIn(inserts);
	        jsonCallback.setResponseBody(responseBody);
	        CachingUtil.updateCallBackCache(notify_field, jsonCallback);
	        pQuery = new PreparedQueryObject();
	        pQuery.appendQueryString(cql);
	        ResultType nonKeyRelatedPut = MusicCore.nonKeyRelatedPut(pQuery, MusicUtil.EVENTUAL);
	        logger.info(EELFLoggerDelegate.applicationLogger, "Created trigger");
        //callBackCache.put(jsonCallback.getApplicationName(), jsonMap);
        } catch (InvalidQueryException e) {
            logger.error(EELFLoggerDelegate.errorLogger,"Exception callback_api table not configured."+e.getMessage());
            resultMap.put("Exception", "Please make sure admin.notification_master table is configured.");
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        } catch(Exception e) {
        	e.printStackTrace();
        	resultMap.put("Exception", "Exception Occured.");
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }
       return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setMessage("Callback api successfully registered").toMap()).build();
    }
    
    @DELETE
    @Path("/onboardCallback")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response deleteCallbackProp(JsonNotification jsonNotification) {
    	Map<String, Object> resultMap = new HashMap<>();
        ResponseBuilder response =
                        Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        String notifyOn = jsonNotification.getNotify_field();
        PreparedQueryObject pQuery = new PreparedQueryObject();
        try {
	        pQuery.appendQueryString("DELETE FROM admin.notification_master WHERE notifyon = ?");
	        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), notifyOn));
	        MusicCore.nonKeyRelatedPut(pQuery, MusicUtil.EVENTUAL);
        } catch(Exception e) {
        	e.printStackTrace();
        	return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setMessage("Callback api registration failed").toMap()).build();
        }
    	return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setMessage("Callback api successfully deleted").toMap()).build();
    }

    /*public String encodePwd(String password) {
    	return Base64.getEncoder().encodeToString(password.getBytes());
    }
    
    public String decodePwd(String password) {
    	byte[] bytes = Base64.getDecoder().decode(password); 
    	return new String(bytes);
    }*/
}
