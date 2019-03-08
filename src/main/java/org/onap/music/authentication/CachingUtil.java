/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2018 IBM
 *  Modifications Copyright (c) 2019 Samsung
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

package org.onap.music.authentication;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.jcs.JCS;
import org.apache.commons.jcs.access.CacheAccess;
import org.mindrot.jbcrypt.BCrypt;
import org.onap.music.authentication.MusicAuthenticator.Operation;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * All Caching related logic is handled by this class and a schedule cron runs to update cache.
 * 
 * @author Vikram
 *
 */
public class CachingUtil implements Runnable {

    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(CachingUtil.class);

    /** keyspace & ns */
    private static CacheAccess<String, String> musicCache = JCS.getInstance("musicCache");
    /** cache to hold isaaf application */
    private static CacheAccess<String, String> appNameCache = JCS.getInstance("appNameCache");
    /** hold user creds for namespace */
    private static CacheAccess<String, Map<String, String>> musicValidateCache = JCS.getInstance("musicValidateCache");
    private static Map<String, Number> userAttempts = new HashMap<>();
    private static Map<String, Calendar> lastFailedTime = new HashMap<>();
    private static CacheAccess<String, String> adminUserCache = JCS.getInstance("adminUserCache");
    
    public static CacheAccess<String, String> getAdminUserCache() {
        return adminUserCache;
    }
    
    public static void updateAdminUserCache(String authorization,String userId) {
        adminUserCache.put(authorization,userId);
    }
    
    private static final String USERNAME="username";
    private static final String PASSWORD="password";


    public void initializeAafCache() throws MusicServiceException {
        logger.info(EELFLoggerDelegate.applicationLogger,"Resetting and initializing AAF Cache...");

        String query = "SELECT uuid, application_name, keyspace_name, username, password FROM admin.keyspace_master WHERE is_api = ? allow filtering";
        PreparedQueryObject pQuery = new PreparedQueryObject();
        pQuery.appendQueryString(query);
        try {
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), false));
        } catch (Exception e1) {
            logger.error(EELFLoggerDelegate.errorLogger, e1.getMessage(),AppMessages.CACHEERROR, ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
        }
        ResultSet rs = MusicCore.get(pQuery);
        Iterator<Row> it = rs.iterator();
        Map<String, String> map = null;
        while (it.hasNext()) {
            Row row = it.next();
            String nameSpace = row.getString("keyspace_name");
            String userId = row.getString(USERNAME);
            String password = row.getString(PASSWORD);
            String keySpace = row.getString("application_name");
            try {
                userAttempts.put(nameSpace, 0);
                boolean responseObj = triggerAAF(nameSpace, userId, password);
                if (responseObj) {
                    map = new HashMap<>();
                    map.put(userId, password);
                    musicValidateCache.put(nameSpace, map);
                    musicCache.put(keySpace, nameSpace);
                    logger.debug("Cronjob: Cache Updated with AAF response for namespace "
                                    + nameSpace);
                }
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.INFO, ErrorTypes.GENERALSERVICEERROR);
                logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),"Something at AAF was changed for ns: " + nameSpace+" So not updating Cache for the namespace. ");
            }
        }

    }

    @Override
    public void run() {
        logger.info(EELFLoggerDelegate.applicationLogger,"Scheduled task invoked. Refreshing Cache...");
        try {
            initializeAafCache();
        } catch (MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.INFO, ErrorTypes.GENERALSERVICEERROR);
        }
    }

    public static boolean authenticateAAFUser(String nameSpace, String userId, String password,
                    String keySpace) throws Exception {

        if (musicValidateCache.get(nameSpace) != null && musicCache.get(keySpace)!=null) {
            if (keySpace != null && !musicCache.get(keySpace).equals(nameSpace)) {
                logger.info(EELFLoggerDelegate.applicationLogger,"Create new application for the same namespace.");
            } else if (musicValidateCache.get(nameSpace).get(userId).equals(password)) {
                logger.info(EELFLoggerDelegate.applicationLogger,"Authenticated with cache value..");
                // reset invalid attempts to 0
                userAttempts.put(nameSpace, 0);
                return true;
            } else {
                // call AAF update cache with new password
                if (userAttempts.get(nameSpace) == null)
                    userAttempts.put(nameSpace, 0);
                if ((Integer) userAttempts.get(nameSpace) >= 3) {
                    logger.info(EELFLoggerDelegate.applicationLogger,"Reached max attempts. Checking if time out..");
                    logger.info(EELFLoggerDelegate.applicationLogger,"Failed time: "+lastFailedTime.get(nameSpace).getTime());
                    Calendar calendar = Calendar.getInstance();
                    long delayTime = (calendar.getTimeInMillis()-lastFailedTime.get(nameSpace).getTimeInMillis());
                    logger.info(EELFLoggerDelegate.applicationLogger,"Delayed time: "+delayTime);
                    if( delayTime > 120000) {
                        logger.info(EELFLoggerDelegate.applicationLogger,"Resetting failed attempt.");
                        userAttempts.put(nameSpace, 0);
                    } else {
                        logger.info(EELFLoggerDelegate.applicationLogger,"No more attempts allowed. Please wait for atleast 2 min.");
                        throw new Exception("No more attempts allowed. Please wait for atleast 2 min.");
                    }
                }
                logger.error(EELFLoggerDelegate.errorLogger,"",AppMessages.CACHEAUTHENTICATION,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
                logger.info(EELFLoggerDelegate.applicationLogger,"Check AAF again...");
            }
        }

        boolean responseObj = false;
        try {
            responseObj = triggerAAF(nameSpace, userId, password);
        }catch (Exception ex) {
            logger.info("Exception while trigger aaf");
            logger.info("Exception: " + ex.getMessage());
            throw new Exception("Exception raised while triggering AAF authentication" +ex.getMessage());
        }
        if (responseObj) {
            logger.info(EELFLoggerDelegate.applicationLogger,"Valid user. Cache is updated for "+nameSpace);
                Map<String, String> map = new HashMap<>();
                map.put(userId, password);
                musicValidateCache.put(nameSpace, map);
                musicCache.put(keySpace, nameSpace);
                return true;
        }
        logger.info(EELFLoggerDelegate.applicationLogger,"Invalid user. Cache not updated");
        return false;
    }

    private static boolean triggerAAF(String nameSpace, String userId, String password)
                    throws Exception {
        logger.info(EELFLoggerDelegate.applicationLogger,"Inside AAF authorization");
        if (MusicUtil.getAafEndpointUrl() == null) {
            logger.error(EELFLoggerDelegate.errorLogger,"AAF endpoint is not set. Please specify in the properties file.",AppMessages.UNKNOWNERROR,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
            throw new Exception("AAF endpoint is not set. Please specify in the properties file.");
        }
        Client client = Client.create();
        // WebResource webResource =
        // client.resource("https://aaftest.test.att.com:8095/proxy/authz/nss/"+nameSpace);
        WebResource webResource = client.resource(MusicUtil.getAafEndpointUrl().concat(nameSpace));
        String plainCreds = userId + ":" + password;
        byte[] plainCredsBytes = plainCreds.getBytes();
        byte[] base64CredsBytes = Base64.encodeBase64(plainCredsBytes);
        String base64Creds = new String(base64CredsBytes);

        ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON)
                        .header("Authorization", "Basic " + base64Creds)
                        .header("content-type", "application/json").get(ClientResponse.class);
        logger.info(EELFLoggerDelegate.applicationLogger, "aaf response: "+response.toString());
        if (response.getStatus() != 200) {
            if (userAttempts.get(nameSpace) == null)
                userAttempts.put(nameSpace, 0);
            if ((Integer) userAttempts.get(nameSpace) >= 2) {
                lastFailedTime.put(nameSpace, Calendar.getInstance());
                userAttempts.put(nameSpace, ((Integer) userAttempts.get(nameSpace) + 1));
                throw new Exception(
                                "Reached max invalid attempts. Please contact admin and retry with valid credentials.");
            }
            userAttempts.put(nameSpace, ((Integer) userAttempts.get(nameSpace) + 1));
            throw new Exception(
                            "Unable to authenticate. Please check the AAF credentials against namespace.");
            // TODO Allow for 2-3 times and forbid any attempt to trigger AAF with invalid values
            // for specific time.
        }
        /*response.getHeaders().put(HttpHeaders.CONTENT_TYPE,
                        Arrays.asList(MediaType.APPLICATION_JSON));
        // AAFResponse output = response.getEntity(AAFResponse.class);
        response.bufferEntity();
        String x = response.getEntity(String.class);
        AAFResponse responseObj = new ObjectMapper().readValue(x, AAFResponse.class);*/
        
        return true;
    }

    public static void updateMusicCache(String keyspace, String nameSpace) {
        logger.info(EELFLoggerDelegate.applicationLogger,"Updating musicCache for keyspace " + keyspace + " with nameSpace " + nameSpace);
        musicCache.put(keyspace, nameSpace);
    }

    public static void updateCadiCache(String user, String keyspace) {
        musicCache.put(user, keyspace);
    }
    
    public static String getKSFromCadiCache(String user) {
        return musicCache.get(user);
    }
    
    public static void updateMusicValidateCache(String nameSpace, String userId, String password) {
        logger.info(EELFLoggerDelegate.applicationLogger,"Updating musicCache for nameSpacce " + nameSpace + " with userId " + userId);
        Map<String, String> map = new HashMap<>();
        map.put(userId, password);
        musicValidateCache.put(nameSpace, map);
    }
    
    public static void updateisAAFCache(String namespace, String isAAF) {
        appNameCache.put(namespace, isAAF);
    }

    public static String isAAFApplication(String namespace) throws MusicServiceException {
        String isAAF = appNameCache.get(namespace);
        if (isAAF == null) {
            PreparedQueryObject pQuery = new PreparedQueryObject();
            pQuery.appendQueryString(
                            "SELECT is_aaf from admin.keyspace_master where application_name = '"
                                            + namespace + "' allow filtering");
            Row rs = null;
            try {
                rs = MusicCore.get(pQuery).one();
            } catch(InvalidQueryException e) {
                logger.error(EELFLoggerDelegate.errorLogger,"Exception admin keyspace not configured."+e.getMessage());
                throw new MusicServiceException("Please make sure admin.keyspace_master table is configured.");
            }
            try {
                isAAF = String.valueOf(rs.getBool("is_aaf"));
                if(isAAF != null)
                    appNameCache.put(namespace, isAAF);
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.errorLogger,  e.getMessage(), AppMessages.QUERYERROR,ErrorSeverity.ERROR, ErrorTypes.QUERYERROR);
            }
        }
        return isAAF;
    }

    public static String getUuidFromMusicCache(String keyspace) throws MusicServiceException {
        String uuid = null;
        if (uuid == null) {
            PreparedQueryObject pQuery = new PreparedQueryObject();
            pQuery.appendQueryString(
                            "SELECT uuid from admin.keyspace_master where keyspace_name = '"
                                            + keyspace + "' allow filtering");
            Row rs = MusicCore.get(pQuery).one();
            try {
                uuid = rs.getUUID("uuid").toString();
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.errorLogger,"Exception occurred during uuid retrieval from DB."+e.getMessage());
            }
        }
        return uuid;
    }

    public static String getAppName(String keyspace) throws MusicServiceException {
        String appName = null;
        PreparedQueryObject pQuery = new PreparedQueryObject();
        pQuery.appendQueryString(
                        "SELECT application_name from admin.keyspace_master where keyspace_name = '"
                                        + keyspace + "' allow filtering");
        Row rs = MusicCore.get(pQuery).one();
        try {
            appName = rs.getString("application_name");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger,  e.getMessage(), AppMessages.QUERYERROR, ErrorSeverity.ERROR, ErrorTypes.QUERYERROR);
        }
        return appName;
    }

    @Deprecated
    public static Map<String, Object> validateRequest(String nameSpace, String userId,
                    String password, String keyspace, String aid, String operation) {
        Map<String, Object> resultMap = new HashMap<>();
        if (!"createKeySpace".equals(operation)) {
            if (nameSpace == null) {
                resultMap.put("Exception", "Application namespace is mandatory.");
            }
        }
        return resultMap;
    }

    public static Map<String, Object> validateRequest(String nameSpace, String userId,
            String password, String keyspace, String aid, Operation operation) {
        Map<String, Object> resultMap = new HashMap<>();
        if (Operation.CREATE_KEYSPACE!=operation) {
            if (nameSpace == null) {
                resultMap.put("Exception", "Application namespace is mandatory.");
            }
        }
        return resultMap;
    }
    
    public static Map<String, Object> verifyOnboarding(String ns, String userId, String password) {
        Map<String, Object> resultMap = new HashMap<>();
        if (ns == null || userId == null || password == null) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGINFO ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
            logger.error(EELFLoggerDelegate.errorLogger,"One or more required headers is missing. userId: "+userId+" :: password: "+password);
            resultMap.put("Exception",
                            "One or more required headers appName(ns), userId, password is missing. Please check.");
            return resultMap;
        }
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(
                        "select * from admin.keyspace_master where application_name = ? allow filtering");
        try {
            queryObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), ns));
        } catch(Exception e) {
            resultMap.put("Exception",
                    "Unable to process input data. Invalid input data type. Please check ns, userId and password values. "+e.getMessage());
            return resultMap;
        }
        Row rs = null;
        try {
            rs = MusicCore.get(queryObject).one();
        } catch (MusicServiceException e) {
            String errorMsg = "Unable to process operation. Error is "+e.getMessage();
            logger.error(EELFLoggerDelegate.errorLogger, errorMsg);
            resultMap.put("Exception", errorMsg);
            return resultMap;
        } catch (InvalidQueryException e) {
            logger.error(EELFLoggerDelegate.errorLogger,"Exception admin keyspace not configured."+e.getMessage());
            resultMap.put("Exception", "Please make sure admin.keyspace_master table is configured.");
            return resultMap;
        }
        if (rs == null) {
            logger.error(EELFLoggerDelegate.errorLogger,"Application is not onboarded. Please contact admin.");
            resultMap.put("Exception", "Application is not onboarded. Please contact admin.");
        } else {
            if(!(rs.getString(USERNAME).equals(userId)) || !(BCrypt.checkpw(password, rs.getString(PASSWORD)))) {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.AUTHENTICATIONERROR, ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
                logger.error(EELFLoggerDelegate.errorLogger,"Namespace, UserId and password doesn't match. namespace: "+ns+" and userId: "+userId);
                resultMap.put("Exception", "Namespace, UserId and password doesn't match. namespace: "+ns+" and userId: "+userId);
                return resultMap;
            }
        }
        return resultMap;
    }

    public static Map<String, Object> authenticateAIDUser(String nameSpace, String userId, String password,
           String keyspace) {
        Map<String, Object> resultMap = new HashMap<>();
        String pwd = null;
        if((musicCache.get(keyspace) != null) && (musicValidateCache.get(nameSpace) != null) 
                && (musicValidateCache.get(nameSpace).containsKey(userId))) {
            if(!musicCache.get(keyspace).equals(nameSpace)) {
                resultMap.put("Exception", "Namespace and keyspace doesn't match");
                return resultMap;
            }
            if(!BCrypt.checkpw(password,musicValidateCache.get(nameSpace).get(userId))) {
                resultMap.put("Exception", "Namespace, userId and password doesn't match");
                return resultMap;
            }
            return resultMap;
        }
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(
                        "select * from admin.keyspace_master where keyspace_name = ? allow filtering");
        try {
            queryObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), keyspace));
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger,"Adding value to query object failed: " + e.getMessage());
        }
        Row rs = null;
        try {
            rs = MusicCore.get(queryObject).one();
        } catch (MusicServiceException e) {
            String errMsg = "Unable to process operation. Error is "+e.getMessage();
            logger.error(EELFLoggerDelegate.errorLogger, errMsg);
            resultMap.put("Exception", errMsg);
            return resultMap;
        }
        if(rs == null) {
            resultMap.put("Exception", "Please make sure keyspace:"+keyspace+" exists.");
            return resultMap;
        }
        else {
            String user = rs.getString(USERNAME);
            pwd = rs.getString(PASSWORD);
            String ns = rs.getString("application_name");
            if(!ns.equals(nameSpace)) {
            resultMap.put("Exception", "Namespace and keyspace doesn't match");
            return resultMap;
            }
            if(!user.equals(userId)) {
                resultMap.put("Exception", "Invalid userId :"+userId);
                return resultMap;
            }
            if(!BCrypt.checkpw(password, pwd)) {
                resultMap.put("Exception", "Invalid password");
                return resultMap;
            }
        }
        CachingUtil.updateMusicCache(keyspace, nameSpace);
        CachingUtil.updateMusicValidateCache(nameSpace, userId, pwd);
        return resultMap;
    }
}