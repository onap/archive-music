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
package org.onap.music.main;

import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.jcs.JCS;
import org.apache.commons.jcs.access.CacheAccess;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.onap.music.datastore.jsonobjects.AAFResponse;
import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import org.onap.music.datastore.PreparedQueryObject;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
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

    private static EELFLogger logger = EELFManager.getInstance().getLogger(CachingUtil.class);

    private static CacheAccess<String, String> musicCache = JCS.getInstance("musicCache");
    private static CacheAccess<String, Map<String, String>> aafCache = JCS.getInstance("aafCache");
    private static CacheAccess<String, String> appNameCache = JCS.getInstance("appNameCache");
    private static Map<String, Number> userAttempts = new HashMap<>();
    private static Map<String, Calendar> lastFailedTime = new HashMap<>();

    public boolean isCacheRefreshNeeded() {
        if (aafCache.get("initBlankMap") == null)
            return true;
        return false;
    }

    public void initializeMusicCache() {
        logger.info("Initializing Music Cache...");
        musicCache.put("isInitialized", "true");
    }

    public void initializeAafCache() {
        logger.info("Resetting and initializing AAF Cache...");

        // aafCache.clear();
        // loop through aafCache ns .. only the authenticated ns will be re cached. and non
        // authenticated will wait for user to retry.
        String query = "SELECT application_name, keyspace_name, username, password FROM admin.keyspace_master WHERE is_api = ? allow filtering";
        PreparedQueryObject pQuery = new PreparedQueryObject();
        pQuery.appendQueryString(query);
        try {
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), false));
        } catch (Exception e1) {
            e1.printStackTrace();
            logger.error("Exception is " + e1.getMessage() + "during initalizeAafCache");
        }
        ResultSet rs = MusicCore.get(pQuery);
        Iterator<Row> it = rs.iterator();
        Map<String, String> map = null;
        while (it.hasNext()) {
            Row row = it.next();
            String nameSpace = row.getString("keyspace_name");
            String userId = row.getString("username");
            String password = row.getString("password");
            String keySpace = row.getString("application_name");
            try {
                userAttempts.put(nameSpace, 0);
                AAFResponse responseObj = triggerAAF(nameSpace, userId, password);
                if (responseObj.getNs().size() > 0) {
                    map = new HashMap<>();
                    map.put(userId, password);
                    aafCache.put(nameSpace, map);
                    musicCache.put(nameSpace, keySpace);
                    logger.debug("Cronjob: Cache Updated with AAF response for namespace "
                                    + nameSpace);
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                logger.error("Something at AAF was changed for ns: " + nameSpace
                                + ". So not updating Cache for the namespace. ");
                logger.error("Exception is " + e.getMessage());
            }
        }

    }

    @Override
    public void run() {
        logger.debug("Scheduled task invoked. Refreshing Cache...");
        initializeAafCache();
    }

    public static boolean authenticateAAFUser(String nameSpace, String userId, String password,
                    String keySpace) throws Exception {

        if (aafCache.get(nameSpace) != null) {
            if (!musicCache.get(nameSpace).equals(keySpace)) {
                logger.debug("Create new application for the same namespace.");
            } else if (aafCache.get(nameSpace).get(userId).equals(password)) {
                logger.debug("Authenticated with cache value..");
                // reset invalid attempts to 0
                userAttempts.put(nameSpace, 0);
                return true;
            } else {
                // call AAF update cache with new password
                if (userAttempts.get(nameSpace) == null)
                    userAttempts.put(nameSpace, 0);
                if ((Integer) userAttempts.get(nameSpace) >= 3) {
                    logger.info("Reached max attempts. Checking if time out..");
                    logger.info("Failed time: " + lastFailedTime.get(nameSpace).getTime());
                    Calendar calendar = Calendar.getInstance();
                    long delayTime = (calendar.getTimeInMillis()
                                    - lastFailedTime.get(nameSpace).getTimeInMillis());
                    logger.info("Delayed time: " + delayTime);
                    if (delayTime > 120000) {
                        logger.info("Resetting failed attempt.");
                        userAttempts.put(nameSpace, 0);
                    } else {
                        throw new Exception(
                                        "No more attempts allowed. Please wait for atleast 2 min.");
                    }
                }
                logger.error("Cache not authenticated..");
                logger.info("Check AAF again...");
            }
        }

        AAFResponse responseObj = triggerAAF(nameSpace, userId, password);
        if (responseObj.getNs().size() > 0) {
            if (responseObj.getNs().get(0).getAdmin().contains(userId))
                return true;

        }
        logger.info("Invalid user. Cache not updated");
        return false;
    }

    private static AAFResponse triggerAAF(String nameSpace, String userId, String password)
                    throws Exception {
        if (MusicUtil.getAafEndpointUrl() == null) {
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
        response.getHeaders().put(HttpHeaders.CONTENT_TYPE,
                        Arrays.asList(MediaType.APPLICATION_JSON));
        // AAFResponse output = response.getEntity(AAFResponse.class);
        response.bufferEntity();
        String x = response.getEntity(String.class);
        AAFResponse responseObj = new ObjectMapper().readValue(x, AAFResponse.class);
        return responseObj;
    }

    public static Map<String, Object> authenticateAIDUser(String aid, String keyspace)
                    throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        String uuid = null;
        /*
         * if(aid == null || aid.length() == 0) { resultMap.put("Exception Message",
         * "AID is missing for the keyspace requested."); //create a new AID ?? } else
         */
        if (musicCache.get(keyspace) == null) {
            PreparedQueryObject pQuery = new PreparedQueryObject();
            pQuery.appendQueryString(
                            "SELECT uuid from admin.keyspace_master where keyspace_name = '"
                                            + keyspace + "' allow filtering");
            Row rs = MusicCore.get(pQuery).one();
            try {
                uuid = rs.getUUID("uuid").toString();
                musicCache.put(keyspace, uuid);
            } catch (Exception e) {
                String msg = e.getMessage();
                logger.error("Exception occured during uuid retrieval from DB." + e.getMessage());
                resultMap.put("Exception", "Unauthorized operation. Check AID and Keyspace. "
                                + "Exception from MUSIC is: "
                                + (msg == null ? "Keyspace is new so no AID should be passed in Header."
                                                : msg));
                return resultMap;
            }
            if (!musicCache.get(keyspace).toString().equals(aid)) {
                resultMap.put("Exception Message",
                                "Unauthorized operation. Invalid AID for the keyspace");
                return resultMap;
            }
        } else if (musicCache.get(keyspace) != null
                        && !musicCache.get(keyspace).toString().equals(aid)) {
            resultMap.put("Exception Message",
                            "Unauthorized operation. Invalid AID for the keyspace");
            return resultMap;
        }
        resultMap.put("aid", uuid);
        return resultMap;
    }

    public static void updateMusicCache(String aid, String keyspace) {
        logger.info("Updating musicCache for keyspace " + keyspace + " with aid " + aid);
        musicCache.put(keyspace, aid);
    }

    public static void updateisAAFCache(String namespace, String isAAF) {
        appNameCache.put(namespace, isAAF);
    }

    public static Boolean isAAFApplication(String namespace) {

        String isAAF = appNameCache.get(namespace);
        if (isAAF == null) {
            PreparedQueryObject pQuery = new PreparedQueryObject();
            pQuery.appendQueryString(
                            "SELECT is_aaf from admin.keyspace_master where application_name = '"
                                            + namespace + "' allow filtering");
            Row rs = MusicCore.get(pQuery).one();
            try {
                isAAF = String.valueOf(rs.getBool("is_aaf"));
                appNameCache.put(namespace, isAAF);
            } catch (Exception e) {
                logger.error("Exception occured during uuid retrieval from DB." + e.getMessage());
                e.printStackTrace();
            }
        }
        return Boolean.valueOf(isAAF);
    }

    public static String getUuidFromMusicCache(String keyspace) {
        String uuid = musicCache.get(keyspace);
        if (uuid == null) {
            PreparedQueryObject pQuery = new PreparedQueryObject();
            pQuery.appendQueryString(
                            "SELECT uuid from admin.keyspace_master where keyspace_name = '"
                                            + keyspace + "' allow filtering");
            Row rs = MusicCore.get(pQuery).one();
            try {
                uuid = rs.getUUID("uuid").toString();
                musicCache.put(keyspace, uuid);
            } catch (Exception e) {
                logger.error("Exception occured during uuid retrieval from DB." + e.getMessage());
                e.printStackTrace();
            }
        }
        return uuid;
    }

    public static String getAppName(String keyspace) {
        String appName = null;
        PreparedQueryObject pQuery = new PreparedQueryObject();
        pQuery.appendQueryString(
                        "SELECT application_name from admin.keyspace_master where keyspace_name = '"
                                        + keyspace + "' allow filtering");
        Row rs = MusicCore.get(pQuery).one();
        try {
            appName = rs.getString("application_name");
        } catch (Exception e) {
            logger.error("Exception occured during uuid retrieval from DB." + e.getMessage());
            e.printStackTrace();
        }
        return appName;
    }

    public static String generateUUID() {
        String uuid = UUID.randomUUID().toString();
        logger.info("New AID generated: " + uuid);
        return uuid;
    }

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

    public static Map<String, Object> verifyOnboarding(String ns, String userId, String password)
                    throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        if (ns == null || userId == null || password == null) {
            logger.error("One or more required headers is missing. userId: " + userId
                            + " :: password: " + password);
            resultMap.put("Exception",
                            "One or more required headers appName(ns), userId, password is missing. Please check.");
            return resultMap;
        }
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(
                        "select * from admin.keyspace_master where application_name=? and username=? allow filtering");
        queryObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), ns));
        queryObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
        Row rs = MusicCore.get(queryObject).one();
        if (rs == null) {
            logger.error("Namespace and UserId doesn't match. namespace: " + ns + " and userId: "
                            + userId);
            resultMap.put("Exception", "Application " + ns
                            + " doesn't seem to be Onboarded. Please onboard your application with MUSIC. If already onboarded contact Admin");
        } else {
            boolean is_aaf = rs.getBool("is_aaf");
            String keyspace = rs.getString("keyspace_name");
            if (!is_aaf) {
                if (!keyspace.equals(MusicUtil.DEFAULTKEYSPACENAME)) {
                    logger.error("Non AAF applications are allowed to have only one keyspace per application.");
                    resultMap.put("Exception",
                                    "Non AAF applications are allowed to have only one keyspace per application.");
                }
            }
        }
        return resultMap;
    }
}
