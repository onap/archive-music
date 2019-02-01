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

package org.onap.music.authentication;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.commons.jcs.access.CacheAccess;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class MusicAuthentication {
    
     private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicAuthentication.class);
    
    /**
     * authenticate user logic
     *
     * @param nameSpace
     * @param userId
     * @param password
     * @param keyspace
     * @param aid
     * @param operation
     * @return
     * @throws Exception
     */
    public static Map<String, Object> autheticateUser(String nameSpace, String userId,
                    String password, String keyspace, String aid, String operation)
                    throws Exception {
        logger.info(EELFLoggerDelegate.applicationLogger,"Inside User Authentication.......");
        Map<String, Object> resultMap = new HashMap<>();
        String uuid = null;
        if(! MusicUtil.getIsCadi()) {
            resultMap = CachingUtil.validateRequest(nameSpace, userId, password, keyspace, aid,
                            operation);
            if (!resultMap.isEmpty())
                return resultMap;
            String isAAFApp = null;
            try {
                isAAFApp= CachingUtil.isAAFApplication(nameSpace);
            } catch(MusicServiceException e) {
                logger.error(e.getErrorMessage(), e);
               resultMap.put("Exception", e.getMessage());
               return resultMap;
            }
            if(isAAFApp == null) {
                resultMap.put("Exception", "Namespace: "+nameSpace+" doesn't exist. Please make sure ns(appName)"
                        + " is correct and Application is onboarded.");
                return resultMap;
            }
            boolean isAAF = Boolean.parseBoolean(isAAFApp);
            if (userId == null || password == null) {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
                logger.error(EELFLoggerDelegate.errorLogger,"One or more required headers is missing. userId: " + userId
                                + " :: password: " + password);
                resultMap.put("Exception",
                                "UserId and Password are mandatory for the operation " + operation);
                return resultMap;
            }
            if(!isAAF && !(operation.equals("createKeySpace"))) {
                resultMap = CachingUtil.authenticateAIDUser(nameSpace, userId, password, keyspace);
                if (!resultMap.isEmpty())
                    return resultMap;
    
            }
            if (isAAF && nameSpace != null && userId != null && password != null) {
                boolean isValid = true;
                try {
                     isValid = CachingUtil.authenticateAAFUser(nameSpace, userId, password, keyspace);
                } catch (Exception e) {
                    logger.error(EELFLoggerDelegate.errorLogger,"Error while aaf authentication for user:" + userId);
                    logger.error(EELFLoggerDelegate.errorLogger,"Error: "+ e.getMessage());
                    logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.AUTHENTICATIONERROR  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
                    logger.error(EELFLoggerDelegate.errorLogger,"Got exception while AAF authentication for namespace " + nameSpace);
                    resultMap.put("Exception", e.getMessage());
                }
                if (!isValid) {
                    logger.error(EELFLoggerDelegate.errorLogger,"User not authenticated...", AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
                    resultMap.put("Exception", "User not authenticated...");
                }
                if (!resultMap.isEmpty())
                    return resultMap;
    
            }
        } else {
            
            String cachedKS = CachingUtil.getKSFromCadiCache(userId);
            if(cachedKS != null && !cachedKS.equals(keyspace)) {
                resultMap.put("Exception", "User not authenticated to access this keyspace...");
            }
        }
        
        if (operation.equals("createKeySpace")) {
            logger.info(EELFLoggerDelegate.applicationLogger,"AID is not provided. Creating new UUID for keyspace.");
            PreparedQueryObject pQuery = new PreparedQueryObject();
            pQuery.appendQueryString(
                            "select uuid from admin.keyspace_master where application_name=? and username=? and keyspace_name=? allow filtering");
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), nameSpace));
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(),
                            MusicUtil.DEFAULTKEYSPACENAME));

            try {
                Row rs = MusicCore.get(pQuery).one();
                uuid = rs.getUUID("uuid").toString();
                resultMap.put("uuid", "existing");
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.applicationLogger,"No UUID found in DB. So creating new UUID.");
                uuid = CachingUtil.generateUUID();
                resultMap.put("uuid", "new");
            }
            resultMap.put("aid", uuid);
            CachingUtil.updateCadiCache(userId, keyspace);
        }
        
        return resultMap;
    }

    
    public static boolean authenticateAdmin(String id,String password) {
        return (id.equals(MusicUtil.getAdminId()) && password.equals(MusicUtil.getAdminPass()));
    }

    public static boolean authenticateAdmin(Map<String,String> adminCredentials) {
        if(adminCredentials.containsKey("ERROR"))
            return false;
         String admin_id = adminCredentials.get(MusicUtil.USERID);
         String admin_password = adminCredentials.get(MusicUtil.PASSWORD);
         return (admin_id.equals(MusicUtil.getAdminId()) && admin_password.equals(MusicUtil.getAdminPass()));
    }

    public static boolean authenticateAdmin(String authorization) throws Exception {
        logger.info(EELFLoggerDelegate.applicationLogger, "MusicCore.authenticateAdmin: "+authorization);
        String userId = MusicUtil.extractBasicAuthentication(authorization).get(MusicUtil.USERID);
        if(MusicUtil.getIsCadi()) {
            CachingUtil.updateAdminUserCache(authorization, userId);
            return true;
        }
        CacheAccess<String, String> adminCache = CachingUtil.getAdminUserCache();
        if (authorization == null) {
            logger.error(EELFLoggerDelegate.errorLogger, "Authorization cannot be empty..."+authorization);
            throw new Exception("Authorization cannot be empty");
        }
        if (adminCache.get(authorization) != null && adminCache.get(authorization).equals(userId)) {
            logger.info(EELFLoggerDelegate.applicationLogger, "MusicCore.authenticateAdmin: Validated against admincache.. "+authorization);
            return true;
        }
        else {
            Client client = Client.create();
            WebResource webResource = client.resource(
                    MusicUtil.getAafAdminUrl().concat(userId).concat("/").concat(MusicUtil.getAdminAafRole()));
            ;

            ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON)
                    .header("Authorization", authorization).get(ClientResponse.class);
            if (response.getStatus() == 200) {
                CachingUtil.updateAdminUserCache(authorization, userId);
                return true;
            }
        }
        return false;

    }
    
}
