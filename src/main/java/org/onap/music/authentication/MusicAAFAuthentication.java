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
import org.onap.music.authentication.MusicAuthenticator.Operation;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class MusicAAFAuthentication implements MusicAuthenticator {
    
     private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicAAFAuthentication.class);
    
    @Override
    public boolean authenticateAdmin(String authorization) {
        logger.info(EELFLoggerDelegate.applicationLogger, "MusicCore.authenticateAdmin: ");
        String userId = MusicUtil.extractBasicAuthentication(authorization).get(MusicUtil.USERID);
        if(MusicUtil.getIsCadi()) {
            CachingUtil.updateAdminUserCache(authorization, userId);
            return true;
        }
        CacheAccess<String, String> adminCache = CachingUtil.getAdminUserCache();
        if (authorization == null) {
            logger.error(EELFLoggerDelegate.errorLogger, "Authorization cannot be empty...");
            return false;
        }
        if (adminCache.get(authorization) != null && adminCache.get(authorization).equals(userId)) {
            logger.info(EELFLoggerDelegate.applicationLogger, "MusicCore.authenticateAdmin: Validated against admincache.. ");
            return true;
        }
        else {
            Client client = Client.create();
            String aafUrl = MusicUtil.getAafAdminUrl();
            if (aafUrl==null) {
                logger.error(EELFLoggerDelegate.errorLogger, "Admin url is not set, please set in properties");
                return false;
            }
            
            WebResource webResource = client.resource(
                    MusicUtil.getAafAdminUrl().concat(userId).concat("/").concat(MusicUtil.getAdminAafRole()));

            ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON)
                    .header("Authorization", authorization).get(ClientResponse.class);
            if (response.getStatus() == 200) {
                CachingUtil.updateAdminUserCache(authorization, userId);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean authenticateUser(String namespace, String authorization, String keyspace,
            String aid, Operation operation) {
        logger.info(EELFLoggerDelegate.applicationLogger,"Inside User Authentication.......");
        Map<String,String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        String userId = userCredentials.get(MusicUtil.USERID);
        String password = userCredentials.get(MusicUtil.PASSWORD);

        Map<String, Object> resultMap = new HashMap<>();
        String uuid = null;
        if(! MusicUtil.getIsCadi()) {
            resultMap = CachingUtil.validateRequest(namespace, userId, password, keyspace, aid,
                            operation);
            if (!resultMap.isEmpty())
                return false;
            String isAAFApp = null;
            try {
                isAAFApp= CachingUtil.isAAFApplication(namespace);
            } catch(MusicServiceException e) {
                logger.error(e.getErrorMessage(), e);
               resultMap.put("Exception", e.getMessage());
               return false;
            }
            if(isAAFApp == null) {
                resultMap.put("Exception", "Namespace: "+namespace+" doesn't exist. Please make sure ns(appName)"
                        + " is correct and Application is onboarded.");
                return false;
            }
            boolean isAAF = Boolean.parseBoolean(isAAFApp);
            if (userId == null || password == null) {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
                logger.error(EELFLoggerDelegate.errorLogger,"UserId/Password or more required headers is missing.");
                resultMap.put("Exception",
                                "UserId and Password are mandatory for the operation " + operation);
                return false;
            }
            if(!isAAF && !(operation==Operation.CREATE_KEYSPACE)) {
                resultMap = CachingUtil.authenticateAIDUser(namespace, userId, password, keyspace);
                if (!resultMap.isEmpty())
                    return false;
    
            }
            if (isAAF && namespace != null && userId != null && password != null) {
                boolean isValid = true;
                try {
                     isValid = CachingUtil.authenticateAAFUser(namespace, userId, password, keyspace);
                } catch (Exception e) {
                    logger.error(EELFLoggerDelegate.errorLogger,"Error while aaf authentication for user:" + userId);
                    logger.error(EELFLoggerDelegate.errorLogger,"Error: "+ e.getMessage());
                    logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.AUTHENTICATIONERROR  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
                    logger.error(EELFLoggerDelegate.errorLogger,"Got exception while AAF authentication for namespace " + namespace);
                    resultMap.put("Exception", e.getMessage());
                }
                if (!isValid) {
                    logger.error(EELFLoggerDelegate.errorLogger,"User not authenticated...", AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
                    resultMap.put("Exception", "User not authenticated...");
                }
                if (!resultMap.isEmpty())
                    return false;
    
            }
        } else {
            
            String cachedKS = CachingUtil.getKSFromCadiCache(userId);
            if(cachedKS != null && !cachedKS.equals(keyspace)) {
                resultMap.put("Exception", "User not authenticated to access this keyspace...");
                return false;
            }
        }
        
        if (operation==Operation.CREATE_KEYSPACE) {
            try {
                logger.info(EELFLoggerDelegate.applicationLogger,"AID is not provided. Creating new UUID for keyspace.");
                PreparedQueryObject pQuery = new PreparedQueryObject();
                pQuery.appendQueryString(
                                "select uuid from admin.keyspace_master where application_name=? and username=? and keyspace_name=? allow filtering");
                pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), namespace));
                pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
                pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(),
                                MusicUtil.DEFAULTKEYSPACENAME));
                Row rs = MusicCore.get(pQuery).one();
                uuid = rs.getUUID("uuid").toString();
                resultMap.put("uuid", "existing");
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.applicationLogger,"No UUID found in DB. So creating new UUID.");
                uuid = MusicUtil.generateUUID();
                resultMap.put("uuid", "new");
            }
            resultMap.put("aid", uuid);
            CachingUtil.updateCadiCache(userId, keyspace);
        }
        return true;
    }
    
}
