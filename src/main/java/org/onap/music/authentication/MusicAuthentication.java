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

import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicUtil;
import org.onap.music.service.MusicCoreService;
import org.onap.music.service.impl.MusicCassaCore;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

public class MusicAuthentication {
	
	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicAuthentication.class);
	
	private static MusicCoreService musicCore = MusicCassaCore.getInstance();
	
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
    public static Map<String, Object> authenticate(String nameSpace, String userId,
                    String password, String keyspace, String aid, String operation)
                    throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        String uuid = null;
        resultMap = CachingUtil.validateRequest(nameSpace, userId, password, keyspace, aid,
                        operation);
        if (!resultMap.isEmpty())
            return resultMap;
        String isAAFApp = null;
        try {
            isAAFApp= CachingUtil.isAAFApplication(nameSpace);
        } catch(MusicServiceException e) {
           resultMap.put("Exception", e.getMessage());
           return resultMap;
        }
        if(isAAFApp == null) {
            resultMap.put("Exception", "Namespace: "+nameSpace+" doesn't exist. Please make sure ns(appName)"
                    + " is correct and Application is onboarded.");
            return resultMap;
        }
        boolean isAAF = Boolean.valueOf(isAAFApp);
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
            	logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.AUTHENTICATIONERROR  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
                logger.error(EELFLoggerDelegate.errorLogger,"Got exception while AAF authentication for namespace " + nameSpace);
                resultMap.put("Exception", e.getMessage());
            }
            if (!isValid) {
            	logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.AUTHENTICATIONERROR  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
                resultMap.put("Exception", "User not authenticated...");
            }
            if (!resultMap.isEmpty())
                return resultMap;

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
                Row rs = musicCore.get(pQuery).one();
                uuid = rs.getUUID("uuid").toString();
                resultMap.put("uuid", "existing");
            } catch (Exception e) {
                logger.info(EELFLoggerDelegate.applicationLogger,"No UUID found in DB. So creating new UUID.");
                uuid = CachingUtil.generateUUID();
                resultMap.put("uuid", "new");
            }
            resultMap.put("aid", uuid);
        }

        return resultMap;
    }

}
