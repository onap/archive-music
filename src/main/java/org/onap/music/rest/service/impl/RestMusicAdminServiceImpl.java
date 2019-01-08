/*
 * ============LICENSE_START==========================================
 *  org.onap.music
 * ===================================================================
 *  Copyright (c) 2018 IBM.
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

package org.onap.music.rest.service.impl;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.onap.music.datastore.jsonobjects.JsonOnboard;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicUtil;
import org.onap.music.rest.repository.RestMusicAdminRepository;
import org.onap.music.rest.service.RestMusicAdminService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.datastax.driver.core.ResultSet;

@Service("service1")
@Transactional
public class RestMusicAdminServiceImpl implements RestMusicAdminService {

    @Autowired
    private RestMusicAdminRepository restMusicAdminRepository;

    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(RestMusicAdminServiceImpl.class);

    @Override
    public Response onboardAppWithMusic(JsonOnboard jsonObj) throws Exception {
        ResponseBuilder response = Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        Map<String, Object> resultMap = new HashMap<>();
        String appName = jsonObj.getAppname();
        String userId = jsonObj.getUserId();
        String isAAF = jsonObj.getIsAAF();
        String password = jsonObj.getPassword();
        if (appName == null || userId == null || isAAF == null || password == null) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            resultMap.put("Exception",
                    "Unauthorized: Please check the request parameters. Some of the required values appName(ns), userId, password, isAAF are missing.");
            return Response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }

        ResultSet rs = restMusicAdminRepository.getUuidFromKeySpaceMasterUsingAppName(appName);
        if (!rs.all().isEmpty()) {
            resultMap.put("Exception", "Application " + appName + " has already been onboarded. Please contact admin.");
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        String uuid = CachingUtil.generateUUID();
        String returnStr = restMusicAdminRepository.insertValuesIntoKeySpaceMaster(uuid, appName, userId, isAAF,
                password);
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

}
