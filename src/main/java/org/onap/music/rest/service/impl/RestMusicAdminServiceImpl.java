/*
 * ============LICENSE_START==========================================
 *  org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 IBM.
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.mindrot.jbcrypt.BCrypt;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonOnboard;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.rest.repository.RestMusicAdminRepository;
import org.onap.music.rest.service.RestMusicAdminService;
import org.onap.music.rest.util.RestMusicAdminAPIUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

@Service("restAdminService")
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
            String message = "Unauthorized: Please check the request parameters. Enter atleast one of the following parameters: appName(ns), aid, isAAF.";
            return RestMusicAdminAPIUtil.sendUnauthorisedResponseForEmptyParams(logger, message);
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

    @Override
    public Response getOnboardedInfoSearch(JsonOnboard jsonObj) throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        ResponseBuilder response = Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        String appName = jsonObj.getAppname();
        String uuid = jsonObj.getAid();
        String isAAF = jsonObj.getIsAAF();
        String message = "Unauthorized: Please check the request parameters. Enter atleast one of the following parameters: appName(ns), aid, isAAF.";
        if (appName == null && uuid == null && isAAF == null) {
            return RestMusicAdminAPIUtil.sendUnauthorisedResponseForEmptyParams(logger, message);
        }

        ResultSet rs = restMusicAdminRepository.fetchOnboardedInfoSearch(appName, uuid, isAAF);
        Iterator<Row> it = rs.iterator();
        while (it.hasNext()) {
            Row row = (Row) it.next();
            resultMap.put(row.getUUID("uuid").toString(), row.getString("keyspace_name"));
        }
        if (resultMap.isEmpty()) {
            if (uuid != null) {
                resultMap.put("Exception", "Please make sure Aid is correct and application is onboarded.");
                return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
            } else {
                resultMap.put("Exception",
                        "Application is not onboarded. Please make sure all the information is correct.");
                return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
            }
        }
        return Response.status(Status.OK).entity(resultMap).build();
    }

    @Override
    public Response deleteOnboardApp(JsonOnboard jsonObj) throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        ResponseBuilder response = Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        String appName = jsonObj.getAppname();
        String aid = jsonObj.getAid();
        PreparedQueryObject pQuery = new PreparedQueryObject();
        String consistency = MusicUtil.EVENTUAL;
        ;
        if (appName == null && aid == null) {
            String message = "Please make sure either appName(ns) or Aid is present";
            return RestMusicAdminAPIUtil.sendUnauthorisedResponseForEmptyParams(logger, message);
        }
        if (aid != null) {

            ResultSet rs = restMusicAdminRepository.getKeySpaceNameFromKeySpaceMasterWithUuid(aid);
            Row row = rs.one();
            if (row != null) {
                String ks = row.getString("keyspace_name");
                if (!ks.equals(MusicUtil.DEFAULTKEYSPACENAME)) {
                    restMusicAdminRepository.dropKeySpace(ks, consistency);
                }
            }
            ResultType result = restMusicAdminRepository.deleteFromKeySpaceMasterWithUuid(aid, consistency);
            if (result == ResultType.SUCCESS) {
                resultMap.put("Success", "Your application has been deleted successfully");
            } else {
                resultMap.put("Exception",
                        "Oops. Something went wrong. Please make sure Aid is correct or Application is onboarded");
                logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                        ErrorTypes.DATAERROR);
                return Response.status(Status.BAD_REQUEST).entity(resultMap).build();

            }
            return Response.status(Status.OK).entity(resultMap).build();
        }

        ResultSet rs = restMusicAdminRepository.getKeySpaceNameFromKeySpaceMasterWithAppName(appName);
        List<Row> rows = rs.all();
        String uuid = null;
        if (rows.size() == 0) {
            resultMap.put("Exception", "Application not found. Please make sure Application exists.");
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.DATAERROR);
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        } else if (rows.size() == 1) {
            uuid = rows.get(0).getUUID("uuid").toString();
            pQuery = new PreparedQueryObject();
            ResultSet rs1 = restMusicAdminRepository.getKeySpaceNameFromKeySpaceMasterWithUuid(aid);
            Row row = rs1.one();
            String ks = row.getString("keyspace_name");
            if (!ks.equals(MusicUtil.DEFAULTKEYSPACENAME)) {
                restMusicAdminRepository.dropKeySpace(ks, consistency);
            }

            restMusicAdminRepository.deleteFromKeySpaceMasterWithUuid(uuid, consistency);
            resultMap.put("Success", "Your application " + appName + " has been deleted.");
            return Response.status(Status.OK).entity(resultMap).build();
        } else {
            resultMap.put("Failure", "More than one Aid exists for this application, so please provide Aid.");
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MULTIPLERECORDS, ErrorSeverity.CRITICAL,
                    ErrorTypes.DATAERROR);
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }
    }

    @Override
    public Response updateOnboardApp(JsonOnboard jsonObj) throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        ResponseBuilder response = Response.noContent().header("X-latestVersion", MusicUtil.getVersion());
        String aid = jsonObj.getAid();
        String appName = jsonObj.getAppname();
        String userId = jsonObj.getUserId();
        String isAAF = jsonObj.getIsAAF();
        String password = jsonObj.getPassword();
        String consistency = "eventual";
        PreparedQueryObject pQuery;

        if (aid == null) {
            resultMap.put("Exception", "Please make sure Aid is present");
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.DATAERROR);
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        if (appName == null && userId == null && password == null && isAAF == null) {
            resultMap.put("Exception", "No parameters found to update. Please update atleast one parameter.");
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.DATAERROR);
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        if (appName != null) {
            ResultSet rs = restMusicAdminRepository.getUuidFromKeySpaceMasterUsingAppName(appName);
            if (!rs.all().isEmpty()) {
                resultMap.put("Exception",
                        "Application " + appName + " has already been onboarded. Please contact admin.");
                logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.ALREADYEXIST, ErrorSeverity.CRITICAL,
                        ErrorTypes.DATAERROR);
                return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
            }
        }

        ResultType result = restMusicAdminRepository.updateKeySpaceMaster(appName, userId, password, isAAF, aid,
                consistency);

        if (result == ResultType.SUCCESS) {
            resultMap.put("Success", "Your application has been updated successfully");
        } else {
            resultMap.put("Exception",
                    "Oops. Something went wrong. Please make sure Aid is correct and application is onboarded");
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.DATAERROR);
            return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        return Response.status(Status.OK).entity(resultMap).build();
    }

}
