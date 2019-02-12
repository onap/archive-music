/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2019 IBM
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

import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.onap.music.authentication.MusicAuthentication;
import org.onap.music.datastore.jsonobjects.CassaKeyspaceObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.response.jsonobjects.JsonResponse;
import org.onap.music.rest.repository.MusicDataAPIRepository;
import org.onap.music.rest.service.MusicDataAPIService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.MultiValueMap;

@Service
@Transactional
public class MusicDataAPIServiceImpl implements MusicDataAPIService {

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicDataAPIServiceImpl.class);

    private final MusicDataAPIRepository musicDataAPIRepository;

    @Autowired
    public MusicDataAPIServiceImpl(final MusicDataAPIRepository musicDataAPIRepository) {
        this.musicDataAPIRepository = musicDataAPIRepository;
    }

    @Override
    public Response createKeySpace(String version, String minorVersion, String patchVersion, String authorization,
            String aid, String ns, CassaKeyspaceObject cassaKeyspaceObject, String keyspaceName) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);

        Map<String, String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        String userId = userCredentials.get(MusicUtil.USERID);
        String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = CachingUtil.verifyOnboarding(ns, userId, password);

        if (!authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            response.status(Status.UNAUTHORIZED);
            return response.entity(
                    new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap())
                    .build();
        }

        if (cassaKeyspaceObject == null || cassaKeyspaceObject.getReplicationInfo() == null) {
            authMap.put(ResultType.EXCEPTION.getResult(), ResultType.BODYMISSING.getResult());
            response.status(Status.BAD_REQUEST);
            return response.entity(authMap).build();
        }

        try {
            authMap = musicDataAPIRepository.musicAuthentication(ns, userId, password,
                    cassaKeyspaceObject.getKeyspaceName(), aid, "createKeySpace");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.MISSINGDATA,
                    ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            response.status(Status.BAD_REQUEST);
            return response.entity(new JsonResponse(ResultType.FAILURE).setError("Unable to authenticate.").toMap())
                    .build();
        }

        String newAid = null;
        if (!authMap.isEmpty()) {
            if (authMap.containsKey("aid")) {
                newAid = (String) authMap.get("aid");
            } else {
                logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
                        ErrorTypes.AUTHENTICATIONERROR);
                response.status(Status.UNAUTHORIZED);
                return response.entity(
                        new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap())
                        .build();
            }
        }

        String consistency = MusicUtil.EVENTUAL;// for now this needs only eventual consistency

        ResultType result = ResultType.FAILURE;
        try {
            if (null != keyspaceName && !keyspaceName.isEmpty()) {
                cassaKeyspaceObject.setKeyspaceName(keyspaceName);
            }
            result = musicDataAPIRepository.createKeySpace(cassaKeyspaceObject);
            logger.info(EELFLoggerDelegate.applicationLogger, "result = " + result);
        } catch (MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(), AppMessages.UNKNOWNERROR, ErrorSeverity.WARN,
                    ErrorTypes.MUSICSERVICEERROR);
            return response.status(Status.BAD_REQUEST)
                    .entity(new JsonResponse(ResultType.FAILURE).setError("err:" + ex.getMessage()).toMap()).build();
        }

        try {
            ResultType resultType = musicDataAPIRepository.createRole(userId, password,
                    cassaKeyspaceObject.getKeyspaceName(), consistency);
            logger.info(EELFLoggerDelegate.applicationLogger, "resultType => " + resultType);
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.UNKNOWNERROR, ErrorSeverity.WARN,
                    ErrorTypes.MUSICSERVICEERROR);
        }

        try {
            musicDataAPIRepository.createKeySpaceMasterEntry(userId, password, consistency, newAid,
                    cassaKeyspaceObject.getKeyspaceName());
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.UNKNOWNERROR, ErrorSeverity.WARN,
                    ErrorTypes.MUSICSERVICEERROR);
            return response.status(Response.Status.BAD_REQUEST)
                    .entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }

        return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS)
                .setMessage("Keyspace " + cassaKeyspaceObject.getKeyspaceName() + " Created").toMap()).build();
    }

    @Override
    public Response dropKeySpace(String version, String minorVersion, String patchVersion, String authorization,
            String aid, String ns, CassaKeyspaceObject cassaKeyspaceObject, String keyspaceName) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);

        Map<String, String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        String userId = userCredentials.get(MusicUtil.USERID);
        String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = null;
        try {
            authMap = musicDataAPIRepository.musicAuthentication(ns, userId, password, keyspaceName, aid,
                    "dropKeySpace");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.MISSINGDATA,
                    ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            response.status(Status.BAD_REQUEST);
            return response.entity(new JsonResponse(ResultType.FAILURE).setError("Unable to authenticate.").toMap())
                    .build();
        }
        if (authMap.containsKey("aid"))
            authMap.remove("aid");
        if (!authMap.isEmpty()) {
            return response.status(Status.UNAUTHORIZED).entity(authMap).build();
        }

        String consistency = MusicUtil.EVENTUAL;// for now this needs only eventual consistency
        long count = 0L;
        try {
            count = musicDataAPIRepository.findKeySpaceMasterResultCountByKeySpaceName(keyspaceName);
        } catch (Exception ex) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.DATAERROR);

        }

        if (count == 0) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.DATAERROR);
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("Keyspace not found. Please make sure keyspace exists.").toMap()).build();
        } else if (count == 1) {
            try {
                musicDataAPIRepository.updateKeyspaceMaster(keyspaceName, consistency);
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                        ErrorTypes.DATAERROR);
            }
        } else {
            try {
                musicDataAPIRepository.deleteKeyspaceMaster(keyspaceName, consistency);
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                        ErrorTypes.DATAERROR);
            }
        }

        ResultType result = null;

        try {
            cassaKeyspaceObject.setKeyspaceName(keyspaceName);
            result = musicDataAPIRepository.dropKeyspace(cassaKeyspaceObject);
        } catch (MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(), AppMessages.UNKNOWNERROR, ErrorSeverity.WARN,
                    ErrorTypes.MUSICSERVICEERROR);
            return response.status(Status.BAD_REQUEST)
                    .entity(new JsonResponse(ResultType.FAILURE).setError("err:" + ex.getMessage()).toMap()).build();
        }

        if (null != result && result.equals(ResultType.FAILURE)) {
            return response.status(Status.BAD_REQUEST)
                    .entity(new JsonResponse(result).setError("Error Deleteing Keyspace " + keyspaceName).toMap())
                    .build();
        }

        return response.status(Status.OK).entity(
                new JsonResponse(ResultType.SUCCESS).setMessage("Keyspace " + keyspaceName + " Deleted").toMap())
                .build();
    }

    @Override
    public Response createTable(String version, String minorVersion, String patchVersion, String authorization,
            String aid, String ns, JsonTable tableObj, String keyspace, String tablename) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);
        Map<String, String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        String userId = userCredentials.get(MusicUtil.USERID);
        String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = null;
        try {
            authMap = MusicAuthentication.authenticate(ns, userId, password, keyspace, aid, "createTable");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.MISSINGDATA,
                    ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            response.status(Status.BAD_REQUEST);
            return response.entity(new JsonResponse(ResultType.FAILURE).setError("Unable to authenticate.").toMap())
                    .build();
        }
        if (authMap.containsKey("aid"))
            authMap.remove("aid");
        if (!authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(
                    new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap())
                    .build();
        }

        Response tableCreationResponse = musicDataAPIRepository.createTable(response, tableObj, keyspace, tablename);

        logger.info("Table :" + tablename + "Created successfully with keyspacce  :" + keyspace);

        return tableCreationResponse;

    }

    @Override
    public Response createIndex(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, String keyspace, String tablename, String fieldName,
            MultiValueMap<String, String> requestParam) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);

        Map<String, String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        String userId = userCredentials.get(MusicUtil.USERID);
        String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = null;
        try {
            authMap = MusicAuthentication.authenticate(ns, userId, password, keyspace, aid, "createIndex");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.MISSINGDATA,
                    ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            response.status(Status.BAD_REQUEST);
            return response.entity(new JsonResponse(ResultType.FAILURE).setError("Unable to authenticate.").toMap())
                    .build();
        }
        if (null != authMap && authMap.containsKey("aid"))
            authMap.remove("aid");
        if (null != authMap && !authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            response.status(Status.UNAUTHORIZED);
            return response.entity(
                    new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap())
                    .build();
        }

        Response indexCreationResponse = musicDataAPIRepository.createIndex(response, fieldName, keyspace, tablename,
                requestParam);

        return indexCreationResponse;
    }

    @Override
    public Response insertIntoTable(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, JsonInsert insObj, String keyspace, String tablename) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);

        Map<String, String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        String userId = userCredentials.get(MusicUtil.USERID);
        String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = null;

        try {
            authMap = MusicAuthentication.authenticate(ns, userId, password, keyspace, aid, "insertIntoTable");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED)
                    .entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        if (authMap.containsKey("aid"))
            authMap.remove("aid");
        if (!authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(
                    new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap())
                    .build();
        }

        Response insertTableResponse = musicDataAPIRepository.insertIntoTable(response, insObj, keyspace, tablename);

        return insertTableResponse;
    }

    @Override
    public Response updateTable(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, JsonUpdate updateObj, String keyspace, String tablename,
            MultiValueMap<String, String> requestParam) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);

        Map<String, String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        String userId = userCredentials.get(MusicUtil.USERID);
        String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap;
        try {
            authMap = MusicAuthentication.authenticate(ns, userId, password, keyspace, aid, "updateTable");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.MISSINGINFO, ErrorSeverity.WARN,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED)
                    .entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        if (authMap.containsKey("aid"))
            authMap.remove("aid");
        if (!authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.WARN,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(
                    new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap())
                    .build();
        }

        Response updateTableResponse = musicDataAPIRepository.updateTable(response, updateObj, keyspace, tablename,
                requestParam);

        return updateTableResponse;

    }

    @Override
    public Response deleteFromTable(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, JsonDelete delObj, String keyspace, String tablename,
            MultiValueMap<String, String> requestParam) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);

        Map<String, String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        String userId = userCredentials.get(MusicUtil.USERID);
        String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = null;
        try {
            authMap = MusicAuthentication.authenticate(ns, userId, password, keyspace, aid, "deleteFromTable");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.MISSINGINFO, ErrorSeverity.WARN,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED)
                    .entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        if (authMap.containsKey("aid"))
            authMap.remove("aid");
        if (!authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.WARN,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(
                    new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap())
                    .build();
        }

        Response deleteFromTableRes = musicDataAPIRepository.deleteFromTable(response, delObj, keyspace, tablename,
                requestParam);

        return deleteFromTableRes;

    }

    @Override
    public Response dropTable(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, String keyspace, String tablename) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);

        Map<String, String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        String userId = userCredentials.get(MusicUtil.USERID);
        String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = null;
        try {
            authMap = MusicAuthentication.authenticate(ns, userId, password, keyspace, aid, "dropTable");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.MISSINGINFO, ErrorSeverity.WARN,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED)
                    .entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        if (null != authMap && authMap.containsKey("aid"))
            authMap.remove("aid");
        if (null != authMap && !authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.WARN,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(
                    new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap())
                    .build();
        }

        Response dropTableResponse = musicDataAPIRepository.dropTable(response, keyspace, tablename);

        return dropTableResponse;
    }

    @Override
    public Response selectCritical(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, JsonInsert selObj, String keyspace, String tablename,
            MultiValueMap<String, String> requestParam) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);

        Map<String, String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        String userId = userCredentials.get(MusicUtil.USERID);
        String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = null;
        try {
            authMap = MusicAuthentication.authenticate(ns, userId, password, keyspace, aid, "selectCritical");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.MISSINGINFO, ErrorSeverity.WARN,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED)
                    .entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        if (null != authMap && authMap.containsKey("aid"))
            authMap.remove("aid");
        if (null != authMap && !authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "Error while authentication... ", AppMessages.MISSINGINFO,
                    ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(
                    new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap())
                    .build();
        }

        Response selectCriticalRes = musicDataAPIRepository.selectCritical(response, selObj, keyspace, tablename,
                requestParam);

        return selectCriticalRes;
    }

    @Override
    public Response select(String version, String minorVersion, String patchVersion, String aid, String ns,
            String authorization, String keyspace, String tablename, MultiValueMap<String, String> requestParam) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);

        Map<String, String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        String userId = userCredentials.get(MusicUtil.USERID);
        String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = null;
        try {
            authMap = MusicAuthentication.authenticate(ns, userId, password, keyspace, aid, "select");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.MISSINGINFO, ErrorSeverity.WARN,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED)
                    .entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        if (null != authMap && authMap.containsKey("aid"))
            authMap.remove("aid");
        if (null != authMap && !authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.AUTHENTICATIONERROR, ErrorSeverity.WARN,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(
                    new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap())
                    .build();
        }

        Response selectResponse = musicDataAPIRepository.select(response, version, minorVersion, patchVersion, aid, ns,
                userId, password, keyspace, tablename, requestParam);

        return selectResponse;
    }

}
