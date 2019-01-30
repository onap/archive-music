/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 IBM Intellectual Property
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

import org.onap.music.datastore.jsonobjects.JsonLeasedLock;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.lockingservice.cassandra.MusicLockState;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.response.jsonobjects.JsonResponse;
import org.onap.music.rest.repository.MusicLocksAPIRepository;
import org.onap.music.rest.service.MusicLocksAPIService;
import org.onap.music.service.impl.MusicCassaCore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MusicLocksAPIServiceImpl implements MusicLocksAPIService {

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicLocksAPIServiceImpl.class);

    private final MusicLocksAPIRepository musicLocksAPIRepository;

    @Autowired
    public MusicLocksAPIServiceImpl(final MusicLocksAPIRepository musicLocksAPIRepository) {
        this.musicLocksAPIRepository = musicLocksAPIRepository;
    }

    @Override
    public Response createLockReference(String lockName, String version, String minorVersion, String patchVersion,
            String authorization, String aid, String ns) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);
        Map<String, Object> resultMap = MusicCassaCore.validateLock(lockName);
        if (resultMap.containsKey("Exception")) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        Map<String, String> userCredentials = null;
        try {
            userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        } catch (Exception e) {
            logger.error("Error Occured while extracting basic authentication " + e.getLocalizedMessage());
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }

        String userId = null;
        String password = null;
        if (null != userCredentials) {
            userId = userCredentials.get(MusicUtil.USERID);
            password = userCredentials.get(MusicUtil.PASSWORD);
        }

        String keyspaceName = (String) resultMap.get("keyspace");
        resultMap.remove("keyspace");
        try {
            resultMap = musicLocksAPIRepository.musicAuthentication(ns, userId, password, keyspaceName, aid,
                    "createLockReference");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }
        if (resultMap.containsKey("aid"))
            resultMap.remove("aid");
        if (!resultMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }
        ResultType status = ResultType.SUCCESS;

        String lockId = musicLocksAPIRepository.createLockReference(lockName);

        if (lockId == null) {
            status = ResultType.FAILURE;
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.LOCKINGERROR, ErrorSeverity.CRITICAL,
                    ErrorTypes.LOCKINGERROR);
            return response.status(Status.BAD_REQUEST)
                    .entity(new JsonResponse(status).setError("Lock Id is null").toMap()).build();
        }

        return response.status(Status.OK).entity(new JsonResponse(status).setLock(lockId).toMap()).build();

    }

    @Override
    public Response accquireLock(String version, String minorVersion, String patchVersion, String lockId,
            String authorization, String ns, String aid) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);
        Map<String, Object> resultMap = MusicCassaCore.validateLock(lockId);
        if (resultMap.containsKey("Exception")) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        Map<String, String> userCredentials = null;
        try {
            userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        } catch (Exception e) {
            logger.error("Error Occured while extracting basic authentication " + e.getLocalizedMessage());
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }

        String userId = null;
        String password = null;
        if (null != userCredentials) {
            userId = userCredentials.get(MusicUtil.USERID);
            password = userCredentials.get(MusicUtil.PASSWORD);
        }

        String keyspaceName = (String) resultMap.get("keyspace");
        resultMap.remove("keyspace");
        try {
            resultMap = musicLocksAPIRepository.musicAuthentication(ns, userId, password, keyspaceName, aid,
                    "accquireLock");
        } catch (Exception e1) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }
        if (resultMap.containsKey("aid"))
            resultMap.remove("aid");
        if (!resultMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }
        try {
            String lockName = lockId.substring(lockId.indexOf('$') + 1, lockId.lastIndexOf('$'));
            ReturnType lockStatus = null;

            try {
                lockStatus = musicLocksAPIRepository.acquireLock(lockName, lockId);
            } catch (Exception e) {
                logger.error("Error while acquiring lock with lease :: " + e.getLocalizedMessage());
                response.status(Status.BAD_REQUEST);
            }
            if (lockStatus.getResult().equals(ResultType.SUCCESS)) {
                response.status(Status.OK);
            } else {
                response.status(Status.BAD_REQUEST);
            }
            return response.entity(new JsonResponse(lockStatus.getResult()).setLock(lockId)
                    .setMessage(lockStatus.getMessage()).toMap()).build();
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, AppMessages.INVALIDLOCK + lockId, ErrorSeverity.CRITICAL,
                    ErrorTypes.LOCKINGERROR);
            return response.status(Status.BAD_REQUEST)
                    .entity(new JsonResponse(ResultType.FAILURE).setError("Unable to aquire lock").toMap()).build();
        }
    }

    @Override
    public Response accquireLockWithLease(String version, String minorVersion, String patchVersion, String lockId,
            String authorization, String aid, String ns, JsonLeasedLock lockObj) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);
        Map<String, Object> resultMap = MusicCassaCore.validateLock(lockId);
        if (resultMap.containsKey("Exception")) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        Map<String, String> userCredentials = null;
        try {
            userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        } catch (Exception e) {
            logger.error("Error Occured while extracting basic authentication " + e.getLocalizedMessage());
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }

        String userId = null;
        String password = null;
        if (null != userCredentials) {
            userId = userCredentials.get(MusicUtil.USERID);
            password = userCredentials.get(MusicUtil.PASSWORD);
        }

        String keyspaceName = (String) resultMap.get("keyspace");
        resultMap.remove("keyspace");
        try {
            resultMap = musicLocksAPIRepository.musicAuthentication(ns, userId, password, keyspaceName, aid,
                    "accquireLockWithLease");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }

        if (resultMap.containsKey("aid"))
            resultMap.remove("aid");
        if (!resultMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        String lockName = lockId.substring(lockId.indexOf('$') + 1, lockId.lastIndexOf('$'));
        ReturnType lockLeaseStatus = null;
        try {
            lockLeaseStatus = musicLocksAPIRepository.acquireLockWithLease(lockName, lockId, lockObj.getLeasePeriod());
        } catch (Exception e) {
            logger.error("Error while acquiring lock with lease :: " + e.getLocalizedMessage());
            response.status(Status.BAD_REQUEST);
        }
        if (null != lockLeaseStatus && lockLeaseStatus.getResult().equals(ResultType.SUCCESS)) {
            response.status(Status.OK);
        } else {
            response.status(Status.BAD_REQUEST);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }
        return response.entity(
                new JsonResponse(lockLeaseStatus.getResult()).setLock(lockName).setMessage(lockLeaseStatus.getMessage())
                        .setLockLease(String.valueOf(lockObj.getLeasePeriod())).toMap())
                .build();
    }

    @Override
    public Response currentLockHolder(String version, String minorVersion, String patchVersion, String authorization,
            String lockName, String aid, String ns) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);
        Map<String, Object> resultMap = MusicCassaCore.validateLock(lockName);
        if (resultMap.containsKey("Exception")) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        Map<String, String> userCredentials = null;
        try {
            userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        } catch (Exception e) {
            logger.error("Error Occured while extracting basic authentication " + e.getLocalizedMessage());
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }

        String userId = null;
        String password = null;
        if (null != userCredentials) {
            userId = userCredentials.get(MusicUtil.USERID);
            password = userCredentials.get(MusicUtil.PASSWORD);
        }

        String keyspaceName = (String) resultMap.get("keyspace");
        resultMap.remove("keyspace");
        try {
            resultMap = musicLocksAPIRepository.musicAuthentication(ns, userId, password, keyspaceName, aid,
                    "currentLockHolder");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }
        if (resultMap.containsKey("aid"))
            resultMap.remove("aid");
        if (!resultMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        String who = musicLocksAPIRepository.whoseTurnIsIt(lockName);

        ResultType status = ResultType.SUCCESS;
        String error = "";
        if (who == null) {
            status = ResultType.FAILURE;
            error = "There was a problem getting the lock holder";
            logger.error(EELFLoggerDelegate.errorLogger, "There was a problem getting the lock holder",
                    AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST)
                    .entity(new JsonResponse(status).setError(error).setLock(lockName).setLockHolder(who).toMap())
                    .build();
        }
        return response.status(Status.OK)
                .entity(new JsonResponse(status).setError(error).setLock(lockName).setLockHolder(who).toMap()).build();
    }

    @Override
    public Response currentLockState(String version, String minorVersion, String patchVersion, String lockName,
            String ns, String userId, String password, String aid) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);
        Map<String, Object> resultMap = MusicCassaCore.validateLock(lockName);
        if (resultMap.containsKey("Exception")) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        String keyspaceName = (String) resultMap.get("keyspace");
        resultMap.remove("keyspace");
        try {
            resultMap = musicLocksAPIRepository.musicAuthentication(ns, userId, password, keyspaceName, aid,
                    "currentLockState");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }

        if (resultMap.containsKey("aid"))
            resultMap.remove("aid");
        if (!resultMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        org.onap.music.lockingservice.cassandra.MusicLockState mls = musicLocksAPIRepository
                .getMusicLockState(lockName);

        JsonResponse jsonResponse = new JsonResponse(ResultType.FAILURE).setLock(lockName);
        if (mls == null) {
            jsonResponse.setError("");
            jsonResponse.setMessage("No lock object created yet..");
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(jsonResponse.toMap()).build();
        } else {
            jsonResponse.setStatus(ResultType.SUCCESS);
            jsonResponse.setLockStatus(mls.getLockStatus());
            jsonResponse.setLockHolder(mls.getLockHolder());
            return response.status(Status.OK).entity(jsonResponse.toMap()).build();
        }
    }

    @Override
    public Response unLock(String version, String minorVersion, String patchVersion, String lockId,
            String authorization, String ns, String aid) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);
        Map<String, Object> resultMap = MusicCassaCore.validateLock(lockId);
        if (resultMap.containsKey("Exception")) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        Map<String, String> userCredentials = null;
        try {
            userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        } catch (Exception e) {
            logger.error("Error Occured while extracting basic authentication " + e.getLocalizedMessage());
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }

        String userId = null;
        String password = null;
        if (null != userCredentials) {
            userId = userCredentials.get(MusicUtil.USERID);
            password = userCredentials.get(MusicUtil.PASSWORD);
        }

        String keyspaceName = (String) resultMap.get("keyspace");
        resultMap.remove("keyspace");
        try {
            resultMap = musicLocksAPIRepository.musicAuthentication(ns, userId, password, keyspaceName, aid, "unLock");

        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }
        if (resultMap.containsKey("aid"))
            resultMap.remove("aid");
        if (!resultMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }
        String fullyQualifiedKey = lockId.substring(lockId.indexOf('$') + 1, lockId.lastIndexOf('$'));
        MusicLockState mls = null;
        try {
            mls = musicLocksAPIRepository.voluntaryReleaseLock(fullyQualifiedKey, lockId);
        } catch (MusicLockingException e) {
            logger.error("Error occured while calling voluntaryReleaseLock " + e.getLocalizedMessage());
        }

        if (null != mls && mls.getErrorMessage() != null) {
            resultMap.put(ResultType.EXCEPTION.getResult(), mls.getErrorMessage());
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }
        Map<String, Object> returnMap = null;
        if (null != mls && mls.getLockStatus() == MusicLockState.LockStatus.UNLOCKED) {
            returnMap = new JsonResponse(ResultType.SUCCESS).setLock(lockId).setLockStatus(mls.getLockStatus()).toMap();
            response.status(Status.OK);
        }
        if (null != mls && mls.getLockStatus() == MusicLockState.LockStatus.LOCKED) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.LOCKINGERROR, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            returnMap = new JsonResponse(ResultType.FAILURE).setLock(lockId).setLockStatus(mls.getLockStatus()).toMap();
            response.status(Status.BAD_REQUEST);
        }
        return response.entity(returnMap).build();
    }

    @Override
    public Response deleteLock(String VERSION, String minorVersion, String patchVersion, String lockName,
            String authorization, String ns, String aid) {

        ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
        Map<String, Object> resultMap = MusicCassaCore.validateLock(lockName);
        if (resultMap.containsKey("Exception")) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        Map<String, String> userCredentials = null;
        try {
            userCredentials = MusicUtil.extractBasicAuthentication(authorization);
        } catch (Exception e) {
            logger.error("Error Occured while extracting basic authentication " + e.getLocalizedMessage());
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }

        String userId = null;
        String password = null;
        if (null != userCredentials) {
            userId = userCredentials.get(MusicUtil.USERID);
            password = userCredentials.get(MusicUtil.PASSWORD);
        }

        String keyspaceName = (String) resultMap.get("keyspace");
        resultMap.remove("keyspace");
        try {
            resultMap = musicLocksAPIRepository.musicAuthentication(ns, userId, password, keyspaceName, aid,
                    "deleteLock");

        } catch (Exception e1) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                    ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(resultMap).build();
        }
        if (resultMap.containsKey("aid"))
            resultMap.remove("aid");
        if (!resultMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL,
                    ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(resultMap).build();
        }

        try {
            musicLocksAPIRepository.destroyLockRef(null, lockName);
        } catch (Exception e) {
            return response.status(Status.BAD_REQUEST)
                    .entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).toMap()).build();
    }

}
