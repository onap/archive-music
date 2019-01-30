/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2018-2019 IBM
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
package org.onap.music.rest.repository.impl;

import java.util.Map;

import org.onap.music.authentication.MusicAuthentication;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.MusicLockState;
import org.onap.music.main.MusicCore;
import org.onap.music.main.ReturnType;
import org.onap.music.rest.repository.MusicLocksAPIRepository;
import org.onap.music.service.impl.MusicCassaCore;
import org.springframework.stereotype.Repository;

@Repository
public class MusicLocksAPIRepositoryImpl implements MusicLocksAPIRepository {

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicLocksAPIRepository.class);

    @Override
    public Map<String, Object> musicAuthentication(String ns, String userId, String password, String keyspaceName,
            String aid, String operationType) throws Exception {

        if (logger.isDebugEnabled()) {
            logger.debug("Coming inside MusicLocksAPIRepositoryImpl musicAuthenticate ns ::" + ns + " userId " + userId
                    + "keyspaceName " + keyspaceName + " aid" + aid);
        }

        Map<String, Object> resultMap = null;
        resultMap = MusicAuthentication.authenticate(ns, userId, password, keyspaceName, aid, operationType);

        logger.info("Music Authentication process is completed");

        return resultMap;
    }

    @Override
    public String createLockReference(String lockName) {

        if (logger.isDebugEnabled()) {
            logger.info("Coming inside MusicLocksAPIRepositoryImpl createLockReference lock name is ::" + lockName);
        }
        String lockId = null;
        lockId = MusicCore.createLockReference(lockName);

        if (logger.isDebugEnabled()) {
            logger.info("Lock Reference is created now and the lock id is ::" + lockId);
        }

        return lockId;
    }

    @Override
    public ReturnType acquireLock(String lockName, String lockId)
            throws MusicLockingException, MusicQueryException, MusicServiceException {

        if (logger.isDebugEnabled()) {
            logger.debug("Coming inside MusicLocksAPIRepositoryImpl acquireLock lockId ::" + lockId + " lockName "
                    + lockName);
        }

        ReturnType lockStatus = null;

        lockStatus = MusicCore.acquireLock(lockName, lockId);

        return lockStatus;
    }

    @Override
    public ReturnType acquireLockWithLease(String lockName, String lockId, long leasePeriod)
            throws MusicLockingException, MusicQueryException, MusicServiceException {
        ReturnType lockLeaseStatus = null;

        if (logger.isDebugEnabled()) {
            logger.debug("Coming inside MusicLocksAPIRepositoryImpl acquireLockWithLease lockId ::" + lockId
                    + " lockName " + lockName + "leasePeriod " + leasePeriod);
        }

        lockLeaseStatus = MusicCore.acquireLockWithLease(lockName, lockId, leasePeriod);

        if (null != lockLeaseStatus) {
            if (logger.isDebugEnabled()) {
                logger.debug("lockLeaseStatus  " + lockLeaseStatus.getResult());
            }
        }

        return lockLeaseStatus;
    }

    @Override
    public String whoseTurnIsIt(String lockName) {

        if (logger.isDebugEnabled()) {
            logger.debug("Coming inside MusicLocksAPIRepositoryImpl whoseTurnIsIt lockName ::" + lockName);
        }

        String who = MusicCore.whoseTurnIsIt(lockName);

        if (logger.isDebugEnabled()) {
            logger.debug("whoseTurnIsIt who value ::" + who);
        }

        return who;
    }

    @Override
    public MusicLockState getMusicLockState(String lockName) {
        MusicLockState musicLockState = null;
        if (logger.isDebugEnabled()) {
            logger.debug("Coming to getMusicLockState method.. Lock Name is " + lockName);
        }

        musicLockState = MusicCassaCore.getMusicLockState(lockName);

        if (null != musicLockState) {
            if (logger.isDebugEnabled()) {
                logger.debug("inside getMusicLockState Lock Status is :: " + musicLockState.getLockStatus());
                logger.debug("inside getMusicLockState Lock Holder is :: " + musicLockState.getLockHolder());
            }
        }
        return musicLockState;
    }

    @Override
    public MusicLockState voluntaryReleaseLock(String fullyQualifiedKey, String lockId) throws MusicLockingException {
        if (logger.isDebugEnabled()) {
            logger.debug("Coming to voluntaryReleaseLock method.. fullyQualifiedKey is " + fullyQualifiedKey
                    + " lockId " + lockId);
        }

        MusicLockState musicLockState = null;
        musicLockState = MusicCore.voluntaryReleaseLock(fullyQualifiedKey, lockId);

        if (null != musicLockState) {
            if (logger.isDebugEnabled()) {
                logger.debug("inside getMusicLockState Lock Status is :: " + musicLockState.getLockStatus());
                logger.debug("inside getMusicLockState Lock Holder is :: " + musicLockState.getLockHolder());
            }
        }

        return musicLockState;
    }

    @Override
    public MusicLockState destroyLockRef(String obj, String lockName) throws Exception {
        MusicLockState musicLockState = null;
        if (logger.isDebugEnabled()) {
            logger.debug("Coming to destroyLockRef method.. obj is " + obj + " lockName " + lockName);
        }
        musicLockState = MusicCore.destroyLockRef(obj, lockName);

        if (null != musicLockState) {
            if (logger.isDebugEnabled()) {
                logger.debug("inside getMusicLockState Lock Status is :: " + musicLockState.getLockStatus());
                logger.debug("inside getMusicLockState Lock Holder is :: " + musicLockState.getLockHolder());
            }
        }

        return musicLockState;
    }
}
