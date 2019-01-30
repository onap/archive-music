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
package org.onap.music.rest.repository;

import java.util.Map;

import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.lockingservice.cassandra.MusicLockState;
import org.onap.music.main.ReturnType;

public interface MusicLocksAPIRepository {

    /**
     * Method to authenticate to Music
     * 
     * @param ns
     * @param userId
     * @param password
     * @param keyspaceName
     * @param aid
     * @return
     * @throws Exception
     */
    public Map<String, Object> musicAuthentication(String ns, String userId, String password, String keyspaceName,
            String aid, String operationType) throws Exception;

    /**
     * This is for Creating Lock Reference by passing lock name.
     * 
     * @param lockName
     * @return
     */
    public String createLockReference(String lockName);

    /**
     * This is for acquireLock
     * 
     * @param lockName
     * @param lockId
     * @return
     */
    public ReturnType acquireLock(String lockName, String lockId)
            throws MusicLockingException, MusicQueryException, MusicServiceException;

    /**
     * This is for acquiring lock with Lease.
     * 
     * @param lockName
     * @param lockId
     * @param leasePeriod
     * @return
     */
    public ReturnType acquireLockWithLease(String lockName, String lockId, long leasePeriod)

            throws MusicLockingException, MusicQueryException, MusicServiceException;

    /**
     * To check whose turns is it
     * 
     * @param lockName
     * @return
     */
    public String whoseTurnIsIt(String lockName);

    /**
     * To get Music Lock State.
     * 
     * @param lockName
     * @return
     */
    public org.onap.music.lockingservice.cassandra.MusicLockState getMusicLockState(String lockName);

    /**
     * To voluntary Release Lock
     * 
     * @param fullyQualifiedKey
     * @param lockId
     * @return
     * @throws MusicLockingException
     */
    public MusicLockState voluntaryReleaseLock(String fullyQualifiedKey, String lockId) throws MusicLockingException;

    /**
     * To destroy Lock
     * 
     * @param obj
     * @param lockName
     * @return
     * @throws Exception
     */
    public MusicLockState destroyLockRef(String obj, String lockName) throws Exception;

}
