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
package org.onap.music.rest.service;

import javax.ws.rs.core.Response;

import org.onap.music.datastore.jsonobjects.JsonLeasedLock;

public interface MusicLocksAPIService {

    /**
     * To Create Lock Reference
     * 
     * @param lockName
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param authorization
     * @param aid
     * @param ns
     * @return
     */
    public Response createLockReference(String lockName, String version, String minorVersion, String patchVersion,
            String authorization, String aid, String ns);

    /**
     * To Acquire Lock
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param lockId
     * @param authorization
     * @param ns
     * @param aid
     * @return
     */
    public Response accquireLock(String version, String minorVersion, String patchVersion, String lockId,
            String authorization, String ns, String aid);

    /**
     * To Acquire Lock with Release
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param lockId
     * @param authorization
     * @param aid
     * @param ns
     * @param lockObj
     * @return
     */
    public Response accquireLockWithLease(String version, String minorVersion, String patchVersion, String lockId,
            String authorization, String aid, String ns, JsonLeasedLock lockObj);

    /**
     * Current Lock Holder
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param authorization
     * @param lockName
     * @param aid
     * @param ns
     * @return
     */
    public Response currentLockHolder(String version, String minorVersion, String patchVersion, String authorization,
            String lockName, String aid, String ns);

    /**
     * To know Current Lock State
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param lockName
     * @param ns
     * @param userId
     * @param password
     * @param aid
     * @return
     */
    public Response currentLockState(String version, String minorVersion, String patchVersion, String lockName,
            String ns, String userId, String password, String aid);

    /**
     * To Unlock
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param lockId
     * @param authorization
     * @param ns
     * @param aid
     * @return
     */
    public Response unLock(String version, String minorVersion, String patchVersion, String lockId,
            String authorization, String ns, String aid);

    /**
     * To Delete Lock
     * 
     * @param VERSION
     * @param minorVersion
     * @param patchVersion
     * @param lockName
     * @param authorization
     * @param ns
     * @param aid
     * @return
     */
    public Response deleteLock(String VERSION, String minorVersion, String patchVersion, String lockName,
            String authorization, String ns, String aid);
}
