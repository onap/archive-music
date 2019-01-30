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
package org.onap.music.rest;

import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.onap.music.datastore.jsonobjects.JsonLeasedLock;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.main.MusicUtil;
import org.onap.music.rest.service.MusicLocksAPIService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@RestController
@RequestMapping(value = "/rest/v2/locks")
@Api(value = "Lock Api")
public class RestMusicLocksAPI {

    @Autowired
    MusicLocksAPIService musicLocksAPIService;

    /**
     * This is for creating lock by providing lock name
     * 
     * @param lockname
     * @return
     */
    @PostMapping(value = "/create/{lockname}", produces = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create Lock", notes = "Puts the requesting process in the q for this lock."
            + " The corresponding node will be created in zookeeper if it did not already exist."
            + " Lock Name is the \"key\" of the form keyspaceName.tableName.rowId", response = Map.class)
    public Response createLockReference(
            @ApiParam(value = "Lock Name", required = true) @PathVariable("lockname") String lockName,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(value = MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "AID", required = true) @RequestHeader(value = "aid") String aid,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(value = "ns") String ns)
            throws Exception {

        logger.info("Comes inside RestMusicLocksAPI createLockReference controller Lock Name :: " + lockName);

        return musicLocksAPIService.createLockReference(lockName, VERSION, minorVersion, patchVersion, authorization,
                aid, ns);
    }

    /**
     * This is for acquiring lock if the node is in the top of the queue.
     * 
     * @return
     */
    @GetMapping(value = "/acquire/{lockreference}", produces = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Aquire Lock", notes = "Checks if the node is in the top of the queue and hence acquires the lock", response = Map.class)
    public Response accquireLock(
            @ApiParam(value = "Lock Reference", required = true) @PathVariable("lockreference") String lockId,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(value = MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "AID", required = true) @RequestHeader(value = "aid") String aid,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(value = "ns") String ns)
            throws Exception {

        logger.info("Coming inside accquireLock controller lockreference : " + lockId);

        return musicLocksAPIService.accquireLock(VERSION, minorVersion, patchVersion, lockId, authorization, ns, aid);

    }

    /**
     * This is for acquiring lock with lease
     * 
     * @return
     */
    @PostMapping(value = "/acquire-with-lease/{lockreference}", consumes = MediaType.APPLICATION_JSON, produces = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Aquire Lock with Lease", response = Map.class)
    public Response accquireLockWithLease(@RequestBody JsonLeasedLock lockObj,
            @ApiParam(value = "Lock Reference", required = true) @PathVariable("lockreference") String lockId,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(value = MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "AID", required = true) @RequestHeader(value = "aid") String aid,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(value = "ns") String ns)
            throws Exception {

        logger.info("Coming inside accquireLockWithLease controller lockreference " + lockId);

        return musicLocksAPIService.accquireLockWithLease(VERSION, minorVersion, patchVersion, lockId, authorization,
                aid, ns, lockObj);
    }

    /**
     * This is to get Lock Holder
     * 
     * @return
     */
    @GetMapping(value = "/enquire/{lockname}", produces = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get Lock Holder", notes = "Gets the current Lock Holder", response = Map.class)
    public Response currentLockHolder(
            @ApiParam(value = "Lock Name", required = true) @PathVariable("lockname") String lockName,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(value = MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "AID", required = true) @RequestHeader(value = "aid") String aid,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(value = "ns") String ns)
            throws Exception {

        logger.info("Coming inside currentLockHolder controller lock name : " + lockName);

        return musicLocksAPIService.currentLockHolder(VERSION, minorVersion, patchVersion, authorization, lockName, aid,
                ns);
    }

    /**
     * This is to know current Lock state and Holder
     * 
     * @return
     */
    @GetMapping(value = "/{lockname}", produces = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Lock State", notes = "Returns current Lock State and Holder", response = Map.class)
    public Response currentLockState(
            @ApiParam(value = "Lock Name", required = true) @PathVariable("lockname") String lockName,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(value = MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "AID", required = true) @RequestHeader(value = "aid") String aid,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(value = "ns") String ns,
            @ApiParam(value = "userId", required = true) @RequestHeader(value = "userId") String userId,
            @ApiParam(value = "Password", required = true) @RequestHeader(value = "password") String password)
            throws Exception {

        logger.info("Coming inside currentLockState controller lockName : " + lockName);

        return musicLocksAPIService.currentLockState(VERSION, minorVersion, patchVersion, lockName, ns, userId,
                password, aid);
    }

    /**
     * This is to delete process from ZK queue.
     * 
     * @param lockId
     * @param minorVersion
     * @param patchVersion
     * @param authorization
     * @param aid
     * @param ns
     * @return
     * @throws Exception
     */
    @DeleteMapping(value = "/release/{lockreference}", produces = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Release Lock", notes = "deletes the process from the zk queue controller", response = Map.class)
    public Response unLock(@PathVariable("lockreference") String lockId,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(required = false, value = MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "AID", required = true) @RequestHeader(value = "aid") String aid,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(value = "ns") String ns)
            throws Exception {

        logger.info("Coming inside unLock");

        return musicLocksAPIService.unLock(VERSION, minorVersion, patchVersion, lockId, authorization, ns, aid);
    }

    /**
     * This is to delete Lock.
     * 
     * @return
     */
    @DeleteMapping(value = "/delete/{lockname}", produces = MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Delete Lock", response = Map.class)
    public Response deleteLock(@PathVariable("lockname") String lockName,
            @ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader(value = "aid") String aid,
            @ApiParam(value = "Authorization", required = true) @RequestHeader(value = MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "Application namespace", required = true) @RequestHeader(value = "ns") String ns)
            throws Exception {

        logger.info("Coming inside deleteLock controller lockName :: " + lockName);

        return musicLocksAPIService.deleteLock(VERSION, minorVersion, patchVersion, lockName, authorization, ns, aid);
    }

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(RestMusicLocksAPI.class);
    private static final String XMINORVERSION = "X-minorVersion";
    private static final String XPATCHVERSION = "X-patchVersion";
    private static final String VERSION = "v2";
}
