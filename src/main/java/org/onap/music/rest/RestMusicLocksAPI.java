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
package org.onap.music.rest;

import java.util.Map;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.onap.music.datastore.jsonobjects.JsonLeasedLock;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.lockingservice.MusicLockState;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.response.jsonobjects.JsonResponse;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;


@Path("/v{version: [0-9]+}/locks/")
@Api(value="Lock Api")
public class RestMusicLocksAPI {

	@SuppressWarnings("unused")
    private EELFLoggerDelegate logger =EELFLoggerDelegate.getLogger(RestMusicLocksAPI.class);
	private static String xLatestVersion = "X-latestVersion";

	/**
	 * Puts the requesting process in the q for this lock. The corresponding
	 * node will be created in zookeeper if it did not already exist
	 * 
	 * @param lockName
	 * @return
	 * @throws Exception 
	 */

	@POST
	@Path("/create/{lockname}")
	@ApiOperation(value = "Create Lock",
		notes = "Puts the requesting process in the q for this lock." +
		" The corresponding node will be created in zookeeper if it did not already exist." +
		" Lock Name is the \"key\" of the form keyspaceName.tableName.rowId",
		response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String,Object> createLockReference(
			@ApiParam(value="Lock Name",required=true) @PathParam("lockname") String lockName,
			@ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
            @ApiParam(value = "Application namespace",
                            required = true) @HeaderParam("ns") String ns,
            @ApiParam(value = "userId",
                            required = true) @HeaderParam("userId") String userId,
            @ApiParam(value = "Password",
                            required = true) @HeaderParam("password") String password,
			@Context HttpServletResponse response) throws Exception{
		response.addHeader(xLatestVersion,MusicUtil.getVersion());	
        Map<String, Object> resultMap = MusicCore.validateLock(lockName);
        if (resultMap.containsKey("Exception")) {
            return resultMap;
        }
        String keyspaceName = (String) resultMap.get("keyspace");
        resultMap.remove("keyspace");
        resultMap = MusicCore.autheticateUser(ns, userId, password, keyspaceName, aid,
                "createLockReference");
        if (resultMap.containsKey("aid"))
            resultMap.remove("aid");
        if (!resultMap.isEmpty()) {
                return resultMap;
        }
		ResultType status = ResultType.SUCCESS;
		String lockId = MusicCore.createLockReference(lockName);
		
		if (lockId == null) { 
			status = ResultType.FAILURE; 
			response.setStatus(400);
		}
		return new JsonResponse(status).setLock(lockId).toMap();
	}

	/**
	 * 
	 * Checks if the node is in the top of the queue and hence acquires the lock
	 * 
	 * @param lockId
	 * @return
	 * @throws Exception 
	 */
	@GET
	@Path("/acquire/{lockreference}")
	@ApiOperation(value = "Aquire Lock", 
		notes = "Checks if the node is in the top of the queue and hence acquires the lock",
		response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String,Object> accquireLock(
			@ApiParam(value="Lock Reference",required=true) @PathParam("lockreference") String lockId,
			@ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
            @ApiParam(value = "Application namespace",
                            required = true) @HeaderParam("ns") String ns,
            @ApiParam(value = "userId",
                            required = true) @HeaderParam("userId") String userId,
            @ApiParam(value = "Password",
                            required = true) @HeaderParam("password") String password,
			@Context HttpServletResponse response) throws Exception{
		response.addHeader(xLatestVersion,MusicUtil.getVersion());
        Map<String, Object> resultMap = MusicCore.validateLock(lockId);
        if (resultMap.containsKey("Exception")) {
            return resultMap;
        }
        String keyspaceName = (String) resultMap.get("keyspace");
        resultMap.remove("keyspace");
        resultMap = MusicCore.autheticateUser(ns, userId, password, keyspaceName, aid,
                "accquireLock");
        if (resultMap.containsKey("aid"))
            resultMap.remove("aid");
        if (!resultMap.isEmpty()) {
                return resultMap;
        }
		try {
			String lockName = lockId.substring(lockId.indexOf('$')+1, lockId.lastIndexOf('$'));
			ReturnType lockStatus = MusicCore.acquireLock(lockName,lockId);
			return new JsonResponse(lockStatus.getResult()).setLock(lockId)
										.setMessage(lockStatus.getMessage()).toMap();
		} catch (Exception e) {
			logger.error(EELFLoggerDelegate.errorLogger,AppMessages.INVALIDLOCK + lockId, ErrorSeverity.CRITICAL, ErrorTypes.LOCKINGERROR);
			resultMap.put("Exception","Unable to aquire lock");
			response.setStatus(400);
			return new JsonResponse(ResultType.FAILURE).setError("Unable to aquire lock").toMap();
		}
	}
	


	
	@POST
	@Path("/acquire-with-lease/{lockreference}")
	@ApiOperation(value = "Aquire Lock with Lease", response = Map.class)
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String,Object> accquireLockWithLease(JsonLeasedLock lockObj, 
			@ApiParam(value="Lock Reference",required=true) @PathParam("lockreference") String lockId,
			@ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
            @ApiParam(value = "Application namespace",
                            required = true) @HeaderParam("ns") String ns,
            @ApiParam(value = "userId",
                            required = true) @HeaderParam("userId") String userId,
            @ApiParam(value = "Password",
                            required = true) @HeaderParam("password") String password,
			@Context HttpServletResponse response) throws Exception{
		response.addHeader(xLatestVersion,MusicUtil.getVersion());
        Map<String, Object> resultMap = MusicCore.validateLock(lockId);
        if (resultMap.containsKey("Exception")) {
            return resultMap;
        }
        String keyspaceName = (String) resultMap.get("keyspace");
        resultMap.remove("keyspace");
        resultMap = MusicCore.autheticateUser(ns, userId, password, keyspaceName, aid,
                "accquireLockWithLease");

        if (resultMap.containsKey("aid"))
            resultMap.remove("aid");
        if (!resultMap.isEmpty()) {
            response.setStatus(400);    
        	return resultMap;
        }
		String lockName = lockId.substring(lockId.indexOf('$')+1, lockId.lastIndexOf('$'));
		ReturnType lockLeaseStatus = MusicCore.acquireLockWithLease(lockName, lockId, lockObj.getLeasePeriod());
		return new JsonResponse(lockLeaseStatus.getResult()).setLock(lockName)
									.setMessage(lockLeaseStatus.getMessage())
									.setLockLease(String.valueOf(lockObj.getLeasePeriod())).toMap();
	} 
	

	@GET
	@Path("/enquire/{lockname}")
	@ApiOperation(value = "Get Lock Holder", 
		notes = "Gets the current Lock Holder",
		response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String,Object> currentLockHolder(
			@ApiParam(value="Lock Name",required=true) @PathParam("lockname") String lockName,
			@ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
            @ApiParam(value = "Application namespace",
                            required = true) @HeaderParam("ns") String ns,
            @ApiParam(value = "userId",
                            required = true) @HeaderParam("userId") String userId,
            @ApiParam(value = "Password",
                            required = true) @HeaderParam("password") String password,
			@Context HttpServletResponse response) throws Exception{
		response.addHeader(xLatestVersion,MusicUtil.getVersion());
        Map<String, Object> resultMap = MusicCore.validateLock(lockName);
        if (resultMap.containsKey("Exception")) {
            return resultMap;
        }
        String keyspaceName = (String) resultMap.get("keyspace");
        resultMap.remove("keyspace");
        resultMap = MusicCore.autheticateUser(ns, userId, password, keyspaceName, aid,
                "currentLockHolder");
        if (resultMap.containsKey("aid"))
            resultMap.remove("aid");
        if (!resultMap.isEmpty()) {
                return resultMap;
        }
		String who = MusicCore.whoseTurnIsIt(lockName);
		ResultType status = ResultType.SUCCESS;
		String error = "";
		if ( who == null ) { 
			status = ResultType.FAILURE; 
			error = "There was a problem getting the lock holder";
			response.setStatus(400);
		}
		return new JsonResponse(status).setError(error)
						.setLock(lockName).setLockHolder(who).toMap();
	}

	@GET
	@Path("/{lockname}")
	@ApiOperation(value = "Lock State",
		notes = "Returns current Lock State and Holder.",
		response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String,Object> currentLockState(
			@ApiParam(value="Lock Name",required=true) @PathParam("lockname") String lockName,
			@ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
            @ApiParam(value = "Application namespace",
                            required = true) @HeaderParam("ns") String ns,
            @ApiParam(value = "userId",
                            required = true) @HeaderParam("userId") String userId,
            @ApiParam(value = "Password",
                            required = true) @HeaderParam("password") String password,
			@Context HttpServletResponse response) throws Exception{
		response.addHeader(xLatestVersion,MusicUtil.getVersion());
        Map<String, Object> resultMap = MusicCore.validateLock(lockName);
        if (resultMap.containsKey("Exception")) {
            return resultMap;
        }
        String keyspaceName = (String) resultMap.get("keyspace");
        resultMap.remove("keyspace");
        resultMap = MusicCore.autheticateUser(ns, userId, password, keyspaceName, aid,
                "currentLockState");
        
        if (resultMap.containsKey("aid"))
            resultMap.remove("aid");
        if (!resultMap.isEmpty()) {
        	response.setStatus(400);
                return resultMap;
        }
		
        MusicLockState mls = MusicCore.getMusicLockState(lockName);
		Map<String,Object> returnMap = null;
		JsonResponse jsonResponse = new JsonResponse(ResultType.FAILURE).setLock(lockName);
		if(mls == null) {
			jsonResponse.setError("");
			jsonResponse.setMessage("No lock object created yet..");
		} else { 
			jsonResponse.setStatus(ResultType.SUCCESS);
			jsonResponse.setLockStatus(mls.getLockStatus());
			jsonResponse.setLockHolder(mls.getLockHolder());
		} 
		return jsonResponse.toMap();
	}

	/**
	 * 
	 * deletes the process from the zk queue
	 * 
	 * @param lockId
	 * @throws Exception 
	 */
	@DELETE
	@Path("/release/{lockreference}")
	@ApiOperation(value = "Release Lock",
		notes = "deletes the process from the zk queue",
		response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String,Object> unLock(@PathParam("lockreference") String lockId,
			@ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
            @ApiParam(value = "Application namespace",
                            required = true) @HeaderParam("ns") String ns,
            @ApiParam(value = "userId",
                            required = true) @HeaderParam("userId") String userId,
            @ApiParam(value = "Password",
                            required = true) @HeaderParam("password") String password,
			@Context HttpServletResponse response) throws Exception{
		response.addHeader(xLatestVersion,MusicUtil.getVersion());
        Map<String, Object> resultMap = MusicCore.validateLock(lockId);
        if (resultMap.containsKey("Exception")) {
            return resultMap;
        }
        String keyspaceName = (String) resultMap.get("keyspace");
        resultMap.remove("keyspace");
        resultMap = MusicCore.autheticateUser(ns, userId, password, keyspaceName, aid,
                "unLock");
        if (resultMap.containsKey("aid"))
            resultMap.remove("aid");
        if (!resultMap.isEmpty()) {
        	response.setStatus(400);
            return resultMap;
        }
		boolean voluntaryRelease = true; 
		MusicLockState mls = MusicCore.releaseLock(lockId,voluntaryRelease);
		if(mls.getErrorMessage() != null) {
			resultMap.put(ResultType.EXCEPTION.getResult(), mls.getErrorMessage());
			return resultMap;
		}
		Map<String,Object> returnMap = null;
		if (mls.getLockStatus() == MusicLockState.LockStatus.UNLOCKED) {
			returnMap = new JsonResponse(ResultType.SUCCESS).setLock(lockId)
								.setLockStatus(mls.getLockStatus()).toMap();
		}
		if (mls.getLockStatus() == MusicLockState.LockStatus.LOCKED) {
			returnMap = new JsonResponse(ResultType.FAILURE).setLock(lockId)
								.setLockStatus(mls.getLockStatus()).toMap();
		}
		return returnMap;
	}

	/**
	 * 
	 * @param lockName
	 * @throws Exception 
	 */
	@DELETE
	@Path("/delete/{lockname}")
	@ApiOperation(value = "Delete Lock", response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String,Object> deleteLock(@PathParam("lockname") String lockName,
			@ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
            @ApiParam(value = "Application namespace",
                            required = true) @HeaderParam("ns") String ns,
            @ApiParam(value = "userId",
                            required = true) @HeaderParam("userId") String userId,
            @ApiParam(value = "Password",
                            required = true) @HeaderParam("password") String password,
			@Context HttpServletResponse response) throws Exception{
		response.addHeader(xLatestVersion,MusicUtil.getVersion());
        Map<String, Object> resultMap = MusicCore.validateLock(lockName);
        if (resultMap.containsKey("Exception")) {
            return resultMap;
        }
        String keyspaceName = (String) resultMap.get("keyspace");
        resultMap.remove("keyspace");
        resultMap = MusicCore.autheticateUser(ns, userId, password, keyspaceName, aid,
                "deleteLock");
        if (resultMap.containsKey("aid"))
            resultMap.remove("aid");
        if (!resultMap.isEmpty()) {
        	response.setStatus(400);
            return resultMap;
        }
		MusicCore.deleteLock(lockName);
		return new JsonResponse(ResultType.SUCCESS).toMap();
	}

}
