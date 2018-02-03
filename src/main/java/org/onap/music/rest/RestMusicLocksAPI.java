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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.onap.music.datastore.jsonobjects.JsonLeasedLock;
import org.onap.music.lockingservice.MusicLockState;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.response.jsonobjects.JsonLockResponse;

import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;


@Path("/v{version: [0-9]+}/locks/")
@Api(value="Lock Api")
public class RestMusicLocksAPI {

	private static EELFLogger logger = EELFManager.getInstance().getLogger(RestMusicLocksAPI.class);
	private static String xLatestVersion = "X-latestVersion";
	/**
	 * Puts the requesting process in the q for this lock. The corresponding
	 * node will be created in zookeeper if it did not already exist
	 * 
	 * @param lockName
	 * @return
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
			@Context HttpServletResponse response){
		response.addHeader(xLatestVersion,MusicUtil.getVersion());		
		Boolean status = true;
		String lockId = MusicCore.createLockReference(lockName);
		if ( lockId == null ) { status = false; }
		return new JsonLockResponse(status.toString(),"",lockId).toMap();
	}

	/**
	 * 
	 * Checks if the node is in the top of the queue and hence acquires the lock
	 * 
	 * @param lockId
	 * @return
	 */
	@GET
	@Path("/acquire/{lockreference}")
	@ApiOperation(value = "Aquire Lock", 
		notes = "Checks if the node is in the top of the queue and hence acquires the lock",
		response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String,Object> accquireLock(
			@ApiParam(value="Lock Reference",required=true) @PathParam("lockreference") String lockId,
			@Context HttpServletResponse response){
		response.addHeader(xLatestVersion,MusicUtil.getVersion());
		String lockName = lockId.substring(lockId.indexOf('$')+1, lockId.lastIndexOf('$'));
		Boolean lockStatus = MusicCore.acquireLock(lockName,lockId);
		return new JsonLockResponse(lockStatus.toString(),"",lockId,lockStatus.toString(),"").toMap();
	}
	


	
	@POST
	@Path("/acquire-with-lease/{lockreference}")
	@ApiOperation(value = "Aquire Lock with Lease", response = Map.class)
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String,Object> accquireLockWithLease(JsonLeasedLock lockObj, 
			@ApiParam(value="Lock Reference",required=true) @PathParam("lockreference") String lockId,
			@Context HttpServletResponse response){
		response.addHeader(xLatestVersion,MusicUtil.getVersion());
		String lockName = lockId.substring(lockId.indexOf('$')+1, lockId.lastIndexOf('$'));
		String lockLeaseStatus = MusicCore.acquireLockWithLease(lockName, lockId, lockObj.getLeasePeriod()).toString();
		return new JsonLockResponse(lockLeaseStatus,"",lockName,lockLeaseStatus,"",String.valueOf(lockObj.getLeasePeriod())).toMap(); 
	} 
	

	@GET
	@Path("/enquire/{lockname}")
	@ApiOperation(value = "Get Lock Holder", 
		notes = "Gets the current Lock Holder",
		response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String,Object> currentLockHolder(
			@ApiParam(value="Lock Name",required=true) @PathParam("lockname") String lockName,
			@Context HttpServletResponse response){
		response.addHeader(xLatestVersion,MusicUtil.getVersion());
		String who = MusicCore.whoseTurnIsIt(lockName);
		String status = "true";
		String error = "";
		if ( who == null ) { 
			status = "false"; 
			error = "There was a problem getting the lock holder";
		}
		return new JsonLockResponse(status,error,lockName,"",who).toMap();
	}

	@GET
	@Path("/{lockname}")
	@ApiOperation(value = "Lock State",
		notes = "Returns current Lock State and Holder.",
		response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String,Object> currentLockState(
			@ApiParam(value="Lock Name",required=true) @PathParam("lockname") String lockName,
			@Context HttpServletResponse response){
		response.addHeader(xLatestVersion,MusicUtil.getVersion());
		MusicLockState mls = MusicCore.getMusicLockState(lockName);
		Map<String,Object> returnMap = null;
		JsonLockResponse jsonResponse = new JsonLockResponse("false","",lockName);
		if(mls == null) {
			jsonResponse.setError("");
			jsonResponse.setMessage("No lock object created yet..");
		} else { 
			jsonResponse.setStatus("true");
			jsonResponse.setLockStatus(mls.getLockStatus().toString());
			jsonResponse.setLockHolder(mls.getLockHolder());
		} 
		return returnMap;
	}

	/**
	 * 
	 * deletes the process from the zk queue
	 * 
	 * @param lockId
	 */
	@DELETE
	@Path("/release/{lockreference}")
	@ApiOperation(value = "Release Lock",
		notes = "deletes the process from the zk queue",
		response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String,Object> unLock(@PathParam("lockreference") String lockId,
			@Context HttpServletResponse response){
		response.addHeader(xLatestVersion,MusicUtil.getVersion());
		boolean voluntaryRelease = true; 
		MusicLockState mls = MusicCore.releaseLock(lockId,voluntaryRelease);
		Map<String,Object> returnMap = null;
		if ( mls.getLockStatus() == MusicLockState.LockStatus.UNLOCKED ) {
			returnMap = new JsonLockResponse("Unlocked","","").toMap();
		}
		if ( mls.getLockStatus() == MusicLockState.LockStatus.LOCKED) {
			returnMap = new JsonLockResponse("Locked","","").toMap();
		}
		return returnMap;
	}

	/**
	 * 
	 * @param lockName
	 */
	@DELETE
	@Path("/delete/{lockname}")
	@ApiOperation(value = "Delete Lock", response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String,Object> deleteLock(@PathParam("lockname") String lockName,
			@Context HttpServletResponse response){
		response.addHeader(xLatestVersion,MusicUtil.getVersion());
		MusicCore.deleteLock(lockName);
		return new JsonLockResponse("true","","").toMap();
	}

}
