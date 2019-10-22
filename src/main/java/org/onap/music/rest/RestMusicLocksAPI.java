/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  *  Modifications Copyright (c) 2019 Samsung
 * ===================================================================
 * 
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

import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.onap.music.datastore.jsonobjects.JsonLeasedLock;
import org.onap.music.datastore.jsonobjects.JsonLock;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.lockingservice.cassandra.LockType;
import org.onap.music.lockingservice.cassandra.MusicLockState;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.response.jsonobjects.JsonResponse;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;


@Path("/v2/locks/")
@Api(value="Locking Api")
public class RestMusicLocksAPI {

    private EELFLoggerDelegate logger =EELFLoggerDelegate.getLogger(RestMusicLocksAPI.class);
    private static final String XMINORVERSION = "X-minorVersion";
    private static final String XPATCHVERSION = "X-patchVersion";
    private static final String VERSION = "v2";

    /**
     * Puts the requesting process in the q for this lock. The corresponding
     * node will be created if it did not already exist
     * 
     * @param lockName
     * @return
     * @throws Exception 
     */
    @POST
    @Path("/create/{lockname}")
    @ApiOperation(value = "Create and Acquire a Lock Id for a single row.",
        notes = "Creates and Acquires a Lock Id for a specific Row in a table based on the key of that row.\n"
        + " The corresponding lock will be created if it did not already exist."
        + " Lock Name also the Lock is in the form of keyspaceName.tableName.rowId.\n"
        + " The Response will be in the form of \"$kesypaceName.tableName.rowId$lockRef\" "
        + " where the lockRef is a integer representing the Lock Name buffered by \"$\" " 
        + " followed by the lock number. This term for "
        + " this response is a lockId and it will be used in other /locks API calls where a "
        + " lockId is required. If just a lock is required then the form that would be "
        + " the original lockname(without the buffered \"$\").",
        response = Map.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"lock\" : {\"lock\" : \"$keyspace.table.rowId$<integer>\"},"
                + "\"status\" : \"SUCCESS\"}")
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"Unable to aquire lock\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })  
    public Response createLockReference(
            @ApiParam(value="Lock Name",required=true) @PathParam("lockname") String lockName,
            @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
            @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
            JsonLock lockObject,
            @ApiParam(value = "Application namespace",
                            required = false, hidden = true) @HeaderParam("ns") String ns) throws Exception{
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            Map<String, Object> resultMap = MusicCore.validateLock(lockName);
            if (resultMap.containsKey("Error")) {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGINFO  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                response.status(Status.BAD_REQUEST);
                return response.entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(resultMap.get("Error"))).toMap()).build();
            }
            String keyspaceName = (String) resultMap.get("keyspace");
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspaceName + " ) ");
            
            //default lock type is write, as this is always semantically safe
            LockType locktype = LockType.WRITE;
            if (lockObject!=null && lockObject.getLocktype()!=null) {
                locktype = lockObject.getLocktype();
            }
            long leasePeriod=MusicUtil.getDefaultLockLeasePeriod();
            if (lockObject!=null && lockObject.getLeasePeriod()!=(Long)null) {
               leasePeriod = lockObject.getLeasePeriod();
            }
            String lockId;
            try {
                lockId= MusicCore.createLockReference(lockName, locktype,leasePeriod);
            } catch (MusicLockingException e) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
            }
            
            if (lockId == null) {  
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.LOCKINGERROR  ,ErrorSeverity.CRITICAL, ErrorTypes.LOCKINGERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Lock Id is null").toMap()).build();
            }
            return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setLock(lockId).toMap()).build();
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
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
    @Path("/acquire/{lockId}")
    @ApiOperation(value = "Aquire Lock Id ", 
        notes = "Checks if the node is in the top of the queue and hence acquires the lock",
        response = Map.class)
    @Produces(MediaType.APPLICATION_JSON)    
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"lock\" : {\"lock\" : \"$keyspace.table.rowId$<integer>\"},"
                + "\"message\" : \"<integer> is the lock holder for the key\","
                + "\"status\" : \"SUCCESS\"}") 
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"Unable to aquire lock\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })  
    public Response accquireLock(
            @ApiParam(value="Lock Id",required=true) @PathParam("lockId") String lockId,
            @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
            @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
            @ApiParam(value = "Application namespace",
                            required = false, hidden = true) @HeaderParam("ns") String ns) throws Exception{
        try { 
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            Map<String, Object> resultMap = MusicCore.validateLock(lockId);
            if (resultMap.containsKey("Error")) {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                response.status(Status.BAD_REQUEST);
                return response.entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(resultMap.get("Error"))).toMap()).build();
            }
            
            String keyspaceName = (String) resultMap.get("keyspace");
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspaceName + " ) ");
            try {
                String lockName = lockId.substring(lockId.indexOf('$')+1, lockId.lastIndexOf('$'));
                ReturnType lockStatus = MusicCore.acquireLock(lockName,lockId);
                if ( lockStatus.getResult().equals(ResultType.SUCCESS)) {
                    response.status(Status.OK);
                } else {
                    response.status(Status.BAD_REQUEST);
                }
                return response.entity(new JsonResponse(lockStatus.getResult()).setLock(lockId).setMessage(lockStatus.getMessage()).toMap()).build();
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.errorLogger,AppMessages.INVALIDLOCK + lockId, ErrorSeverity.CRITICAL,
                    ErrorTypes.LOCKINGERROR, e);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Unable to aquire lock").toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }
    

    @POST
    @Path("/acquire-with-lease/{lockId}")
    @ApiOperation(
        hidden = false,
        value = " ** DEPRECATED ** - Aquire Lock with Lease", 
        notes = "Acquire the lock with a lease, where lease period is in Milliseconds.\n"
        + "This will ensure that a lock will expire in set milliseconds.\n"
        + "This is no longer available after v3.2.0",
        response = Map.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON) 
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"lock\" : {\"lock\" : \"$keyspace.table.rowId$<integer>\","
                + "\"lock-lease\" : \"6000\"},"
                + "\"message\" : \"<integer> is the lock holder for the key\","
                + "\"status\" : \"SUCCESS\"}")
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"Unable to aquire lock\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })  
    @Deprecated
    public Response accquireLockWithLease(
        JsonLeasedLock lockObj, 
        @ApiParam(value="Lock Id",required=true) @PathParam("lockId") String lockId,
        @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",required = false, hidden = true) @HeaderParam("ns") String ns) throws Exception{
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            Map<String, Object> resultMap = MusicCore.validateLock(lockId);
            if (resultMap.containsKey("Error")) {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                response.status(Status.BAD_REQUEST);
                return response.entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(resultMap.get("Error"))).toMap()).build();
            }
            String keyspaceName = (String) resultMap.get("keyspace");
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspaceName + " ) ");
            String lockName = lockId.substring(lockId.indexOf('$')+1, lockId.lastIndexOf('$'));
            ReturnType lockLeaseStatus = MusicCore.acquireLockWithLease(lockName, lockId, lockObj.getLeasePeriod());
            if ( lockLeaseStatus.getResult().equals(ResultType.SUCCESS)) {
                response.status(Status.OK);
            } else {
                response.status(Status.BAD_REQUEST);
            }
            return response.entity(new JsonResponse(lockLeaseStatus.getResult()).setLock(lockName)
                .setMessage(lockLeaseStatus.getMessage())
                .setLockLease(String.valueOf(lockObj.getLeasePeriod())).toMap()).build();
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }


    } 
    

    @GET
    @Path("/enquire/{lockname}")
    @ApiOperation(value = "Get the top of the lock queue", 
        notes = "Gets the current single lockholder at top of lock queue",
        response = Map.class)
    @Produces(MediaType.APPLICATION_JSON)    
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"lock\" : {\"lock\" : \"keyspace.table.rowId\","
                + "\"lock-holder\" : \"$tomtest.employees.tom$<integer>\"}},"
                + "\"status\" : \"SUCCESS\"}") 
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"Error Message\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })  
    public Response enquireLock(
            @ApiParam(value="Lock Name",required=true) @PathParam("lockname") String lockName,
            @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
            @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
            @ApiParam(value = "Application namespace",
                            required = false, hidden = true) @HeaderParam("ns") String ns) throws Exception{
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            Map<String, Object> resultMap = MusicCore.validateLock(lockName);
            if (resultMap.containsKey("Error")) {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                response.status(Status.BAD_REQUEST);
                return response.entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(resultMap.get("Error"))).toMap()).build();
            }
            String keyspaceName = (String) resultMap.get("keyspace");
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspaceName + " ) ");
            String who = MusicCore.whoseTurnIsIt(lockName);
            ResultType status = ResultType.SUCCESS;
            String error = "";
            if ( who == null ) { 
                status = ResultType.FAILURE; 
                error = "There was a problem getting the lock holder";
                logger.error(EELFLoggerDelegate.errorLogger,"There was a problem getting the lock holder", AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(status).setError(error).setLock(lockName).setLockHolder(who).toMap()).build();
            }
            return response.status(Status.OK).entity(new JsonResponse(status).setError(error).setLock(lockName).setLockHolder(who).toMap()).build();
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    @GET
    @Path("/holders/{lockname}")
    @ApiOperation(value = "Get Lock Holders", 
        notes = "Gets the current Lock Holders.\n"
        + "Will return an array of READ Lock References.",
        response = Map.class)
    @Produces(MediaType.APPLICATION_JSON)    
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"lock\" : {\"lock\" : \"keyspace.table.rowId\","
                + "\"lock-holder\" : [\"$keyspace.table.rowId$<integer1>\",\"$keyspace.table.rowId$<integer2>\"]}},"
                + "\"status\" : \"SUCCESS\"}") 
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"Error message\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })  
    public Response currentLockHolder(@ApiParam(value="Lock Name",required=true) @PathParam("lockname") String lockName,
            @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
            @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
            @ApiParam(value = "Application namespace",
                            required = false, hidden = true) @HeaderParam("ns") String ns) throws Exception{
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            Map<String, Object> resultMap = MusicCore.validateLock(lockName);
            if (resultMap.containsKey("Error")) {
                logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL,
                        ErrorTypes.GENERALSERVICEERROR);
                response.status(Status.BAD_REQUEST);
                return response.entity(
                        new JsonResponse(ResultType.FAILURE).setError(String.valueOf(resultMap.get("Error"))).toMap())
                        .build();
            }
            String keyspaceName = (String) resultMap.get("keyspace");
            List<String> who = MusicCore.getCurrentLockHolders(lockName);
            ResultType status = ResultType.SUCCESS;
            String error = "";
            if (who == null || who.isEmpty()) {
                status = ResultType.FAILURE;
                error = (who !=null && who.isEmpty()) ? "No lock holders for the key":"There was a problem getting the lock holder";
                logger.error(EELFLoggerDelegate.errorLogger, "There was a problem getting the lock holder",
                        AppMessages.INCORRECTDATA, ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST)
                        .entity(new JsonResponse(status).setError(error).setLock(lockName).setLockHolder(who).toMap())
                        .build();
            }
            return response.status(Status.OK)
                .entity(new JsonResponse(status).setError(error).setLock(lockName).setLockHolder(who).setisLockHolders(true).toMap())
                .build();
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }
    
    
    @GET
    @Path("/{lockname}")
    @ApiOperation(value = "Lock State",
        notes = "Returns current Lock State and Holder.",
        response = Map.class,hidden = true)
    @Produces(MediaType.APPLICATION_JSON)    
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"lock\" : {\"lock\" : \"$keyspace.table.rowId$<integer>\"},"
                + "\"message\" : \"<integer> is the lock holder for the key\","
                + "\"status\" : \"SUCCESS\"}") 
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"Unable to aquire lock\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })  
    public Response currentLockState(
            @ApiParam(value="Lock Name",required=true) @PathParam("lockname") String lockName,
            @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
            @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
            @ApiParam(value = "Application namespace",
                            required = false, hidden = true) @HeaderParam("ns") String ns) throws Exception{
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            Map<String, Object> resultMap = MusicCore.validateLock(lockName);
            if (resultMap.containsKey("Error")) {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                response.status(Status.BAD_REQUEST);
                return response.entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(resultMap.get("Error"))).toMap()).build();
            }
            String keyspaceName = (String) resultMap.get("keyspace");
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspaceName+" ) ");
            String who = MusicCore.whoseTurnIsIt(lockName);
            ResultType status = ResultType.SUCCESS;
            String error = "";
            if ( who == null ) {
                status = ResultType.FAILURE; 
                error = "There was a problem getting the lock holder";
                logger.error(EELFLoggerDelegate.errorLogger,"There was a problem getting the lock holder", AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(status).setError(error).setLock(lockName).setLockHolder(who).toMap()).build();
            }
            return response.status(Status.OK).entity(new JsonResponse(status).setError(error).setLock(lockName).setLockHolder(who).toMap()).build();
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }

    }

    /**
     * 
     * deletes the process from the lock queue
     * 
     * @param lockId
     * @throws Exception 
     */
    @DELETE
    @Path("/release/{lockreference}")
    @ApiOperation(value = "Release Lock",
        notes = "Releases the lock from the lock queue.",
        response = Map.class)
    @Produces(MediaType.APPLICATION_JSON)    
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success - UNLOCKED = Lock Removed.",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"lock\" : {\"lock\" : \"$keyspace.table.rowId$<integer>\"},"
                + "\"lock-status\" : \"UNLOCKED\"},"
                + "\"status\" : \"SUCCESS\"}")
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"Unable to aquire lock\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })  
    public Response unLock(@PathParam("lockreference") String lockId,
            @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
            @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
            @ApiParam(value = "Application namespace",
                required = false, hidden = true) @HeaderParam("ns") String ns) throws Exception{
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            Map<String, Object> resultMap = MusicCore.validateLock(lockId);
            if (resultMap.containsKey("Error")) {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                response.status(Status.BAD_REQUEST);
                return response.entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(resultMap.get("Error"))).toMap()).build();
            }

            String keyspaceName = (String) resultMap.get("keyspace");
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspaceName+" ) ");
            boolean voluntaryRelease = true; 
            MusicLockState mls = MusicCore.releaseLock(lockId,voluntaryRelease);
            if(mls.getErrorMessage() != null) {
                resultMap.put(ResultType.EXCEPTION.getResult(), mls.getErrorMessage());
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(resultMap).build();
            }
            Map<String,Object> returnMap = null;
            if (mls.getLockStatus() == MusicLockState.LockStatus.UNLOCKED) {
                returnMap = new JsonResponse(ResultType.SUCCESS).setLock(lockId)
                    .setLockStatus(mls.getLockStatus()).toMap();
                response.status(Status.OK);
            }
            if (mls.getLockStatus() == MusicLockState.LockStatus.LOCKED) {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.LOCKINGERROR  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                returnMap = new JsonResponse(ResultType.FAILURE).setLock(lockId)
                    .setLockStatus(mls.getLockStatus()).toMap();
                response.status(Status.BAD_REQUEST);
            }
            return response.entity(returnMap).build();
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     * 
     * @param lockName
     * @throws Exception 
     */
    @Deprecated
    @DELETE
    @Path("/delete/{lockname}")
    @ApiOperation(
        hidden = true,
        value = "-DEPRECATED- Delete Lock", response = Map.class,
        notes = "-DEPRECATED- Delete the lock.")
    @Produces(MediaType.APPLICATION_JSON)    
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"status\" : \"SUCCESS\"}") 
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"Error Message if any\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })
    public Response deleteLock(@PathParam("lockname") String lockName,
            @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
            @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
            @ApiParam(value = "Application namespace",
                            required = false, hidden = true) @HeaderParam("ns") String ns) throws Exception{
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            Map<String, Object> resultMap = MusicCore.validateLock(lockName);
            if (resultMap.containsKey("Error")) {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.UNKNOWNERROR  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                response.status(Status.BAD_REQUEST);
                return response.entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(resultMap.get("Error"))).toMap()).build();
            }

            String keyspaceName = (String) resultMap.get("keyspace");
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspaceName + " ) ");
            try{
                MusicCore.destroyLockRef(lockName);
            }catch (Exception e) {
                logger.error(EELFLoggerDelegate.errorLogger, e);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
            }
            return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).toMap()).build();
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

}
