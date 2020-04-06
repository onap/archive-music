/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2019 Samsung
 * ===================================================================
 *  Modifications Copyright (C) 2019 IBM 
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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import org.onap.music.datastore.FeedReturnStreamingOutput;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonIndex;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonKeySpace;
import org.onap.music.datastore.jsonobjects.JsonSelect;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.response.jsonobjects.JsonResponse;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TableMetadata;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;

/* Version 2 Class */
//@Path("/v{version: [0-9]+}/keyspaces")
@Path("/v2/keyspaces")
@Api(value = "Data Api")
public class RestMusicDataAPI {
    /*
     * Header values for Versioning X-minorVersion *** - Used to request or communicate a MINOR
     * version back from the client to the server, and from the server back to the client - This
     * will be the MINOR version requested by the client, or the MINOR version of the last MAJOR
     * version (if not specified by the client on the request) - Contains a single position value
     * (e.g. if the full version is 1.24.5, X-minorVersion = "24") - Is optional for the client on
     * request; however, this header should be provided if the client needs to take advantage of
     * MINOR incremented version functionality - Is mandatory for the server on response
     *
     *** X-patchVersion *** - Used only to communicate a PATCH version in a response for
     * troubleshooting purposes only, and will not be provided by the client on request - This will
     * be the latest PATCH version of the MINOR requested by the client, or the latest PATCH version
     * of the MAJOR (if not specified by the client on the request) - Contains a single position
     * value (e.g. if the full version is 1.24.5, X-patchVersion = "5") - Is mandatory for the
     * server on response  (CURRENTLY NOT USED)
     *
     *** X-latestVersion *** - Used only to communicate an API's latest version - Is mandatory for the
     * server on response, and shall include the entire version of the API (e.g. if the full version
     * is 1.24.5, X-latestVersion = "1.24.5") - Used in the response to inform clients that they are
     * not using the latest version of the API (CURRENTLY NOT USED)
     *
     */

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(RestMusicDataAPI.class);
    private static final String XMINORVERSION = "X-minorVersion";
    private static final String XPATCHVERSION = "X-patchVersion";
    private static final String NS = "ns";
    private static final String VERSION = "v2";
    private static final String PARAMETER_ERROR = "Missing Row Identifier. Please provide the parameter of key=value for the row being selected.";


    private class RowIdentifier {
        public String primarKeyValue;
        public StringBuilder rowIdString;
        @SuppressWarnings("unused")
        public PreparedQueryObject queryObject; // the string with all the row
                                                // identifiers separated by AND

        public RowIdentifier(String primaryKeyValue, StringBuilder rowIdString,
                        PreparedQueryObject queryObject) {
            this.primarKeyValue = primaryKeyValue;
            this.rowIdString = rowIdString;
            this.queryObject = queryObject;
        }
    }


    /**
     * Create Keyspace REST
     *
     * @param kspObject
     * @param keyspaceName
     * @return
     * @throws Exception
     */
    @POST
    @Path("/{name}")
    @ApiOperation(value = "Create Keyspace", response = String.class,
        notes = "This API will not work if MUSIC properties has keyspace.active=false ")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"message\" : \"Keysapce <keyspace> Created\","
                + "\"status\" : \"SUCCESS\"}")
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"<errorMessage>\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })  
    public Response createKeySpace(
        @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        @ApiParam(value = "Application namespace",required = false, hidden = true) @HeaderParam(NS) String ns,
        JsonKeySpace kspObject,
        @ApiParam(value = "Keyspace Name",required = true) @PathParam("name") String keyspaceName) {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspaceName+" ) ");
            logger.info(EELFLoggerDelegate.applicationLogger,"In Create Keyspace " + keyspaceName);
            if (MusicUtil.isKeyspaceActive() ) {
                logger.info(EELFLoggerDelegate.applicationLogger,"Creating Keyspace " + keyspaceName);
                
                if(kspObject == null || kspObject.getReplicationInfo() == null) {
                    response.status(Status.BAD_REQUEST);
                    return response.entity(new JsonResponse(ResultType.FAILURE).setError(ResultType.BODYMISSING.getResult()).toMap()).build();
                }
                ResultType result = ResultType.FAILURE;
                try {
                    kspObject.setKeyspaceName(keyspaceName);
                    result = MusicCore.createKeyspace(kspObject, MusicUtil.EVENTUAL);
                    logger.info(EELFLoggerDelegate.applicationLogger, "result = " + result);
                } catch ( MusicQueryException ex) {
                    logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.QUERYERROR 
                    ,ErrorSeverity.WARN, ErrorTypes.QUERYERROR);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
                } catch ( MusicServiceException ex) {
                    logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity
                        .WARN, ErrorTypes.MUSICSERVICEERROR, ex);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
                }
        
                return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setMessage("Keyspace " + keyspaceName + " Created").toMap()).build();
            } else {
                String vError = "Keyspace Creation has been turned off. Contact DBA to create the keyspace or set keyspace.active to true.";
                logger.info(EELFLoggerDelegate.applicationLogger,vError);
                logger.error(EELFLoggerDelegate.errorLogger,vError, AppMessages.UNKNOWNERROR,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
                return response.status(Response.Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(vError).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
        
    }

    /**
     *
     * @param kspObject
     * @param keyspaceName
     * @return
     * @throws Exception
     */
    @DELETE
    @Path("/{name}")
    @ApiOperation(value = "Delete Keyspace", response = String.class,
        notes = "This API will not work if MUSIC properties has keyspace.active=false ")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"message\" : \"Keysapce <keyspace> Deleted\","
                + "\"status\" : \"SUCCESS\"}")
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"<errorMessage>\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })  
    public Response dropKeySpace(
        @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        @ApiParam(value = "Application namespace",required = false, hidden = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Keyspace Name",required = true) @PathParam("name") String keyspaceName) throws Exception {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspaceName + " ) ");
            logger.info(EELFLoggerDelegate.applicationLogger,"In Drop Keyspace " + keyspaceName);
            if (MusicUtil.isKeyspaceActive()) {
                String consistency = MusicUtil.EVENTUAL;// for now this needs only
                String droperror = "Error Deleteing Keyspace " + keyspaceName;
                JsonKeySpace kspObject = new JsonKeySpace();
                kspObject.setKeyspaceName(keyspaceName);
                try{
                    ResultType result = MusicCore.dropKeyspace(kspObject, consistency);
                    if ( result.equals(ResultType.FAILURE) ) {
                        return response.status(Status.BAD_REQUEST).entity(new JsonResponse(result).setError(droperror).toMap()).build();
                    }
                } catch ( MusicQueryException ex) {
                    logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.QUERYERROR 
                    ,ErrorSeverity.WARN, ErrorTypes.QUERYERROR);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(droperror + " " + ex.getMessage()).toMap()).build();
                } catch ( MusicServiceException ex) {
                    logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR
                        ,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR, ex);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(droperror + " " + ex.getMessage()).toMap()).build();
                }
                return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setMessage("Keyspace " + keyspaceName + " Deleted").toMap()).build();
            } else {
                String vError = "Keyspace deletion has been turned off. Contact DBA to delete the keyspace or set keyspace.active to true.";
                logger.info(EELFLoggerDelegate.applicationLogger,vError);
                logger.error(EELFLoggerDelegate.errorLogger,vError, AppMessages.UNKNOWNERROR,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
                return response.status(Response.Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(vError).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param tableObj
     * @param version
     * @param keyspace
     * @param tablename
     * @param headers
     * @return
     * @throws Exception -
     */
    @POST
    @Path("/{keyspace: .*}/tables/{tablename: .*}")
    @ApiOperation(value = "Create Table", response = String.class,
        notes = "Create a table with the required json in the body.")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"message\" : \"Tablename <tablename> Created under keyspace <keyspace>\","
                + "\"status\" : \"SUCCESS\"}")
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"<errorMessage>\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })  
    public Response createTable(
        @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",required = false, hidden = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        JsonTable tableObj,
        @ApiParam(value = "Keyspace Name",required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",required = true) @PathParam("tablename") String tablename) throws Exception {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if ( null == tableObj ) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                        .setError(ResultType.BODYMISSING.getResult()).toMap()).build();
            }
            if(keyspace == null || keyspace.isEmpty() || tablename == null || tablename.isEmpty()){
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                        .setError("One or more path parameters are not set, please check and try again."
                            + "Parameter values: keyspace='" + keyspace + "' tablename='" + tablename + "'")
                            .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspace+" ) ");
            String consistency = MusicUtil.EVENTUAL;
            // for now this needs only eventual consistency
            ResultType result = ResultType.FAILURE;
            try {
                tableObj.setKeyspaceName(keyspace);
                tableObj.setTableName(tablename);
                result = MusicCore.createTable(tableObj, consistency);
            } catch (MusicQueryException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), ex.getMessage()  ,ErrorSeverity
                        .WARN, ErrorTypes.MUSICSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.CRITICAL, ErrorTypes.MUSICSERVICEERROR);
                response.status(Status.BAD_REQUEST);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
            if ( result.equals(ResultType.FAILURE) ) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(result).setError("Error Creating Table " + tablename).toMap()).build();
            }
            return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setMessage("TableName " + tablename.trim() + " Created under keyspace " + keyspace.trim()).toMap()).build();
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param keyspace
     * @param tablename
     * @param fieldName
     * @param info
     * @throws Exception
     */
    @POST
    @Path("/{keyspace: .*}/tables/{tablename: .*}/index/{field: .*}")
    @ApiOperation(value = "Create Index", response = String.class,
        notes = "An index provides a means to access data using attributes "
        + "other than the partition key. The benefit is fast, efficient lookup "
        + "of data matching a given condition.")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"message\" : \"Index Created on <keyspace>.<table>.<field>\","
                + "\"status\" : \"SUCCESS\"}")
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"<errorMessage>\","
                + "\"status\" : \"FAILURE\"}") 
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"Unknown Error in create index.\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })  
    public Response createIndex(
        @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",required = false, hidden = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        @ApiParam(value = "Keyspace Name",required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",required = true) @PathParam("tablename") String tablename,
        @ApiParam(value = "Field Name",required = true) @PathParam("field") String fieldName,
        @Context UriInfo info) throws Exception {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if ((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty()) || (fieldName == null || fieldName.isEmpty())){
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("one or more path parameters are not set, please check and try again")
                    .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspace+" ) ");
            MultivaluedMap<String, String> rowParams = info.getQueryParameters();
            String indexName = "";
            if (rowParams.getFirst("index_name") != null)
                indexName = rowParams.getFirst("index_name");
            
            JsonIndex jsonIndexObject = new JsonIndex(indexName, keyspace, tablename, fieldName);
            
            ResultType result = ResultType.FAILURE;
            try {
                result = MusicCore.createIndex(jsonIndexObject, MusicUtil.EVENTUAL);
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity
                .CRITICAL, ErrorTypes.GENERALSERVICEERROR, ex);
                response.status(Status.BAD_REQUEST);
                return response.entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
            if ( result.equals(ResultType.SUCCESS) ) {
                return response.status(Status.OK).entity(new JsonResponse(result).setMessage("Index Created on " + keyspace+"."+tablename+"."+fieldName).toMap()).build();
            } else {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(result).setError("Unknown Error in create index.").toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param insObj
     * @param keyspace
     * @param tablename
     * @return
     * @throws Exception
     */
    @POST
    @Path("/{keyspace: .*}/tables/{tablename: .*}/rows")
    @ApiOperation(value = "Insert Into Table", response = String.class,
        notes = "Insert into table with data in json body.")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"message\" : \"Insert Successful\","
                + "\"status\" : \"SUCCESS\"}")
        })),
        @ApiResponse(code=400, message = "Failure - Generic",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"<errorMessage>\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })  
    public Response insertIntoTable(
        @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",required = false, hidden = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        JsonInsert insObj,
        @ApiParam(value = "Keyspace Name",
            required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",
            required = true) @PathParam("tablename") String tablename) {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if ( null == insObj ) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                        .setError(ResultType.BODYMISSING.getResult()).toMap()).build();
            }
            if((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty())){
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                .setError("one or more path parameters are not set, please check and try again")
                .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace","(" + keyspace + ")");
            ReturnType result = null;
            String consistency = insObj.getConsistencyInfo().get("type");
            try {
                insObj.setKeyspaceName(keyspace);
                insObj.setTableName(tablename);
                if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                    String lockId = insObj.getConsistencyInfo().get("lockId");
                    if(lockId == null) {
                        logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                                + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                        return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                                + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
                    }
                }
                result = MusicCore.insertIntoTable(insObj);
            }catch (MusicQueryException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), ex.getMessage()  ,ErrorSeverity
                        .WARN, ErrorTypes.MUSICSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }catch (Exception ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity
                    .WARN, ErrorTypes.MUSICSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
            if (result==null) {
                logger.error(EELFLoggerDelegate.errorLogger,"Null result - Please Contact admin", AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Null result - Please Contact admin").toMap()).build();
            }else if(result.getResult() == ResultType.FAILURE) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(result.getResult()).setError(result.getMessage()).toMap()).build();
            }
            return response.status(Status.OK).entity(new JsonResponse(result.getResult()).setMessage("Insert Successful").toMap()).build();
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param insObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws MusicServiceException 
     * @throws MusicQueryException 
     * @throws Exception
     */
    @PUT
    @Path("/{keyspace: .*}/tables/{tablename: .*}/rows")
    @ApiOperation(value = "Update Table", response = String.class,
        notes = "Update the table with the data in the JSON body.")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateTable(
        @ApiParam(value = "Major Version",
            required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",
            required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",
            required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",
            required = false, hidden = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        JsonUpdate updateObj,
        @ApiParam(value = "Keyspace Name",
            required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",
            required = true) @PathParam("tablename") String tablename,
        @Context UriInfo info) throws MusicQueryException, MusicServiceException {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if ( null == updateObj ) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                        .setError(ResultType.BODYMISSING.getResult()).toMap()).build();
            }
            if((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty())){
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("one or more path parameters are not set, please check and try again")
                    .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspace+" ) ");
            long startTime = System.currentTimeMillis();
            String operationId = UUID.randomUUID().toString();  // just for infoging
                                                                // purposes.
            String consistency = updateObj.getConsistencyInfo().get("type");
            ReturnType operationResult = null;
            logger.info(EELFLoggerDelegate.applicationLogger, "--------------Music " + consistency
                + " update-" + operationId + "-------------------------");
            
            updateObj.setKeyspaceName(keyspace);
            updateObj.setTableName(tablename);
            
            try {
                if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                    String lockId = updateObj.getConsistencyInfo().get("lockId");
                    if(lockId == null) {
                        logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                                + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                        return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                                + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
                    }
                }
                operationResult = MusicCore.updateTable(updateObj,info.getQueryParameters());
            }catch (MusicLockingException e) {
                 logger.error(EELFLoggerDelegate.errorLogger,e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN,
                            ErrorTypes.GENERALSERVICEERROR, e);
                        return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
            }catch (MusicQueryException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), ex.getMessage()  ,ErrorSeverity
                        .WARN, ErrorTypes.MUSICSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }catch (Exception ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity
                        .WARN, ErrorTypes.MUSICSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
            long actualUpdateCompletionTime = System.currentTimeMillis();

            long endTime = System.currentTimeMillis();
            long jsonParseCompletionTime = System.currentTimeMillis();
            String timingString = "Time taken in ms for Music " + consistency + " update-" + operationId
                + ":" + "|total operation time:" + (endTime - startTime)
                + "|json parsing time:" + (jsonParseCompletionTime - startTime)
                + "|update time:" + (actualUpdateCompletionTime - jsonParseCompletionTime)
                + "|";

            if (operationResult != null && operationResult.getTimingInfo() != null) {
                String lockManagementTime = operationResult.getTimingInfo();
                timingString = timingString + lockManagementTime;
            }
            logger.info(EELFLoggerDelegate.applicationLogger, timingString);

            if (operationResult==null) {
                logger.error(EELFLoggerDelegate.errorLogger,"Null result - Please Contact admin", AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Null result - Please Contact admin").toMap()).build();
            }
            if ( operationResult.getResult() == ResultType.SUCCESS ) {
                return response.status(Status.OK).entity(new JsonResponse(operationResult.getResult()).setMessage(operationResult.getMessage()).toMap()).build();
            } else {
                logger.error(EELFLoggerDelegate.errorLogger,operationResult.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(operationResult.getResult()).setError(operationResult.getMessage()).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param delObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws MusicServiceException 
     * @throws MusicQueryException 
     * @throws Exception
     */
    @DELETE
    @Path("/{keyspace: .*}/tables/{tablename: .*}/rows")
    @ApiOperation(value = "Delete From table", response = String.class,
        notes = "Delete from a table, the row or parts of a row. Based on JSON body.")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteFromTable(
        @ApiParam(value = "Major Version",
            required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",
            required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",
            required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",
            required = false, hidden = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        JsonDelete delObj,
        @ApiParam(value = "Keyspace Name",
            required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",
            required = true) @PathParam("tablename") String tablename,
        @Context UriInfo info) throws MusicQueryException, MusicServiceException {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty())){
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("one or more path parameters are not set, please check and try again")
                    .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspace+" ) ");
            if(delObj == null) {
                logger.error(EELFLoggerDelegate.errorLogger,ResultType.BODYMISSING.getResult(), AppMessages.MISSINGDATA  ,ErrorSeverity.WARN, ErrorTypes.DATAERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ResultType.BODYMISSING.getResult()).toMap()).build();
            }
            ReturnType operationResult = null;
            String consistency = delObj.getConsistencyInfo().get("type");
            delObj.setKeyspaceName(keyspace);
            delObj.setTableName(tablename);
            try {
                if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                    String lockId = delObj.getConsistencyInfo().get("lockId");
                    if(lockId == null) {
                        logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                            + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                        return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                            + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
                    }
                }
                
                operationResult = MusicCore.deleteFromTable(delObj,info.getQueryParameters());
            } catch (MusicQueryException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), ex.getMessage()  ,ErrorSeverity
                        .WARN, ErrorTypes.MUSICSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
                
            } catch (MusicLockingException e) {
                logger.error(EELFLoggerDelegate.errorLogger,e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR, e);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                        .setError("Unable to perform Delete operation. Exception from music").toMap()).build();
            }
            if (operationResult==null) {
                logger.error(EELFLoggerDelegate.errorLogger,"Null result - Please Contact admin", AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Null result - Please Contact admin").toMap()).build();
            }
            if (operationResult.getResult().equals(ResultType.SUCCESS)) {
                return response.status(Status.OK).entity(new JsonResponse(operationResult.getResult()).setMessage(operationResult.getMessage()).toMap()).build();
            } else {
                logger.error(EELFLoggerDelegate.errorLogger,operationResult.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(operationResult.getMessage()).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param tabObj
     * @param keyspace
     * @param tablename
     * @throws Exception
     */
    @DELETE
    @Path("/{keyspace: .*}/tables/{tablename: .*}")
    @ApiOperation(value = "Drop Table", response = String.class)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"status\" : \"SUCCESS\"}")
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"<errorMessage>\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })
    public Response dropTable(
        @ApiParam(value = "Major Version",
            required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",
            required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",
            required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",
            required = false, hidden = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        @ApiParam(value = "Keyspace Name",
            required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",
            required = true) @PathParam("tablename") String tablename) throws Exception {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty())){
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("one or more path parameters are not set, please check and try again")
                    .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( "+keyspace+" ) ");
            JsonTable jsonTable = new JsonTable();
            jsonTable.setKeyspaceName(keyspace);
            jsonTable.setTableName(tablename);
            try {
                return response.status(Status.OK).entity(new JsonResponse(MusicCore.dropTable(jsonTable, MusicUtil.EVENTUAL)).toMap()).build();
            } catch (MusicQueryException ex) {
                logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(), AppMessages.QUERYERROR,ErrorSeverity.WARN
                    , ErrorTypes.QUERYERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(), AppMessages.MISSINGINFO  ,ErrorSeverity.WARN
                , ErrorTypes.GENERALSERVICEERROR,ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param selObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     */
    @PUT
    @Path("/{keyspace: .*}/tables/{tablename: .*}/rows/criticalget")
    @ApiOperation(value = "** Depreciated ** - Select Critical", response = Map.class,
        notes = "This API is depreciated in favor of the regular select api.\n"
        + "Avaliable to use with the select api by providing a minorVersion of 1 "
        + "and patchVersion of 0.\n"
        + "Critical Get requires parameter rowId=value and consistency in order to work.\n"
        + "It will fail if either are missing.")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"result\":{\"row 0\":{\"address\":"
                + "{\"city\":\"Someplace\",\"street\":\"1 Some way\"},"
                + "\"emp_salary\":50,\"emp_name\":\"tom\",\"emp_id\":"
                + "\"cfd66ccc-d857-4e90-b1e5-df98a3d40cd6\"}},\"status\":\"SUCCESS\"}")
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"<errorMessage>\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })
    public Response selectCritical(
        @ApiParam(value = "Major Version",
    required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",example = "0",
            required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",example = "0",
            required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",
            required = false, hidden = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        JsonInsert selObj,
        @ApiParam(value = "Keyspace Name",
            required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name",
            required = true) @PathParam("tablename") String tablename,
        @Context UriInfo info) throws Exception {
        try {
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty())) { 
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("one or more path parameters are not set, please check and try again")
                    .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspace + " )");
            if (info.getQueryParameters().isEmpty()) {
                logger.error(EELFLoggerDelegate.errorLogger,RestMusicDataAPI.PARAMETER_ERROR, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes
                .GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(RestMusicDataAPI.PARAMETER_ERROR).toMap()).build();
            }
            if (selObj == null || selObj.getConsistencyInfo().isEmpty()) {
                String error = " Missing Body or Consistency type.";
                logger.error(EELFLoggerDelegate.errorLogger,ResultType.BODYMISSING.getResult() + error, AppMessages.MISSINGDATA  ,ErrorSeverity.WARN, ErrorTypes.DATAERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ResultType.BODYMISSING.getResult() + error).toMap()).build();
            }
            ResultSet results = null;
            String consistency = selObj.getConsistencyInfo().get("type");
            String lockId = selObj.getConsistencyInfo().get("lockId");
            selObj.setKeyspaceName(keyspace);
            selObj.setTableName(tablename);
            try {
                if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                    if(lockId == null) {
                        logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                            + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                        return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                            + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
                    }
                }
                results = MusicCore.selectCritical(selObj, info.getQueryParameters());
            }catch (MusicQueryException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), ex.getMessage()  ,ErrorSeverity
                        .WARN, ErrorTypes.MUSICSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
                
            }catch(Exception ex) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }

            if(results!=null && results.getAvailableWithoutFetching() >0) {
                return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setDataResult(MusicDataStoreHandle.marshallResults(results)).toMap()).build();
            }
            return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setError("No data found").toMap()).build();
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }
    
    /**
     * This API will replace the original select and provide a single API fro select and critical. 
     * The idea is to depreciate the older api of criticalGet and use a single API. 
     * 
     * @param selObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     */
    @GET
    @Path("/{keyspace: .*}/tables/{tablename: .*}/rows")
    @ApiOperation(value = "Select", response = Map.class,
        notes = "This has 2 versions: if minorVersion and patchVersion is null or 0, this will be a Eventual Select only.\n"
        + "If minorVersion is 1 and patchVersion is 0, this will act like the Critical Select \n"
        + "Critical Get requires parameter rowId=value and consistency in order to work.\n"
        + "If parameters are missing or consistency information is missing. An eventual select will be preformed.")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"result\":{\"row 0\":{\"address\":"
                + "{\"city\":\"Someplace\",\"street\":\"1 Some way\"},"
                + "\"emp_salary\":50,\"emp_name\":\"tom\",\"emp_id\":"
                + "\"cfd66ccc-d857-4e90-b1e5-df98a3d40cd6\"}},\"status\":\"SUCCESS\"}")
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"<errorMessage>\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })
    public Response selectWithCritical(
        @ApiParam(value = "Major Version",example = "v2", 
            required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",example = "1",
            required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",example = "0",
            required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",
            required = false,hidden = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        JsonInsert selObj,
        @ApiParam(value = "Keyspace Name", required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name", required = true) @PathParam("tablename") String tablename,
        @Context UriInfo info) throws Exception {
        if ((minorVersion != null && patchVersion != null) &&
            (Integer.parseInt(minorVersion) == 1 && Integer.parseInt(patchVersion) == 0) &&
            (!(null == selObj) && !selObj.getConsistencyInfo().isEmpty())) {
            return selectCritical(version, minorVersion, patchVersion, aid, ns, authorization, selObj, keyspace, tablename, info);
        } else {
            return select(version, minorVersion, patchVersion, aid, ns, authorization, keyspace, tablename, info);
        }
    }

    /**
     * This API will replace the original select and provide a single API fro select and critical. 
     * The idea is to depreciate the older api of criticalGet and use a single API. 
     * 
     * @param selObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     */
    @GET
    @Path("/{keyspace: .*}/tables/{tablename: .*}/stream")
    @ApiOperation(value = "Select", response = Map.class,
        notes = "This API returns a stream of records. This should be used while selecting big data.")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces("application/octet-stream")
    @ApiResponses(value={
        @ApiResponse(code=200, message = "Success",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"result\":{\"row 0\":{\"address\":"
                + "{\"city\":\"Someplace\",\"street\":\"1 Some way\"},"
                + "\"emp_salary\":50,\"emp_name\":\"tom\",\"emp_id\":"
                + "\"cfd66ccc-d857-4e90-b1e5-df98a3d40cd6\"}},\"status\":\"SUCCESS\"}")
        })),
        @ApiResponse(code=400, message = "Failure",examples = @Example( value =  {
            @ExampleProperty(mediaType="application/json",value = 
                "{\"error\" : \"<errorMessage>\","
                + "\"status\" : \"FAILURE\"}") 
        }))
    })
    public Response selectStream(
        @ApiParam(value = "Major Version",example = "v2", 
            required = true) @PathParam("version") String version,
        @ApiParam(value = "Minor Version",example = "1",
            required = false) @HeaderParam(XMINORVERSION) String minorVersion,
        @ApiParam(value = "Patch Version",example = "0",
            required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
        @ApiParam(value = "AID", required = false, hidden = true) @HeaderParam("aid") String aid,
        @ApiParam(value = "Application namespace",
            required = false,hidden = true) @HeaderParam(NS) String ns,
        @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
        JsonInsert selObj,
        @ApiParam(value = "Keyspace Name", required = true) @PathParam("keyspace") String keyspace,
        @ApiParam(value = "Table Name", required = true) @PathParam("tablename") String tablename,
        @Context UriInfo info) throws Exception {
        
        try {
            if((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty())) { 
                return Response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                        .setError("one or more path parameters are not set, please check and try again")
                        .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspace + " )");
      
            Response response;
            JsonSelect jsonSelect = new JsonSelect();
            jsonSelect.setKeyspaceName(keyspace);
            jsonSelect.setTableName(tablename);
          
            response = Response.ok(selectStream(jsonSelect, info.getQueryParameters())).build();
            return response;
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }
    
    /* For unit testing purpose only */ 
    public FeedReturnStreamingOutput selectStream(JsonSelect jsonSelect, MultivaluedMap<String, String> rowParams) throws MusicServiceException, MusicQueryException {
        return MusicCore.selectStream(jsonSelect, rowParams);
    }

    
    /**
     *
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws Exception
     */
    private Response select(
        String version,String minorVersion,String patchVersion,
        String aid,String ns,String authorization,String keyspace,        
        String tablename,UriInfo info) throws Exception {
        
        try { 
            ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
            if((keyspace == null || keyspace.isEmpty()) || (tablename == null || tablename.isEmpty())){
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("one or more path parameters are not set, please check and try again")
                    .toMap()).build();
            }
            EELFLoggerDelegate.mdcPut("keyspace", "( " + keyspace + " ) ");
            try {
                JsonSelect jsonSelect = new JsonSelect();
                jsonSelect.setKeyspaceName(keyspace);
                jsonSelect.setTableName(tablename);
                
                ResultSet results = MusicCore.select(jsonSelect, info.getQueryParameters());
                
                if(results.getAvailableWithoutFetching() >0) {
                    return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setDataResult(MusicDataStoreHandle.marshallResults(results)).toMap()).build();
                }
                return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setDataResult(MusicDataStoreHandle.marshallResults(results)).setError("No data found").toMap()).build();
            } catch (MusicQueryException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), ex.getMessage()  ,ErrorSeverity
                        .WARN, ErrorTypes.MUSICSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
                
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger, ex, AppMessages.UNKNOWNERROR  ,ErrorSeverity.ERROR,
                    ErrorTypes.MUSICSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
        } finally {
            EELFLoggerDelegate.mdcRemove("keyspace");
        }
    }

    /**
     *
     * @param keyspace
     * @param tablename
     * @param info
     * @param limit
     * @return
     * @throws MusicServiceException
     */
    public PreparedQueryObject selectSpecificQuery(String keyspace,
        String tablename, UriInfo info, int limit)
        throws MusicServiceException {
        PreparedQueryObject queryObject = new PreparedQueryObject();
        StringBuilder rowIdString = getRowIdentifier(keyspace, 
            tablename,info.getQueryParameters(),queryObject).rowIdString;
        queryObject.appendQueryString(
            "SELECT *  FROM " + keyspace + "." + tablename + " WHERE " + rowIdString);
        if (limit != -1) {
            queryObject.appendQueryString(" LIMIT " + limit);
        }
        queryObject.appendQueryString(";");
        return queryObject;
    }

    /**
     *
     * @param keyspace
     * @param tablename
     * @param rowParams
     * @param queryObject
     * @return
     * @throws MusicServiceException
     */
    private RowIdentifier getRowIdentifier(String keyspace, String tablename,
        MultivaluedMap<String, String> rowParams, PreparedQueryObject queryObject)
        throws MusicServiceException {
        StringBuilder rowSpec = new StringBuilder();
        int counter = 0;
        TableMetadata tableInfo = MusicDataStoreHandle.returnColumnMetadata(keyspace, tablename);
        if (tableInfo == null) {
            logger.error(EELFLoggerDelegate.errorLogger,
                "Table information not found. Please check input for table name= "
                + keyspace + "." + tablename);
            throw new MusicServiceException(
                "Table information not found. Please check input for table name= "
                + keyspace + "." + tablename);
        }
        StringBuilder primaryKey = new StringBuilder();
        for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()) {
            String keyName = entry.getKey();
            List<String> valueList = entry.getValue();
            String indValue = valueList.get(0);
            DataType colType = null;
            Object formattedValue = null;
            try {
                colType = tableInfo.getColumn(entry.getKey()).getType();
                formattedValue = MusicUtil.convertToActualDataType(colType, indValue);
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.errorLogger,e);
            }
            if(tableInfo.getPrimaryKey().get(0).getName().equals(entry.getKey())) {
                primaryKey.append(indValue);
            }
            rowSpec.append(keyName + "= ?");
            queryObject.addValue(formattedValue);
            if (counter != rowParams.size() - 1) {
                rowSpec.append(" AND ");
            }
            counter = counter + 1;
        }
        return new RowIdentifier(primaryKey.toString(), rowSpec, queryObject);
    }
}
