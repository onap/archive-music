/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2019 Samsung
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

import java.nio.ByteBuffer;
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

import org.apache.commons.lang3.StringUtils;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonKeySpace;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.datastore.Condition;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.response.jsonobjects.JsonResponse;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TableMetadata;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.ApiResponse;
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
        
                String consistency = MusicUtil.EVENTUAL;// for now this needs only eventual consistency
        
                PreparedQueryObject queryObject = new PreparedQueryObject();
                if(consistency.equalsIgnoreCase(MusicUtil.EVENTUAL) && kspObject.getConsistencyInfo().get("consistency") != null) {
                    if(MusicUtil.isValidConsistency(kspObject.getConsistencyInfo().get("consistency")))
                        queryObject.setConsistency(kspObject.getConsistencyInfo().get("consistency"));
                    else
                        return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.SYNTAXERROR).setError("Invalid Consistency type").toMap()).build();
                }
                long start = System.currentTimeMillis();
                Map<String, Object> replicationInfo = kspObject.getReplicationInfo();
                String repString = null;
                try {
                    repString = "{" + MusicUtil.jsonMaptoSqlString(replicationInfo, ",") + "}";
                } catch (Exception e) {
                    logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.MISSINGDATA  ,ErrorSeverity
                        .CRITICAL, ErrorTypes.DATAERROR, e);
        
                }
                queryObject.appendQueryString(
                                "CREATE KEYSPACE " + keyspaceName + " WITH replication = " + repString);
                if (kspObject.getDurabilityOfWrites() != null) {
                    queryObject.appendQueryString(
                                    " AND durable_writes = " + kspObject.getDurabilityOfWrites());
                }
        
                queryObject.appendQueryString(";");
                long end = System.currentTimeMillis();
                logger.info(EELFLoggerDelegate.applicationLogger,
                                "Time taken for setting up query in create keyspace:" + (end - start));
        
                ResultType result = ResultType.FAILURE;
                try {
                    result = MusicCore.nonKeyRelatedPut(queryObject, consistency);
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
                PreparedQueryObject queryObject = new PreparedQueryObject();
                queryObject.appendQueryString("DROP KEYSPACE " + keyspaceName + ";");
                String droperror = "Error Deleteing Keyspace " + keyspaceName;
                try{
                    ResultType result = MusicCore.nonKeyRelatedPut(queryObject, consistency);
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
            String primaryKey = null;
            String partitionKey = tableObj.getPartitionKey();
            String clusterKey = tableObj.getClusteringKey();
            String filteringKey = tableObj.getFilteringKey();
            if(filteringKey != null) {
                clusterKey = clusterKey + "," + filteringKey;
            }
            primaryKey = tableObj.getPrimaryKey(); // get primaryKey if available

            PreparedQueryObject queryObject = new PreparedQueryObject();
            // first read the information about the table fields
            Map<String, String> fields = tableObj.getFields();
            if (fields == null) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("Create Table Error: No fields in request").toMap()).build();
            }

            StringBuilder fieldsString = new StringBuilder("(vector_ts text,");
            int counter = 0;
            for (Map.Entry<String, String> entry : fields.entrySet()) {
                if (entry.getKey().equals("PRIMARY KEY")) {
                    primaryKey = entry.getValue(); // replaces primaryKey
                    primaryKey = primaryKey.trim();
                } else {
                    if (counter == 0 )  fieldsString.append("" + entry.getKey() + " " + entry.getValue() + "");
                    else fieldsString.append("," + entry.getKey() + " " + entry.getValue() + "");
                }

                if (counter != (fields.size() - 1) ) {
                    counter = counter + 1; 
                } else {
            
                    if((primaryKey != null) && (partitionKey == null)) {
                        primaryKey = primaryKey.trim();
                        int count1 = StringUtils.countMatches(primaryKey, ')');
                        int count2 = StringUtils.countMatches(primaryKey, '(');
                        if (count1 != count2) {
                            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                                .setError("Create Table Error: primary key '(' and ')' do not match, primary key=" + primaryKey)
                                .toMap()).build();
                        }

                        if ( primaryKey.indexOf('(') == -1  || ( count2 == 1 && (primaryKey.lastIndexOf(')') +1) ==  primaryKey.length() ) ) {
                            if (primaryKey.contains(",") ) {
                                partitionKey= primaryKey.substring(0,primaryKey.indexOf(','));
                                partitionKey=partitionKey.replaceAll("[\\(]+","");
                                clusterKey=primaryKey.substring(primaryKey.indexOf(',')+1);  // make sure index
                                clusterKey=clusterKey.replaceAll("[)]+", "");
                            } else {
                                partitionKey=primaryKey;
                                partitionKey=partitionKey.replaceAll("[\\)]+","");
                                partitionKey=partitionKey.replaceAll("[\\(]+","");
                                clusterKey="";
                            }
                        } else {   // not null and has ) before the last char
                            partitionKey= primaryKey.substring(0,primaryKey.indexOf(')'));
                            partitionKey=partitionKey.replaceAll("[\\(]+","");
                            partitionKey = partitionKey.trim();
                            clusterKey= primaryKey.substring(primaryKey.indexOf(')'));
                            clusterKey=clusterKey.replaceAll("[\\(]+","");
                            clusterKey=clusterKey.replaceAll("[\\)]+","");
                            clusterKey = clusterKey.trim();
                            if (clusterKey.indexOf(',') == 0) {
                                clusterKey=clusterKey.substring(1);
                            }
                            clusterKey = clusterKey.trim();
                            if (clusterKey.equals(",") ) clusterKey=""; // print error if needed    ( ... ),)
                        }

                        if (!(partitionKey.isEmpty() || clusterKey.isEmpty())
                            && (partitionKey.equalsIgnoreCase(clusterKey) ||
                            clusterKey.contains(partitionKey) || partitionKey.contains(clusterKey)) ) {
                            logger.error("DataAPI createTable partition/cluster key ERROR: partitionKey="+partitionKey+", clusterKey=" + clusterKey + " and primary key=" + primaryKey );
                            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(
                                "Create Table primary key error: clusterKey(" + clusterKey + ") equals/contains/overlaps partitionKey(" +partitionKey+ ")  of"
                                + " primary key=" + primaryKey)
                                .toMap()).build();

                        }

                        if (partitionKey.isEmpty() )  primaryKey="";
                        else  if (clusterKey.isEmpty() ) primaryKey=" (" + partitionKey  + ")";
                        else  primaryKey=" (" + partitionKey + ")," + clusterKey;

                
                        if (primaryKey != null) fieldsString.append(", PRIMARY KEY (" + primaryKey + " )");

                    } else { // end of length > 0
                    
                        if (!(partitionKey.isEmpty() || clusterKey.isEmpty())
                            && (partitionKey.equalsIgnoreCase(clusterKey) ||
                            clusterKey.contains(partitionKey) || partitionKey.contains(clusterKey)) ) {
                            logger.error("DataAPI createTable partition/cluster key ERROR: partitionKey="+partitionKey+", clusterKey=" + clusterKey);
                            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(
                                "Create Table primary key error: clusterKey(" + clusterKey + ") equals/contains/overlaps partitionKey(" +partitionKey+ ")")
                                .toMap()).build();
                        }

                        if (partitionKey.isEmpty() )  primaryKey="";
                        else  if (clusterKey.isEmpty() ) primaryKey=" (" + partitionKey  + ")";
                        else  primaryKey=" (" + partitionKey + ")," + clusterKey;

                        if (primaryKey != null) fieldsString.append(", PRIMARY KEY (" + primaryKey + " )");
                    }
                    fieldsString.append(")");

                } // end of last field check

            } // end of for each
            // information about the name-value style properties
            Map<String, Object> propertiesMap = tableObj.getProperties();
            StringBuilder propertiesString = new StringBuilder();
            if (propertiesMap != null) {
                counter = 0;
                for (Map.Entry<String, Object> entry : propertiesMap.entrySet()) {
                    Object ot = entry.getValue();
                    String value = ot + "";
                    if (ot instanceof String) {
                        value = "'" + value + "'";
                    } else if (ot instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> otMap = (Map<String, Object>) ot;
                        value = "{" + MusicUtil.jsonMaptoSqlString(otMap, ",") + "}";
                    }

                    propertiesString.append(entry.getKey() + "=" + value + "");
                    if (counter != propertiesMap.size() - 1)
                        propertiesString.append(" AND ");

                    counter = counter + 1;
                }
            }

            String clusteringOrder = tableObj.getClusteringOrder();

            if (clusteringOrder != null && !(clusteringOrder.isEmpty())) {
                String[] arrayClusterOrder = clusteringOrder.split("[,]+");

                for (int i = 0; i < arrayClusterOrder.length; i++) {
                    String[] clusterS = arrayClusterOrder[i].trim().split("[ ]+");
                    if ( (clusterS.length ==2)  && (clusterS[1].equalsIgnoreCase("ASC") || clusterS[1].equalsIgnoreCase("DESC"))) {
                        continue;
                    } else {
                        return response.status(Status.BAD_REQUEST)
                        .entity(new JsonResponse(ResultType.FAILURE)
                        .setError("createTable/Clustering Order vlaue ERROR: valid clustering order is ASC or DESC or expecting colname  order; please correct clusteringOrder:"+ clusteringOrder+".")
                        .toMap()).build();
                    }
                    // add validation for column names in cluster key
                }

                if (!(clusterKey.isEmpty())) {
                    clusteringOrder = "CLUSTERING ORDER BY (" +clusteringOrder +")";
                    //cjc check if propertiesString.length() >0 instead propertiesMap
                    if (propertiesMap != null) {
                        propertiesString.append(" AND  "+ clusteringOrder);
                    } else {
                        propertiesString.append(clusteringOrder);
                    }
                } else {
                    logger.warn("Skipping clustering order=("+clusteringOrder+ ") since clustering key is empty ");
                }
            } //if non empty

            queryObject.appendQueryString(
                "CREATE TABLE " + keyspace + "." + tablename + " " + fieldsString);


            if (propertiesString != null &&  propertiesString.length()>0 )
                queryObject.appendQueryString(" WITH " + propertiesString);
            queryObject.appendQueryString(";");
            ResultType result = ResultType.FAILURE;
            try {
                result = MusicCore.createTable(keyspace, tablename, queryObject, consistency);
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
            PreparedQueryObject query = new PreparedQueryObject();
            query.appendQueryString("Create index if not exists " + indexName + "  on " + keyspace + "."
                            + tablename + " (" + fieldName + ");");

            ResultType result = ResultType.FAILURE;
            try {
                result = MusicCore.nonKeyRelatedPut(query, "eventual");
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
            PreparedQueryObject queryObject = new PreparedQueryObject();
            TableMetadata tableInfo = null;
            try {
                tableInfo = MusicDataStoreHandle.returnColumnMetadata(keyspace, tablename);
                if(tableInfo == null) {
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Table name doesn't exists. Please check the table name.").toMap()).build();
                }
            } catch (MusicServiceException e) {
                logger.error(EELFLoggerDelegate.errorLogger, e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
            }
            String primaryKeyName = tableInfo.getPrimaryKey().get(0).getName();
            StringBuilder fieldsString = new StringBuilder("(vector_ts,");
            String vectorTs =
                            String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
            StringBuilder valueString = new StringBuilder("(" + "?" + ",");
            queryObject.addValue(vectorTs);
            
            Map<String, Object> valuesMap = insObj.getValues();
            if (valuesMap==null) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                        .setError("Nothing to insert. No values provided in request.").toMap()).build();
            }
            int counter = 0;
            String primaryKey = "";
            for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
                fieldsString.append("" + entry.getKey());
                Object valueObj = entry.getValue();
                if (primaryKeyName.equals(entry.getKey())) {
                    primaryKey = entry.getValue() + "";
                    primaryKey = primaryKey.replace("'", "''");
                }
                DataType colType = null;
                try {
                    colType = tableInfo.getColumn(entry.getKey()).getType();
                } catch(NullPointerException ex) {
                    logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage() +" Invalid column name : "+entry.getKey
                        (), AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR, ex);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Invalid column name : "+entry.getKey()).toMap()).build();
                }

                Object formattedValue = null;
                try {
                    formattedValue = MusicUtil.convertToActualDataType(colType, valueObj);
                } catch (Exception e) {
                    logger.error(EELFLoggerDelegate.errorLogger,e);
                }
                valueString.append("?");

                queryObject.addValue(formattedValue);

                if (counter == valuesMap.size() - 1) {
                    fieldsString.append(")");
                    valueString.append(")");
                } else {
                    fieldsString.append(",");
                    valueString.append(",");
                }
                counter = counter + 1;
            }

            //blobs..
            Map<String, byte[]> objectMap = insObj.getObjectMap();
            if(objectMap != null) {
                for (Map.Entry<String, byte[]> entry : objectMap.entrySet()) {
                    if(counter > 0) {
                        fieldsString.replace(fieldsString.length()-1, fieldsString.length(), ",");
                        valueString.replace(valueString.length()-1, valueString.length(), ",");
                    }
                    fieldsString.append("" + entry.getKey());
                    byte[] valueObj = entry.getValue();
                    if (primaryKeyName.equals(entry.getKey())) {
                        primaryKey = entry.getValue() + "";
                        primaryKey = primaryKey.replace("'", "''");
                    }
                    DataType colType = tableInfo.getColumn(entry.getKey()).getType();
                    ByteBuffer formattedValue = null;
                    if(colType.toString().toLowerCase().contains("blob")) {
                        formattedValue = MusicUtil.convertToActualDataType(colType, valueObj);
                    }
                    valueString.append("?");
                    queryObject.addValue(formattedValue);
                    counter = counter + 1;
                    fieldsString.append(",");
                    valueString.append(",");
                } 
            }

            if(primaryKey == null || primaryKey.length() <= 0) {
                logger.error(EELFLoggerDelegate.errorLogger, "Some required partition key parts are missing: "+primaryKeyName );
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.SYNTAXERROR).setError("Some required partition key parts are missing: "+primaryKeyName).toMap()).build();
            }

            fieldsString.replace(fieldsString.length()-1, fieldsString.length(), ")");
            valueString.replace(valueString.length()-1, valueString.length(), ")");

            queryObject.appendQueryString("INSERT INTO " + keyspace + "." + tablename + " "
                            + fieldsString + " VALUES " + valueString);

            String ttl = insObj.getTtl();
            String timestamp = insObj.getTimestamp();

            if ((ttl != null) && (timestamp != null)) {
                logger.info(EELFLoggerDelegate.applicationLogger, "both there");
                queryObject.appendQueryString(" USING TTL ? AND TIMESTAMP ?");
                queryObject.addValue(Integer.parseInt(ttl));
                queryObject.addValue(Long.parseLong(timestamp));
            }

            if ((ttl != null) && (timestamp == null)) {
                logger.info(EELFLoggerDelegate.applicationLogger, "ONLY TTL there");
                queryObject.appendQueryString(" USING TTL ?");
                queryObject.addValue(Integer.parseInt(ttl));
            }

            if ((ttl == null) && (timestamp != null)) {
                logger.info(EELFLoggerDelegate.applicationLogger, "ONLY timestamp there");
                queryObject.appendQueryString(" USING TIMESTAMP ?");
                queryObject.addValue(Long.parseLong(timestamp));
            }

            queryObject.appendQueryString(";");

            ReturnType result = null;
            String consistency = insObj.getConsistencyInfo().get("type");
            if(consistency.equalsIgnoreCase(MusicUtil.EVENTUAL) && insObj.getConsistencyInfo().get("consistency") != null) {
                if(MusicUtil.isValidConsistency(insObj.getConsistencyInfo().get("consistency"))) {
                    queryObject.setConsistency(insObj.getConsistencyInfo().get("consistency"));
                } else {
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.SYNTAXERROR).setError("Invalid Consistency type").toMap()).build();
                }
            }
            queryObject.setOperation("insert");
            try {
                if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL)) {
                    result = MusicCore.eventualPut(queryObject);
                } else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                    String lockId = insObj.getConsistencyInfo().get("lockId");
                    if(lockId == null) {
                        logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                                + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                        return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                                + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
                    }
                    result = MusicCore.criticalPut(keyspace, tablename, primaryKey, queryObject, lockId,null);
                } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
                    result = MusicCore.atomicPut(keyspace, tablename, primaryKey, queryObject, null);
                }
            } catch (Exception ex) {
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

            logger.info(EELFLoggerDelegate.applicationLogger, "--------------Music " + consistency
                + " update-" + operationId + "-------------------------");
            // obtain the field value pairs of the update

            PreparedQueryObject queryObject = new PreparedQueryObject();
            Map<String, Object> valuesMap = updateObj.getValues();

            TableMetadata tableInfo;
            try {
                tableInfo = MusicDataStoreHandle.returnColumnMetadata(keyspace, tablename);
            } catch (MusicServiceException e) {
                logger.error(EELFLoggerDelegate.errorLogger,e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes
                    .GENERALSERVICEERROR, e);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
            }
            if (tableInfo == null) {
                logger.error(EELFLoggerDelegate.errorLogger,"Table information not found. Please check input for table name= "+tablename, AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("Table information not found. Please check input for table name= "
                    + keyspace + "." + tablename).toMap()).build();
            }
            String vectorTs = String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
            StringBuilder fieldValueString = new StringBuilder("vector_ts=?,");
            queryObject.addValue(vectorTs);
            int counter = 0;
            for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
                Object valueObj = entry.getValue();
                DataType colType = null;
                try {
                    colType = tableInfo.getColumn(entry.getKey()).getType();
                } catch(NullPointerException ex) {
                    logger.error(EELFLoggerDelegate.errorLogger, ex, "Invalid column name : "+entry.getKey(), ex);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Invalid column name : "+entry.getKey()).toMap()).build();
                }
                Object valueString = null;
                try {
                    valueString = MusicUtil.convertToActualDataType(colType, valueObj);
                } catch (Exception e) {
                    logger.error(EELFLoggerDelegate.errorLogger,e);
                }
                fieldValueString.append(entry.getKey() + "= ?");
                queryObject.addValue(valueString);
                if (counter != valuesMap.size() - 1) {
                    fieldValueString.append(",");
                }    
                counter = counter + 1;
            }
            String ttl = updateObj.getTtl();
            String timestamp = updateObj.getTimestamp();

            queryObject.appendQueryString("UPDATE " + keyspace + "." + tablename + " ");
            if ((ttl != null) && (timestamp != null)) {
                logger.info("both there");
                queryObject.appendQueryString(" USING TTL ? AND TIMESTAMP ?");
                queryObject.addValue(Integer.parseInt(ttl));
                queryObject.addValue(Long.parseLong(timestamp));
            }

            if ((ttl != null) && (timestamp == null)) {
                logger.info("ONLY TTL there");
                queryObject.appendQueryString(" USING TTL ?");
                queryObject.addValue(Integer.parseInt(ttl));
            }

            if ((ttl == null) && (timestamp != null)) {
                logger.info("ONLY timestamp there");
                queryObject.appendQueryString(" USING TIMESTAMP ?");
                queryObject.addValue(Long.parseLong(timestamp));
            }
            // get the row specifier
            RowIdentifier rowId = null;
            try {
                rowId = getRowIdentifier(keyspace, tablename, info.getQueryParameters(), queryObject);
                if(rowId == null || rowId.primarKeyValue.isEmpty()) {
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                            .setError("Mandatory WHERE clause is missing. Please check the input request.").toMap()).build();
                }
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes
                    .GENERALSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }

            queryObject.appendQueryString(
                " SET " + fieldValueString + " WHERE " + rowId.rowIdString + ";");

            // get the conditional, if any
            Condition conditionInfo;
            if (updateObj.getConditions() == null) {
                conditionInfo = null;
            } else {
                // to avoid parsing repeatedly, just send the select query to obtain row
                PreparedQueryObject selectQuery = new PreparedQueryObject();
                selectQuery.appendQueryString("SELECT *  FROM " + keyspace + "." + tablename + " WHERE "
                    + rowId.rowIdString + ";");
                selectQuery.addValue(rowId.primarKeyValue);
                conditionInfo = new Condition(updateObj.getConditions(), selectQuery);
            }

            ReturnType operationResult = null;
            long jsonParseCompletionTime = System.currentTimeMillis();

            if(consistency.equalsIgnoreCase(MusicUtil.EVENTUAL) && updateObj.getConsistencyInfo().get("consistency") != null) {
                if(MusicUtil.isValidConsistency(updateObj.getConsistencyInfo().get("consistency"))) {
                    queryObject.setConsistency(updateObj.getConsistencyInfo().get("consistency"));
                } else {
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.SYNTAXERROR).setError("Invalid Consistency type").toMap()).build();
                }
            }
            queryObject.setOperation("update");
            if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL)) {
                operationResult = MusicCore.eventualPut(queryObject);
            } else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                String lockId = updateObj.getConsistencyInfo().get("lockId");
                if(lockId == null) {
                    logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                            + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                            + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
                }
                try {
                    operationResult = MusicCore.criticalPut(keyspace, tablename, rowId.primarKeyValue,
                        queryObject, lockId, conditionInfo);
                } catch ( Exception e) {
                    logger.error(EELFLoggerDelegate.errorLogger,e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN,
                        ErrorTypes.GENERALSERVICEERROR, e);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Error doing critical put: " + e.getMessage()).toMap()).build();
                }
            } else if (consistency.equalsIgnoreCase("atomic_delete_lock")) {
                // this function is mainly for the benchmarks
                try {
                    operationResult = MusicCore.atomicPutWithDeleteLock(keyspace, tablename,
                        rowId.primarKeyValue, queryObject, conditionInfo);
                } catch (MusicLockingException e) {
                    logger.error(EELFLoggerDelegate.errorLogger,e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN,
                        ErrorTypes.GENERALSERVICEERROR, e);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
                }
            } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
                try {
                    operationResult = MusicCore.atomicPut(keyspace, tablename, rowId.primarKeyValue,
                        queryObject, conditionInfo);
                } catch (MusicLockingException e) {
                    logger.error(EELFLoggerDelegate.errorLogger,e, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR, e);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
                }
            } else if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL_NB)) {
                operationResult = MusicCore.eventualPut_nb(queryObject, keyspace, tablename, rowId.primarKeyValue);
            }
            long actualUpdateCompletionTime = System.currentTimeMillis();

            long endTime = System.currentTimeMillis();
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
            PreparedQueryObject queryObject = new PreparedQueryObject();
            StringBuilder columnString = new StringBuilder();

            int counter = 0;
            List<String> columnList = delObj.getColumns();
            if (columnList != null) {
                for (String column : columnList) {
                    columnString.append(column);
                    if (counter != columnList.size() - 1)
                        columnString.append(",");
                    counter = counter + 1;
                }
            }

            // get the row specifier
            RowIdentifier rowId = null;
            try {
                rowId = getRowIdentifier(keyspace, tablename, info.getQueryParameters(), queryObject);
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes
                    .GENERALSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
            String rowSpec = rowId.rowIdString.toString();

            if ((columnList != null) && (!rowSpec.isEmpty())) {
                queryObject.appendQueryString("DELETE " + columnString + " FROM " + keyspace + "."
                                + tablename + " WHERE " + rowSpec + ";");
            }

            if ((columnList == null) && (!rowSpec.isEmpty())) {
                queryObject.appendQueryString("DELETE FROM " + keyspace + "." + tablename + " WHERE "
                                + rowSpec + ";");
            }

            if ((columnList != null) && (rowSpec.isEmpty())) {
                queryObject.appendQueryString(
                                "DELETE " + columnString + " FROM " + keyspace + "." + rowSpec + ";");
            }
            // get the conditional, if any
            Condition conditionInfo;
            if (delObj.getConditions() == null) {
                conditionInfo = null;
            } else {
                // to avoid parsing repeatedly, just send the select query to
                // obtain row
                PreparedQueryObject selectQuery = new PreparedQueryObject();
                selectQuery.appendQueryString("SELECT *  FROM " + keyspace + "." + tablename + " WHERE "
                    + rowId.rowIdString + ";");
                selectQuery.addValue(rowId.primarKeyValue);
                conditionInfo = new Condition(delObj.getConditions(), selectQuery);
            }

            String consistency = delObj.getConsistencyInfo().get("type");


            if(consistency.equalsIgnoreCase(MusicUtil.EVENTUAL) && delObj.getConsistencyInfo().get("consistency")!=null) {
                if(MusicUtil.isValidConsistency(delObj.getConsistencyInfo().get("consistency"))) {
                    queryObject.setConsistency(delObj.getConsistencyInfo().get("consistency"));
                } else {
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.SYNTAXERROR)
                        .setError("Invalid Consistency type").toMap()).build();
                }
            }
            ReturnType operationResult = null;
            queryObject.setOperation("delete");
            try {
                if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL))
                    operationResult = MusicCore.eventualPut(queryObject);
                else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                    String lockId = delObj.getConsistencyInfo().get("lockId");
                    if(lockId == null) {
                        logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                            + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                        return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                            + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
                    }
                    operationResult = MusicCore.criticalPut(keyspace, tablename, rowId.primarKeyValue,
                        queryObject, lockId, conditionInfo);
                } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
                    operationResult = MusicCore.atomicPut(keyspace, tablename, rowId.primarKeyValue,
                        queryObject, conditionInfo);
                } else if(consistency.equalsIgnoreCase(MusicUtil.EVENTUAL_NB)) {                    
                    operationResult = MusicCore.eventualPut_nb(queryObject, keyspace, tablename, rowId.primarKeyValue);
                }
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
            String consistency = "eventual";// for now this needs only eventual consistency
            PreparedQueryObject query = new PreparedQueryObject();
            query.appendQueryString("DROP TABLE  " + keyspace + "." + tablename + ";");
            try {
                return response.status(Status.OK).entity(new JsonResponse(MusicCore.nonKeyRelatedPut(query, consistency)).toMap()).build();
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

            String lockId = selObj.getConsistencyInfo().get("lockId");
            PreparedQueryObject queryObject = new PreparedQueryObject();
            RowIdentifier rowId = null;
            try {
                rowId = getRowIdentifier(keyspace, tablename, info.getQueryParameters(), queryObject);
                if ( "".equals(rowId)) {
                    logger.error(EELFLoggerDelegate.errorLogger,RestMusicDataAPI.PARAMETER_ERROR, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes
                    .GENERALSERVICEERROR);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(RestMusicDataAPI.PARAMETER_ERROR).toMap()).build();
                }
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes
                    .GENERALSERVICEERROR, ex);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
            queryObject.appendQueryString(
                "SELECT *  FROM " + keyspace + "." + tablename + " WHERE " + rowId.rowIdString + ";");

            ResultSet results = null;

            String consistency = selObj.getConsistencyInfo().get("type");
            try {
                if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                    if(lockId == null) {
                        logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                            + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                        return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                            + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
                    }
                    results = MusicCore.criticalGet(keyspace, tablename, rowId.primarKeyValue, queryObject,lockId);
                } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
                    results = MusicCore.atomicGet(keyspace, tablename, rowId.primarKeyValue, queryObject);
                } else {
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                        .setError("Consistency must be: " + MusicUtil.ATOMIC + " or " + MusicUtil.CRITICAL)
                        .toMap()).build();
                }
            } catch ( MusicLockingException | MusicServiceException me ) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Music Exception" + me.getMessage()).toMap()).build();
            } catch ( Exception ex) {
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
            PreparedQueryObject queryObject = new PreparedQueryObject();

            if (info.getQueryParameters().isEmpty()) { // select all
                queryObject.appendQueryString("SELECT *  FROM " + keyspace + "." + tablename + ";");
            } else {
                int limit = -1; // do not limit the number of results
                try {
                    queryObject = selectSpecificQuery(keyspace, tablename, info, limit);
                } catch (MusicServiceException ex) {
                    logger.error(EELFLoggerDelegate.errorLogger, ex, AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN,
                        ErrorTypes.GENERALSERVICEERROR, ex);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
                }
            }
            try {
                ResultSet results = MusicCore.get(queryObject);
                if(results.getAvailableWithoutFetching() >0) {
                    return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setDataResult(MusicDataStoreHandle.marshallResults(results)).toMap()).build();
                }
                return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setDataResult(MusicDataStoreHandle.marshallResults(results)).setError("No data found").toMap()).build();
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
