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



import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;
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
import javax.ws.rs.core.UriInfo;

import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.main.MusicCore;

import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import org.onap.music.datastore.PreparedQueryObject;
import com.datastax.driver.core.ResultSet;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

//@Path("/v{version: [0-9]+}/priorityq/")
@Path("/priorityq/")
@Api(value="Q Api")
public class RestMusicQAPI {
	
	private EELFLoggerDelegate logger =EELFLoggerDelegate.getLogger(RestMusicDataAPI.class);


	/**
	 * 
	 * @param tableObj 
	 * @param keyspace
	 * @param tablename
	 * @throws Exception
	 */

	@POST
	@Path("/keyspaces/{keyspace}/{qname}")
	@ApiOperation(value = "", response = Void.class)
	@Consumes(MediaType.APPLICATION_JSON)
	public Map<String,Object> createQ( 
		@ApiParam(value="Major Version",required=true) @PathParam("version") String version,
		@ApiParam(value="Minor Version",required=false) @HeaderParam("X-minorVersion") String minorVersion,
		@ApiParam(value="Patch Version",required=false) @HeaderParam("X-patchVersion") String patchVersion,
		@ApiParam(value="AID",required=true) @HeaderParam("aid") String aid,
		@ApiParam(value="Application namespace",required=true) @HeaderParam("ns") String ns, 
		@ApiParam(value="userId",required=true) @HeaderParam("userId") String userId, 
		@ApiParam(value="Password",required=true) @HeaderParam("password") String password, JsonTable tableObj, 
		@ApiParam(value="Key Space",required=true) @PathParam("keyspace") String keyspace, 
		@ApiParam(value="Table Name",required=true) @PathParam("tablename") String tablename,
		@Context HttpServletResponse response) throws Exception{ 
		return new RestMusicDataAPI().createTable(version,minorVersion,patchVersion,aid, ns, userId, password, tableObj, keyspace, tablename,response);
	}

	/**
	 * 
	 * @param insObj
	 * @param keyspace
	 * @param tablename
	 * @throws Exception
	 */
	@POST
	@Path("/keyspaces/{keyspace}/{qname}/rows")
	@ApiOperation(value = "", response = Void.class)
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String,Object> insertIntoQ(
		@ApiParam(value="Major Version",required=true) @PathParam("version") String version,
		@ApiParam(value="Minor Version",required=false) @HeaderParam("X-minorVersion") String minorVersion,
		@ApiParam(value="Patch Version",required=false) @HeaderParam("X-patchVersion") String patchVersion,
		@ApiParam(value="AID",required=true) @HeaderParam("aid") String aid, 
		@ApiParam(value="Application namespace",required=true) @HeaderParam("ns") String ns, @ApiParam(value="userId",required=true) @HeaderParam("userId") String userId, 
		@ApiParam(value="Password",required=true) @HeaderParam("password") String password, JsonInsert insObj, 
		@ApiParam(value="Key Space",required=true) @PathParam("keyspace") String keyspace, 
		@ApiParam(value="Table Name",required=true) @PathParam("tablename") String tablename,
		@Context HttpServletResponse response) throws Exception{
		return new RestMusicDataAPI().insertIntoTable(version,minorVersion,patchVersion,aid, ns, userId, password, insObj, keyspace, tablename,response);
	}

	/**
	 * 
	 * @param updateObj
	 * @param keyspace
	 * @param tablename
	 * @param info
	 * @return
	 * @throws Exception
	 */
	@PUT
	@Path("/keyspaces/{keyspace}/{qname}/rows")
	@ApiOperation(value = "", response = String.class)
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String,Object> updateQ(
		@ApiParam(value="Major Version",required=true) @PathParam("version") String version,
		@ApiParam(value="Minor Version",required=false) @HeaderParam("X-minorVersion") String minorVersion,
		@ApiParam(value="Patch Version",required=false) @HeaderParam("X-patchVersion") String patchVersion,
		@ApiParam(value="AID",required=true) @HeaderParam("aid") String aid, 
		@ApiParam(value="Application namespace",required=true) @HeaderParam("ns") String ns, @ApiParam(value="userId",required=true) @HeaderParam("userId") String userId, 
		@ApiParam(value="Password",required=true) @HeaderParam("password") String password, JsonUpdate updateObj, 
		@ApiParam(value="Key Space",required=true) @PathParam("keyspace") String keyspace, 
		@ApiParam(value="Table Name",required=true) @PathParam("tablename") String tablename, 
		@Context UriInfo info,
		@Context HttpServletResponse response) throws Exception{
		return new RestMusicDataAPI().updateTable(version,minorVersion,patchVersion,aid, ns, userId, password, updateObj, keyspace, tablename, info,response);
	}

	/**
	 * 
	 * @param delObj
	 * @param keyspace
	 * @param tablename
	 * @param info
	 * @return
	 * @throws Exception
	 */
	@DELETE
	@Path("/keyspaces/{keyspace}/{qname}/rows")
	@ApiOperation(value = "", response = String.class)
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String,Object> deleteFromQ(
		@ApiParam(value="Major Version",required=true) @PathParam("version") String version,
		@ApiParam(value="Minor Version",required=false) @HeaderParam("X-minorVersion") String minorVersion,
		@ApiParam(value="Patch Version",required=false) @HeaderParam("X-patchVersion") String patchVersion,
		@ApiParam(value="AID",required=true) @HeaderParam("aid") String aid, 
		@ApiParam(value="Application namespace",required=true) @HeaderParam("ns") String ns, 
		@ApiParam(value="userId",required=true) @HeaderParam("userId") String userId, 
		@ApiParam(value="Password",required=true) @HeaderParam("password") String password, JsonDelete delObj, 
		@ApiParam(value="Key Space",required=true) @PathParam("keyspace") String keyspace, 
		@ApiParam(value="Table Name",required=true) @PathParam("tablename") String tablename, 
		@Context UriInfo info,
		@Context HttpServletResponse response) throws Exception{ 
		return new RestMusicDataAPI().deleteFromTable(version,minorVersion,patchVersion,aid, ns, userId, password, delObj, keyspace, tablename, info,response);
	}

	/**
	 * 
	 * @param keyspace
	 * @param tablename
	 * @param info
	 * @return
	 * @throws Exception
	 */
	@GET
	@Path("/keyspaces/{keyspace}/{qname}/peek")
	@ApiOperation(value = "", response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String, HashMap<String, Object>> peek(
		@ApiParam(value="Major Version",required=true) @PathParam("version") String version,
		@ApiParam(value="Minor Version",required=false) @HeaderParam("X-minorVersion") String minorVersion,
		@ApiParam(value="Patch Version",required=false) @HeaderParam("X-patchVersion") String patchVersion,
		@ApiParam(value="AID",required=true) @HeaderParam("aid") String aid, 
		@ApiParam(value="Application namespace",required=true) @HeaderParam("ns") String ns, 
		@ApiParam(value="userId",required=true) @HeaderParam("userId") String userId, 
		@ApiParam(value="Password",required=true) @HeaderParam("password") String password,
		@ApiParam(value="Key Space",required=true) @PathParam("keyspace") String keyspace, 
		@ApiParam(value="Table Name",required=true) @PathParam("tablename") String tablename, 
		@Context UriInfo info) throws Exception{
		int limit =1; //peek must return just the top row
		PreparedQueryObject query = new RestMusicDataAPI().selectSpecificQuery(version,minorVersion,patchVersion,aid, ns, userId, password,keyspace,tablename,info,limit);
		ResultSet results = MusicCore.get(query);
		return MusicCore.marshallResults(results);

	} 
	
	/**
	 * 
	 *
	 * @param keyspace
	 * @param tablename
	 * @param info
	 * @return
	 * @throws Exception
	 */
	@GET
	@Path("/keyspaces/{keyspace}/{qname}/filter")
	@ApiOperation(value = "", response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String, HashMap<String, Object>> filter(
		@ApiParam(value="Major Version",required=true) @PathParam("version") String version,
		@ApiParam(value="Minor Version",required=false) @HeaderParam("X-minorVersion") String minorVersion,
		@ApiParam(value="Patch Version",required=false) @HeaderParam("X-patchVersion") String patchVersion,
		@ApiParam(value="AID",required=true) @HeaderParam("aid") String aid, 
		@ApiParam(value="Application namespace",required=true) @HeaderParam("ns") String ns, 
		@ApiParam(value="userId",required=true) @HeaderParam("userId") String userId, 
		@ApiParam(value="Password",required=true) @HeaderParam("password") String password,
		@ApiParam(value="Key Space",required=true) @PathParam("keyspace") String keyspace, 
		@ApiParam(value="Table Name",required=true) @PathParam("tablename") String tablename, 
		@Context UriInfo info) throws Exception{
		int limit =-1; 
		PreparedQueryObject query = new RestMusicDataAPI().selectSpecificQuery(version,minorVersion,patchVersion,aid, ns, userId, password,keyspace,tablename,info,limit);
		ResultSet results = MusicCore.get(query);
		return MusicCore.marshallResults(results);
	} 

	/**
	 * 
	 * @param tabObj
	 * @param keyspace
	 * @param tablename
	 * @throws Exception
	 */
	@DELETE
	@ApiOperation(value = "", response = Void.class)
	@Path("/keyspaces/{keyspace}/{qname}")
	public Map<String,Object> dropQ(
		@ApiParam(value="Major Version",required=true) @PathParam("version") String version,
		@ApiParam(value="Minor Version",required=false) @HeaderParam("X-minorVersion") String minorVersion,
		@ApiParam(value="Patch Version",required=false) @HeaderParam("X-patchVersion") String patchVersion,
		@ApiParam(value="AID",required=true) @HeaderParam("aid") String aid, 
		@ApiParam(value="Application namespace",required=true) @HeaderParam("ns") String ns, 
		@ApiParam(value="userId",required=true) @HeaderParam("userId") String userId, 
		@ApiParam(value="Password",required=true) @HeaderParam("password") String password, JsonTable tabObj,
		@ApiParam(value="Key Space",required=true) @PathParam("keyspace") String keyspace, 
		@ApiParam(value="Table Name",required=true) @PathParam("tablename") String tablename,
		@Context HttpServletResponse response) throws Exception{ 
		return new RestMusicDataAPI().dropTable(version,minorVersion,patchVersion,aid, ns, userId, password, tabObj, keyspace, tablename,response);
	}
}
