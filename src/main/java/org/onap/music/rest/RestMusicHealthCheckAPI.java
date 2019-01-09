/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 *
 *  Modifications Copyright (C) 2018 IBM.
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
/**
 * @author inam
 *
 */
import java.util.Map;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;


import org.onap.music.eelf.healthcheck.MusicHealthCheck;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.main.MusicUtil;

import com.datastax.driver.core.ConsistencyLevel;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;




@Path("/v{version: [0-9]+}/service")
@Api(value="Healthcheck Api")
public class RestMusicHealthCheckAPI {
	
	
	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicUtil.class);
	private static final String activeStatus = "ACTIVE";
	
	@GET
	@Path("/pingCassandra/{consistency}")
	@ApiOperation(value = "Get Health Status", response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)
	public Response cassandraStatus(@Context HttpServletResponse response, @ApiParam(value = "Consistency level",
            required = true) @PathParam("consistency") String consistency) {
		logger.info(EELFLoggerDelegate.applicationLogger,"Replying to request for MUSIC Health Check status for Cassandra");
		
		Map<String, Object> resultMap = new HashMap<>();
		if(ConsistencyLevel.valueOf(consistency) == null) {
			resultMap.put("INVALID", "Consistency level is invalid...");
			return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
		}
		MusicHealthCheck cassHealthCheck = new MusicHealthCheck();
		String status = cassHealthCheck.getCassandraStatus(consistency);
		if(status.equals(activeStatus)) {
			resultMap.put(activeStatus, "Cassandra Running and Listening to requests");
			return Response.status(Status.OK).entity(resultMap).build();
		} else {
			resultMap.put("INACTIVE", "One or more nodes in the Cluster is/are down or not responding.");
			return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
		}
		
		
		
	}
	
	@GET
	@Path("/pingZookeeper")
	@ApiOperation(value = "Get Health Status", response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)
	public Response ZKStatus(@Context HttpServletResponse response) {
		logger.info(EELFLoggerDelegate.applicationLogger,"Replying to request for MUSIC Health Check status for Zookeeper");
		Map<String, Object> resultMap = new HashMap<>();
		MusicHealthCheck ZKHealthCheck = new MusicHealthCheck();
		String status = ZKHealthCheck.getZookeeperStatus();
		if(status.equals(activeStatus)) {
			resultMap.put(activeStatus, "Zookeeper is Active and Running");
			return Response.status(Status.OK).entity(resultMap).build();
		}else {
			resultMap.put("INACTIVE", "Zookeeper is not responding");
			return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
		}
	}
	
	@GET
	@Path("/musicHealthCheck")
	@ApiOperation(value = "Get Health Status", response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)
	public Response musicHealthCheck() {
		logger.info(EELFLoggerDelegate.applicationLogger,"Replying to request for Health Check status for MUSIC");
		Map<String, Object> resultMap = new HashMap<>();
		MusicHealthCheck healthCheck = new MusicHealthCheck();
		String status = healthCheck.getZookeeperStatus();
		if(status.equals(activeStatus)) {
			resultMap.put("ZooKeeper", "Active");
		}else {
			resultMap.put("ZooKeeper", "Inactive");
		}
		status = healthCheck.getCassandraStatus(ConsistencyLevel.ANY.toString());
		if(status.equals(activeStatus)) {
			resultMap.put("Cassandra", "Active");
		} else {
			resultMap.put("Cassandra", "Inactive");
		}
		resultMap.put("MUSIC", "Active");
		return Response.status(Status.OK).entity(resultMap).build();
	}

}
