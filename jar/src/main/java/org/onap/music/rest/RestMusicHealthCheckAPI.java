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
/**
 * @author inam
 *
 */
import java.util.Map;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.Consumes;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;


import org.onap.music.response.jsonobjects.JsonResponse;
import org.onap.music.eelf.healthcheck.MusicHealthCheck;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;

import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;




@Path("/v{version: [0-9]+}/service")
@Api(value="Healthcheck Api")
public class RestMusicHealthCheckAPI {
	
	
	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicUtil.class);
	
	
	@GET
	@Path("/cs")
	@ApiOperation(value = "Get Health Status", response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)
	public Response cassandraStatus(@Context HttpServletResponse response) {
		logger.info(EELFLoggerDelegate.applicationLogger,"Replying to request for MUSIC Health Check status for Cassandra");
		
		Map<String, Object> resultMap = new HashMap<>();
		
		MusicHealthCheck cassHealthCheck = new MusicHealthCheck();
		String status = cassHealthCheck.getCassandraStatus();
		if(status.equals("ACTIVE")) {
			resultMap.put("ACTIVE", "Cassandra Running and Listening to requests");
			return Response.status(Status.OK).entity(resultMap).build();
		}else {
			resultMap.put("INACTIVE", "Cassandra Service is not responding");
			return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
		}
		
		
		
	}
	
	@GET
	@Path("/zk")
	@ApiOperation(value = "Get Health Status", response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)
	public Response ZKStatus(@Context HttpServletResponse response) {
		logger.info(EELFLoggerDelegate.applicationLogger,"Replying to request for MUSIC Health Check status for Zookeeper");
		Map<String, Object> resultMap = new HashMap<>();
		MusicHealthCheck ZKHealthCheck = new MusicHealthCheck();
		String status = ZKHealthCheck.getZookeeperStatus();
		if(status.equals("ACTIVE")) {
			resultMap.put("ACTIVE", "Zookeeper is Active and Running");
			return Response.status(Status.OK).entity(resultMap).build();
		}else {
			resultMap.put("INACTIVE", "Zookeeper is not responding");
			return Response.status(Status.BAD_REQUEST).entity(resultMap).build();
		}
	}
	
	
	
	
	
	

}
