/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2019 IBM
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
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.response.jsonobjects.JsonResponse;
import org.onap.music.rest.service.MusicDataAPIService;
import org.onap.music.rest.service.RestMusicQAPIService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TableMetadata;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@RestController
@RequestMapping(value = "/rest/v{version:[0-9]+}/priorityq")
@Api(value = "Q Api")
public class RestMusicQAPI {

	private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(RestMusicQAPI.class);
	
	 @Autowired
	 MusicDataAPIService musicDataAPIService;
	 
	 @Autowired
	 RestMusicQAPIService restMusicQAPIService;

	/**
	 * 
	 * @param version
	 * @param minorVersion
	 * @param patchVersion
	 * @param aid
	 * @param ns
	 * @param authorization
	 * @param tableObj
	 * @param keyspace
	 * @param tablename
	 * @return
	 * @throws Exception
	 */
	@PostMapping(value = "/keyspaces/{keyspace}/{qname}", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Create Q", response = String.class)
	public Response createQ(@ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
			@ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
			@ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
			@ApiParam(value = "Application namespace", required = true) @RequestHeader("ns") String ns,
			@ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
			@RequestBody JsonTable tableObj, @ApiParam(value = "Key Space", required = true) @PathVariable("keyspace") String keyspace,
			@ApiParam(value = "Table Name", required = true) @PathVariable("qname") String tablename) throws Exception {

		if (logger.isDebugEnabled()) {
			logger.info(logger, "cjc before start in q 1** major version=" + version);
		}

		return restMusicQAPIService.createQueue(version, minorVersion, patchVersion, tableObj, authorization, aid, ns,
				keyspace, tablename, musicDataAPIService);
	}

	/**
	 * 
	 * @param version
	 * @param minorVersion
	 * @param patchVersion
	 * @param aid
	 * @param ns
	 * @param authorization
	 * @param insObj
	 * @param keyspace
	 * @param tablename
	 * @return
	 * @throws Exception
	 */
	@PostMapping(value = "/keyspaces/{keyspace}/{qname}/rows", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON)
	@ApiOperation(value = "", response = Void.class)
	public Response insertIntoQ(
			@ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
			@ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
			@ApiParam(value = "Application namespace", required = true) @RequestHeader("ns") String ns,
			@ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
			JsonInsert insObj, @ApiParam(value = "Key Space", required = true) @PathVariable("keyspace") String keyspace,
			@ApiParam(value = "Table Name", required = true) @PathVariable("qname") String tablename) throws Exception {

		ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);
		if (insObj.getValues().isEmpty()) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
					ErrorTypes.DATAERROR);
			return response.status(Status.BAD_REQUEST).entity(
					new JsonResponse(ResultType.FAILURE).setError("Required HTTP Request body is missing.").toMap())
					.build();
		}
		
		return musicDataAPIService.insertIntoTable(VERSION, minorVersion, patchVersion, aid, ns, authorization, insObj, keyspace, tablename);
	}

	/**
	 * 
	 * @param version
	 * @param minorVersion
	 * @param patchVersion
	 * @param aid
	 * @param ns
	 * @param authorization
	 * @param updateObj
	 * @param keyspace
	 * @param tablename
	 * @param info
	 * @return
	 * @throws Exception
	 */
	@PutMapping(value = "/keyspaces/{keyspace}/{qname}/rows", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON)
	@ApiOperation(value = "updateQ", response = String.class)
	public Response updateQ(@ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
			@ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
			@ApiParam(value = "Application namespace", required = true) @RequestHeader("ns") String ns,
			@ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
			JsonUpdate updateObj,
			@ApiParam(value = "Key Space", required = true) @PathVariable("keyspace") String keyspace,
			@ApiParam(value = "Table Name", required = true) @PathVariable("qname") String tablename,
			@RequestParam MultiValueMap<String, String> info) throws Exception {

		ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);
		if (updateObj.getValues().isEmpty()) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
					ErrorTypes.DATAERROR);
			return response.status(Status.BAD_REQUEST)
					.entity(new JsonResponse(ResultType.FAILURE).setError(
							"Required HTTP Request body is missing. JsonUpdate updateObj.getValues() is empty. ")
							.toMap())
					.build();
		}

		return musicDataAPIService.updateTable(VERSION, minorVersion, patchVersion, aid, ns, authorization,
				updateObj, keyspace, tablename, info);
	}

	/**
	 * 
	 * @param delObj
	 * @param keyspace
	 * @param tablename
	 * @param info
	 * 
	 * @return
	 * @throws Exception
	 */

	@DeleteMapping(value = "/keyspaces/{keyspace}/{qname}/rows", produces = MediaType.APPLICATION_JSON, consumes = MediaType.APPLICATION_JSON)
	@ApiOperation(value = "deleteQ", response = String.class)
	public Response deleteFromQ(
			@ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
			@ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
			@ApiParam(value = "Application namespace", required = true) @RequestHeader("ns") String ns,
			@ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
			JsonDelete delObj, @ApiParam(value = "Key Space", required = true) @PathVariable("keyspace") String keyspace,
			@ApiParam(value = "Table Name", required = true) @PathVariable("qname") String tablename,
			@RequestParam MultiValueMap<String, String> info) throws Exception {
		
		ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);
		if (delObj == null) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
					ErrorTypes.DATAERROR);
			return response.status(Status.BAD_REQUEST).entity(
					new JsonResponse(ResultType.FAILURE).setError("deleteFromQ JsonDelete delObjis empty").toMap())
					.build();
		}

		return musicDataAPIService.deleteFromTable(VERSION, minorVersion, patchVersion, aid, ns,
				authorization, delObj, keyspace, tablename, info);
	}

	/**
	 * 
	 * @param keyspace
	 * @param tablename
	 * @param info
	 * @return
	 * @throws Exception
	 */
	@GetMapping(value = "/keyspaces/{keyspace}/{qname}/peek", produces = MediaType.APPLICATION_JSON)
	@ApiOperation(value = "", response = Map.class)
	public Response peek(@ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
			@ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
			@ApiParam(value = "Application namespace", required = true) @RequestHeader("ns") String ns,
			@ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
			@ApiParam(value = "Key Space", required = true) @PathVariable("keyspace") String keyspace,
			@ApiParam(value = "Table Name", required = true) @PathVariable("qname") String tablename,
			@RequestParam MultiValueMap<String, String> info) throws Exception {
		
		int limit = 1; // peek must return just the top row
		Map<String, String> auth = new HashMap<>();
		String userId = auth.get(MusicUtil.USERID);
		String password = auth.get(MusicUtil.PASSWORD);
		ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);

		PreparedQueryObject queryObject = new PreparedQueryObject();
		if (info == null)
			queryObject.appendQueryString("SELECT *  FROM " + keyspace + "." + tablename + " LIMIT " + limit + ";");
		else {

			try {
				queryObject = selectSpecificQuery(version, minorVersion, patchVersion, aid, ns,
						userId, password, keyspace, tablename, info, limit);
				
			} catch (MusicServiceException ex) {
				logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.UNKNOWNERROR, ErrorSeverity.WARN,
						ErrorTypes.GENERALSERVICEERROR);
				return response.status(Status.BAD_REQUEST)
						.entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
			}
		}

		try {
			ResultSet results = MusicCore.get(queryObject);
			return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS)
					.setDataResult(MusicDataStoreHandle.marshallResults(results)).toMap()).build();
		} catch (MusicServiceException ex) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.UNKNOWNERROR, ErrorSeverity.ERROR,
					ErrorTypes.MUSICSERVICEERROR);
			return response.status(Status.BAD_REQUEST)
					.entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
		}
	}

	/**
	 * 
	 * @param version
	 * @param minorVersion
	 * @param patchVersion
	 * @param aid
	 * @param ns
	 * @param authorization
	 * @param keyspace
	 * @param tablename
	 * @param info
	 * @return
	 * @throws Exception
	 */
	@GetMapping(value = "/keyspaces/{keyspace}/{qname}/filter", produces = MediaType.APPLICATION_JSON)
	@ApiOperation(value = "filter", response = Map.class)
	public Response filter(@ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
			@ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
			@ApiParam(value = "Application namespace", required = true) @RequestHeader("ns") String ns,
			@ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
			@ApiParam(value = "Key Space", required = true) @PathVariable("keyspace") String keyspace,
			@ApiParam(value = "Table Name", required = true) @PathVariable("qname") String tablename,
			@RequestParam MultiValueMap<String, String> info) throws Exception {
		
		return musicDataAPIService.select(VERSION, minorVersion, patchVersion, aid, ns, authorization, keyspace, tablename, info);

	}

	/**
	 * 
	 * @param tabObj
	 * @param keyspace
	 * @param tablename
	 * @throws Exception
	 */
	@DeleteMapping(value = "/keyspaces/{keyspace}/{qname}", produces = MediaType.APPLICATION_JSON)
	@ApiOperation(value = "DropQ", response = String.class)
	public Response dropQ(@ApiParam(value = "Major Version", required = true) @PathVariable("version") String version,
			@ApiParam(value = "Minor Version", required = false) @RequestHeader(required = false, value = XMINORVERSION) String minorVersion,
            @ApiParam(value = "Patch Version", required = false) @RequestHeader(required = false, value = XPATCHVERSION) String patchVersion,
            @ApiParam(value = "AID", required = true) @RequestHeader("aid") String aid,
			@ApiParam(value = "Application namespace", required = true) @RequestHeader("ns") String ns,
			@ApiParam(value = "Authorization", required = true) @RequestHeader(MusicUtil.AUTHORIZATION) String authorization,
			@ApiParam(value = "Key Space", required = true) @PathVariable("keyspace") String keyspace,
			@ApiParam(value = "Table Name", required = true) @PathVariable("qname") String tablename) throws Exception {

		return musicDataAPIService.dropTable(VERSION, minorVersion, patchVersion, aid, ns, authorization, keyspace,
                tablename);
	}
	
	private static final String XMINORVERSION = "X-minorVersion";
    private static final String XPATCHVERSION = "X-patchVersion";
	private static final String VERSION = "v2";
	
	/**
	 * 
	 * @param keyspace
	 * @param tablename
	 * @param info
	 * @param limit
	 * @return
	 * @throws MusicServiceException
	 */
	public PreparedQueryObject selectSpecificQuery(String version, String minorVersion, String patchVersion, String aid,
			String ns, String userId, String password, String keyspace, String tablename,
			MultiValueMap<String, String> info, int limit) throws MusicServiceException {

		PreparedQueryObject queryObject = new PreparedQueryObject();
		StringBuilder rowIdString = getRowIdentifier(keyspace, tablename, info, queryObject).rowIdString;

		queryObject.appendQueryString("SELECT *  FROM " + keyspace + "." + tablename + " WHERE " + rowIdString);

		if (limit != -1) {
			queryObject.appendQueryString(" LIMIT " + limit);
		}

		queryObject.appendQueryString(";");
		return queryObject;

	}

	
	private class RowIdentifier {
		public String primarKeyValue;
		public StringBuilder rowIdString;
		@SuppressWarnings("unused")
		public PreparedQueryObject queryObject;// the string with all the row
												// identifiers separated by AND

		public RowIdentifier(String primaryKeyValue, StringBuilder rowIdString, PreparedQueryObject queryObject) {
			this.primarKeyValue = primaryKeyValue;
			this.rowIdString = rowIdString;
			this.queryObject = queryObject;
		}
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
	private RowIdentifier getRowIdentifier(String keyspace, String tablename, MultiValueMap<String, String> rowParams,
			PreparedQueryObject queryObject) throws MusicServiceException {
		StringBuilder rowSpec = new StringBuilder();
		int counter = 0;
		TableMetadata tableInfo = MusicDataStoreHandle.returnColumnMetadata(keyspace, tablename);
		if (tableInfo == null) {
			logger.error(EELFLoggerDelegate.errorLogger,
					"Table information not found. Please check input for table name= " + keyspace + "." + tablename);
			throw new MusicServiceException(
					"Table information not found. Please check input for table name= " + keyspace + "." + tablename);
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
				logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
			}
			if (tableInfo.getPrimaryKey().get(0).getName().equals(entry.getKey()))
				primaryKey.append(indValue);
			rowSpec.append(keyName + "= ?");
			queryObject.addValue(formattedValue);
			if (counter != rowParams.size() - 1)
				rowSpec.append(" AND ");
			counter = counter + 1;
		}
		return new RowIdentifier(primaryKey.toString(), rowSpec, queryObject);
	}
}
