package org.onap.music.rest.repository.impl;

import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.response.jsonobjects.JsonResponse;
import org.onap.music.rest.repository.RestMusicQAPIRepository;
import org.onap.music.rest.service.MusicDataAPIService;
import org.springframework.stereotype.Repository;

@Repository
public class RestMusicQAPIRepositoryImpl implements RestMusicQAPIRepository{
	
	private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(RestMusicQAPIRepositoryImpl.class);

	@Override
	public Response createQueue(String version, String minorVersion, String patchVersion, 
			JsonTable tableObj, String authorization, String aid, String ns, String keyspace, String tablename,MusicDataAPIService musicDataAPIService) {
		
		ResponseBuilder response = MusicUtil.buildVersionResponse(version, minorVersion, patchVersion);

		Map<String, String> fields = tableObj.getFields();
		if (fields == null) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
					ErrorTypes.DATAERROR);
			return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
					.setError("CreateQ/Required table fields are empty or not set").toMap()).build();
		}

		String primaryKey = tableObj.getPrimaryKey();
		String partitionKey = tableObj.getPartitionKey();
		String clusteringKey = tableObj.getClusteringKey();
		String filteringKey = tableObj.getFilteringKey();
		String clusteringOrder = tableObj.getClusteringOrder();

		if (primaryKey == null) {
			primaryKey = tableObj.getFields().get("PRIMARY KEY");
		}

		if ((primaryKey == null) && (partitionKey == null)) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
					ErrorTypes.DATAERROR);
			return response.status(Status.BAD_REQUEST).entity(
					new JsonResponse(ResultType.FAILURE).setError("CreateQ: Partition key cannot be empty").toMap())
					.build();
		}

		if ((primaryKey == null) && (clusteringKey == null)) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
					ErrorTypes.DATAERROR);
			return response.status(Status.BAD_REQUEST).entity(
					new JsonResponse(ResultType.FAILURE).setError("CreateQ: Clustering key cannot be empty").toMap())
					.build();
		}

		if (clusteringOrder == null) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
					ErrorTypes.DATAERROR);
			return response.status(Status.BAD_REQUEST).entity(
					new JsonResponse(ResultType.FAILURE).setError("CreateQ: Clustering Order cannot be empty").toMap())
					.build();
		}

		if ((primaryKey != null) && (partitionKey == null)) {
			primaryKey.trim();
			int count1 = StringUtils.countMatches(primaryKey, ')');
			int count2 = StringUtils.countMatches(primaryKey, '(');
			if (count1 != count2) {
				return response.status(Status.BAD_REQUEST)
						.entity(new JsonResponse(ResultType.FAILURE).setError(
								"CreateQ Error: primary key '(' and ')' do not match, primary key=" + primaryKey)
								.toMap())
						.build();
			}

			if (primaryKey.indexOf('(') == -1
					|| (count2 == 1 && (primaryKey.lastIndexOf(")") + 1) == primaryKey.length())) {
				if (primaryKey.contains(",")) {
					partitionKey = primaryKey.substring(0, primaryKey.indexOf(","));
					partitionKey = partitionKey.replaceAll("[\\(]+", "");
					clusteringKey = primaryKey.substring(primaryKey.indexOf(',') + 1); // make sure index
					clusteringKey = clusteringKey.replaceAll("[)]+", "");
				} else {
					partitionKey = primaryKey;
					partitionKey = partitionKey.replaceAll("[\\)]+", "");
					partitionKey = partitionKey.replaceAll("[\\(]+", "");
					clusteringKey = "";
				}
			} else {
				partitionKey = primaryKey.substring(0, primaryKey.indexOf(')'));
				partitionKey = partitionKey.replaceAll("[\\(]+", "");
				partitionKey.trim();
				clusteringKey = primaryKey.substring(primaryKey.indexOf(')'));
				clusteringKey = clusteringKey.replaceAll("[\\(]+", "");
				clusteringKey = clusteringKey.replaceAll("[\\)]+", "");
				clusteringKey.trim();
				if (clusteringKey.indexOf(",") == 0)
					clusteringKey = clusteringKey.substring(1);
				clusteringKey.trim();
				if (clusteringKey.equals(","))
					clusteringKey = ""; // print error if needed ( ... ),)
			}
		}

		if (partitionKey.trim().isEmpty()) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
					ErrorTypes.DATAERROR);
			return response.status(Status.BAD_REQUEST).entity(
					new JsonResponse(ResultType.FAILURE).setError("CreateQ: Partition key cannot be empty").toMap())
					.build();
		}

		if (clusteringKey.trim().isEmpty()) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
					ErrorTypes.DATAERROR);
			return response.status(Status.BAD_REQUEST).entity(
					new JsonResponse(ResultType.FAILURE).setError("CreateQ: Clustering key cannot be empty").toMap())
					.build();
		}

		if ((filteringKey != null) && (filteringKey.equalsIgnoreCase(partitionKey))) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA, ErrorSeverity.CRITICAL,
					ErrorTypes.DATAERROR);
			return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
					.setError("CreateQ: Filtering key cannot be same as Partition Key").toMap()).build();
		}
		
		return musicDataAPIService.createTable(version, minorVersion, patchVersion, authorization, aid, ns, tableObj, keyspace, tablename);
	}

}
