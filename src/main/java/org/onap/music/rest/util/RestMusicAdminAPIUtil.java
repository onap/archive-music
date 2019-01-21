/*
 * ============LICENSE_START==========================================
 *  org.onap.music
 * ===================================================================
 *  Copyright (c) 2019 IBM.
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

package org.onap.music.rest.util;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.main.MusicUtil;

import com.datastax.driver.core.DataType;

public class RestMusicAdminAPIUtil {
    public static Response sendUnauthorisedResponseForEmptyParams(EELFLoggerDelegate logger, String message)

    {
        Map<String, Object> resultMap = new HashMap<>();

        logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
                ErrorTypes.AUTHENTICATIONERROR);
        resultMap.put("Exception", message);
        return Response.status(Status.UNAUTHORIZED).entity(resultMap).build();

    }

    public static PreparedQueryObject getQueryString(String appName, String uuid, String isAAF) throws Exception {
        PreparedQueryObject pQuery = new PreparedQueryObject();
        String cql = "select uuid, keyspace_name from admin.keyspace_master where ";
        if (appName != null)
            cql = cql + "application_name = ? AND ";
        if (uuid != null)
            cql = cql + "uuid = ? AND ";
        if (isAAF != null)
            cql = cql + "is_aaf = ?";

        if (cql.endsWith("AND "))
            cql = cql.trim().substring(0, cql.length() - 4);
        System.out.println("Query is: " + cql);
        cql = cql + " allow filtering";
        System.out.println("Get OnboardingInfo CQL: " + cql);
        pQuery.appendQueryString(cql);
        if (appName != null)
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        if (uuid != null)
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
        if (isAAF != null)
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), Boolean.parseBoolean(isAAF)));
        return pQuery;

    }
}
