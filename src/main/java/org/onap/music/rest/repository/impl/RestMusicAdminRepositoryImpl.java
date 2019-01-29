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

package org.onap.music.rest.repository.impl;

import java.util.UUID;

import org.mindrot.jbcrypt.BCrypt;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.rest.repository.RestMusicAdminRepository;
import org.onap.music.rest.util.RestMusicAdminAPIUtil;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;

@Repository
public class RestMusicAdminRepositoryImpl implements RestMusicAdminRepository {

    private PreparedQueryObject pQuery;

    @Override
    public ResultSet getUuidFromKeySpaceMasterUsingAppName(String appName) throws Exception {
        pQuery = new PreparedQueryObject();
        pQuery.appendQueryString("select uuid from admin.keyspace_master where application_name = ? allow filtering");
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        ResultSet rs = MusicCore.get(pQuery);
        return rs;
    }

    @Override
    public String insertValuesIntoKeySpaceMaster(String uuid, String appName, String userId, String isAAF,
            String password) throws Exception {
        pQuery = new PreparedQueryObject();
        pQuery.appendQueryString("INSERT INTO admin.keyspace_master (uuid, keyspace_name, application_name, is_api, "
                + "password, username, is_aaf) VALUES (?,?,?,?,?,?,?)");
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), MusicUtil.DEFAULTKEYSPACENAME));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), "True"));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), BCrypt.hashpw(password, BCrypt.gensalt())));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));

        String returnStr = MusicCore.eventualPut(pQuery).toString();
        return returnStr;
    }

    @Override
    public ResultSet fetchOnboardedInfoSearch(String appName, String uuid, String isAAF) throws Exception {
        pQuery = new PreparedQueryObject();
        pQuery = RestMusicAdminAPIUtil.getQueryString(appName, uuid, isAAF);
        ResultSet rs = MusicCore.get(pQuery);
        return rs;
    }

    @Override
    public ResultSet getKeySpaceNameFromKeySpaceMasterWithUuid(String aid) throws Exception {
        pQuery = new PreparedQueryObject();
        pQuery.appendQueryString("SELECT keyspace_name FROM admin.keyspace_master WHERE uuid = ?");
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), UUID.fromString(aid)));
        ResultSet rs = MusicCore.get(pQuery);
        return rs;
    }

    @Override
    public void dropKeySpace(String ks, String consistency) throws MusicServiceException {
        pQuery = new PreparedQueryObject();
        pQuery.appendQueryString("DROP KEYSPACE IF EXISTS " + ks + ";");
        MusicCore.nonKeyRelatedPut(pQuery, consistency);

    }

    @Override
    public ResultType deleteFromKeySpaceMasterWithUuid(String aid, String consistency) throws Exception {
        pQuery = new PreparedQueryObject();
        pQuery.appendQueryString("delete from admin.keyspace_master where uuid = ? IF EXISTS");
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), UUID.fromString(aid)));

        ResultType rt = MusicCore.nonKeyRelatedPut(pQuery, consistency);
        return rt;
    }

    @Override
    public ResultSet getKeySpaceNameFromKeySpaceMasterWithAppName(String appName) throws Exception {
        pQuery = new PreparedQueryObject();
        pQuery.appendQueryString("select uuid from admin.keyspace_master where application_name = ? allow filtering");
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        ResultSet rs = MusicCore.get(pQuery);
        return rs;
    }

    @Override
    public ResultType updateKeySpaceMaster(String appName, String userId, String password, String isAAF, String aid,
            String consistency) throws Exception {
        pQuery = new PreparedQueryObject();
        StringBuilder preCql = new StringBuilder("UPDATE admin.keyspace_master SET ");
        if (appName != null)
            preCql.append(" application_name = ?,");
        if (userId != null)
            preCql.append(" username = ?,");
        if (password != null)
            preCql.append(" password = ?,");
        if (isAAF != null)
            preCql.append(" is_aaf = ?,");
        preCql.deleteCharAt(preCql.length() - 1);
        preCql.append(" WHERE uuid = ? IF EXISTS");
        pQuery.appendQueryString(preCql.toString());
        if (appName != null)
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        if (userId != null)
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
        if (password != null)
            pQuery.addValue(
                    MusicUtil.convertToActualDataType(DataType.text(), BCrypt.hashpw(password, BCrypt.gensalt())));
        if (isAAF != null)
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));

        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), UUID.fromString(aid)));
        ResultType rs = MusicCore.nonKeyRelatedPut(pQuery, consistency);
        return rs;
    }

}
