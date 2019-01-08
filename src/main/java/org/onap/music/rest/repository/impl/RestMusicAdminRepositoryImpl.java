/*
 * ============LICENSE_START==========================================
 *  org.onap.music
 * ===================================================================
 *  Copyright (c) 2018 IBM.
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

import org.mindrot.jbcrypt.BCrypt;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.rest.repository.RestMusicAdminRepository;
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
    public String insertValuesIntoKeySpaceMaster(String uuid, String appName, String userId, String isAAF, String password) throws Exception {
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

}
