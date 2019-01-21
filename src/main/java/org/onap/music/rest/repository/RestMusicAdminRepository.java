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

package org.onap.music.rest.repository;

import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.ResultType;

import com.datastax.driver.core.ResultSet;

public interface RestMusicAdminRepository {

	ResultSet getUuidFromKeySpaceMasterUsingAppName(String appName) throws Exception;

	String insertValuesIntoKeySpaceMaster(String uuid, String appName, String userId, String isAAF, String password)
			throws Exception;

	ResultSet fetchOnboardedInfoSearch(String appName, String uuid, String isAAF) throws Exception;

	ResultSet getKeySpaceNameFromKeySpaceMasterWithUuid(String aid) throws Exception;

	void dropKeySpace(String ks, String consistency) throws MusicServiceException;

	ResultType deleteFromKeySpaceMasterWithUuid(String aid, String consistency) throws Exception;

	ResultSet getKeySpaceNameFromKeySpaceMasterWithAppName(String appName) throws Exception;

	ResultType updateKeySpaceMaster(String appName, String userId, String password, String isAAF, String aid,
			String consistency) throws Exception;
}
