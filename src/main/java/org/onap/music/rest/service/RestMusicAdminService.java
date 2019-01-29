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

package org.onap.music.rest.service;

import javax.ws.rs.core.Response;

import org.onap.music.datastore.jsonobjects.JsonOnboard;

public interface RestMusicAdminService {
	Response onboardAppWithMusic(JsonOnboard jsonObj) throws Exception;

	Response getOnboardedInfoSearch(JsonOnboard jsonObj) throws Exception;

	Response deleteOnboardApp(JsonOnboard jsonObj) throws Exception;

	Response updateOnboardApp(JsonOnboard jsonObj) throws Exception;
}
