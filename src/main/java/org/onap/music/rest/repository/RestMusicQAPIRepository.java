/*
 * ============LICENSE_START==========================================
 * org.onap.music
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

import javax.ws.rs.core.Response;

import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.rest.service.MusicDataAPIService;

public interface RestMusicQAPIRepository {

    /**
     * Create Queue
     * 
     * @param version
     * @param minorVersion
     * @param patchVersion
     * @param tableObj
     * @param authorization
     * @param aid
     * @param ns
     * @param keyspace
     * @param tablename
     * @param musicDataAPIService
     * @return
     */
    public Response createQueue(String version, String minorVersion, String patchVersion, JsonTable tableObj,
            String authorization, String aid, String ns, String keyspace, String tablename,
            MusicDataAPIService musicDataAPIService);
}
