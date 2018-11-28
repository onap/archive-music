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
package org.onap.music.datastore;

import java.util.Map;

import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.main.MusicCore;
import org.onap.music.service.MusicCoreService;
import org.onap.music.service.impl.MusicCassaCore;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;



    public class Condition {
        Map<String, Object> conditions;
        PreparedQueryObject selectQueryForTheRow;
        private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(Condition.class);
       //private static MusicCoreService musicCore = MusicCassaCore.getInstance();

        public Condition(Map<String, Object> conditions, PreparedQueryObject selectQueryForTheRow) {
            this.conditions = conditions;
            this.selectQueryForTheRow = selectQueryForTheRow;
        }

        public boolean testCondition() throws Exception {
            // first generate the row
            ResultSet results = MusicCore.quorumGet(selectQueryForTheRow);
            Row row = results.one();
            return MusicDataStoreHandle.getDSHandle().doesRowSatisfyCondition(row, conditions);
        }
    }

