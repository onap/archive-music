/*-
 * ============LICENSE_START=======================================================
 *  Copyright (C) 2019 Samsung Electronics Co., Ltd. All rights reserved.
 * ================================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * ============LICENSE_END=========================================================
 */
package org.onap.music.conductor.conditionals;

import java.util.Map;
import org.onap.music.datastore.PreparedQueryObject;

public class UpdateDataObject {

    Map<String, PreparedQueryObject> queryBank; 
    String keyspace;
    String tableName;
    String primaryKey;
    String primaryKeyValue;
    String planId;
    String cascadeColumnName;
    Map<String, String> cascadeColumnValues;
    String lockId;

    public Map<String, PreparedQueryObject> getQueryBank() {
        return queryBank;
    }

    public UpdateDataObject setQueryBank(Map<String, PreparedQueryObject> queryBank) {
        this.queryBank = queryBank;
        return this;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public UpdateDataObject setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public UpdateDataObject setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public UpdateDataObject setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public String getPrimaryKeyValue() {
        return primaryKeyValue;
    }

    public UpdateDataObject setPrimaryKeyValue(String primaryKeyValue) {
        this.primaryKeyValue = primaryKeyValue;
        return this;
    }

    public String getPlanId() {
        return planId;
    }

    public UpdateDataObject setPlanId(String planId) {
        this.planId = planId;
        return this;
    }

    public String getCascadeColumnName() {
        return cascadeColumnName;
    }

    public UpdateDataObject setCascadeColumnName(String cascadeColumnName) {
        this.cascadeColumnName = cascadeColumnName;
        return this;
    }

    public Map<String, String> getCascadeColumnValues() {
        return cascadeColumnValues;
    }

    public UpdateDataObject setLockId(String lockId) {
        this.lockId=lockId;
        return this;
    }
    
    public String getLockId() {
        return lockId;
    }
    
    public UpdateDataObject setCascadeColumnValues(Map<String, String> cascadeColumnValues) {
        this.cascadeColumnValues = cascadeColumnValues;
        return this;
    }


}
