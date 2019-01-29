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

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author srupane
 *
 */
public class PreparedQueryObject {


    private List<Object> values;
    private StringBuilder query;
    private String consistency;
    private String keyspaceName;
    private String tableName;
    private String operation;
    private String primaryKeyValue;



    public String getKeyspaceName() {
        return keyspaceName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getPrimaryKeyValue() {
        return primaryKeyValue;
    }

    public void setPrimaryKeyValue(String primaryKeyValue) {
        this.primaryKeyValue = primaryKeyValue;
    }

    public String getConsistency() {
        return consistency;
    }

    public void setConsistency(String consistency) {
        this.consistency = consistency;
    }

    /**
     * 
     */
    public PreparedQueryObject() {

        this.values = new ArrayList<>();
        this.query = new StringBuilder();
    }

    /**
     * @return
     */
    public List<Object> getValues() {
        return values;
    }

    /**
     * @param o
     */
    public void addValue(Object o) {
        this.values.add(o);
    }

    /**
     * @param s
     */
    public void appendQueryString(String s) {
        this.query.append(s);
    }
    public void replaceQueryString(String s) {
        this.query.replace(0, query.length(), s);
    }

    /**
     * @return
     */
    public String getQuery() {
        return this.query.toString();
    }



}
