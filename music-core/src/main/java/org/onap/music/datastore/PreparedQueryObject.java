/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017-2019 AT&T Intellectual Property
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


    /**
     * Create PreparedQueryObject
     */
    public PreparedQueryObject() {
        this.values = new ArrayList<>();
        this.query = new StringBuilder();
    }
    
    /**
     * Create PreparedQueryObject
     * @param query query portion of the prepared query
     */
    public PreparedQueryObject(String query) {
        this.values = new ArrayList<>();
        this.query = new StringBuilder(query);
    }
    
    /**
     * Create PreparedQueryObject
     * @param query query portion of the prepared query
     * @param values to be added to the query string as prepared query
     */
    public PreparedQueryObject(String query, Object...values) {
        this.query = new StringBuilder(query);
        this.values = new ArrayList<>();
        for (Object value: values) {
            this.values.add(value);
        }
    }

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
        if (operation!=null)
        	return operation;
        if (query.length()==0)
        	return null;
        String queryStr = query.toString().toLowerCase();
        String firstOp = null;
        int firstOpChar = query.length();
        if (queryStr.indexOf("insert")>-1 && queryStr.indexOf("insert")<firstOpChar) {
            firstOp = "insert";
            firstOpChar = queryStr.indexOf("insert");
        }
        if (queryStr.indexOf("update")>-1 && queryStr.indexOf("update")<firstOpChar) {
            firstOp = "update";
            firstOpChar = queryStr.indexOf("update");
        }
        if (queryStr.indexOf("delete")>-1 && queryStr.indexOf("delete")<firstOpChar) {
            firstOp = "delete";
            firstOpChar = queryStr.indexOf("delete");
        }
        if (queryStr.indexOf("select")>-1 && queryStr.indexOf("select")<firstOpChar) {
            firstOp = "select";
            firstOpChar = queryStr.indexOf("select");
        }
        return firstOp;
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
     * @return values to be set as part of the prepared query
     */
    public List<Object> getValues() {
        return values;
    }

    /**
     * @param o object to be added as a value to the prepared query, in order
     */
    public void addValue(Object o) {
        this.values.add(o);
    }
    
    /**
     * Add values to the preparedQuery
     * @param objs ordered list of objects to be added as values to the prepared query
     */
    public void addValues(Object... objs) {
        for (Object obj: objs) {
            this.values.add(obj);
        }
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
     * @return the query
     */
    public String getQuery() {
        return this.query.toString();
    }
}
