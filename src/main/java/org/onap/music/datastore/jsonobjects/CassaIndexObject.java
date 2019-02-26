/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2019 IBM
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
package org.onap.music.datastore.jsonobjects;

import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;

public class CassaIndexObject {

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(CassaIndexObject.class);

    private String indexName;
    private String keyspaceName;
    private String tableName;
    private String fieldName;
    
    public String getIndexName() {
        return indexName;
    }

    public CassaIndexObject setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public String getKeyspaceName() {
        return keyspaceName;
    }

    public CassaIndexObject setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public CassaIndexObject setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getFieldName() {
        return fieldName;
    }

    public CassaIndexObject setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public PreparedQueryObject genCreateIndexQuery() {

        if (logger.isDebugEnabled()) {
            logger.debug("Came inside genCreateIndexQuery method");
        }

        logger.info("genCreateIndexQuery indexName ::" + indexName);
        logger.info("genCreateIndexQuery keyspaceName ::" + keyspaceName);
        logger.info("genCreateIndexQuery tableName ::" + tableName);
        logger.info("genCreateIndexQuery fieldName ::" + fieldName);

        PreparedQueryObject queryObject = new PreparedQueryObject();

        long start = System.currentTimeMillis();

        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString("Create index if not exists " + this.getIndexName() + " on " + this.getKeyspaceName() + "."
                        + this.getTableName() + " (" + this.getFieldName() + ");");

        long end = System.currentTimeMillis();

        logger.info(EELFLoggerDelegate.applicationLogger,
                "Time taken for setting up query in create index:" + (end - start));
        
        logger.info(EELFLoggerDelegate.applicationLogger,
                " create index query :" + query.getQuery());

        return queryObject;
    }

}
