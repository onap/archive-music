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

package org.onap.music.rest;

public class Application {

    private String application_name;
    private String username;
    private String password;
    private String keyspace_name;
    private boolean is_aaf;
    private String uuid;
    private boolean is_api;
    
    public String getApplication_name() {
        return application_name;
    }
    public void setApplication_name(String application_name) {
        this.application_name = application_name;
    }
    public String getUsername() {
        return username;
    }
    public void setUsername(String username) {
        this.username = username;
    }
    public String getPassword() {
        return password;
    }
    public void setPassword(String password) {
        this.password = password;
    }
    public String getKeyspace_name() {
        return keyspace_name;
    }
    public void setKeyspace_name(String keyspace_name) {
        this.keyspace_name = keyspace_name;
    }
    public boolean isIs_aaf() {
        return is_aaf;
    }
    public void setIs_aaf(boolean is_aaf) {
        this.is_aaf = is_aaf;
    }
    public String getUuid() {
        return uuid;
    }
    public void setUuid(String uuid) {
        this.uuid = uuid;
    }
    public boolean getIs_api() {
        return is_api;
    }
    public void setIs_api(boolean is_api) {
        this.is_api = is_api;
    }
    
    
}
