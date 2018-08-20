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

package org.onap.music.datastore.jsonobjects;

import java.util.Map;

public class JSONCallbackResponse {

	private String full_table;
	private String keyspace;
	private Map<String, String> changeValue;
	private String operation;
	private String table_name;
	private String primary_key;
	private Object miscObjects;
	public String getFull_table() {
		return full_table;
	}
	public void setFull_table(String full_table) {
		this.full_table = full_table;
	}
	public String getKeyspace() {
		return keyspace;
	}
	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	public String getTable_name() {
		return table_name;
	}
	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}
	public String getPrimary_key() {
		return primary_key;
	}
	public void setPrimary_key(String primary_key) {
		this.primary_key = primary_key;
	}
	public Object getMiscObjects() {
		return miscObjects;
	}
	public void setMiscObjects(Object miscObjects) {
		this.miscObjects = miscObjects;
	}
	public void setChangeValue(Map<String, String> changeValue) {
		this.changeValue = changeValue;
	}
	public Map<String, String> getChangeValue() {
		return changeValue;
	}
	
	
}
