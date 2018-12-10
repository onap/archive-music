/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 *  Modifications Copyright (C) 2018 IBM.
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

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;

import io.swagger.annotations.ApiModel;

@ApiModel(value = "JsonNotification", description = "Json model for callback")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL) 
public class JsonNotification implements Serializable {
	
	private String notify_field;
    private String endpoint;
    private String username;
    private String password;
    private String notify_change;
    private String notify_insert;
    private String notify_delete;
    private String operation_type;
    private String triggerName;
    private Map<String, String> response_body;
	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(JsonNotification.class);
    
	public String getNotify_field() {
		return notify_field;
	}
	public void setNotify_field(String notify_field) {
		this.notify_field = notify_field;
	}
	public String getEndpoint() {
		return endpoint;
	}
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
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
	public Map<String, String> getResponse_body() {
		return response_body;
	}
	public void setResponse_body(Map<String, String> response_body) {
		this.response_body = response_body;
	}
	public String getNotify_change() {
		return notify_change;
	}
	public void setNotify_change(String notify_change) {
		this.notify_change = notify_change;
	}
	public String getNotify_insert() {
		return notify_insert;
	}
	public void setNotify_insert(String notify_insert) {
		this.notify_insert = notify_insert;
	}
	public String getNotify_delete() {
		return notify_delete;
	}
	public void setNotify_delete(String notify_delete) {
		this.notify_delete = notify_delete;
	}
	public String getOperation_type() {
		return operation_type;
	}
	public void setOperation_type(String operation_type) {
		this.operation_type = operation_type;
	}
	public String getTriggerName() {
		return triggerName;
	}
	public void setTriggerName(String triggerName) {
		this.triggerName = triggerName;
	}
	@Override
	public String toString() {
		try {
	        return new com.fasterxml.jackson.databind.ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
	    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
			logger.error(EELFLoggerDelegate.errorLogger, ex,AppMessages.EXECUTIONINTERRUPTED, ErrorSeverity.ERROR, ErrorTypes.GENERALSERVICEERROR);
			return notify_field+ " : "+endpoint+ " : "+username+ " : "+password+ " : "+response_body;
	    }

	}
	
}
