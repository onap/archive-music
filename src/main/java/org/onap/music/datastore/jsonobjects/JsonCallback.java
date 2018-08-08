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

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "JsonCallback", description = "Json model for callback")
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonCallback implements Serializable {
	private String applicationName;
    private String applicationUsername;
    private String applicationPassword;
    private String applicationNotificationEndpoint;
    private String notifyOn;
    private String notifyWhenChangeIn;
    private String notifyWhenInsertsIn;
    private String notifyWhenDeletesIn;

    @ApiModelProperty(value = "application name")
    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }
    
    @ApiModelProperty(value = "notify On")
    public String getNotifyOn() {
        return notifyOn;
    }

    public void setNotifyOn(String notifyOn) {
        this.notifyOn = notifyOn;
    }
    
    @ApiModelProperty(value = "application User name")
    public String getApplicationUsername() {
        return applicationUsername;
    }

    public void setApplicationUsername(String applicationUsername) {
        this.applicationUsername = applicationUsername;
    }

    @ApiModelProperty(value = "application password")
    public String getApplicationPassword() {
        return applicationPassword;
    }

    public void setApplicationPassword(String applicationPassword) {
        this.applicationPassword = applicationPassword;
    }

    @ApiModelProperty(value = "application notification endpoint")
    public String getApplicationNotificationEndpoint() {
        return applicationNotificationEndpoint;
    }

    public void setApplicationNotificationEndpoint(String applicationNotificationEndpoint) {
        this.applicationNotificationEndpoint = applicationNotificationEndpoint;
    }

    @ApiModelProperty(value = "notify when updates")
    public String getNotifyWhenChangeIn() {
        return notifyWhenChangeIn;
    }

    public void setNotifyWhenChangeIn(String notifyWhenChangeIn) {
        this.notifyWhenChangeIn = notifyWhenChangeIn;
    }

    @ApiModelProperty(value = "notify when inserts")
    public String getNotifyWhenInsertsIn() {
        return notifyWhenInsertsIn;
    }

    public void setNotifyWhenInsertsIn(String notifyWhenInsertsIn) {
        this.notifyWhenInsertsIn = notifyWhenInsertsIn;
    }

    @ApiModelProperty(value = "notify when deletes")
    public String getNotifyWhenDeletesIn() {
        return notifyWhenDeletesIn;
    }

    public void setNotifyWhenDeletesIn(String notifyWhenDeletesIn) {
        this.notifyWhenDeletesIn = notifyWhenDeletesIn;
    }

}
