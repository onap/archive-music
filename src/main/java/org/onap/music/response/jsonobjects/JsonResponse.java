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
package org.onap.music.response.jsonobjects;


import java.util.HashMap;
import java.util.Map;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "JsonResponse", description = "General Response JSON")
public class JsonResponse {

    private Boolean status = false;
    private String error = "";
    private String version = "";

    public JsonResponse(Boolean status, String error, String version) {
        this.status = status;
        this.error = error;
        this.version = version;
    }

    public JsonResponse() {
        this.status = false;
        this.error = "";
        this.version = "";
    }

    @ApiModelProperty(value = "Status value")
    public Boolean getStatus() {
        return status;
    }

    /**
     * 
     * @param statusIn
     * @return
     */
    private String fixStatus(String statusIn) {
        if (statusIn.equalsIgnoreCase("false")) {
            return "FAILURE";
        }
        return "SUCCESS";
    }

    public void setStatus(Boolean status) {
        this.status = status;
    }

    @ApiModelProperty(value = "Error value")
    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    @ApiModelProperty(value = "Version value")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> newMap = new HashMap<>();
        newMap.put("status", fixStatus(String.valueOf(status)));
        newMap.put("error", error);
        newMap.put("version", version);
        return newMap;
    }
}
