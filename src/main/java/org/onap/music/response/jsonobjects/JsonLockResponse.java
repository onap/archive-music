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
public class JsonLockResponse {

    private String status = "";
    private String error = "";
    private String message = "";
    private String lock = "";
    private String lockStatus = "";
    private String lockHolder = "";
    private String lockLease = "";

    /**
     * 
     * @param status
     * @param error
     * @param lock
     */
    public JsonLockResponse(String status, String error, String lock) {
        this.status = fixStatus(status);
        this.error = error;
        this.lock = lock;
    }

    /**
     * 
     * @param status
     * @param error
     * @param lock
     * @param lockStatus
     * @param lockHolder
     */
    public JsonLockResponse(String status, String error, String lock, String lockStatus,
                    String lockHolder) {
        this.status = fixStatus(status);
        this.error = error;
        this.lock = lock;
        this.lockStatus = lockStatus;
        this.lockHolder = lockHolder;
    }

    /**
     * 
     * @param status
     * @param error
     * @param lock
     * @param lockStatus
     * @param lockHolder
     * @param lockLease
     */
    public JsonLockResponse(String status, String error, String lock, String lockStatus,
                    String lockHolder, String lockLease) {
        this.status = fixStatus(status);
        this.error = error;
        this.lock = lock;
        this.lockStatus = lockStatus;
        this.lockHolder = lockHolder;
    }


    /**
     * Lock
     * 
     * @return
     */
    public String getLock() {
        return lock;
    }

    /**
     * 
     * @param lock
     */
    public void setLock(String lock) {
        this.lock = lock;
    }

    /**
     * 
     */
    public JsonLockResponse() {
        this.status = "";
        this.error = "";
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

    /**
     * 
     * @return
     */
    @ApiModelProperty(value = "Overall status of the response.",
                    allowableValues = "Success,Failure")
    public String getStatus() {
        return status;
    }

    /**
     * 
     * @param status
     */
    public void setStatus(String status) {
        this.status = fixStatus(status);
    }

    /**
     * 
     * @return the error
     */
    @ApiModelProperty(value = "Error value")
    public String getError() {
        return error;
    }

    /**
     * 
     * @param error
     */
    public void setError(String error) {
        this.error = error;
    }

    /**
     * 
     * @return the message
     */
    @ApiModelProperty(value = "Message if any need to be conveyed about the lock")
    public String getMessage() {
        return message;
    }

    /**
     * 
     * @param message
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * 
     * @return the lockStatus
     */
    @ApiModelProperty(value = "Status of the lock",
                    allowableValues = "UNLOCKED,BEING_LOCKED,LOCKED")
    public String getLockStatus() {
        return lockStatus;
    }

    /**
     * 
     * @param lockStatus
     */
    public void setLockStatus(String lockStatus) {
        this.lockStatus = lockStatus;
    }

    /**
     * 
     * 
     * @return the lockHolder
     */
    @ApiModelProperty(value = "Holder of the Lock")
    public String getLockHolder() {
        return lockHolder;
    }

    /**
     * 
     * @param lockHolder
     */
    public void setLockHolder(String lockHolder) {
        this.lockHolder = lockHolder;
    }



    /**
     * @return the lockLease
     */
    public String getLockLease() {
        return lockLease;
    }

    /**
     * @param lockLease the lockLease to set
     */
    public void setLockLease(String lockLease) {
        this.lockLease = lockLease;
    }

    /**
     * Convert to Map
     * 
     * @return
     */
    public Map<String, Object> toMap() {
        Map<String, Object> newMap = new HashMap<>();
        Map<String, Object> lockMap = new HashMap<>();
        lockMap.put("lock-status", lockStatus);
        lockMap.put("lock", lock);
        lockMap.put("message", message);
        lockMap.put("lock-holder", lockHolder);
        lockMap.put("lock-lease", lockLease);
        newMap.put("status", status);
        newMap.put("error", error);
        newMap.put("lock", lockMap);
        return newMap;
    }

    /**
     * Convert to String
     */
    @Override
    public String toString() {
        return "JsonLockResponse [status=" + status + ", error=" + error + ", message=" + message
                        + ", lock=" + lock + ", lockStatus=" + lockStatus + ", lockHolder="
                        + lockHolder + "]";
    }

}
