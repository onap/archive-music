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

import org.onap.music.lockingservice.MusicLockState.LockStatus;
import org.onap.music.main.ResultType;
import org.powermock.core.spi.testresult.Result;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "JsonResponse", description = "General Response JSON")
public class JsonLockResponse {

    private ResultType status;
    private String error;
    private String message;
    private String lock;
    private LockStatus lockStatus;
    private String lockHolder;
    private String lockLease;


    /**
     * Create a JSONLock Response
     * Use setters to provide more information as in
     * JsonLockResponse(ResultType.SUCCESS).setMessage("We did it").setLock(mylockname)
     * @param status
     */
    public JsonLockResponse(ResultType status) {
        this.status = status;
    }
    


	/**
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
    public JsonLockResponse setLock(String lock) {
        this.lock = lock;
        return this;
    }

    /**
     * 
     * @return
     */
    @ApiModelProperty(value = "Overall status of the response.",
                    allowableValues = "Success,Failure")
    public ResultType getStatus() {
        return status;
    }

    /**
     * 
     * @param status
     */
    public JsonLockResponse setStatus(ResultType status) {
        this.status = status;
        return this;
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
    public JsonLockResponse setError(String error) {
        this.error = error;
        return this;
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
    public JsonLockResponse setMessage(String message) {
        this.message = message;
        return this;
    }

    /**
     * 
     * @return the lockStatus
     */
    @ApiModelProperty(value = "Status of the lock")
    public LockStatus getLockStatus() {
        return lockStatus;
    }

    /**
     * 
     * @param lockStatus
     */
    public JsonLockResponse setLockStatus(LockStatus lockStatus) {
        this.lockStatus = lockStatus;
        return this;
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
    public JsonLockResponse setLockHolder(String lockHolder) {
        this.lockHolder = lockHolder;
        return this;
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
    public JsonLockResponse setLockLease(String lockLease) {
        this.lockLease = lockLease;
        return this;
    }

    /**
     * Convert to Map
     * 
     * @return
     */
    public Map<String, Object> toMap() {
        Map<String, Object> fullMap = new HashMap<>();
        Map<String, Object> lockMap = new HashMap<>();
        if (lockStatus!=null) {lockMap.put("lock-status", lockStatus); }
        if (lock!=null) {lockMap.put("lock", lock);}
        if (message!=null) {lockMap.put("message", message);}
        if (lockHolder!=null) {lockMap.put("lock-holder", lockHolder);}
        if (lockLease!=null) {lockMap.put("lock-lease", lockLease);}
        
        fullMap.put("status", status);
        fullMap.put("lock", lockMap);
        if (error!=null) {fullMap.put("error", error);}
        return fullMap;
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
