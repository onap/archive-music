/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
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

package org.onap.music.exceptions;

/**
 * @author inam
 *
 */
public class MusicServiceException extends Exception {


    private int errorCode;
    private String errorMessage;

    public MusicServiceException() {
        super();
    }


    public MusicServiceException(String message) {
        super(message);

    }
  
    public MusicServiceException(String message, int errorCode) {
        super(message);
        this.errorCode=errorCode;
    }
  
    public MusicServiceException(String message, int errorCode, String errorMessage) {
        super(message);
        this.errorCode=errorCode;
        this.errorMessage=errorMessage;
    }
  
    public MusicServiceException(Throwable cause) {
        super(cause);

    }


    public MusicServiceException(String message, Throwable cause) {
        super(message, cause);

    }


    public MusicServiceException(String message, Throwable cause, boolean enableSuppression,
                                 boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);

    }
    public void setErrorCode(int errorCode) {
        this.errorCode=errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }
    public void setErrorMessage(String errorMessage) {
        this.errorMessage=errorMessage;
    }
    public String getErrorMessage() {
        return errorMessage;
    }
}
