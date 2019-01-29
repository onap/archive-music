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

package org.onap.music.lockingservice.cassandra;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;

// the state variable that will be stored in zookeeper, capturing the transitions of
public class MusicLockState implements Serializable {
    public enum LockStatus {
        UNLOCKED, BEING_LOCKED, LOCKED
    };// captures the state of the lock

    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicLockState.class);
    LockStatus lockStatus;
    boolean needToSyncQuorum = false;
    String lockHolder;
    long leasePeriod = Long.MAX_VALUE, leaseStartTime = -1;
    
    private String errorMessage = null;
    
    public MusicLockState(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public MusicLockState(LockStatus lockStatus, String lockHolder) {
        this.lockStatus = lockStatus;
        this.lockHolder = lockHolder;
    }

    public MusicLockState(LockStatus lockStatus, String lockHolder, boolean needToSyncQuorum) {
        this.lockStatus = lockStatus;
        this.lockHolder = lockHolder;
        this.needToSyncQuorum = needToSyncQuorum;
    }


    public long getLeasePeriod() {
        return leasePeriod;
    }

    public boolean isNeedToSyncQuorum() {
        return needToSyncQuorum;
    }



    public void setLeasePeriod(long leasePeriod) {
        this.leasePeriod = leasePeriod;
    }


    public long getLeaseStartTime() {
        return leaseStartTime;
    }


    public void setLeaseStartTime(long leaseStartTime) {
        this.leaseStartTime = leaseStartTime;
    }



    public LockStatus getLockStatus() {
        return lockStatus;
    }

    public void setLockStatus(LockStatus lockStatus) {
        this.lockStatus = lockStatus;
    }

    public String getLockHolder() {
        return lockHolder;
    }

    public void setLockHolder(String lockHolder) {
        this.lockHolder = lockHolder;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
    
    public byte[] serialize() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
        } catch (IOException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.IOERROR, ErrorSeverity.ERROR, ErrorTypes.CONNECTIONERROR);
        }
        return bos.toByteArray();
    }

    public static MusicLockState deSerialize(byte[] data) {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        Object o = null;
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            o = in.readObject();
        } catch (ClassNotFoundException | IOException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.ERROR, ErrorTypes.UNKNOWN);
        }
        return (MusicLockState) o;
    }
}
