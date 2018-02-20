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
package org.onap.music.lockingservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.lockingservice.ZooKeeperOperation;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A base class for protocol implementations which provides a number of higher level helper methods
 * for working with ZooKeeper along with retrying synchronous operations if the connection to
 * ZooKeeper closes such as {@link #retryOperation(ZooKeeperOperation)}
 *
 */
class ProtocolSupport {
    private EELFLoggerDelegate LOG = EELFLoggerDelegate.getLogger(ProtocolSupport.class);

    protected ZooKeeper zookeeper;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private long retryDelay = 500L;
    private int retryCount = 10;
    private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    // public ProtocolSupport(ZooKeeper zookeeper) {
    // this.zookeeper = zookeeper;
    // }

    /**
     * Closes this strategy and releases any ZooKeeper resources; but keeps the ZooKeeper instance
     * open
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            doClose();
        }
    }

    /**
     * return zookeeper client instance
     * 
     * @return zookeeper client instance
     */
    public ZooKeeper getZookeeper() {
        return zookeeper;
    }

    /**
     * return the acl its using
     * 
     * @return the acl.
     */
    public List<ACL> getAcl() {
        return acl;
    }

    /**
     * set the acl
     * 
     * @param acl the acl to set to
     */
    public void setAcl(List<ACL> acl) {
        this.acl = acl;
    }

    /**
     * get the retry delay in milliseconds
     * 
     * @return the retry delay
     */
    public long getRetryDelay() {
        return retryDelay;
    }

    /**
     * Sets the time waited between retry delays
     * 
     * @param retryDelay the retry delay
     */
    public void setRetryDelay(long retryDelay) {
        this.retryDelay = retryDelay;
    }

    /**
     * Allow derived classes to perform some custom closing operations to release resources
     */
    protected void doClose() {}


    /**
     * Perform the given operation, retrying if the connection fails
     * 
     * @return object. it needs to be cast to the callee's expected return type.
     * @param operation FILL IN
     * @throws KeeperException FILL IN
     * @throws InterruptedException FILL IN
     */
    protected Object retryOperation(ZooKeeperOperation operation)
                    throws KeeperException, InterruptedException {
        KeeperException exception = null;
        for (int i = 0; i < retryCount; i++) {
            try {
                return operation.execute();
            } catch (KeeperException.SessionExpiredException e) {
                LOG.debug("Session expired for: " + zookeeper + " so reconnecting due to: " + e, e);
                throw e;
            } catch (KeeperException.ConnectionLossException e) {
                if (exception == null) {
                    exception = e;
                }
                LOG.debug("Attempt " + i + " failed with connection loss so "
                                + "attempting to reconnect: " + e, e);
                retryDelay(i);
            }
        }
        throw exception;
    }

    /**
     * Ensures that the given path exists with no data, the current ACL and no flags
     * 
     * @param path the lock path
     */
    protected void ensurePathExists(String path) {
        ensureExists(path, null, acl, CreateMode.PERSISTENT);
    }

    /**
     * Ensures that the given path exists with the given data, ACL and flags
     * 
     * @param path the lock path
     * @param data the data
     * @param acl list of ACLs applying to the path
     * @param flags create mode flags
     */
    protected void ensureExists(final String path, final byte[] data, final List<ACL> acl,
                    final CreateMode flags) {
        try {
            retryOperation(new ZooKeeperOperation() {
                public boolean execute() throws KeeperException, InterruptedException {
                    Stat stat = zookeeper.exists(path, false);
                    if (stat != null) {
                        return true;
                    }
                    zookeeper.create(path, data, acl, flags);
                    return true;
                }
            });
        } catch (KeeperException e) {
            LOG.error(EELFLoggerDelegate.errorLogger,"Caught: " + e, e);
        } catch (InterruptedException e) {
            LOG.error(EELFLoggerDelegate.errorLogger,"Caught: " + e, e);
        }
    }

    /**
     * Returns true if this protocol has been closed
     * 
     * @return true if this protocol is closed
     */
    protected boolean isClosed() {
        return closed.get();
    }

    /**
     * Performs a retry delay if this is not the first attempt
     * 
     * @param attemptCount the number of the attempts performed so far
     */
    protected void retryDelay(int attemptCount) {
        if (attemptCount > 0) {
            try {
                Thread.sleep(attemptCount * retryDelay);
            } catch (InterruptedException e) {
                LOG.error(EELFLoggerDelegate.errorLogger,"Failed to sleep: " + e, e);
            }
        }
    }
}
