/*
 * ============LICENSE_START========================================== org.onap.music
 * =================================================================== Copyright (c) 2017 AT&T
 * Intellectual Property ===================================================================
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 * 
 * ============LICENSE_END=============================================
 * ====================================================================
 */
package org.onap.music.lockingservice;


import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;

/**
 * A <a href="package.html">protocol to implement an exclusive write lock or to elect a leader</a>.
 * <p/>
 * You invoke {@link #lock()} to start the process of grabbing the lock; you may get the lock then
 * or it may be some time later.
 * <p/>
 * You can register a listener so that you are invoked when you get the lock; otherwise you can ask
 * if you have the lock by calling {@link #isOwner()}
 *
 */
public class ZkStatelessLockService extends ProtocolSupport {
    public ZkStatelessLockService(ZooKeeper zk) {
        zookeeper = zk;
    }

    private static EELFLoggerDelegate logger =
                    EELFLoggerDelegate.getLogger(ZkStatelessLockService.class);

    protected void createLock(final String path, final byte[] data) {
        final List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
        try {
            retryOperation(new ZooKeeperOperation() {
                public boolean execute() throws KeeperException, InterruptedException {
                    zookeeper.create(path, data, acl, CreateMode.PERSISTENT);
                    return true;
                }
            });
        }catch (InterruptedException e) {
        	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.EXECUTIONINTERRUPTED, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
        }catch (KeeperException e) {
        	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.KEEPERERROR, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
        }
    }

    public void close() {
        try {
            zookeeper.close();
        }catch (InterruptedException e) {
        	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.EXECUTIONINTERRUPTED, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
        }
    }

    public void setNodeData(final String lockName, final byte[] data) {
        try {
            retryOperation(new ZooKeeperOperation() {
                public boolean execute() throws KeeperException, InterruptedException {
                    zookeeper.getSessionId();
                    zookeeper.setData("/" + lockName, data, -1);
                    return true;
                }
            });
        }catch (InterruptedException e) {
        	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.EXECUTIONINTERRUPTED, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
        }catch (KeeperException e) {
        	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.KEEPERERROR, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
        }

    }

    public byte[] getNodeData(final String lockName) {
        try {
            if (zookeeper.exists("/" + lockName, null) != null)
                return zookeeper.getData("/" + lockName, false, null);
            else
                return null;

        }catch (InterruptedException e) {
        	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.EXECUTIONINTERRUPTED, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
        }catch (KeeperException e) {
        	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.KEEPERERROR, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
        }
        return null;
    }

    public boolean checkIfLockExists(String lockName) {
        boolean result = false;
        try {
            Stat stat = zookeeper.exists(lockName, false);
            if (stat != null) {
                result = true;
            }
        }catch (InterruptedException e) {
        	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.EXECUTIONINTERRUPTED, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
        }catch (KeeperException e) {
        	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.KEEPERERROR, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
        }
        return result;
    }

    public void createNode(String nodeName) {
        ensurePathExists(nodeName);
    }

    public String createLockId(String dir) {
        ensurePathExists(dir);
        LockZooKeeperOperation zop = new LockZooKeeperOperation(dir);
        try {
            retryOperation(zop);
        }catch (InterruptedException e) {
        	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.EXECUTIONINTERRUPTED, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
        }catch (KeeperException e) {
        	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.KEEPERERROR, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
        }
        return zop.getId();
    }

    /**
     * Attempts to acquire the exclusive write lock returning whether or not it was acquired. Note
     * that the exclusive lock may be acquired some time later after this method has been invoked
     * due to the current lock owner going away.
     */
    public synchronized boolean lock(String dir, String lockId)
                    throws KeeperException, InterruptedException {
        if (isClosed()) {
            return false;
        }
        LockZooKeeperOperation zop = new LockZooKeeperOperation(dir, lockId);
        return (Boolean) retryOperation(zop);
    }

    /**
     * Removes the lock or associated znode if you no longer require the lock. this also removes
     * your request in the queue for locking in case you do not already hold the lock.
     * 
     * @throws RuntimeException throws a runtime exception if it cannot connect to zookeeper.
     * @throws NoNodeException 
     */
    public synchronized void unlock(String lockId) throws RuntimeException, KeeperException.NoNodeException {
        final String id = lockId;
        if (!isClosed() && id != null) {
            try {
                ZooKeeperOperation zopdel = new ZooKeeperOperation() {
                    public boolean execute() throws KeeperException, InterruptedException {
                        zookeeper.delete(id, -1);
                        return Boolean.TRUE;
                    }
                };
                zopdel.execute();
            } catch (InterruptedException e) {
            	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.EXECUTIONINTERRUPTED, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
                // set that we have been interrupted.
                Thread.currentThread().interrupt();
            } catch (KeeperException.NoNodeException e) {
                // do nothing
            	throw new KeeperException.NoNodeException("Lock doesn't exists. Release lock operation failed.");
            } catch (KeeperException e) {
            	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.KEEPERERROR, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
                throw (RuntimeException) new RuntimeException(e.getMessage()).initCause(e);
            }
        }
    }

    public synchronized String currentLockHolder(String mainLock) {
        final String id = mainLock;
        if (!isClosed() && id != null) {
            List<String> names;
            try {
                names = zookeeper.getChildren(id, false);
                if (names.isEmpty())
                    return "";
                SortedSet<ZNodeName> sortedNames = new TreeSet<ZNodeName>();
                for (String name : names) {
                    sortedNames.add(new ZNodeName(id + "/" + name));
                }
                return sortedNames.first().getName();
            } catch (InterruptedException e) {
            	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.EXECUTIONINTERRUPTED, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
                // set that we have been interrupted.
                Thread.currentThread().interrupt();
            } catch (KeeperException.NoNodeException e) {
                // do nothing
            } catch (KeeperException e) {
            	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.KEEPERERROR, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
                throw (RuntimeException) new RuntimeException(e.getMessage()).initCause(e);
            }
        }
        return "No lock holder!";
    }

    public synchronized void deleteLock(String mainLock) {
        final String id = mainLock;
        if (!isClosed() && id != null) {
            try {
                ZooKeeperOperation zopdel = new ZooKeeperOperation() {
                    public boolean execute() throws KeeperException, InterruptedException {
                        List<String> names = zookeeper.getChildren(id, false);
                        for (String name : names) {
                            zookeeper.delete(id + "/" + name, -1);
                        }
                        zookeeper.delete(id, -1);
                        return Boolean.TRUE;
                    }
                };
                zopdel.execute();
            } catch (InterruptedException e) {
            	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.EXECUTIONINTERRUPTED, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
                // set that we have been interrupted.
                Thread.currentThread().interrupt();
            } catch (KeeperException.NoNodeException e) {
            	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.KEEPERERROR, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
                // do nothing
            } catch (KeeperException e) {
            	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.KEEPERERROR, ErrorSeverity.ERROR, ErrorTypes.LOCKINGERROR);
                throw (RuntimeException) new RuntimeException(e.getMessage()).initCause(e);
            }
        }

    }

    /**
     * a zoookeeper operation that is mainly responsible for all the magic required for locking.
     */
    private class LockZooKeeperOperation implements ZooKeeperOperation {

        /**
         * find if we have been created earlier if not create our node
         * 
         * @param prefix the prefix node
         * @param zookeeper the zookeeper client
         * @param dir the dir parent
         * @throws KeeperException
         * @throws InterruptedException
         */
        private String dir;
        private String id = null;

        public String getId() {
            return id;
        }

        public LockZooKeeperOperation(String dir) {
            this.dir = dir;
        }

        public LockZooKeeperOperation(String dir, String id) {
            this.dir = dir;
            this.id = id;
        }

        /**
         * the command that is run and retried for actually obtaining the lock
         * 
         * @return if the command was successful or not
         */
        public boolean execute() throws KeeperException, InterruptedException {
            do {
                if (id == null) {
                    String prefix = "x-";
                    byte[] data = {0x12, 0x34};
                    id = zookeeper.create(dir + "/" + prefix, data, getAcl(),
                                    CreateMode.PERSISTENT_SEQUENTIAL);

                    if (logger.isDebugEnabled()) {
                    	logger.debug(EELFLoggerDelegate.debugLogger, "Created id: " + id);
                    }
                    if (id != null)
                        break;
                }
                if (id != null) {
                    List<String> names = zookeeper.getChildren(dir, false);
                    if (names.isEmpty()) {
                        logger.info(EELFLoggerDelegate.applicationLogger, "No children in: " + dir
                                        + " when we've just " + "created one! Lets recreate it...");
                        // lets force the recreation of the id
                        id = null;
                        return Boolean.FALSE;

                    } else {
                        // lets sort them explicitly (though they do seem to come back in order
                        // ususally :)
                        ZNodeName idName = new ZNodeName(id);
                        SortedSet<ZNodeName> sortedNames = new TreeSet<ZNodeName>();
                        for (String name : names) {
                            sortedNames.add(new ZNodeName(dir + "/" + name));
                        }
                        if (!sortedNames.contains(idName))
                            return Boolean.FALSE;

                        SortedSet<ZNodeName> lessThanMe = sortedNames.headSet(idName);
                        if (!lessThanMe.isEmpty()) {
                            ZNodeName lastChildName = lessThanMe.last();
                            String lastChildId = lastChildName.getName();
                            if (logger.isDebugEnabled()) {
                            	logger.debug(EELFLoggerDelegate.debugLogger, "watching less than me node: " + lastChildId);
                            }
                            Stat stat = zookeeper.exists(lastChildId, false);
                            if (stat != null) {
                                return Boolean.FALSE;
                            } else {
                                logger.info(EELFLoggerDelegate.applicationLogger,
                                                "Could not find the" + " stats for less than me: "
                                                                + lastChildName.getName());
                            }
                        } else
                            return Boolean.TRUE;
                    }
                }
            } while (id == null);
            return Boolean.FALSE;
        }
    }

}

