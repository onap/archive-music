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
package org.onap.music.unittests;

import org.apache.log4j.Logger;
import org.onap.music.lockingservice.MusicLockingService;

public class TestLockStore {
    final static Logger logger = Logger.getLogger(TestLockStore.class);

    public static void main(String[] args) throws Exception {
        String lockName = "/achristmkllas";
        MusicLockingService ml = new MusicLockingService();
        ml.deleteLock(lockName);


        logger.info("lockname:" + lockName);

        String lockId1 = ml.createLockId(lockName);
        logger.info("lockId1 " + lockId1);
        logger.info(ml.isMyTurn(lockId1));

        String lockId2 = ml.createLockId(lockName);
        logger.info("lockId2 " + lockId2);
        logger.info("check " + ml.isMyTurn("$bank$x-94608776321630264-0000000000"));
        logger.info(ml.isMyTurn(lockId2));

        // zkClient.unlock(lockId1);
        // logger.info(ml.lock(lockId2));
        // zkClient.unlock(lockId2);
    }


}
