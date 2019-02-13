/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2019 Samsung
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

package org.onap.music.main;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicServiceException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

@Component
public class CronJobManager {

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(CronJobManager.class);

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");


    @Scheduled(cron = "0 0 0 * * ?")
    public void scheduleTaskWithFixedRate() {
        logger.info("Executing cronjob to cleanup locks..", dateTimeFormatter.format(LocalDateTime.now()) );
        deleteLocksFromDB();
    }

    public void deleteLocksFromDB() {
        PreparedQueryObject pQuery = new PreparedQueryObject();
        String consistency = MusicUtil.EVENTUAL;
        pQuery.appendQueryString("CREATE TABLE IF NOT EXISTS admin.locks ( lock_id text PRIMARY KEY, ctime text)");
        try {
            ResultType result = MusicCore.nonKeyRelatedPut(pQuery, consistency);
            if ( result.equals(ResultType.FAILURE)) {
                logger.error(EELFLoggerDelegate.errorLogger,"Error creating Admin.locks table.",AppMessages.QUERYERROR,ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
            }
        } catch (MusicServiceException e1) {
            logger.error(EELFLoggerDelegate.errorLogger,e1,AppMessages.QUERYERROR,ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
        }

        pQuery = new PreparedQueryObject();
        pQuery.appendQueryString(
                        "select * from admin.locks");
            try {
                ResultSet rs = MusicCore.get(pQuery);
                Iterator<Row> it = rs.iterator();
                StringBuilder deleteKeys = new StringBuilder();
                Boolean expiredKeys = false;
                while (it.hasNext()) {
                    Row row = it.next();
                    String id = row.getString("lock_id");
                    long ctime = Long.parseLong(row.getString("ctime"));
                    if(System.currentTimeMillis() >= ctime + 24 * 60 * 60 * 1000) {
                        expiredKeys = true;
                        String new_id = id.substring(1);
                        try {
                            MusicCore.deleteLock(new_id);
                        } catch (MusicLockingException e) {
                            logger.info(EELFLoggerDelegate.applicationLogger,
                                     e.getMessage());
                        }
                        deleteKeys.append("'").append(id).append("'").append(",");
                    }
                }
                if(expiredKeys) {
                    deleteKeys.deleteCharAt(deleteKeys.length()-1);
                    CachingUtil.deleteKeysFromDB(deleteKeys.toString());
               }
            } catch (MusicServiceException e) {
                logger.error(EELFLoggerDelegate.errorLogger,e,AppMessages.CACHEERROR,ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            }
    }
}
