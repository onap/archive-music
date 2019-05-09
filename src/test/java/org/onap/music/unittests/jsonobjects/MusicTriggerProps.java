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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.util.Properties;

import javax.imageio.stream.FileImageInputStream;

public class MusicTriggerProps  {

    private static final Logger logger = LoggerFactory.getLogger(MusicTriggerProps.class);
    private static String fileDir = "/music/app/trigger";
    private static String propertiesFile = "musictrigger.properties";

    public static Properties getProperties() {
        File pf = new File(fileDir,propertiesFile);

        Properties prop = new Properties();
        InputStream input = null;
        try {
            // load the properties file
            logger.info("Properties File: " + fileDir + "/" + propertiesFile);
            input = new FileInputStream(pf);
            if ( input != null ) {
                prop.load(input);
            } else {
                logger.info("Properties null");
            }
        } catch ( IOException ex) {
            logger.info("IO Error while loading properties file. Error: "+ex.getMessage());
        } catch ( Exception ex) {
            logger.info("LocalMessage: " + ex.getLocalizedMessage());
            logger.info("Error while loading properties file. Error: "+ex.getMessage());
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (Exception e) {
                    logger.info("Error: "+e.getMessage());
                }
            } 
        }
        return prop;
    }
}