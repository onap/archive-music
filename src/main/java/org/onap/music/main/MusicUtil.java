/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2018 IBM.
 *  Modifications Copyright (c) 2019 Samsung.
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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.service.MusicCoreService;
import org.onap.music.service.impl.MusicCassaCore;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.sun.jersey.core.util.Base64;

/**
 * @author nelson24
 *
 *         Properties This will take Properties and load them into MusicUtil.
 *         This is a hack for now. Eventually it would bebest to do this in
 *         another way.
 *
 */
public class MusicUtil {
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicUtil.class);

    // Consistancy Constants
    public static final String ATOMIC = "atomic";
    public static final String EVENTUAL = "eventual";
    public static final String CRITICAL = "critical";
    public static final String EVENTUAL_NB = "eventual_nb";
    public static final String ALL = "all";
    public static final String QUORUM = "quorum";
    public static final String ONE = "one";
    public static final String ATOMICDELETELOCK = "atomic_delete_lock";

    // Header Constants
    private static final String XLATESTVERSION = "X-latestVersion";
    private static final String XMINORVERSION = "X-minorVersion";
    private static final String XPATCHVERSION = "X-patchVersion";
    public static final String AUTHORIZATION = "Authorization";

    // CQL Constants
    public static final String SELECT = "select";
    public static final String INSERT = "insert";
    public static final String UPDATE = "update";
    public static final String UPSERT = "upsert";
    public static final String USERID = "userId";
    public static final String PASSWORD = "";
    public static final String CASSANDRA = "cassandra";

    private static final String LOCALHOST = "localhost";
    private static final String PROPERTIES_FILE = "/opt/app/music/etc/music.properties";
    public static final String DEFAULTKEYSPACENAME = "TBD";

    private static String myCassaHost = LOCALHOST;
    private static String defaultMusicIp = LOCALHOST;
    private static int cassandraPort = 9042;
    private static int notifytimeout = 30000;
    private static int notifyinterval = 5000;
    private static long defaultLockLeasePeriod = 6000;
    private static int retryCount = 3;
    private static int cacheObjectMaxLife = -1;
    private static String lockUsing = MusicUtil.CASSANDRA;
    private static boolean isCadi = false;
    private static boolean isKeyspaceActive = false;
    private static boolean debug = true;
    private static String version = "0.0.0";
    private static String build = "";

    private static String musicPropertiesFilePath = PROPERTIES_FILE;
    private static final String[] propKeys = new String[] { "cassandra.host", "music.ip", "debug",
        "version", "music.rest.ip", "music.properties", "lock.lease.period", "id", "all.ids", 
        "public.ip","all.pubic.ips", "cassandra.user", "cassandra.password", "aaf.endpoint.url",
        "admin.username","admin.password","aaf.admin.url","music.namespace","admin.aaf.role",
        "cassandra.port","lock.using","retry.count","transId.header.required",
        "conversation.header.required","clientId.header.required","messageId.header.required",
        "transId.header.prefix","conversation.header.prefix","clientId.header.prefix",
        "messageId.header.prefix"};
    private static final String[] cosistencyLevel = new String[] {
        "ALL","EACH_QUORUM","QUORUM","LOCAL_QUORUM","ONE","TWO",
        "THREE","LOCAL_ONE","ANY","SERIAL","LOCAL_SERIAL"};
    private static final Map<String,ConsistencyLevel> consistencyName = new HashMap<>();
    static {
        consistencyName.put("ONE",ConsistencyLevel.ONE);
        consistencyName.put("TWO",ConsistencyLevel.TWO);
        consistencyName.put("THREE",ConsistencyLevel.THREE);
        consistencyName.put("SERIAL",ConsistencyLevel.SERIAL);
        consistencyName.put("ALL",ConsistencyLevel.ALL);
        consistencyName.put("EACH_QUORUM",ConsistencyLevel.EACH_QUORUM);
        consistencyName.put("QUORUM",ConsistencyLevel.QUORUM);
        consistencyName.put("LOCAL_QUORUM",ConsistencyLevel.LOCAL_QUORUM);
        consistencyName.put("LOCAL_ONE",ConsistencyLevel.LOCAL_ONE);
        consistencyName.put("LOCAL_SERIAL",ConsistencyLevel.LOCAL_SERIAL);
    }
    private static String cassName = "cassandra";
    private static String cassPwd;
    private static String aafEndpointUrl = null;
    private static String adminId = "username";
    private static String adminPass= "password";
    private static String aafAdminUrl= null;
    private static String musicNamespace= "org.onap.music.api";
    private static String adminAafRole= "org.onap.music.api.admin_api";

    public static final long MusicEternityEpochMillis = 1533081600000L; // Wednesday, August 1, 2018 12:00:00 AM
    public static final long MaxLockReferenceTimePart = 1000000000000L; // millis after eternity (eq sometime in 2050)
    public static final long MaxCriticalSectionDurationMillis = 1L * 24 * 60 * 60 * 1000; // 1 day

    private static String transIdPrefix= "false";
	private static String conversationIdPrefix= "false";
    private static String clientIdPrefix= "false";
    private static String messageIdPrefix= "false";
	private static String transIdRequired= "false";
	private static String conversationIdRequired= "false";
    private static String clientIdRequired= "false";
    private static String messageIdRequired= "false";

    public static String getLockUsing() {
        return lockUsing;
    }


    public static void setLockUsing(String lockUsing) {
        MusicUtil.lockUsing = lockUsing;
    }
    
    public static String getAafAdminUrl() {
        return aafAdminUrl;
    }


    public static void setAafAdminUrl(String aafAdminUrl) {
        MusicUtil.aafAdminUrl = aafAdminUrl;
    }


    public static String getMusicNamespace() {
        return musicNamespace;
    }


    public static void setMusicNamespace(String musicNamespace) {
        MusicUtil.musicNamespace = musicNamespace;
    }


    public static String getAdminAafRole() {
        return adminAafRole;
    }


    public static void setAdminAafRole(String adminAafRole) {
        MusicUtil.adminAafRole = adminAafRole;
    }



    public static String getAdminId() {
        return adminId;
    }


    public static void setAdminId(String adminId) {
        MusicUtil.adminId = adminId;
    }


    public static String getAdminPass() {
        return adminPass;
    }

    public static void setAdminPass(String adminPass) {
        MusicUtil.adminPass = adminPass;
    }

    private MusicUtil() {
        throw new IllegalStateException("Utility Class");
    }
    /**
     *
     * @return cassandra port
     */
    public static int getCassandraPort() {
        return cassandraPort;
    }

    /**
     * set cassandra port
     * @param cassandraPort
     */
    public static void setCassandraPort(int cassandraPort) {
        MusicUtil.cassandraPort = cassandraPort;
    }
    /**
     * @return the cassName
     */
    public static String getCassName() {
        return cassName;
    }

    /**
     * @return the cassPwd
     */
    public static String getCassPwd() {
        return cassPwd;
    }

    /**
     * @return the aafEndpointUrl
     */
    public static String getAafEndpointUrl() {
        return aafEndpointUrl;
    }

    /**
     *
     * @param aafEndpointUrl
     */
    public static void setAafEndpointUrl(String aafEndpointUrl) {
        MusicUtil.aafEndpointUrl = aafEndpointUrl;
    }


    /**
     * Returns An array of property names that should be in the Properties
     * files.
     *
     * @return
     */
    public static String[] getPropkeys() {
        return propKeys.clone();
    }

    /**
     * Get MusicPropertiesFilePath - Default = /opt/music/music.properties
     * property file value - music.properties
     *
     * @return
     */
    public static String getMusicPropertiesFilePath() {
        return musicPropertiesFilePath;
    }

    /**
     * Set MusicPropertiesFilePath
     *
     * @param musicPropertiesFilePath
     */
    public static void setMusicPropertiesFilePath(String musicPropertiesFilePath) {
        MusicUtil.musicPropertiesFilePath = musicPropertiesFilePath;
    }

    /**
     * Get DefaultLockLeasePeriod - Default = 6000 property file value -
     * lock.lease.period
     *
     * @return
     */
    public static long getDefaultLockLeasePeriod() {
        return defaultLockLeasePeriod;
    }

    /**
     * Set DefaultLockLeasePeriod
     *
     * @param defaultLockLeasePeriod
     */
    public static void setDefaultLockLeasePeriod(long defaultLockLeasePeriod) {
        MusicUtil.defaultLockLeasePeriod = defaultLockLeasePeriod;
    }

    /**
     * Set Debug
     *
     * @param debug
     */
    public static void setDebug(boolean debug) {
        MusicUtil.debug = debug;
    }

    /**
     * Is Debug - Default = true property file value - debug
     *
     * @return
     */
    public static boolean isDebug() {
        return debug;
    }

    /**
     * Set Version
     *
     * @param version
     */
    public static void setVersion(String version) {
        MusicUtil.version = version;
    }

    /**
     * Return the version property file value - version.
     *
     * @return
     */
    public static String getVersion() {
        return version;
    }

    /**
     * Set the build of project which is a combination of the 
     * version and the date.
     * 
     * @param build - version-date.
     */
    public static void setBuild(String build) {
        MusicUtil.build = build;
    }

    /**
     * Return the build version-date.
     */
    public static String getBuild() {
        return build;
    }

    /**
     * Get MyCassHost - Cassandra Hostname - Default = localhost property file
     * value - cassandra.host
     *
     * @return
     */
    public static String getMyCassaHost() {
        return myCassaHost;
    }

    /**
     * Set MyCassHost - Cassandra Hostname
     *
     * @param myCassaHost .
     */
    public static void setMyCassaHost(String myCassaHost) {
        MusicUtil.myCassaHost = myCassaHost;
    }

    /**
     * Get DefaultMusicIp - Default = localhost property file value - music.ip
     *
     * @return
     */
    public static String getDefaultMusicIp() {
        return defaultMusicIp;
    }

    /**
     * Set DefaultMusicIp
     *
     * @param defaultMusicIp .
     */
    public static void setDefaultMusicIp(String defaultMusicIp) {
        MusicUtil.defaultMusicIp = defaultMusicIp;
    }
    
    /**
     * Gey default retry count
     * @return
     */
    public static int getRetryCount() {
        return retryCount;
    }

    /**
     * Set retry count
     * @param retryCount .
     */
    public static void setRetryCount(int retryCount) {
        MusicUtil.retryCount = retryCount;
    }


    /**
     * This is used to turn keyspace creation api on/off.
     * 
     */
    public static void setKeyspaceActive(Boolean keyspaceActive) {
        MusicUtil.isKeyspaceActive = keyspaceActive;
    }

    /**
     * This is used to turn keyspace creation api on/off.
     * @return boolean isKeyspaceActive
     */
    public static boolean isKeyspaceActive() {
        return isKeyspaceActive;
    }

    /**
     * .
     * @return String
     */
    public static String getTestType() {
        String testType = "";
        try {
            Scanner fileScanner = new Scanner(new File(""));
            testType = fileScanner.next();// ignore the my id line
            @SuppressWarnings("unused")
            String batchSize = fileScanner.next();// ignore the my public ip line
            fileScanner.close();
        } catch (FileNotFoundException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), e);
        }
        return testType;

    }

    /**
     *
     * @param time
     */
    public static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Utility function to check if the query object is valid.
     *
     * @param withparams
     * @param queryObject
     * @return
     */
    public static boolean isValidQueryObject(boolean withparams, PreparedQueryObject queryObject) {
        if (withparams) {
            int noOfValues = queryObject.getValues().size();
            int noOfParams = 0;
            char[] temp = queryObject.getQuery().toCharArray();
            for (int i = 0; i < temp.length; i++) {
                if (temp[i] == '?')
                    noOfParams++;
            }
            return (noOfValues == noOfParams);
        } else {
            return !queryObject.getQuery().isEmpty();
        }

    }

    public static void setCassName(String cassName) {
        MusicUtil.cassName = cassName;
    }

    public static void setCassPwd(String cassPwd) {
        MusicUtil.cassPwd = cassPwd;
    }

    @SuppressWarnings("unchecked")
    public static String convertToCQLDataType(DataType type, Object valueObj) throws Exception {

        String value = "";
        switch (type.getName()) {
        case UUID:
            value = valueObj + "";
            break;
        case TEXT:
        case VARCHAR:
            String valueString = valueObj + "";
            valueString = valueString.replace("'", "''");
            value = "'" + valueString + "'";
            break;
        case MAP: {
            Map<String, Object> otMap = (Map<String, Object>) valueObj;
            value = "{" + jsonMaptoSqlString(otMap, ",") + "}";
            break;
        }
        default:
            value = valueObj + "";
            break;
        }
        return value;
    }

    /**
     *
     * @param colType
     * @param valueObj
     * @return
     * @throws MusicTypeConversionException
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static Object convertToActualDataType(DataType colType, Object valueObj) throws Exception {
        String valueObjString = valueObj + "";
        switch (colType.getName()) {
            case UUID:
                return UUID.fromString(valueObjString);
            case VARINT:
                return BigInteger.valueOf(Long.parseLong(valueObjString));
            case BIGINT:
                return Long.parseLong(valueObjString);
            case INT:
                return Integer.parseInt(valueObjString);
            case FLOAT:
                return Float.parseFloat(valueObjString);
            case DOUBLE:
                return Double.parseDouble(valueObjString);
            case BOOLEAN:
                return Boolean.parseBoolean(valueObjString);
            case MAP:
                return (Map<String, Object>) valueObj;
            case LIST:
                return (List<Object>)valueObj;
            case BLOB:

            default:
                return valueObjString;
        }
    }

    public static ByteBuffer convertToActualDataType(DataType colType, byte[] valueObj) {
        ByteBuffer buffer = ByteBuffer.wrap(valueObj);
        return buffer;
    }

    /**
     *
     * Utility function to parse json map into sql like string
     *
     * @param jMap
     * @param lineDelimiter
     * @return
     */

    public static String jsonMaptoSqlString(Map<String, Object> jMap, String lineDelimiter) throws Exception{
        StringBuilder sqlString = new StringBuilder();
        int counter = 0;
        for (Map.Entry<String, Object> entry : jMap.entrySet()) {
            Object ot = entry.getValue();
            String value = ot + "";
            if (ot instanceof String) {
                value = "'" + value.replace("'", "''") + "'";
            }
            sqlString.append("'" + entry.getKey() + "':" + value);
            if (counter != jMap.size() - 1)
                sqlString.append(lineDelimiter);
            counter = counter + 1;
        }
        return sqlString.toString();
    }

    @SuppressWarnings("unused")
    public static String buildVersion(String major, String minor, String patch) {
        if (minor != null) {
            major += "." + minor;
            if (patch != null) {
                major += "." + patch;
            }
        }
        return major;
    }

    /**
     * Currently this will build a header with X-latestVersion, X-minorVersion and X-pathcVersion
     * X-latestVerstion will be equal to the latest full version.
     * X-minorVersion - will be equal to the latest minor version.
     * X-pathVersion - will be equal to the latest patch version.
     * Future plans will change this.
     * @param response
     * @param major
     * @param minor
     * @param patch
     * @return
     */
    public static ResponseBuilder buildVersionResponse(String major, String minor, String patch) {
        ResponseBuilder response = Response.noContent();
        String versionIn = buildVersion(major,minor,patch);
        String version = MusicUtil.getVersion();
        String[] verArray = version.split("\\.",3);
        if ( minor != null ) {
            response.header(XMINORVERSION,minor);
        } else {
            response.header(XMINORVERSION,verArray[1]);
        }
        if ( patch != null ) {
            response.header(XPATCHVERSION,patch);
        } else {
            response.header(XPATCHVERSION,verArray[2]);
        }
        response.header(XLATESTVERSION,version);
        logger.info(EELFLoggerDelegate.applicationLogger,"Version In:" + versionIn);
        return response;
    }

    public static boolean isValidConsistency(String consistency) {
        for (String string : cosistencyLevel) {
            if (string.equalsIgnoreCase(consistency))
                return true;
        }
        return false;

    }

    public static ConsistencyLevel getConsistencyLevel(String consistency) {
        return consistencyName.get(consistency.toUpperCase());
    }

    public static void setNotifyInterval(int notifyinterval) {
        MusicUtil.notifyinterval = notifyinterval;
    }
    public static void setNotifyTimeOut(int notifytimeout) {
        MusicUtil.notifytimeout = notifytimeout;
    }

    public static int getNotifyInterval() {
        return MusicUtil.notifyinterval;
    }

    public static int getNotifyTimeout() {
        return MusicUtil.notifytimeout;
    }

    public static int getCacheObjectMaxLife() {
        return MusicUtil.cacheObjectMaxLife;
    }

    public static void setCacheObjectMaxLife(int cacheObjectMaxLife) {
        MusicUtil.cacheObjectMaxLife = cacheObjectMaxLife;
    }
    
    /**
     * Given the time of write for an update in a critical section, this method provides a transformed timestamp
     * that ensures that a previous lock holder who is still alive can never corrupt a later critical section.
     * The main idea is to us the lock reference to clearly demarcate the timestamps across critical sections.
     * @param the UUID lock reference associated with the write.
     * @param the long timeOfWrite which is the actual time at which the write took place
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public static long v2sTimeStampInMicroseconds(long ordinal, long timeOfWrite) throws MusicServiceException, MusicQueryException {
        // TODO: use acquire time instead of music eternity epoch
        long ts = ordinal * MaxLockReferenceTimePart + (timeOfWrite - MusicEternityEpochMillis);

        return ts;
    }
    
    public static MusicCoreService  getMusicCoreService() {
        if(getLockUsing().equals(MusicUtil.CASSANDRA))
            return MusicCassaCore.getInstance();
        else
            return MusicCassaCore.getInstance();
    }
    
    /**
     * @param lockName
     * @return
     */
    public static Map<String, Object> validateLock(String lockName) {
        Map<String, Object> resultMap = new HashMap<>();
        String[] locks = lockName.split("\\.");
        if(locks.length < 3) {
            resultMap.put("Error", "Invalid lock. Please make sure lock is of the type keyspaceName.tableName.primaryKey");
            return resultMap;
        }
        String keyspace= locks[0];
        if(keyspace.startsWith("$"))
            keyspace = keyspace.substring(1);
        resultMap.put("keyspace",keyspace);
        return resultMap;
    }


    public static void setIsCadi(boolean isCadi) {
        MusicUtil.isCadi = isCadi;
    }

    public static void writeBackToQuorum(PreparedQueryObject selectQuery, String primaryKeyName,
        PreparedQueryObject updateQuery, String keyspace, String table,
        Object cqlFormattedPrimaryKeyValue)
        throws Exception {
        try {
            ResultSet results = MusicDataStoreHandle.getDSHandle().executeQuorumConsistencyGet(selectQuery);
            // write it back to a quorum
            Row row = results.one();
            ColumnDefinitions colInfo = row.getColumnDefinitions();
            int totalColumns = colInfo.size();
            int counter = 1;
            StringBuilder fieldValueString = new StringBuilder("");
            for (Definition definition : colInfo) {
                String colName = definition.getName();
                if (colName.equals(primaryKeyName))
                    continue;
                DataType colType = definition.getType();
                Object valueObj = MusicDataStoreHandle.getDSHandle().getColValue(row, colName, colType);
                Object valueString = MusicUtil.convertToActualDataType(colType, valueObj);
                fieldValueString.append(colName + " = ?");
                updateQuery.addValue(valueString);
                if (counter != (totalColumns - 1))
                    fieldValueString.append(",");
                counter = counter + 1;
            }
            updateQuery.appendQueryString("UPDATE " + keyspace + "." + table + " SET "
                + fieldValueString + " WHERE " + primaryKeyName + "= ? " + ";");
            updateQuery.addValue(cqlFormattedPrimaryKeyValue);

            MusicDataStoreHandle.getDSHandle().executePut(updateQuery, "critical");
        } catch (MusicServiceException | MusicQueryException e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.QUERYERROR +""+updateQuery ,
                ErrorSeverity.MAJOR, ErrorTypes.QUERYERROR, e);
        }
    }
    
    public static boolean getIsCadi() {
        return MusicUtil.isCadi;
    }


    /**
     * @return a random uuid
     */
    public static String generateUUID() {
        String uuid = UUID.randomUUID().toString();
        logger.info(EELFLoggerDelegate.applicationLogger,"New AID generated: "+uuid);
        return uuid;
    }

    private static String checkPrefix(String prefix){
        if (prefix == null || "".equals(prefix) || prefix.endsWith("-")) {
            return prefix;
        } else {
            return prefix + "-";
        }
    }

    /**
     * @return the transIdPrefix
     */
    public static String getTransIdPrefix() {
        return transIdPrefix;
    }

    /**
     * @param transIdPrefix the transIdPrefix to set
     */
    public static void setTransIdPrefix(String transIdPrefix) {
        MusicUtil.transIdPrefix = checkPrefix(transIdPrefix);
    }

    /**
     * @return the conversationIdPrefix
     */
    public static String getConversationIdPrefix() {
        return conversationIdPrefix;
    }

    /**
     * @param conversationIdPrefix the conversationIdPrefix to set
     */
    public static void setConversationIdPrefix(String conversationIdPrefix) {
        MusicUtil.conversationIdPrefix = checkPrefix(conversationIdPrefix);
    }

    /**
     * @return the clientIdPrefix
     */
    public static String getClientIdPrefix() {
        return clientIdPrefix;
    }

    /**
     * @param clientIdPrefix the clientIdPrefix to set
     */
    public static void setClientIdPrefix(String clientIdPrefix) {
        MusicUtil.clientIdPrefix = checkPrefix(clientIdPrefix);
    }

    /**
     * @return the messageIdPrefix
     */
    public static String getMessageIdPrefix() {
        return messageIdPrefix;
    }

    /**
     * @param messageIdPrefix the messageIdPrefix to set
     */
    public static void setMessageIdPrefix(String messageIdPrefix) {
        MusicUtil.messageIdPrefix = checkPrefix(messageIdPrefix);
    }

        /**
	 * @return the transIdRequired
	 */
	public static String getTransIdRequired() {
		return transIdRequired;
	}


	/**
	 * @param transIdRequired the transIdRequired to set
	 */
	public static void setTransIdRequired(String transIdRequired) {
		MusicUtil.transIdRequired = transIdRequired;
	}


	/**
	 * @return the conversationIdRequired
	 */
	public static String getConversationIdRequired() {
		return conversationIdRequired;
	}


	/**
	 * @param conversationIdRequired the conversationIdRequired to set
	 */
	public static void setConversationIdRequired(String conversationIdRequired) {
		MusicUtil.conversationIdRequired = conversationIdRequired;
	}


	/**
	 * @return the clientIdRequired
	 */
	public static String getClientIdRequired() {
		return clientIdRequired;
	}


	/**
	 * @param clientIdRequired the clientIdRequired to set
	 */
	public static void setClientIdRequired(String clientIdRequired) {
		MusicUtil.clientIdRequired = clientIdRequired;
	}


	/**
	 * @return the messageIdRequired
	 */
	public static String getMessageIdRequired() {
		return messageIdRequired;
	}


	/**
	 * @param messageIdRequired the messageIdRequired to set
	 */
	public static void setMessageIdRequired(String messageIdRequired) {
		MusicUtil.messageIdRequired = messageIdRequired;
	}



}
