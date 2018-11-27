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
package org.onap.music.datastore;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.*;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicUtil;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * @author nelson24
 * @author bharathb
 */
public class MusicDataStore {

    public static final String CONSISTENCY_LEVEL_ONE = "ONE";
    public static final String CONSISTENCY_LEVEL_QUORUM = "QUORUM";

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicDataStore.class);

    private Session session;
    private Cluster cluster;

    public Session getSession() {
        return session;
    }


    /**
     * Constructs DataStore by connecting to local Cassandra
     */
    public MusicDataStore() {
        connectToLocalCassandraCluster();
    }

    /**
     * Constructs DataStore by providing existing cluster and session
     * @param cluster
     * @param session
     */
    public MusicDataStore(Cluster cluster, Session session) {
        this.session = session;
        this.cluster = cluster;
    }

    /**
     * Constructs DataStore by connecting to provided remote Cassandra
     * @param remoteAddress
     * @throws MusicServiceException
     */
    public MusicDataStore(String remoteAddress) {
        try {
            connectToRemoteCassandraCluster(remoteAddress);
        } catch (MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
        }
    }

    private void createCassandraSession(String address) throws NoHostAvailableException {
        cluster = Cluster.builder().withPort(9042)
                .withCredentials(MusicUtil.getCassName(), MusicUtil.getCassPwd())
                .addContactPoint(address).build();
        Metadata metadata = cluster.getMetadata();
        logger.info(EELFLoggerDelegate.applicationLogger, "Connected to cassa cluster "
                + metadata.getClusterName() + " at " + address);
        session = cluster.connect();
    }

    /**
     *
     * @return
     */
    private ArrayList<String> getAllPossibleLocalIps() {
        ArrayList<String> allPossibleIps = new ArrayList<String>();
        try {
            Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
            while (en.hasMoreElements()) {
                NetworkInterface ni = (NetworkInterface) en.nextElement();
                Enumeration<InetAddress> ee = ni.getInetAddresses();
                while (ee.hasMoreElements()) {
                    InetAddress ia = (InetAddress) ee.nextElement();
                    allPossibleIps.add(ia.getHostAddress());
                }
            }
        } catch (SocketException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.CONNCECTIVITYERROR, ErrorSeverity.ERROR, ErrorTypes.CONNECTIONERROR);
        }catch(Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), ErrorSeverity.ERROR, ErrorTypes.GENERALSERVICEERROR);
        }
        return allPossibleIps;
    }

    /**
     * This method iterates through all available local IP addresses and tries to connect to first successful one
     */
    private void connectToLocalCassandraCluster() {
        ArrayList<String> localAddrs = getAllPossibleLocalIps();
        localAddrs.add(0, "localhost");
        logger.info(EELFLoggerDelegate.applicationLogger,
                        "Connecting to cassa cluster: Iterating through possible ips:"
                                        + getAllPossibleLocalIps());
        for (String address: localAddrs) {
            try {
                createCassandraSession(address);
                break;
            } catch (NoHostAvailableException e) {
                logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.HOSTUNAVAILABLE, ErrorSeverity.ERROR, ErrorTypes.CONNECTIONERROR);
            }
        }
    }

    /**
     * This method connects to cassandra cluster on specific address.
     * 
     * @param address
     */
    private void connectToRemoteCassandraCluster(String address) throws MusicServiceException {
        try {
            createCassandraSession(address);
        } catch (Exception ex) {
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(),AppMessages.CASSANDRACONNECTIVITY, ErrorSeverity.ERROR, ErrorTypes.SERVICEUNAVAILABLE);
            throw new MusicServiceException(
                            "Error while connecting to Cassandra cluster.. " + ex.getMessage());
        }
    }

    /**
     *
     */
    public void close() {
        session.close();
    }

    /**
     * 
     * @param keyspace
     * @param tableName
     * @param columnName
     * @return DataType
     */
    public DataType returnColumnDataType(String keyspace, String tableName, String columnName) {
        KeyspaceMetadata ks = cluster.getMetadata().getKeyspace(keyspace);
        TableMetadata table = ks.getTable(tableName);
        return table.getColumn(columnName).getType();

    }

    /**
     * 
     * @param keyspace
     * @param tableName
     * @return TableMetadata
     */
    public TableMetadata returnColumnMetadata(String keyspace, String tableName) {
        KeyspaceMetadata ks = cluster.getMetadata().getKeyspace(keyspace);
        return ks.getTable(tableName);
    }


    /**
     * Utility function to return the Java specific object type.
     * 
     * @param row
     * @param colName
     * @param colType
     * @return
     */
    public Object getColValue(Row row, String colName, DataType colType) {

        switch (colType.getName()) {
            case VARCHAR:
                return row.getString(colName);
            case UUID:
                return row.getUUID(colName);
            case VARINT:
                return row.getVarint(colName);
            case BIGINT:
                return row.getLong(colName);
            case INT:
                return row.getInt(colName);
            case FLOAT:
                return row.getFloat(colName);
            case DOUBLE:
                return row.getDouble(colName);
            case BOOLEAN:
                return row.getBool(colName);
            case MAP:
                return row.getMap(colName, String.class, String.class);
            case LIST:
            	return row.getList(colName, String.class);
            default:
                return null;
        }
    }
    
    public byte[] getBlobValue(Row row, String colName, DataType colType) {
    	ByteBuffer bb = row.getBytes(colName);
    	byte[] data = bb.array();
    	return data;
    }

    public boolean doesRowSatisfyCondition(Row row, Map<String, Object> condition) throws Exception {
        ColumnDefinitions colInfo = row.getColumnDefinitions();

        for (Map.Entry<String, Object> entry : condition.entrySet()) {
            String colName = entry.getKey();
            DataType colType = colInfo.getType(colName);
            Object columnValue = getColValue(row, colName, colType);
            Object conditionValue = MusicUtil.convertToActualDataType(colType, entry.getValue());
            if (columnValue.equals(conditionValue) == false)
                return false;
        }
        return true;
    }

    /**
     * Utility function to store ResultSet values in to a MAP for output.
     * 
     * @param results
     * @return MAP
     */
    public Map<String, HashMap<String, Object>> marshalData(ResultSet results) {
        Map<String, HashMap<String, Object>> resultMap =
                        new HashMap<String, HashMap<String, Object>>();
        int counter = 0;
        for (Row row : results) {
            ColumnDefinitions colInfo = row.getColumnDefinitions();
            HashMap<String, Object> resultOutput = new HashMap<String, Object>();
            for (Definition definition : colInfo) {
                if (!definition.getName().equals("vector_ts")) {
                	if(definition.getType().toString().toLowerCase().contains("blob")) {
                		resultOutput.put(definition.getName(),
                                getBlobValue(row, definition.getName(), definition.getType()));
                	} 
                	else
                		resultOutput.put(definition.getName(),
                                    getColValue(row, definition.getName(), definition.getType()));
                }
            }
            resultMap.put("row " + counter, resultOutput);
            counter++;
        }
        return resultMap;
    }

    /**
     * This Method performs DDL and DML operations on Cassandra using specified consistency level outside any time-slot
     *
     * @param queryObject Object containing cassandra prepared query and values.
     * @param consistency Specify consistency level for data synchronization across cassandra
     *        replicas
     * @return Boolean Indicates operation success or failure
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public boolean executePut(PreparedQueryObject queryObject, String consistency)
            throws MusicServiceException, MusicQueryException {
        return executePut(queryObject, consistency, 0);
    }

    // Prepared Statements 1802 additions
    /**
     * This Method performs DDL and DML operations on Cassandra using specified consistency level
     * 
     * @param queryObject Object containing cassandra prepared query and values.
     * @param consistencyLevel Specify consistency level for data synchronization across cassandra
     *        replicas
     * @param timeSlot Specify timestamp time-slot
     * @return Boolean Indicates operation success or failure
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public boolean executePut(PreparedQueryObject queryObject, String consistencyLevel, long timeSlot)
            throws MusicServiceException, MusicQueryException {

        boolean result = false;
        long timeOfWrite = System.currentTimeMillis();

        if (!MusicUtil.isValidQueryObject(!queryObject.getValues().isEmpty(), queryObject)) {
        	logger.error(EELFLoggerDelegate.errorLogger, queryObject.getQuery(),AppMessages.QUERYERROR, ErrorSeverity.ERROR, ErrorTypes.QUERYERROR);
            throw new MusicQueryException("Ill formed queryObject for the request = " + "["
                            + queryObject.getQuery() + "]");
        }
        logger.info(EELFLoggerDelegate.applicationLogger,
                        "In preprared Execute Put: the actual insert query:"
                                        + queryObject.getQuery() + "; the values"
                                        + queryObject.getValues());
        SimpleStatement statement;
        try {

             statement = new SimpleStatement(queryObject.getQuery(), queryObject.getValues().toArray());
        } catch(InvalidQueryException iqe) {
        	logger.error(EELFLoggerDelegate.errorLogger, iqe.getMessage(),AppMessages.QUERYERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
        	throw new MusicQueryException(iqe.getMessage());
        }catch(Exception e) {
        	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.QUERYERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
        	throw new MusicQueryException(e.getMessage());
        }
        
        try {
            if (consistencyLevel.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                logger.info(EELFLoggerDelegate.applicationLogger, "Executing critical put query");
                statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
            } else if (consistencyLevel.equalsIgnoreCase(MusicUtil.EVENTUAL)) {
                logger.info(EELFLoggerDelegate.applicationLogger, "Executing simple put query");
                statement.setConsistencyLevel(ConsistencyLevel.ONE);
            }

            long timestamp = MusicUtil.v2sTimeStampInMicroseconds(timeSlot, timeOfWrite);
            statement.setDefaultTimestamp(timestamp);

            ResultSet rs = session.execute(statement);
            result = rs.wasApplied();
        }
        catch (AlreadyExistsException ae) {
            logger.error(EELFLoggerDelegate.errorLogger, ae.getMessage(),AppMessages.SESSIONFAILED+ " [" + queryObject.getQuery() + "]", ErrorSeverity.ERROR, ErrorTypes.QUERYERROR);
        	throw new MusicServiceException(ae.getMessage());
        }
        catch (Exception e) {
        	logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.SESSIONFAILED+ " [" + queryObject.getQuery() + "]", ErrorSeverity.ERROR, ErrorTypes.QUERYERROR);
        	throw new MusicQueryException("Executing Session Failure for Request = " + "["
                            + queryObject.getQuery() + "]" + " Reason = " + e.getMessage());
        }


        return result;
    }

    /**
     * This method performs DDL operations on Cassandra using consistency specified consistency.
     *
     * @param queryObject Object containing cassandra prepared query and values.
     */
    public ResultSet executeGet(PreparedQueryObject queryObject, String consistencyLevel)
            throws MusicServiceException, MusicQueryException {

        if (!MusicUtil.isValidQueryObject(!queryObject.getValues().isEmpty(), queryObject)) {
            logger.error(EELFLoggerDelegate.errorLogger, "",AppMessages.QUERYERROR+ " [" + queryObject.getQuery() + "]", ErrorSeverity.ERROR, ErrorTypes.QUERYERROR);
            throw new MusicQueryException("Ill formed queryObject for the request = " + "["
                    + queryObject.getQuery() + "]");
        }
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Executing Eventual get query:" + queryObject.getQuery());

        ResultSet results = null;
        try {
            SimpleStatement statement = new SimpleStatement(queryObject.getQuery(), queryObject.getValues().toArray());

            if (consistencyLevel.equalsIgnoreCase(CONSISTENCY_LEVEL_ONE)) {
                statement.setConsistencyLevel(ConsistencyLevel.ONE);
            }
            else if (consistencyLevel.equalsIgnoreCase(CONSISTENCY_LEVEL_QUORUM)) {
                statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
            }

            results = session.execute(statement);

        } catch (Exception ex) {
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(),AppMessages.UNKNOWNERROR+ "[" + queryObject.getQuery() + "]", ErrorSeverity.ERROR, ErrorTypes.QUERYERROR);
            throw new MusicServiceException(ex.getMessage());
        }
        return results;
    }

    /**
     * This method performs DDL operations on Cassandra using consistency level ONE.
     * 
     * @param queryObject Object containing cassandra prepared query and values.
     */
    public ResultSet executeOneConsistencyGet(PreparedQueryObject queryObject)
                    throws MusicServiceException, MusicQueryException {
        return executeGet(queryObject, CONSISTENCY_LEVEL_ONE);
    }

    /**
     * 
     * This method performs DDL operation on Cassandra using consistency level QUORUM.
     * 
     * @param queryObject Object containing cassandra prepared query and values.
     */
    public ResultSet executeQuorumConsistencyGet(PreparedQueryObject queryObject)
                    throws MusicServiceException, MusicQueryException {
        return executeGet(queryObject, CONSISTENCY_LEVEL_QUORUM);
    }
}

