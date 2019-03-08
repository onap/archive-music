/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (c) 2018-2019 IBM
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
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.jcs.access.CacheAccess;
import org.onap.music.authentication.CachingUtil;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicUtil;

import com.codahale.metrics.JmxReporter;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.sun.jersey.core.util.Base64;

/**
 * @author nelson24
 *
 */
public class MusicDataStore {

    public static final String CONSISTENCY_LEVEL_ONE = "ONE";
    public static final String CONSISTENCY_LEVEL_QUORUM = "QUORUM";
    private Session session;
    private Cluster cluster;


    /**
     * @param session
     */
    public void setSession(Session session) {
        this.session = session;
    }

    /**
     * @param session
     */
    public Session getSession() {
        return session;
    }

    /**
     * @param cluster
     */
    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }


    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicDataStore.class);

    /**
     *
     */
    public MusicDataStore() {
        connectToCassaCluster();
    }


    /**
     * @param cluster
     * @param session
     */
    public MusicDataStore(Cluster cluster, Session session) {
        this.session = session;
        this.cluster = cluster;
    }

    /**
     *
     * @param remoteIp
     * @throws MusicServiceException
     */
    public MusicDataStore(String remoteIp) {
        try {
            connectToCassaCluster(remoteIp);
        } catch (MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
        }
    }

    /**
     *
     * @return
     */
    private ArrayList<String> getAllPossibleLocalIps() {
        ArrayList<String> allPossibleIps = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
            while (en.hasMoreElements()) {
                NetworkInterface ni = en.nextElement();
                Enumeration<InetAddress> ee = ni.getInetAddresses();
                while (ee.hasMoreElements()) {
                    InetAddress ia = ee.nextElement();
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
     * This method iterates through all available IP addresses and connects to multiple cassandra
     * clusters.
     */
    private void connectToCassaCluster() {
        Iterator<String> it = getAllPossibleLocalIps().iterator();
        String address = "localhost";
        String[] addresses = null;
        address = MusicUtil.getMyCassaHost();
        addresses = address.split(",");

        logger.info(EELFLoggerDelegate.applicationLogger,
                        "Connecting to cassa cluster: Iterating through possible ips:"
                                        + getAllPossibleLocalIps());
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
        .setConnectionsPerHost(HostDistance.LOCAL,  4, 10)
        .setConnectionsPerHost(HostDistance.REMOTE, 2, 4);
        while (it.hasNext()) {
            try {
                if(MusicUtil.getCassName() != null && MusicUtil.getCassPwd() != null) {
                    logger.info(EELFLoggerDelegate.applicationLogger,
                            "Building with credentials "+MusicUtil.getCassName()+" & "+MusicUtil.getCassPwd());
                    cluster = Cluster.builder().withPort(MusicUtil.getCassandraPort())
                                       .withCredentials(MusicUtil.getCassName(), MusicUtil.getCassPwd())
                                       //.withLoadBalancingPolicy(new RoundRobinPolicy())
                                       .withoutJMXReporting()
                                       .withPoolingOptions(poolingOptions)
                                       .addContactPoints(addresses).build();
                }
                else
                    cluster = Cluster.builder().withPort(MusicUtil.getCassandraPort())
                                         //.withLoadBalancingPolicy(new RoundRobinPolicy())
                                         .addContactPoints(addresses).build();

                Metadata metadata = cluster.getMetadata();
                logger.info(EELFLoggerDelegate.applicationLogger, "Connected to cassa cluster "
                                + metadata.getClusterName() + " at " + address);
                session = cluster.connect();

                break;
            } catch (NoHostAvailableException e) {
                address = it.next();
                logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.HOSTUNAVAILABLE, ErrorSeverity.ERROR, ErrorTypes.CONNECTIONERROR);
            }
        }
    }

    /**
     *
     */
    public void close() {
        session.close();
    }

    /**
     * This method connects to cassandra cluster on specific address.
     *
     * @param address
     */
    private void connectToCassaCluster(String address) throws MusicServiceException {
        String[] addresses = null;
        addresses = address.split(",");
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
        .setConnectionsPerHost(HostDistance.LOCAL,  4, 10)
        .setConnectionsPerHost(HostDistance.REMOTE, 2, 4);
        if(MusicUtil.getCassName() != null && MusicUtil.getCassPwd() != null) {
            logger.info(EELFLoggerDelegate.applicationLogger,
                    "Building with credentials "+MusicUtil.getCassName()+" & "+MusicUtil.getCassPwd());
            cluster = Cluster.builder().withPort(MusicUtil.getCassandraPort())
                       .withCredentials(MusicUtil.getCassName(), MusicUtil.getCassPwd())
                       //.withLoadBalancingPolicy(new RoundRobinPolicy())
                       .withoutJMXReporting()
                       .withPoolingOptions(poolingOptions)
                       .addContactPoints(addresses).build();
        }
        else {
            cluster = Cluster.builder().withPort(MusicUtil.getCassandraPort())
                        //.withLoadBalancingPolicy(new RoundRobinPolicy())
                        .withoutJMXReporting()
                        .withPoolingOptions(poolingOptions)
                        .addContactPoints(addresses).build();
        }
        
        // JmxReporter reporter =
        //         JmxReporter.forRegistry(cluster.getMetrics().getRegistry())
        //             .inDomain(cluster.getClusterName() + "-metrics")
        //             .build();

        //     reporter.start();
            
        Metadata metadata = cluster.getMetadata();
        logger.info(EELFLoggerDelegate.applicationLogger, "Connected to cassa cluster "
                        + metadata.getClusterName() + " at " + address);
        try {
            session = cluster.connect();
        } catch (Exception ex) {
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(),AppMessages.CASSANDRACONNECTIVITY, ErrorSeverity.ERROR, ErrorTypes.SERVICEUNAVAILABLE);
            throw new MusicServiceException(
                            "Error while connecting to Cassandra cluster.. " + ex.getMessage());
        }
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
        return bb.array();
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
                        new HashMap<>();
        int counter = 0;
        for (Row row : results) {
            ColumnDefinitions colInfo = row.getColumnDefinitions();
            HashMap<String, Object> resultOutput = new HashMap<>();
            for (Definition definition : colInfo) {
                if (!(("vector_ts").equals(definition.getName()))) {
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


    // Prepared Statements 1802 additions
    
    public boolean executePut(PreparedQueryObject queryObject, String consistency)
            throws MusicServiceException, MusicQueryException {
        return executePut(queryObject, consistency, 0);
    }
    /**
     * This Method performs DDL and DML operations on Cassandra using specified consistency level
     *
     * @param queryObject Object containing cassandra prepared query and values.
     * @param consistency Specify consistency level for data synchronization across cassandra
     *        replicas
     * @return Boolean Indicates operation success or failure
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public boolean executePut(PreparedQueryObject queryObject, String consistency,long timeSlot)
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
/*<<<<<<< HEAD
        PreparedStatement preparedInsert = null;
        try {
            
                preparedInsert = session.prepare(queryObject.getQuery());
            
        } catch(InvalidQueryException iqe) {
            logger.error("Exception", iqe);
            logger.error(EELFLoggerDelegate.errorLogger, iqe.getMessage(),AppMessages.QUERYERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
            throw new MusicQueryException(iqe.getMessage());
        }catch(Exception e) {
            logger.error("Exception", e);
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.QUERYERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
            throw new MusicQueryException(e.getMessage());
        }
        
=======*/
        SimpleStatement preparedInsert = null;

        try {
            preparedInsert = new SimpleStatement(queryObject.getQuery(), queryObject.getValues().toArray());
            if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                logger.info(EELFLoggerDelegate.applicationLogger, "Executing critical put query");
                preparedInsert.setConsistencyLevel(ConsistencyLevel.QUORUM);
            } else if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL)) {
                logger.info(EELFLoggerDelegate.applicationLogger, "Executing simple put query");
                if(queryObject.getConsistency() == null)
                    preparedInsert.setConsistencyLevel(ConsistencyLevel.ONE);
               else
                    preparedInsert.setConsistencyLevel(MusicUtil.getConsistencyLevel(queryObject.getConsistency()));
            } else if (consistency.equalsIgnoreCase(MusicUtil.ONE)) {
                preparedInsert.setConsistencyLevel(ConsistencyLevel.ONE);
            }  else if (consistency.equalsIgnoreCase(MusicUtil.QUORUM)) {
                preparedInsert.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            } else if (consistency.equalsIgnoreCase(MusicUtil.ALL)) {
                preparedInsert.setConsistencyLevel(ConsistencyLevel.ALL);
            }
            long timestamp = MusicUtil.v2sTimeStampInMicroseconds(timeSlot, timeOfWrite);
            preparedInsert.setDefaultTimestamp(timestamp);

            ResultSet rs = session.execute(preparedInsert);
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

 /*   *//**
     * This method performs DDL operations on Cassandra using consistency level ONE.
     *
     * @param queryObject Object containing cassandra prepared query and values.
     * @return ResultSet
     * @throws MusicServiceException
     * @throws MusicQueryException
     *//*
    public ResultSet executeEventualGet(PreparedQueryObject queryObject)
                    throws MusicServiceException, MusicQueryException {
        CacheAccess<String, PreparedStatement> queryBank = CachingUtil.getStatementBank();
        PreparedStatement preparedEventualGet = null;
        if (!MusicUtil.isValidQueryObject(!queryObject.getValues().isEmpty(), queryObject)) {
            logger.error(EELFLoggerDelegate.errorLogger, "",AppMessages.QUERYERROR+ " [" + queryObject.getQuery() + "]", ErrorSeverity.ERROR, ErrorTypes.QUERYERROR);
            throw new MusicQueryException("Ill formed queryObject for the request = " + "["
                            + queryObject.getQuery() + "]");
        }
        logger.info(EELFLoggerDelegate.applicationLogger,
                        "Executing Eventual  get query:" + queryObject.getQuery());

        ResultSet results = null;
        try {
            if(queryBank.get(queryObject.getQuery()) != null )
                preparedEventualGet=queryBank.get(queryObject.getQuery());
            else {
                preparedEventualGet = session.prepare(queryObject.getQuery());
                CachingUtil.updateStatementBank(queryObject.getQuery(), preparedEventualGet);
            }
            if(queryObject.getConsistency() == null) {
                preparedEventualGet.setConsistencyLevel(ConsistencyLevel.ONE);
            }
           else {
               preparedEventualGet.setConsistencyLevel(MusicUtil.getConsistencyLevel(queryObject.getConsistency()));
           }
           results = session.execute(preparedEventualGet.bind(queryObject.getValues().toArray()));

        } catch (Exception ex) {
            logger.error("Exception", ex);
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(),AppMessages.UNKNOWNERROR+ "[" + queryObject.getQuery() + "]", ErrorSeverity.ERROR, ErrorTypes.QUERYERROR);
            throw new MusicServiceException(ex.getMessage());
        }
        return results;
    }

    *//**
     *
     * This method performs DDL operation on Cassandra using consistency level QUORUM.
     *
     * @param queryObject Object containing cassandra prepared query and values.
     * @return ResultSet
     * @throws MusicServiceException
     * @throws MusicQueryException
     *//*
    public ResultSet executeCriticalGet(PreparedQueryObject queryObject)
                    throws MusicServiceException, MusicQueryException {
        if (!MusicUtil.isValidQueryObject(!queryObject.getValues().isEmpty(), queryObject)) {
            logger.error(EELFLoggerDelegate.errorLogger, "",AppMessages.QUERYERROR+ " [" + queryObject.getQuery() + "]", ErrorSeverity.ERROR, ErrorTypes.QUERYERROR);
            throw new MusicQueryException("Error processing Prepared Query Object for the request = " + "["
                            + queryObject.getQuery() + "]");
        }
        logger.info(EELFLoggerDelegate.applicationLogger,
                        "Executing Critical get query:" + queryObject.getQuery());
        PreparedStatement preparedEventualGet = session.prepare(queryObject.getQuery());
        preparedEventualGet.setConsistencyLevel(ConsistencyLevel.QUORUM);
        ResultSet results = null;
        try {
            results = session.execute(preparedEventualGet.bind(queryObject.getValues().toArray()));
        } catch (Exception ex) {
            logger.error("Exception", ex);
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(),AppMessages.UNKNOWNERROR+ "[" + queryObject.getQuery() + "]", ErrorSeverity.ERROR, ErrorTypes.QUERYERROR);
            throw new MusicServiceException(ex.getMessage());
        }
        return results;

    }
    */
    public ResultSet executeGet(PreparedQueryObject queryObject,String consistencyLevel) throws MusicQueryException, MusicServiceException {
        if (!MusicUtil.isValidQueryObject(!queryObject.getValues().isEmpty(), queryObject)) {
            logger.error(EELFLoggerDelegate.errorLogger, "",AppMessages.QUERYERROR+ " [" + queryObject.getQuery() + "]", ErrorSeverity.ERROR, ErrorTypes.QUERYERROR);
            throw new MusicQueryException("Error processing Prepared Query Object for the request = " + "["
                            + queryObject.getQuery() + "]");
        }
        ResultSet results = null;
        try {
            SimpleStatement statement = new SimpleStatement(queryObject.getQuery(), queryObject.getValues().toArray());

            if (consistencyLevel.equalsIgnoreCase(CONSISTENCY_LEVEL_ONE)) {
                if(queryObject.getConsistency() == null) {
                    statement.setConsistencyLevel(ConsistencyLevel.ONE);
                }
               else {
                   statement.setConsistencyLevel(MusicUtil.getConsistencyLevel(queryObject.getConsistency()));
               }
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