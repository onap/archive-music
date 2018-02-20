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
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicUtil;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * @author nelson24
 *
 */
public class MusicDataStore {

    private Session session;
    private Cluster cluster;



    /**
     * @param session
     */
    public void setSession(Session session) {
        this.session = session;
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
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
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
        logger.info(EELFLoggerDelegate.applicationLogger,
                        "Connecting to cassa cluster: Iterating through possible ips:"
                                        + getAllPossibleLocalIps());
        while (it.hasNext()) {
            try {
                cluster = Cluster.builder().withPort(9042)
                                .withCredentials(MusicUtil.getCassName(), MusicUtil.getCassPwd())
                                .addContactPoint(address).build();
                Metadata metadata = cluster.getMetadata();
                logger.info(EELFLoggerDelegate.applicationLogger, "Connected to cassa cluster "
                                + metadata.getClusterName() + " at " + address);
                session = cluster.connect();

                break;
            } catch (NoHostAvailableException e) {
                address = it.next();
                logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
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
        cluster = Cluster.builder().withPort(9042)
                        .withCredentials(MusicUtil.getCassName(), MusicUtil.getCassPwd())
                        .addContactPoint(address).build();
        Metadata metadata = cluster.getMetadata();
        logger.info(EELFLoggerDelegate.applicationLogger, "Connected to cassa cluster "
                        + metadata.getClusterName() + " at " + address);
        try {
            session = cluster.connect();
        } catch (Exception ex) {
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage());
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
            default:
                return null;
        }
    }

    public boolean doesRowSatisfyCondition(Row row, Map<String, Object> condition) {
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
                if (!definition.getName().equals("vector_ts"))
                    resultOutput.put(definition.getName(),
                                    getColValue(row, definition.getName(), definition.getType()));
            }
            resultMap.put("row " + counter, resultOutput);
            counter++;
        }
        return resultMap;
    }


    // Prepared Statements 1802 additions
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
    public boolean executePut(PreparedQueryObject queryObject, String consistency)
                    throws MusicServiceException, MusicQueryException {

        boolean result = false;

        if (!MusicUtil.isValidQueryObject(!queryObject.getValues().isEmpty(), queryObject)) {
            logger.error(EELFLoggerDelegate.errorLogger,
                            "Error while processing prepared query object");
            throw new MusicQueryException("Ill formed queryObject for the request = " + "["
                            + queryObject.getQuery() + "]");
        }
        logger.info(EELFLoggerDelegate.applicationLogger,
                        "In preprared Execute Put: the actual insert query:"
                                        + queryObject.getQuery() + "; the values"
                                        + queryObject.getValues());
        PreparedStatement preparedInsert = session.prepare(queryObject.getQuery());
        try {
            if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                logger.info(EELFLoggerDelegate.applicationLogger, "Executing critical put query");
                preparedInsert.setConsistencyLevel(ConsistencyLevel.QUORUM);
            } else if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL)) {
                logger.info(EELFLoggerDelegate.applicationLogger, "Executing simple put query");
                preparedInsert.setConsistencyLevel(ConsistencyLevel.ONE);
            }

            ResultSet rs = session.execute(preparedInsert.bind(queryObject.getValues().toArray()));
            result = rs.wasApplied();

        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, "Executing Session Failure for Request = "
                            + "[" + queryObject.getQuery() + "]" + " Reason = " + e.getMessage());
            throw new MusicServiceException("Executing Session Failure for Request = " + "["
                            + queryObject.getQuery() + "]" + " Reason = " + e.getMessage());
        }


        return result;
    }

    /**
     * This method performs DDL operations on Cassandra using consistency level ONE.
     * 
     * @param queryObject Object containing cassandra prepared query and values.
     * @return ResultSet
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public ResultSet executeEventualGet(PreparedQueryObject queryObject)
                    throws MusicServiceException, MusicQueryException {

        if (!MusicUtil.isValidQueryObject(!queryObject.getValues().isEmpty(), queryObject)) {
            throw new MusicQueryException("Ill formed queryObject for the request = " + "["
                            + queryObject.getQuery() + "]");
        }
        logger.info(EELFLoggerDelegate.applicationLogger,
                        "Executing Eventual  get query:" + queryObject.getQuery());
        PreparedStatement preparedEventualGet = session.prepare(queryObject.getQuery());
        preparedEventualGet.setConsistencyLevel(ConsistencyLevel.ONE);
        ResultSet results = null;
        try {
            results = session.execute(preparedEventualGet.bind(queryObject.getValues().toArray()));

        } catch (Exception ex) {
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage());
            throw new MusicServiceException(ex.getMessage());
        }
        return results;
    }

    /**
     * 
     * This method performs DDL operation on Cassandra using consistency level QUORUM.
     * 
     * @param queryObject Object containing cassandra prepared query and values.
     * @return ResultSet
     * @throws MusicServiceException
     * @throws MusicQueryException
     */
    public ResultSet executeCriticalGet(PreparedQueryObject queryObject)
                    throws MusicServiceException, MusicQueryException {
        if (!MusicUtil.isValidQueryObject(!queryObject.getValues().isEmpty(), queryObject)) {
            logger.error(EELFLoggerDelegate.errorLogger, "Error processing Prepared Query Object");
            throw new MusicQueryException("Ill formed queryObject for the request = " + "["
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
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage());
            throw new MusicServiceException(ex.getMessage());
        }
        return results;

    }

}
