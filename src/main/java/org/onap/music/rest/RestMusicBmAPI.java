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
package org.onap.music.rest;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.apache.log4j.Logger;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonOnboard;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import org.onap.music.datastore.PreparedQueryObject;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TableMetadata;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

/*
 * These are functions created purely for benchmarking purposes. Commented out Swagger - This should
 * be undocumented API
 * 
 */
@Path("/v{version: [0-9]+}/benchmarks/")
@Api(value = "Benchmark API", hidden = true)
public class RestMusicBmAPI {
    private static EELFLogger logger = EELFManager.getInstance().getLogger(RestMusicBmAPI.class);

    // pure zk calls...

    /**
     * 
     * @param nodeName
     * @throws Exception
     */
    @POST
    @Path("/purezk/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void pureZkCreate(@PathParam("name") String nodeName) throws Exception {
        MusicCore.pureZkCreate("/" + nodeName);
    }


    /**
     * 
     * @param insObj
     * @param nodeName
     * @throws Exception
     */
    @PUT
    @Path("/purezk/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void pureZkUpdate(JsonInsert insObj, @PathParam("name") String nodeName)
                    throws Exception {
        logger.info("--------------Zk normal update-------------------------");
        long start = System.currentTimeMillis();
        MusicCore.pureZkWrite(nodeName, insObj.serialize());
        long end = System.currentTimeMillis();
        logger.info("Total time taken for Zk normal update:" + (end - start) + " ms");
    }

    /**
     * 
     * @param nodeName
     * @return
     * @throws Exception
     */
    @GET
    @Path("/purezk/{name}")
    @Consumes(MediaType.TEXT_PLAIN)
    public byte[] pureZkGet(@PathParam("name") String nodeName) throws Exception {
        return MusicCore.pureZkRead(nodeName);
    }

    /**
     * 
     * @param insObj
     * @param lockName
     * @param nodeName
     * @throws Exception
     */
    @PUT
    @Path("/purezk/atomic/{lockname}/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void pureZkAtomicPut(JsonInsert updateObj, @PathParam("lockname") String lockname,
                    @PathParam("name") String nodeName) throws Exception {
        long startTime = System.currentTimeMillis();
        String operationId = UUID.randomUUID().toString();// just for debugging purposes.
        String consistency = updateObj.getConsistencyInfo().get("type");

        logger.info("--------------Zookeeper " + consistency + " update-" + operationId
                        + "-------------------------");

        byte[] data = updateObj.serialize();
        long jsonParseCompletionTime = System.currentTimeMillis();

        String lockId = MusicCore.createLockReference(lockname);

        long lockCreationTime = System.currentTimeMillis();

        long leasePeriod = MusicUtil.getDefaultLockLeasePeriod();
        ReturnType lockAcqResult = MusicCore.acquireLockWithLease(lockname, lockId, leasePeriod);
        long lockAcqTime = System.currentTimeMillis();
        long zkPutTime = 0, lockReleaseTime = 0;

        if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
            logger.info("acquired lock with id " + lockId);
            MusicCore.pureZkWrite(lockname, data);
            zkPutTime = System.currentTimeMillis();
            boolean voluntaryRelease = true;
            if (consistency.equals("atomic"))
                MusicCore.releaseLock(lockId, voluntaryRelease);
            else if (consistency.equals("atomic_delete_lock"))
                MusicCore.deleteLock(lockname);
            lockReleaseTime = System.currentTimeMillis();
        } else {
            MusicCore.destroyLockRef(lockId);
        }

        long actualUpdateCompletionTime = System.currentTimeMillis();


        long endTime = System.currentTimeMillis();

        String lockingInfo = "|lock creation time:" + (lockCreationTime - jsonParseCompletionTime)
                        + "|lock accquire time:" + (lockAcqTime - lockCreationTime)
                        + "|zk put time:" + (zkPutTime - lockAcqTime);

        if (consistency.equals("atomic"))
            lockingInfo = lockingInfo + "|lock release time:" + (lockReleaseTime - zkPutTime) + "|";
        else if (consistency.equals("atomic_delete_lock"))
            lockingInfo = lockingInfo + "|lock delete time:" + (lockReleaseTime - zkPutTime) + "|";

        String timingString = "Time taken in ms for Zookeeper " + consistency + " update-"
                        + operationId + ":" + "|total operation time:" + (endTime - startTime)
                        + "|json parsing time:" + (jsonParseCompletionTime - startTime)
                        + "|update time:" + (actualUpdateCompletionTime - jsonParseCompletionTime)
                        + lockingInfo;

        logger.info(timingString);
    }

    /**
     * 
     * @param insObj
     * @param lockName
     * @param nodeName
     * @throws Exception
     */
    @GET
    @Path("/purezk/atomic/{lockname}/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void pureZkAtomicGet(JsonInsert insObj, @PathParam("lockname") String lockName,
                    @PathParam("name") String nodeName) throws Exception {
        logger.info("--------------Zk atomic read-------------------------");
        long start = System.currentTimeMillis();
        String lockId = MusicCore.createLockReference(lockName);
        long leasePeriod = MusicUtil.getDefaultLockLeasePeriod();
        ReturnType lockAcqResult = MusicCore.acquireLockWithLease(lockName, lockId, leasePeriod);
        if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
            logger.info("acquired lock with id " + lockId);
            MusicCore.pureZkRead(nodeName);
            boolean voluntaryRelease = true;
            MusicCore.releaseLock(lockId, voluntaryRelease);
        } else {
            MusicCore.destroyLockRef(lockId);
        }

        long end = System.currentTimeMillis();
        logger.info("Total time taken for Zk atomic read:" + (end - start) + " ms");
    }

    /**
     *
     * doing an update directly to cassa but through the rest api
     * 
     * @param insObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws Exception
     */
    @PUT
    @Path("/cassa/keyspaces/{keyspace}/tables/{tablename}/rows")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public boolean updateTableCassa(JsonInsert insObj, @PathParam("keyspace") String keyspace,
                    @PathParam("tablename") String tablename, @Context UriInfo info)
                    throws Exception {
        long startTime = System.currentTimeMillis();
        String operationId = UUID.randomUUID().toString();// just for debugging purposes.
        String consistency = insObj.getConsistencyInfo().get("type");
        logger.info("--------------Cassandra " + consistency + " update-" + operationId
                        + "-------------------------");
        PreparedQueryObject queryObject = new PreparedQueryObject();
        Map<String, Object> valuesMap = insObj.getValues();
        TableMetadata tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
        String vectorTs = "'" + Thread.currentThread().getId() + System.currentTimeMillis() + "'";
        String fieldValueString = "vector_ts= ? ,";
        queryObject.addValue(vectorTs);

        int counter = 0;
        for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
            Object valueObj = entry.getValue();
            DataType colType = tableInfo.getColumn(entry.getKey()).getType();
            Object valueString = MusicUtil.convertToActualDataType(colType, valueObj);
            fieldValueString = fieldValueString + entry.getKey() + "= ?";
            queryObject.addValue(valueString);
            if (counter != valuesMap.size() - 1)
                fieldValueString = fieldValueString + ",";
            counter = counter + 1;
        }

        // get the row specifier
        String rowSpec = "";
        counter = 0;
        queryObject.appendQueryString("UPDATE " + keyspace + "." + tablename + " ");
        MultivaluedMap<String, String> rowParams = info.getQueryParameters();
        String primaryKey = "";
        for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()) {
            String keyName = entry.getKey();
            List<String> valueList = entry.getValue();
            String indValue = valueList.get(0);
            DataType colType = tableInfo.getColumn(entry.getKey()).getType();
            Object formattedValue = MusicUtil.convertToActualDataType(colType, indValue);
            primaryKey = primaryKey + indValue;
            rowSpec = rowSpec + keyName + "= ? ";
            queryObject.addValue(formattedValue);
            if (counter != rowParams.size() - 1)
                rowSpec = rowSpec + " AND ";
            counter = counter + 1;
        }


        String ttl = insObj.getTtl();
        String timestamp = insObj.getTimestamp();

        if ((ttl != null) && (timestamp != null)) {

            logger.info("both there");
            queryObject.appendQueryString(" USING TTL ? AND TIMESTAMP ?");
            queryObject.addValue(Integer.parseInt(ttl));
            queryObject.addValue(Long.parseLong(timestamp));
        }

        if ((ttl != null) && (timestamp == null)) {
            logger.info("ONLY TTL there");
            queryObject.appendQueryString(" USING TTL ?");
            queryObject.addValue(Integer.parseInt(ttl));
        }

        if ((ttl == null) && (timestamp != null)) {
            logger.info("ONLY timestamp there");
            queryObject.appendQueryString(" USING TIMESTAMP ?");
            queryObject.addValue(Long.parseLong(timestamp));
        }
        queryObject.appendQueryString(" SET " + fieldValueString + " WHERE " + rowSpec + ";");

        long jsonParseCompletionTime = System.currentTimeMillis();

        boolean operationResult = true;
        MusicCore.getDSHandle().executePut(queryObject, insObj.getConsistencyInfo().get("type"));

        long actualUpdateCompletionTime = System.currentTimeMillis();

        long endTime = System.currentTimeMillis();

        String timingString = "Time taken in ms for Cassandra " + consistency + " update-"
                        + operationId + ":" + "|total operation time:" + (endTime - startTime)
                        + "|json parsing time:" + (jsonParseCompletionTime - startTime)
                        + "|update time:" + (actualUpdateCompletionTime - jsonParseCompletionTime)
                        + "|";
        logger.info(timingString);

        return operationResult;
    }

}
