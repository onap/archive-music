/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Modifications Copyright (C) 2019 IBM.
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

package org.onap.music.conductor.conditionals;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;
import org.onap.music.datastore.MusicDataStoreHandle;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.rest.RestMusicDataAPI;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;

public class MusicConditional {
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(RestMusicDataAPI.class);

    public static ReturnType conditionalInsert(String keyspace, String tablename, String casscadeColumnName,
            Map<String, Object> casscadeColumnData, String primaryKey, Map<String, Object> valuesMap,
            Map<String, String> status) throws Exception {

        Map<String, PreparedQueryObject> queryBank = new HashMap<>();
        TableMetadata tableInfo = null;
        tableInfo = MusicDataStoreHandle.returnColumnMetadata(keyspace, tablename);
        DataType primaryIdType = tableInfo.getPrimaryKey().get(0).getType();
        String primaryId = tableInfo.getPrimaryKey().get(0).getName();
        DataType casscadeColumnType = tableInfo.getColumn(casscadeColumnName).getType();
        String vector = String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());

        PreparedQueryObject select = new PreparedQueryObject();
        select.appendQueryString("SELECT * FROM " + keyspace + "." + tablename + " where " + primaryId + " = ?");
        select.addValue(MusicUtil.convertToActualDataType(primaryIdType, primaryKey));
        queryBank.put(MusicUtil.SELECT, select);

        PreparedQueryObject update = new PreparedQueryObject();
        //casscade column values
        Map<String, String> updateColumnvalues = getValues(true, casscadeColumnData, status);
        Object formatedValues = MusicUtil.convertToActualDataType(casscadeColumnType, updateColumnvalues);
        update.appendQueryString("UPDATE " + keyspace + "." + tablename + " SET " + casscadeColumnName + " ="
                + casscadeColumnName + " + ? , vector_ts = ?" + " WHERE " + primaryId + " = ? ");
        update.addValue(formatedValues);
        update.addValue(MusicUtil.convertToActualDataType(DataType.text(), vector));
        update.addValue(MusicUtil.convertToActualDataType(primaryIdType, primaryKey));
        queryBank.put(MusicUtil.UPDATE, update);


        //casscade column values
        Map<String, String> insertColumnvalues = getValues(false, casscadeColumnData, status);
        formatedValues = MusicUtil.convertToActualDataType(casscadeColumnType, insertColumnvalues);
        PreparedQueryObject insert = extractQuery(valuesMap, tableInfo, tablename, keyspace, primaryId, primaryKey,casscadeColumnName,formatedValues);
        queryBank.put(MusicUtil.INSERT, insert);
        
        
        String key = keyspace + "." + tablename + "." + primaryKey;
        String lockId;
        try {
            lockId = MusicCore.createLockReferenceAtomic(key);
        } catch (MusicLockingException e) {
            return new ReturnType(ResultType.FAILURE, e.getMessage());
        }
        long leasePeriod = MusicUtil.getDefaultLockLeasePeriod();
        ReturnType lockAcqResult = MusicCore.acquireLockWithLease(key, lockId, leasePeriod);

        try {
            if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
                ReturnType criticalPutResult = conditionalInsertAtomic(lockId, keyspace, tablename, primaryKey,
                        queryBank);
                MusicCore.destroyLockRef(lockId);
                if (criticalPutResult.getMessage().contains("insert"))
                    criticalPutResult
                            .setMessage("Insert values: ");
                else if (criticalPutResult.getMessage().contains("update"))
                    criticalPutResult
                            .setMessage("Update values: " + updateColumnvalues);
                return criticalPutResult;

            } else {
                MusicCore.destroyLockRef(lockId);
                return lockAcqResult;
            }
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.applicationLogger, e);
            MusicCore.destroyLockRef(lockId);
            return new ReturnType(ResultType.FAILURE, e.getMessage());
        }

    }

    public static ReturnType conditionalInsertAtomic(String lockId, String keyspace, String tableName,
            String primaryKey, Map<String, PreparedQueryObject> queryBank) {

        ResultSet results = null;

        try {
            String fullyQualifiedKey = keyspace + "." + tableName + "." + primaryKey;
            ReturnType lockAcqResult = MusicCore.acquireLock(fullyQualifiedKey, lockId);
            if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
                try {
                    results = MusicDataStoreHandle.getDSHandle().executeQuorumConsistencyGet(queryBank.get(MusicUtil.SELECT));
                } catch (Exception e) {
                    logger.error(EELFLoggerDelegate.applicationLogger, e);
                    return new ReturnType(ResultType.FAILURE, e.getMessage());
                }
                if (results.all().isEmpty()) {
                    MusicDataStoreHandle.getDSHandle().executePut(queryBank.get(MusicUtil.INSERT), "critical");
                    return new ReturnType(ResultType.SUCCESS, "insert");
                } else {
                    MusicDataStoreHandle.getDSHandle().executePut(queryBank.get(MusicUtil.UPDATE), "critical");
                    return new ReturnType(ResultType.SUCCESS, "update");
                }
            } else {
                return new ReturnType(ResultType.FAILURE,
                        "Cannot perform operation since you are the not the lock holder");
            }

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            logger.error(EELFLoggerDelegate.applicationLogger, e);
            return new ReturnType(ResultType.FAILURE,
                    "Exception thrown while doing the critical put, check sanctity of the row/conditions:\n"
                            + exceptionAsString);
        }

    }

    public static ReturnType update(UpdateDataObject dataObj)
            throws MusicLockingException, MusicQueryException, MusicServiceException {

        String key = dataObj.getKeyspace() + "." + dataObj.getTableName() + "." + dataObj.getPrimaryKeyValue();
        String lockId = MusicCore.createLockReferenceAtomic(key);
        long leasePeriod = MusicUtil.getDefaultLockLeasePeriod();
        ReturnType lockAcqResult = MusicCore.acquireLockWithLease(key, lockId, leasePeriod);

        try {

            if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
                ReturnType criticalPutResult = updateAtomic(new UpdateDataObject().setLockId(lockId)
                        .setKeyspace(dataObj.getKeyspace())
                        .setTableName( dataObj.getTableName())
                        .setPrimaryKey(dataObj.getPrimaryKey())
                        .setPrimaryKeyValue(dataObj.getPrimaryKeyValue())
                        .setQueryBank(dataObj.getQueryBank())
                        .setPlanId(dataObj.getPlanId())
                        .setCascadeColumnValues(dataObj.getCascadeColumnValues())
                        .setCascadeColumnName(dataObj.getCascadeColumnName()));                     

                MusicCore.destroyLockRef(lockId);
                return criticalPutResult;
            } else {
                MusicCore.destroyLockRef(lockId);
                return lockAcqResult;
            }

        } catch (Exception e) {
            MusicCore.destroyLockRef(lockId);
            logger.error(EELFLoggerDelegate.applicationLogger, e);
            return new ReturnType(ResultType.FAILURE, e.getMessage());

        }
    }

    public static ReturnType updateAtomic(UpdateDataObject dataObj) {
        try {
            String fullyQualifiedKey = dataObj.getKeyspace() + "." + dataObj.getTableName() + "." + dataObj.getPrimaryKeyValue();
            ReturnType lockAcqResult = MusicCore.acquireLock(fullyQualifiedKey, dataObj.getLockId());

            if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
                Row row  = MusicDataStoreHandle.getDSHandle().executeQuorumConsistencyGet(dataObj.getQueryBank().get(MusicUtil.SELECT)).one();
                
                if(row != null) {
                    Map<String, String> updatedValues = cascadeColumnUpdateSpecific(row, dataObj.getCascadeColumnValues(), dataObj.getCascadeColumnName(), dataObj.getPlanId());
                    JSONObject json = new JSONObject(updatedValues);
                    PreparedQueryObject update = new PreparedQueryObject();
                    String vector_ts = String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
                    update.appendQueryString("UPDATE " + dataObj.getKeyspace() + "." + dataObj.getTableName() + " SET " + dataObj.getCascadeColumnName() + "['" + dataObj.getPlanId()
                            + "'] = ?, vector_ts = ? WHERE " + dataObj.getPrimaryKey() + " = ?");
                    update.addValue(MusicUtil.convertToActualDataType(DataType.text(), json.toString()));
                    update.addValue(MusicUtil.convertToActualDataType(DataType.text(), vector_ts));
                    update.addValue(MusicUtil.convertToActualDataType(DataType.text(), dataObj.getPrimaryKeyValue()));
                    try {
                        MusicDataStoreHandle.getDSHandle().executePut(update, "critical");
                    } catch (Exception ex) {
                        logger.error(EELFLoggerDelegate.applicationLogger, ex);
                        return new ReturnType(ResultType.FAILURE, ex.getMessage());
                    }
                }else {
                    return new ReturnType(ResultType.FAILURE,"Cannot find data related to key: "+dataObj.getPrimaryKey());
                }
                MusicDataStoreHandle.getDSHandle().executePut(dataObj.getQueryBank().get(MusicUtil.UPSERT), "critical");
                return new ReturnType(ResultType.SUCCESS, "update success");

            } else {
                return new ReturnType(ResultType.FAILURE,
                        "Cannot perform operation since you are the not the lock holder");
            }

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            logger.error(EELFLoggerDelegate.applicationLogger, e);
            return new ReturnType(ResultType.FAILURE,
                    "Exception thrown while doing the critical put, check sanctity of the row/conditions:\n"
                            + exceptionAsString);
        }

    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getValues(boolean isExists, Map<String, Object> casscadeColumnData,
            Map<String, String> status) {

        Map<String, String> returnMap = new HashMap<>();
        Object key = casscadeColumnData.get("key");
        String setStatus = "";
        Map<String, String> value = (Map<String, String>) casscadeColumnData.get("value");

        if (isExists)
            setStatus = status.get("exists");
        else
            setStatus = status.get("nonexists");

        value.put("status", setStatus);
        JSONObject valueJson = new JSONObject(value);
        returnMap.put(key.toString(), valueJson.toString());
        return returnMap;

    }
    
    public static PreparedQueryObject extractQuery(Map<String, Object> valuesMap, TableMetadata tableInfo, String tableName,
            String keySpaceName,String primaryKeyName,String primaryKey,String casscadeColumn,Object casscadeColumnValues) throws Exception {

        PreparedQueryObject queryObject = new PreparedQueryObject();
        StringBuilder fieldsString = new StringBuilder("(vector_ts"+",");
        StringBuilder valueString = new StringBuilder("(" + "?" + ",");
        String vector = String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
        String localPrimaryKey;
        queryObject.addValue(vector);
        if(casscadeColumn!=null && casscadeColumnValues!=null) {
            fieldsString.append(casscadeColumn).append(" ,");
            valueString.append("?,");
            queryObject.addValue(casscadeColumnValues);
        }
        
        int counter = 0;
        for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
            
            fieldsString.append(entry.getKey());
            Object valueObj = entry.getValue();
            if (primaryKeyName.equals(entry.getKey())) {
                localPrimaryKey = entry.getValue() + "";
                localPrimaryKey = localPrimaryKey.replace("'", "''");
            }
            DataType colType = null;
            try {
                colType = tableInfo.getColumn(entry.getKey()).getType();
            } catch(NullPointerException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage() +" Invalid column name : "+entry.getKey(), 
                    AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR, ex);
            }

            Object formattedValue = null;
            try {
                formattedValue = MusicUtil.convertToActualDataType(colType, valueObj);
            } catch (Exception e) {
                logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), e);
            }
            
            valueString.append("?");
            queryObject.addValue(formattedValue);

            
            if (counter == valuesMap.size() - 1) {
                fieldsString.append(")");
                valueString.append(")");
            } else {
                fieldsString.append(",");
                valueString.append(",");
            }
            counter = counter + 1;
        }
        queryObject.appendQueryString("INSERT INTO " + keySpaceName + "." + tableName + " "
                + fieldsString + " VALUES " + valueString);
        return queryObject;
    }
    
    public static Object getColValue(Row row, String colName, DataType colType) {
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
    
    @SuppressWarnings("unchecked")
    public static Map<String, String> cascadeColumnUpdateSpecific(Row row, Map<String, String> changeOfStatus,
            String cascadeColumnName, String planId) {

        ColumnDefinitions colInfo = row.getColumnDefinitions();
        DataType colType = colInfo.getType(cascadeColumnName);
        Object columnValue = getColValue(row, cascadeColumnName, colType);

        Map<String, String> finalValues = new HashMap<>();
        Map<String, String> values = (Map<String, String>) columnValue;
        if (values != null && values.keySet().contains(planId)) {
            String valueString = values.get(planId);
            String tempValueString = valueString.replaceAll("\\{", "").replaceAll("\"", "").replaceAll("\\}", "");
            String[] elements = tempValueString.split(",");
            for (String str : elements) {
                String[] keyValue = str.split(":");
                if ((changeOfStatus.keySet().contains(keyValue[0].replaceAll("\\s", ""))))
                keyValue[1] = changeOfStatus.get(keyValue[0].replaceAll("\\s", ""));
                finalValues.put(keyValue[0], keyValue[1]);
            }
        }
        return finalValues;

    }

}
