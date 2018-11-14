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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.triggers.ITrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;


public class MusicTrigger implements ITrigger {

	private static final Logger logger = LoggerFactory.getLogger(MusicTrigger.class);
	
    public Collection<Mutation> augment(Partition partition)
    {
    	new Thread(new Runnable() {
    	    public void run() {
    	        makeAsyncCall(partition);
    	    }
    	}).start();
        return Collections.emptyList();
    }
    
    private void makeAsyncCall(Partition partition) {
    	boolean isDelete = false;
    	if(partition.partitionLevelDeletion().isLive()) {
    		
    	} else {
            // Partition Level Deletion
        	isDelete = true;
        }
    	logger.info("MusicTrigger isDelete: " + isDelete);
    	String ksName = partition.metadata().ksName;
        String tableName = partition.metadata().cfName;
        String pkValue = partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey());
        logger.info("MusicTrigger Table: " + tableName);
        boolean isInsert = checkQueryType(partition);
        org.json.simple.JSONObject obj = new org.json.simple.JSONObject();
        
        String operation = null;
        if(isDelete)
        	operation = "delete";
        else if(isInsert)
        	operation = "insert";
        else
        	operation = "update";
        Map<String, Object> changeMap = new HashMap<>();
        
        obj.put("operation", operation);
        obj.put("keyspace", ksName);
        obj.put("table_name", tableName);
        obj.put("full_table", ksName+"."+tableName);
        obj.put("primary_key", partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey()));
        List<String> updateList = new ArrayList<>();
        //obj.put("message_id", partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey()));
        if("update".equals(operation)) {
	        try {
	            UnfilteredRowIterator it = partition.unfilteredIterator();
	            while (it.hasNext()) {
	                Unfiltered un = it.next();
	                Clustering clt = (Clustering) un.clustering();  
	                Iterator<Cell> cells = partition.getRow(clt).cells().iterator();
	                Iterator<ColumnDefinition> columns = partition.getRow(clt).columns().iterator();
	
	                while(columns.hasNext()){
	                    ColumnDefinition columnDef = columns.next();
	                    Cell cell = cells.next();
	                    
	                    String data = null;
	                    if(cell.column().type.toString().equals("org.apache.cassandra.db.marshal.UTF8Type")) {
	                        logger.info(">> type is String");
	                        data = new String(cell.value().array()); // If cell type is text
	                    } else if(cell.column().type.toString().equals("org.apache.cassandra.db.marshal.Int32Type")) {
	                    	//ByteBuffer wrapped = ByteBuffer.wrap(cell.value()); // big-endian by default
	                        int num = fromByteArray(cell.value().array());
	                        logger.info(">> type is Integer1 :: "+num);
	                        data = String.valueOf(num);
	                    }
	                    
	                    logger.info("Inside triggers loop: "+columnDef.name+" : "+data);
	                    //changeMap.put(ksName+"."+tableName+"."+columnDef.name,data);
	                    updateList.add(ksName+"."+tableName+":"+columnDef.name+":"+data);
	                    changeMap.put("field_value",ksName+"."+tableName+":"+columnDef.name+":"+data);
	                    
	                }
	            }
	        } catch (Exception e) {
	        	logger.info("Exception while constructing.. "+e.getMessage());
	        }
	        obj.put("updateList", updateList);
        } else {
        	changeMap.put("field_value", ksName+"."+tableName);
        }
        
        obj.put("changeValue", changeMap);
        logger.info("Sending response: "+obj.toString());
        try {
            notifyMusic(obj.toString());
            logger.info("MUSIC was notified.. "+obj.toString());
        } catch(Exception e) {
            logger.error("Notification failed..."+e.getMessage());
            logger.info("Notification failed..."+e.getMessage());
        }
        
    }
    
    private int fromByteArray(byte[] bytes) {
        return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
    }
    
    private boolean checkQueryType(Partition partition) { 
        UnfilteredRowIterator it = partition.unfilteredIterator();
        while (it.hasNext()) {
            Unfiltered unfiltered = it.next();
            Row row = (Row) unfiltered;
            if (isInsert(row)) {
                return true;
            }
        }
        return false;
    }

    private boolean isInsert(Row row) {
        return row.primaryKeyLivenessInfo().timestamp() != Long.MIN_VALUE;
    }
       
	private void notifyMusic(String request) {
		logger.info("notifyMusic...");
		Client client = Client.create();
		WebResource webResource = client.resource("http://localhost:8080/MUSIC/rest/v2/admin/callbackOps");
        		
		JSONObject data = new JSONObject();
		data.setData(request);
		ClientResponse response = null;
		try { 
			response = webResource.accept("application/json").type("application/json")
                .post(ClientResponse.class, data);
		} catch (Exception e) {
			logger.info("Exception while notifying MUSIC. Retrying..");
			try { 
				response = webResource.accept("application/json").type("application/json")
	                .post(ClientResponse.class, data);
			} catch (Exception e1) {
				logger.info("Exception while notifying MUSIC. Stopping retry attempts..");
			}
		}
		
		if(response.getStatus() != 200){
			logger.info("Exception while notifying MUSIC...");
			try { 
				response = webResource.accept("application/json").type("application/json")
	                .post(ClientResponse.class, data);
			} catch (Exception e) {
				logger.info("Exception while notifying MUSIC. Retrying..");
				try { 
					response = webResource.accept("application/json").type("application/json")
		                .post(ClientResponse.class, data);
				} catch (Exception e1) {
					logger.info("Exception while notifying MUSIC. Stopping retry attempts..");
				}
			}
        }
	}

}