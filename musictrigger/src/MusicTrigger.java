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
import java.util.Iterator;

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
    	boolean isDelete = false;
    	logger.info("Step 1: "+partition.partitionLevelDeletion().isLive());
    	if(partition.partitionLevelDeletion().isLive()) {
    		
    	} else {
            // Partition Level Deletion
        	isDelete = true;
        }
    	logger.info("MusicTrigger isDelete: " + isDelete);
    	String ksName = partition.metadata().ksName;
        String tableName = partition.metadata().cfName;
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
        
        
        obj.put("operation", operation);
        obj.put("keyspace", ksName);
        obj.put("table_name", tableName);
        obj.put("full_table", ksName+"."+tableName);
        obj.put("primary_key", partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey()));
        
        //obj.put("message_id", partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey()));
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
                    String data = new String(cell.value().array()); // If cell type is text
                    logger.info("Inside triggers loop: "+columnDef.toString()+" : "+data);
                    obj.put(columnDef.toString(), data);
                }
            }
        } catch (Exception e) {

        }
        logger.info("Sending response: "+obj.toString());
        try {
            notifyMusic(obj.toString());
        } catch(Exception e) {
            e.printStackTrace();
            logger.error("Notification failed..."+e.getMessage()s);
        }
        return Collections.emptyList();
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
		System.out.println("notifyMusic...");
		Client client = Client.create();
		WebResource webResource = client.resource("http://localhost:8080/MUSIC/rest/v2/admin/callbackOps");
        		
		JSONObject data = new JSONObject();
		data.setData(request);
		
		ClientResponse response = webResource.accept("application/json").type("application/json")
                .post(ClientResponse.class, data);
		
		if(response.getStatus() != 200){
			System.out.println("Exception...");
        }
		response.getHeaders().put(HttpHeaders.CONTENT_TYPE, Arrays.asList(MediaType.APPLICATION_JSON));
		response.bufferEntity();
		String x = response.getEntity(String.class);
		System.out.println("Response: "+x);
		
	}

	/*public Collection<Mutation> augment(Partition partition) {
		
		String tableName = partition.metadata().cfName;
        System.out.println("Table: " + tableName);

        JSONObject obj = new JSONObject();
        obj.put("message_id", partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey()));

        
	    try {
	        UnfilteredRowIterator it = partition.unfilteredIterator();
	        while (it.hasNext()) {
	            Unfiltered un = it.next();
	            Clustering clt = (Clustering) un.clustering();  
	            Iterator<Cell> cls = partition.getRow(clt).cells().iterator();
                Iterator<ColumnDefinition> columns = partition.getRow(clt).columns().iterator();

	            while(cls.hasNext()){
	                Cell cell = cls.next();
	                String data = new String(cell.value().array()); // If cell type is text
	                System.out.println(cell + " : " +data);
	                
	            }
	            while(columns.hasNext()){
                    ColumnDefinition columnDef = columns.next();
                    Cell cell = cls.next();
                    String data = new String(cell.value().array()); // If cell type is text
                    obj.put(columnDef.toString(), data);
                }
	        }
	    } catch (Exception e) {
	    }
	    
	    System.out.println(obj.toString());

        return Collections.emptyList();
	}*/
	
}