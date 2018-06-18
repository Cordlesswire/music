/*
 * 
This licence applies to all files in this repository unless otherwise specifically
stated inside of the file. 

 ---------------------------------------------------------------------------
   Copyright (c) 2016 AT&T Intellectual Property

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at:

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 ---------------------------------------------------------------------------

 */
package com.att.research.music.REST;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
//import java.util.logging.Level;


import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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

import com.att.research.music.ecstore.DataFormatter;
import com.att.research.music.ecstore.RowIdentifier;
import com.att.research.music.ecstore.jsonobjects.JsonKeySpace;
import com.att.research.music.ecstore.jsonobjects.JsonTable;
import com.att.research.music.ecstore.jsonobjects.JsonUpdate;
import com.att.research.music.main.Music;
import com.att.research.music.main.Music.Condition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TableMetadata;

import helpers.MusicUtil;
import helpers.ReadReturnType;
import helpers.WriteReturnType;


@Path("/")
public class RestMusicDataAPI {
	final static Logger logger = Logger.getLogger(RestMusicDataAPI.class);

	@GET
	@Path("/version")
	@Produces(MediaType.TEXT_PLAIN)
	public String version() {
		logger.info("Replying to request for MUSIC version with:"+MusicUtil.getVersion());
		return "MUSIC:"+MusicUtil.getVersion();
	}

	@POST
	@Path("/keyspaces/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void createKeySpace(JsonKeySpace  kspObject,@PathParam("name") String keyspace) throws Exception{
		long start = System.currentTimeMillis();
		Map<String,Object> replicationInfo = kspObject.getReplicationInfo();
		String repString = "{"+new DataFormatter().jsonMaptoSqlString(replicationInfo,",")+"}";
		String query ="CREATE KEYSPACE IF NOT EXISTS "+ keyspace +" WITH replication = " + 
				repString;
		if(kspObject.getDurabilityOfWrites() != null)
			query = query +" AND durable_writes = " + kspObject.getDurabilityOfWrites() ;
		query = query + ";";
		long end = System.currentTimeMillis();
		logger.debug("Time taken for setting up query in create keyspace:"+ (end-start));
		new Music().createKeyspace(query);
	}

	@DELETE
	@Path("/keyspaces/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void dropKeySpace(JsonKeySpace  kspObject,@PathParam("name") String keyspace) throws Exception{ 
		String query ="DROP KEYSPACE "+ keyspace+";"; 
		new Music().dropKeyspace(query);
	}


	@POST
	@Path("/keyspaces/{keyspace}/tables/{tablename}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void createTable(JsonTable tableObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String table) throws Exception{ 
		//first read the information about the table fields
		Map<String,String> fields = tableObj.getFields();
		String fieldsString="";
		int counter =0;
		String primaryKey;
		for (Map.Entry<String, String> entry : fields.entrySet())
		{
			fieldsString = fieldsString+""+entry.getKey()+" "+ entry.getValue()+"";
			if(entry.getKey().equals("PRIMARY KEY")){
				primaryKey = entry.getValue().substring(entry.getValue().indexOf("(") + 1);
				primaryKey = primaryKey.substring(0, primaryKey.indexOf(")"));
			}
			if(counter==fields.size()-1)
				fieldsString = fieldsString+")";
			else 
				fieldsString = fieldsString+",";
			counter = counter +1;
		}	



		//information about the name-value style properties 
		Map<String,Object> propertiesMap = tableObj.getProperties();
		String propertiesString="";

		if(propertiesMap != null){
			counter =0;
			for (Map.Entry<String, Object> entry : propertiesMap.entrySet())
			{
				Object ot = entry.getValue();
				String value = ot+"";
				if(ot instanceof String){
					value = "'"+value+"'";
				}else if(ot instanceof Map){
					Map<String,Object> otMap = (Map<String,Object>)ot;
					value = "{"+new DataFormatter().jsonMaptoSqlString(otMap, ",")+"}";
				}
				propertiesString = propertiesString+entry.getKey()+"="+ value+"";
				if(counter!=propertiesMap.size()-1)
					propertiesString = propertiesString+" AND ";
				counter = counter +1;
			}	
		}

		String query =  "CREATE TABLE IF NOT EXISTS "+keyspace+"."+table+" "+ fieldsString; 

		if(propertiesMap != null)
			query = query + " WITH "+ propertiesString;

		query = query +";";
		new Music().createTable(keyspace, table, query);
	}

	
	
	@POST
	@Path("/keyspaces/{keyspace}/tables/{tablename}/index/{field}")
	public void createIndex(@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @PathParam("field") String fieldName,@Context UriInfo info) throws Exception{
		MultivaluedMap<String, String> rowParams = info.getQueryParameters();
		String indexName="";
		if(rowParams.getFirst("index_name") != null)
			indexName = rowParams.getFirst("index_name");	
		String query = "Create index "+indexName+" if not exists on "+keyspace+"."+tablename+" ("+fieldName+");";
		new Music().createIndex(query);
	}
	
	
	@POST
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public void insertIntoTable(JsonUpdate insObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String table) throws Exception{
		Map<String,Object> valuesMap =  insObj.getValues();
		String primaryKeyName= new Music().getPrimaryKey(keyspace, table);
		String fieldsString="";
		String valueString ="";
		int counter =0;
		String primaryKey="";
		for (Map.Entry<String, Object> entry : valuesMap.entrySet()){
			fieldsString = fieldsString+""+entry.getKey();
			Object valueObj = entry.getValue();	
			if(primaryKeyName.equals(entry.getKey())){
				primaryKey= entry.getValue()+"";
				primaryKey = primaryKey.replace("'", "''");
			}

			DataType colType = new Music().getColType(keyspace, table, entry.getKey());
			String formattedValue = new DataFormatter().convertToCQLDataType(colType, valueObj);
			valueString = valueString + formattedValue;
			if(counter==valuesMap.size()-1){
				fieldsString = fieldsString+")";
				valueString = valueString+")";
			}
			else{ 
				fieldsString = fieldsString+",";
				valueString = valueString+",";
			}
			counter = counter +1;
		}

		//System.out.println(valueString);
		String query =  "INSERT INTO "+keyspace+"."+table+" "+ fieldsString+" VALUES "+ valueString;   

		String ttl = insObj.getTtl();
		String timestamp = insObj.getTimestamp();

		if((ttl != null) && (timestamp != null)){
			query = query + " USING TTL "+ ttl +" AND TIMESTAMP "+ timestamp;
		}

		if((ttl != null) && (timestamp == null)){
			query = query + " USING TTL "+ ttl;
		}

		if((ttl == null) && (timestamp != null)){
			query = query + " USING TIMESTAMP "+ timestamp;
		}

		query = query +";";

		String consistency = insObj.getConsistencyInfo().get("type");
		if(consistency.equalsIgnoreCase("eventual"))
			new Music().eventualPut(query);
		else if(consistency.equalsIgnoreCase("critical")){
			String lockId = insObj.getConsistencyInfo().get("lockId");
			new Music().criticalPut(keyspace,table,primaryKey, query, lockId, null);
		}
		else if(consistency.equalsIgnoreCase("atomic")){
			new Music().atomicPut(keyspace,table,primaryKey, query,null);
		}
	}
	
	

	@PUT
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public String updateTable(JsonUpdate updateObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String table, @Context UriInfo info) throws Exception{
		long startTime = System.currentTimeMillis();
		String operationId = UUID.randomUUID().toString();//just for debugging purposes. 
		String consistency = updateObj.getConsistencyInfo().get("type");
		logger.info("--------------Music "+consistency+" update-"+operationId+"-------------------------");
		//obtain the field value pairs of the update
		Map<String,Object> valuesMap =  updateObj.getValues();
		String fieldValueString="";
		int counter =0;
		for (Map.Entry<String, Object> entry : valuesMap.entrySet()){
			Object valueObj = entry.getValue();	
			DataType colType = new Music().getColType(keyspace, table, entry.getKey());
			String valueString = new DataFormatter().convertToCQLDataType(colType,valueObj);	
			fieldValueString = fieldValueString+ entry.getKey()+"="+valueString;
			if(counter!=valuesMap.size()-1)
				fieldValueString = fieldValueString+",";
			counter = counter +1;
		}

		
		String ttl = updateObj.getTtl();
		String timestamp = updateObj.getTimestamp();

		String updateQuery =  "UPDATE "+keyspace+"."+table+" ";   
		if((ttl != null) && (timestamp != null)){
			updateQuery = updateQuery + " USING TTL "+ ttl +" AND TIMESTAMP "+ timestamp;
		}

		if((ttl != null) && (timestamp == null)){
			updateQuery = updateQuery + " USING TTL "+ ttl;
		}

		if((ttl == null) && (timestamp != null)){
			updateQuery = updateQuery + " USING TIMESTAMP "+ timestamp;
		}
		
		//get the row specifier
		RowIdentifier rowId = getRowIdentifier(keyspace, table,  info.getQueryParameters());

		updateQuery = updateQuery + " SET "+fieldValueString+" WHERE "+rowId.rowIdString+";";
		
		//get the conditional, if any
		Condition conditionInfo;
		if(updateObj.getConditions() == null)
			conditionInfo = null;
		else{//to avoid parsing repeatedly, just send the select query to obtain row
			String selectQuery =  "SELECT *  FROM "+keyspace+"."+table+ " WHERE "+rowId.rowIdString+";"; 
			conditionInfo = new Music().createConditionObject(updateObj.getConditions() , selectQuery);
		}


		WriteReturnType operationResult=null;
		long jsonParseCompletionTime = System.currentTimeMillis();
		try {
			if(consistency.equalsIgnoreCase("eventual"))
				operationResult = new Music().eventualPut(updateQuery);
			else if(consistency.equalsIgnoreCase("critical")){
				String lockId = updateObj.getConsistencyInfo().get("lockId");
				operationResult = new Music().criticalPut(keyspace,table,rowId.primarKeyValue, updateQuery, lockId, conditionInfo);
			}
			else if(consistency.equalsIgnoreCase("atomic")){
				operationResult = new Music().atomicPut(keyspace,table,rowId.primarKeyValue, updateQuery,conditionInfo);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		long actualUpdateCompletionTime = System.currentTimeMillis();

		long endTime = System.currentTimeMillis();
		String timingString = "Time taken in ms for Music "+consistency+" update-"+operationId+":"+"|total operation time:"+
			(endTime-startTime)+"|json parsing time:"+(jsonParseCompletionTime-startTime)+"|update time:"+(actualUpdateCompletionTime-jsonParseCompletionTime)+"|";
		
		if(operationResult.getTimingInfo() != null){
			String lockManagementTime = operationResult.getTimingInfo();
			timingString = timingString+lockManagementTime;
		}
		logger.info(timingString);	
		//System.out.println(timingString);
		return operationResult.toString();
	}
	

	@DELETE
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public String deleteFromTable(JsonUpdate delObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info) throws Exception{ 
		String columnString="";
		int counter =0;
		ArrayList<String> columnList = delObj.getColumns();
		if(columnList != null){
			for (String column : columnList) {
				columnString = columnString + column;
				if(counter!=columnList.size()-1)
					columnString = columnString+",";
				counter = counter+1;
			}
		}
		
		//get the row specifier
		RowIdentifier rowId = getRowIdentifier(keyspace, tablename,  info.getQueryParameters());
		String rowSpec = rowId.rowIdString;
		String query ="";

		if((columnList != null) && (!rowSpec.isEmpty())){
			query =  "DELETE "+columnString+" FROM "+keyspace+"."+tablename+ " WHERE "+ rowSpec+";"; 
		}

		if((columnList == null) && (!rowSpec.isEmpty())){
			query =  "DELETE FROM "+keyspace+"."+tablename+ " WHERE "+ rowSpec+";"; 
		}

		if((columnList != null) && (rowSpec.isEmpty())){
			query =  "DELETE "+columnString+" FROM "+keyspace+"."+tablename+ ";"; 
		}

		
		//get the conditional, if any
		Condition conditionInfo;
		if(delObj.getConditions() == null)
			conditionInfo = null;
		else{//to avoid parsing repeatedly, just send the select query to obtain row
			String selectQuery =  "SELECT *  FROM "+keyspace+"."+tablename+ " WHERE "+rowId.rowIdString+";"; 
			conditionInfo = new Music().createConditionObject(delObj.getConditions() , selectQuery);
		}


		String consistency = delObj.getConsistencyInfo().get("type");
		WriteReturnType operationResult=null;

		if(consistency.equalsIgnoreCase("eventual"))
			operationResult = new Music().eventualPut(query);
		else if(consistency.equalsIgnoreCase("critical")){
			String lockId = delObj.getConsistencyInfo().get("lockId");
			operationResult = new Music().criticalPut(keyspace,tablename,rowId.primarKeyValue, query, lockId, conditionInfo);
		}
		else if(consistency.equalsIgnoreCase("atomic")){
			operationResult = new Music().atomicPut(keyspace,tablename,rowId.primarKeyValue, query,conditionInfo);
		}
		return operationResult.toString();
	}

	@DELETE
	@Path("/keyspaces/{keyspace}/tables/{tablename}")
	public void dropTable(JsonTable tabObj,@PathParam("keyspace") String keyspace, @PathParam("tablename") String table) throws Exception{ 
		String query ="DROP TABLE IF EXISTS "+ keyspace+"."+table+";"; 
		new Music().dropTable(query);
	}

	private RowIdentifier getRowIdentifier(String keyspace,String table, MultivaluedMap<String, String> rowParams){
		String rowIdString="";
		int counter =0;
		String primaryKeyValue="";
		for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()){
			String keyName = entry.getKey();
			List<String> valueList = entry.getValue();
			String indValue = valueList.get(0);
			DataType colType = new Music().getColType(keyspace, table, entry.getKey());
			String formattedValue = new DataFormatter().convertToCQLDataType(colType,indValue);	
			if(counter ==0)
				primaryKeyValue = primaryKeyValue+indValue;
			rowIdString = rowIdString + keyName +"="+ formattedValue;
			if(counter!=rowParams.size()-1)
				rowIdString = rowIdString+" AND ";
			counter = counter +1;
		}
		return new RowIdentifier(primaryKeyValue, rowIdString);	
	}
	
	public String selectSpecificQuery(String keyspace,String tablename, UriInfo info, int limit){	
		String rowIdString = getRowIdentifier(keyspace, tablename, info.getQueryParameters()).rowIdString;

		String query =  "SELECT *  FROM "+keyspace+"."+tablename+ " WHERE "+rowIdString; 

		if(limit != -1){
			query = query + " LIMIT "+limit;
		}

		query = query + ";";
		return query; 
	} 


	@PUT
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows/criticalget")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String, HashMap<String, Object>> selectCritical(JsonUpdate selObj,@PathParam("keyspace") String keyspace, @PathParam("tablename") String table, @Context UriInfo info){
		long startTime = System.currentTimeMillis();
		String operationId = UUID.randomUUID().toString();//just for debugging purposes. 
		String consistency = selObj.getConsistencyInfo().get("type");
		logger.info("--------------Music "+consistency+" select-"+operationId+"-------------------------");

		String lockReference = selObj.getConsistencyInfo().get("lockId");

		RowIdentifier rowId = getRowIdentifier(keyspace, table,  info.getQueryParameters());

		String selectQuery =  "SELECT *  FROM "+keyspace+"."+table+ " WHERE "+rowId.rowIdString+";"; 

		
		long jsonParseCompletionTime = System.currentTimeMillis();

		ReadReturnType operationResult =null;
		if(consistency.equalsIgnoreCase("critical"))
			operationResult =  new Music().criticalGet(keyspace, table, rowId.primarKeyValue, selectQuery, lockReference);
		else if(consistency.equalsIgnoreCase("atomic"))
			operationResult = new Music().atomicGet(keyspace,table,rowId.primarKeyValue, selectQuery);
		long actualSelectTime = System.currentTimeMillis();
		
		long endTime = System.currentTimeMillis();
		String timingString = "Time taken in ms for Music "+consistency+" select-"+operationId+":"+"|total operation time:"+
			(endTime-startTime)+"|json parsing time:"+(jsonParseCompletionTime-startTime)+"|select time:"+(actualSelectTime-jsonParseCompletionTime)+"|";
		
		if(operationResult.getTimingInfo() != null){
			String lockManagementTime = operationResult.getTimingInfo();
			timingString = timingString+lockManagementTime;
		}
		logger.info(timingString);	


		return new DataFormatter().marshalData(operationResult.getPayload());
	}


	@GET
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String, HashMap<String, Object>> select(@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info){
		long startTime = System.currentTimeMillis();
		String operationId = UUID.randomUUID().toString();//just for debugging purposes. 
		logger.info("--------------Music eventual select-"+operationId+"-------------------------");

		String query ="";
		if(info.getQueryParameters().isEmpty())//select all
			query =  "SELECT *  FROM "+keyspace+"."+tablename+ ";"; 
		else{
			int limit =-1; //do not limit the number of results
			query = selectSpecificQuery(keyspace,tablename,info,limit);
		}
		long jsonParseCompletionTime = System.currentTimeMillis();

		ResultSet results = new Music().eventualGet(query);
		
		long actualSelectTime = System.currentTimeMillis();
		
		long endTime = System.currentTimeMillis();
		String timingString = "Time taken in ms for Music eventual select-"+operationId+":"+"|total operation time:"+
			(endTime-startTime)+"|json parsing time:"+(jsonParseCompletionTime-startTime)+"|select time:"+(actualSelectTime-jsonParseCompletionTime)+"|";
		logger.info(query);
		logger.info(timingString);	
		return new DataFormatter().marshalData(results);
	} 
	

	
}
