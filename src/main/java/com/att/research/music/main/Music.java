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
package com.att.research.music.main;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.att.research.music.ecstore.CassaECStore;
import com.att.research.music.ecstore.DataFormatter;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.ColumnDefinitions.Definition;

import helpers.ReadReturnType;
import helpers.ResultType;
import helpers.WriteReturnType;

public class Music {

	final  Logger logger = Logger.getLogger(Music.class);
	CassaECStore   dataStore;
	DataFormatter dataFormatter; 

	public Music() {
		dataStore = new CassaECStore();
		dataFormatter = new DataFormatter();
	}


	public   String createLockReference(String keyspace, String table, String key){
		UUID lockReferenceUUID = dataStore.createLockReference(keyspace, table, key);
		return lockReferenceUUID+"";
	}

	public  WriteReturnType  acquireLock(String keyspace, String table, String key, String lockReference){
		try {
			if(dataStore.isItMyTurn(keyspace, table, key, UUID.fromString(lockReference)) == false)
				return new WriteReturnType(ResultType.FAILURE,"You are the not the lock holder"); 

			syncIfRequired(keyspace, table, key);

			return new WriteReturnType(ResultType.SUCCESS,"You are the lock holder");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			return new WriteReturnType(ResultType.FAILURE,"Exception thrown while doing the acquire lock:\n"+exceptionAsString); 

		}
	}


	public  class Condition{
		Map<String, Object> conditions;
		String selectQueryForTheRow;
		public Condition(Map<String, Object> conditions, String selectQueryForTheRow) {
			this.conditions = conditions;
			this.selectQueryForTheRow = selectQueryForTheRow;
		} 	
		public boolean testCondition(){
			//first generate the row
			ResultSet results = quorumGet(selectQueryForTheRow);
			Row row = results.one();
			return dataStore.doesRowSatisfyCondition(row, conditions);
		}
	}


	public  WriteReturnType criticalPut(String keyspace, String table, String key, String query, String lockReference, Condition conditionInfo){
		if(dataStore.isItMyTurn(keyspace, table, key, UUID.fromString(lockReference)) == false)
			return new WriteReturnType(ResultType.FAILURE,"Cannot perform operation since you are the not the lock holder"); 
		else 
			return conditionalPut(query, conditionInfo);
	}

	private WriteReturnType conditionalPut(String query, Condition conditionInfo) {
		try {
		if(conditionInfo != null)//check if condition is true
			if(conditionInfo.testCondition() == false)
				return new WriteReturnType(ResultType.FAILURE,"Lock acquired but the condition is not true"); 

			dataStore.executePut(query,"critical");
			return new WriteReturnType(ResultType.SUCCESS,"Update performed"); 
		}catch (Exception e) {
			// TODO Auto-generated catch block
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			return new WriteReturnType(ResultType.FAILURE,"Exception thrown while doing the critical put, either you are not connected to a quorum or the condition is wrong:\n"+exceptionAsString); 
		}	
	}
	
	public  ReadReturnType criticalGet(String keyspace, String table, String key, String query, String lockReference){
		if(dataStore.isItMyTurn(keyspace, table, key, UUID.fromString(lockReference)) == false)
			return new ReadReturnType(ResultType.FAILURE,"Cannot perform operation since you are the not the lock holder",null); 
		ResultSet dataReturned = dataStore.executeCriticalGet(query);			
		return new ReadReturnType(ResultType.SUCCESS,"Select performed", dataReturned); 
	}


	public  WriteReturnType atomicPut(String keyspace, String table, String key, String query, Condition conditionInfo){
		long start = System.currentTimeMillis();
		String lockReference = createLockReference(keyspace, table, key);
		long lockCreationTime = System.currentTimeMillis();

		WriteReturnType lockAcqResult = acquireLock(keyspace, table, key, lockReference);
		long lockAcqTime = System.currentTimeMillis();

		if(lockAcqResult.getResult().equals(ResultType.SUCCESS)){
			WriteReturnType conditionalPut = conditionalPut(query, conditionInfo);
			long conditionalPutTime = System.currentTimeMillis();

			dataStore.releaseLockReference(keyspace, table, key, UUID.fromString(lockReference));
			long lockRefReleaseTime = System.currentTimeMillis();

			String timingInfo = "|lock creation time:"+(lockCreationTime-start)+"|lock accquire time:"+(lockAcqTime-lockCreationTime)
					+"|conditional put time:"+(conditionalPutTime-lockAcqTime)+"|lock release time:"+(lockRefReleaseTime-conditionalPutTime)+"|";
			conditionalPut.setTimingInfo(timingInfo);
			return conditionalPut;
		}
		else{
			dataStore.releaseLockReference(keyspace, table, key, UUID.fromString(lockReference));
			return lockAcqResult;	
		}
	}


	public  ReadReturnType atomicGet(String keyspace, String table, String key, String query){
		long start = System.currentTimeMillis();
		String lockReference = createLockReference(keyspace, table, key);
		long lockCreationTime = System.currentTimeMillis();

		WriteReturnType lockAcqResult = acquireLock(keyspace, table, key, lockReference);
		long lockAcqTime = System.currentTimeMillis();
		
		if(lockAcqResult.getResult().equals(ResultType.SUCCESS)){
			ResultSet dataReturned = dataStore.executeCriticalGet(query);		
			ReadReturnType criticalGetResult = new ReadReturnType(ResultType.SUCCESS,"Select performed", dataReturned); 
			long criticalGetTime = System.currentTimeMillis();
			
			dataStore.releaseLockReference(keyspace, table, key, UUID.fromString(lockReference));
			long lockRefReleaseTime = System.currentTimeMillis();
			
			String timingInfo = "|lock creation time:"+(lockCreationTime-start)+"|lock accquire time:"+(lockAcqTime-lockCreationTime)
					+"|get time:"+(criticalGetTime-lockAcqTime)+"|lock release time:"+(lockRefReleaseTime-criticalGetTime)+"|";
			criticalGetResult.setTimingInfo(timingInfo);
			return criticalGetResult;
		}
		else{
			dataStore.releaseLockReference(keyspace, table, key, UUID.fromString(lockReference));
			return new ReadReturnType(lockAcqResult.getResultType(),lockAcqResult.getMessage(),null);	
		}
	}


	public   String topOfLockQ(String keyspace, String table, String key){
		String currentHolder = dataStore.whoIsTopOfLockQ(keyspace, table, key);
		return currentHolder;
	}


	public  void releaseLock(String keyspace, String table, String key, String lockReference){
		dataStore.releaseLockReference(keyspace, table, key, UUID.fromString(lockReference));

	}
	
	public  void deleteLock(String keyspace, String table, String key){
		dataStore.deleteLock(keyspace, table, key);
	}


	//helper functions
	private  void syncIfRequired(String keyspace, String table, String key) {
		//check to see if the key is in an unsynced state
		String query = "select * from music_internal.unsynced_keys where key='"+key+"';";
		ResultSet syncFlagresults = dataStore.executeCriticalGet(query);
		if (syncFlagresults.all().size() != 0) {
			logger.info("In acquire lock: Since there was a forcible release, need to sync quorum!");
			logger.info("Performing sync operation---");
			String[] splitString = key.split("\\.");
			String keyspaceName = splitString[0];
			String tableName = splitString[1];
			String primaryKeyValue = splitString[2];

			//get the primary key d
			TableMetadata tableInfo = dataStore.returnColumnMetadata(keyspace, tableName);
			String primaryKeyName = tableInfo.getPrimaryKey().get(0).getName();//we only support single primary key
			DataType primaryKeyType = tableInfo.getPrimaryKey().get(0).getType();
			String cqlFormattedPrimaryKeyValue = dataFormatter.convertToCQLDataType(primaryKeyType, primaryKeyValue);

			//get the row of data from a quorum
			String selectQuery =  "SELECT *  FROM "+keyspaceName+"."+tableName+ " WHERE "+primaryKeyName+"="+cqlFormattedPrimaryKeyValue+";"; 
			ResultSet results = dataStore.executeCriticalGet(selectQuery);

			//write it back to a quorum
			Row row = results.one();
			ColumnDefinitions colInfo = row.getColumnDefinitions();
			int totalColumns = colInfo.size();
			int counter =1;
			String fieldValueString="";
			for (Definition definition : colInfo){
				String colName = definition.getName();
				if(colName.equals(primaryKeyName))
					continue; 
				DataType colType = definition.getType();
				Object valueObj = dataFormatter.getColValue(row, colName, colType);	
				String valueString = dataFormatter.convertToCQLDataType(colType,valueObj);	
				fieldValueString = fieldValueString+ colName+"="+valueString;
				if(counter!=(totalColumns-1))
					fieldValueString = fieldValueString+",";
				counter = counter +1;
			}

			String updateQuery =  "UPDATE "+keyspaceName+"."+tableName+" SET "+fieldValueString+" WHERE "+primaryKeyName+"="+cqlFormattedPrimaryKeyValue+";";
			dataStore.executePut(updateQuery, "critical");
		}
	}
	
	
	
	/*
	 * Supplementary functions that do not use locking
	 */
	
	public void createKeyspace(String createKeyspaceQuery) throws Exception {
		dataStore.executePut(createKeyspaceQuery, "eventual");
	}

	public void dropKeyspace(String dropKeyspaceQuery) throws Exception {
		dataStore.executePut(dropKeyspaceQuery, "eventual");
	}

	public void createTable(String keyspace, String table, String createTableQuery) {
		dataStore.executePut(createTableQuery, "eventual");
		//internal table just to store locks
		dataStore.createLockingTable(keyspace, table);

	}

	public void dropTable(String dropTableQuery) {
		dataStore.executePut(dropTableQuery, "eventual");
	}
	

	public void createIndex(String createIndexQuery) {
		dataStore.executePut(createIndexQuery, "eventual");
	}

	public WriteReturnType eventualPut(String query){
		dataStore.executePut(query, "eventual");
		return new WriteReturnType(ResultType.SUCCESS,""); 
	}

	public   ResultSet eventualGet(String query){
		ResultSet results = dataStore.executeEventualGet(query);
		return results;
	}

	public   ResultSet quorumGet(String query){
		ResultSet results = dataStore.executeCriticalGet(query);
		return results;

	}
	
	public String getPrimaryKey(String keyspace, String table) {
		TableMetadata tableInfo = dataStore.returnColumnMetadata(keyspace, table);
		String primaryKey = tableInfo.getPrimaryKey().get(0).getName();	
		return primaryKey; 
	}
	
	public DataType getColType(String keyspace, String table, String key) {
		TableMetadata tableInfo = dataStore.returnColumnMetadata(keyspace, table);
		return tableInfo.getColumn(key).getType();

	}

	public Condition createConditionObject(Map<String, Object> conditions, String selectQueryForTheRow) {
		return new Condition(conditions, selectQueryForTheRow);
	}
}
