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
package com.att.research.music.datastore.cassaORM;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

import com.att.research.music.datastore.DataFormatter;
import com.att.research.music.main.Music;
import com.datastax.driver.core.DataType;

public class RowORM implements Serializable {
	private String keyspace;
	private String table;
    private Map<String,Object> values;
    private String ttl, timestamp;
    private Map<String,String> consistencyInfo;
    private Map<String,Object> conditions;
    private ArrayList<String> columns = null;

    
    //start benchmarking stuff
    private int batchSize; 
    private int mixSize; 
    private int dataSize; 
	private String operationType;
    
    
    public ArrayList<String> getColumns() {
		return columns;
	}
	public void setColumns(ArrayList<String> columns) {
		this.columns = columns;
	}

    public String getOperationType() {
		return operationType;
	}

	public void setOperationType(String operationType) {
		this.operationType = operationType;
	}

    

	public int getDataSize() {
		return dataSize;
	}

	public void setDataSize(int dataSize) {
		this.dataSize = dataSize;
	}

	public int getMixSize() {
		return mixSize;
	}

	public void setMixSize(int mixSize) {
		this.mixSize = mixSize;
	}

	public int getEvAtRatio() {
		return evAtRatio;
	}

	public void setEvAtRatio(int evAtRatio) {
		this.evAtRatio = evAtRatio;
	}
	private int startId; 
    private int evAtRatio; 
	private String keyName;
    private String baseKeyValue;

    public String getKeyName() {
		return keyName;
	}

	public void setKeyName(String keyName) {
		this.keyName = keyName;
	}

	public String getBaseKeyValue() {
		return baseKeyValue;
	}

	public void setBaseKeyValue(String baseKeyValue) {
		this.baseKeyValue = baseKeyValue;
	}
    
    public int getStartId() {
		return startId;
	}

	public void setStartId(int startId) {
		this.startId = startId;
	}


    
	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	//end  benchmarking stuff
	
	public Map<String, Object> getConditions() {
		return conditions;
	}

	public void setConditions(Map<String, Object> conditions) {
		this.conditions = conditions;
	}

	public Map<String, Object> getRow_specification() {
		return row_specification;
	}

	public void setRow_specification(Map<String, Object> row_specification) {
		this.row_specification = row_specification;
	}
	private Map<String,Object> row_specification;

	public String getKeyspaceName() {
		return keyspace;
	}

	public void setKeyspaceName(String keyspaceName) {
		this.keyspace = keyspaceName;
	}

	public String getTableName() {
		return table;
	}

	public void setTableName(String tableName) {
		this.table = tableName;
	}

    
	public Map<String, String> getConsistencyInfo() {
		return consistencyInfo;
	}

	public void setConsistencyInfo(Map<String, String> consistencyInfo) {
		this.consistencyInfo = consistencyInfo;
	}

	public String getTtl() {
		return ttl;
	}
	public void setTtl(String ttl) {
		this.ttl = ttl;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public Map<String, Object> getValues() {
		return values;
	}
	public void setValues(Map<String, Object> values) {
		this.values = values;
	}
	public byte[] serialize(){
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);   
			out.writeObject(this);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return bos.toByteArray();
	}

	public String insertQuery() {
		return  "INSERT INTO "+basePartOfInsertQuery();
	}
	
	public String insertIfNotExistsQuery() {
		return  "INSERT IF NOT EXISTS INTO "+basePartOfInsertQuery();
	}
	
	private String basePartOfInsertQuery() {
		Map<String,Object> valuesMap =  getValues();
		String fieldsString="";
		String valueString ="";
		int counter =0;
		for (Map.Entry<String, Object> entry : valuesMap.entrySet()){
			fieldsString = fieldsString+""+entry.getKey();
			Object valueObj = entry.getValue();	

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
		String query = keyspace+"."+table+" "+ fieldsString+" VALUES "+ valueString;   

		String ttl = getTtl();
		String timestamp = getTimestamp();

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
		return query;

		
	}
}
