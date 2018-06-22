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
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.att.research.music.datastore.DataFormatter;

public class TableORM {
	private String keyspace;
	private String table;

	private Map<String,String> fields;
	private Map<String, Object> properties; 
	private String primaryKey; 
	private String sortingKey;
    private String clusteringOrder;
    private Map<String,String> consistencyInfo;

	public Map<String, String> getConsistencyInfo() {
		return consistencyInfo;
	}

	public void setConsistencyInfo(Map<String, String> consistencyInfo) {
		this.consistencyInfo = consistencyInfo;
	}

    public Map<String, Object> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, Object> properties) {
		this.properties = properties;
	}
    
	public Map<String, String> getFields() {
		return fields;
	}

	public void setFields(Map<String, String> fields) {
		this.fields = fields;
	}

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
	public String getSortingKey() {
		return sortingKey;
	}

	public void setSortingKey(String sortingKey) {
		this.sortingKey = sortingKey;
	}

	public String getClusteringOrder() {
		return clusteringOrder;
	}

	public void setClusteringOrder(String clusteringOrder) {
		this.clusteringOrder = clusteringOrder;
	}
	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String createTableQuery() {
		//first read the information about the table fields
		Map<String,String> fields = getFields();
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
		Map<String,Object> propertiesMap = getProperties();
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
		return query;
	}
	
	public String dropTableQuery() {
		String query ="DROP TABLE IF EXISTS "+ keyspace+"."+table+";"; 
		return query;

	}
	
}
