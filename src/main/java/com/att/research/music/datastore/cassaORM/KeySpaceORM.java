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

import com.att.research.music.datastore.DataFormatter;


public class KeySpaceORM {
	private String keyspace;
	private Map<String,Object> replicationInfo;
	private String durabilityOfWrites;
    private Map<String,String> consistencyInfo;

	public Map<String, String> getConsistencyInfo() {
		return consistencyInfo;
	}

	public void setConsistencyInfo(Map<String, String> consistencyInfo) {
		this.consistencyInfo = consistencyInfo;
	}

	public Map<String, Object> getReplicationInfo() {
		return replicationInfo;
	}
	
	public void setReplicationInfo(Map<String, Object> replicationInfo) {
		this.replicationInfo = replicationInfo;
	}

	public String getDurabilityOfWrites() {
		return durabilityOfWrites;
	}
	public void setDurabilityOfWrites(String durabilityOfWrites) {
		this.durabilityOfWrites = durabilityOfWrites;
	}
    public String getKeyspaceName() {
		return keyspace;
	}

	public void setKeyspaceName(String keyspaceName) {
		this.keyspace = keyspaceName;
	}

	public String createKeySpaceQuery() {
		Map<String,Object> replicationInfo = getReplicationInfo();
		String repString = "{"+new DataFormatter().jsonMaptoSqlString(replicationInfo,",")+"}";
		String query ="CREATE KEYSPACE IF NOT EXISTS "+ keyspace +" WITH replication = " + 
				repString;
		if(getDurabilityOfWrites() != null)
			query = query +" AND durable_writes = " + getDurabilityOfWrites() ;
		query = query + ";";		
		return query;
	}
	
	public String dropKeySpaceQuery() {
		String query ="DROP KEYSPACE "+ keyspace+";"; 
		return query;
	}

}
