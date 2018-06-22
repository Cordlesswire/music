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
package com.att.research.music.datastore;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.utils.UUIDs;

public class CassaStore {
	private Session session;
	private Cluster cluster;
	DataFormatter dataFormatter; 
	final static Logger logger = Logger.getLogger(CassaStore.class);
	private static final  long leasePeriod = 5000;//5 seconds
	private static final long epochPeriod = TimeUnit.DAYS.toMillis(365);//1 year in millseconds.


	public CassaStore(){
		connectToCassaCluster();
		dataFormatter = new DataFormatter();
	}

	public CassaStore(String remoteIp){
		connectToCassaCluster(remoteIp);
		dataFormatter = new DataFormatter();
	}

	private ArrayList<String> getAllPossibleLocalIps(){
		ArrayList<String> allPossibleIps = new ArrayList<String>();
		try {
			Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
			while(en.hasMoreElements()){
			    NetworkInterface ni=(NetworkInterface) en.nextElement();
			    Enumeration<InetAddress> ee = ni.getInetAddresses();
			    while(ee.hasMoreElements()) {
			        InetAddress ia= (InetAddress) ee.nextElement();
			        allPossibleIps.add(ia.getHostAddress());
			    }
			 }
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return allPossibleIps;
	}
	
	private void connectToCassaCluster(){
		Iterator<String> it = getAllPossibleLocalIps().iterator();
		String address= "localhost";
		logger.debug("Connecting to cassa cluster: Iterating through possible ips:"+getAllPossibleLocalIps());
		while(it.hasNext()){
			try {
				cluster = Cluster.builder().withPort(9042).addContactPoint(address).build();
				Metadata metadata = cluster.getMetadata();
				logger.debug("Connected to cassa cluster "+metadata.getClusterName()+" at "+address);
				session = cluster.connect();			
				break;
			} catch (NoHostAvailableException e) {
				address= it.next();
			} 
		}
	}
	

	private void connectToCassaCluster(String address){	
		cluster = Cluster.builder().withPort(9042).addContactPoint(address).build();
		Metadata metadata = cluster.getMetadata();
		logger.debug("Connected to cassa cluster "+metadata.getClusterName()+" at "+address);
		session = cluster.connect();
	}

	public ResultSet executeEventualGet(String query){
		logger.info("Executing normal get query:"+query);
		long start = System.currentTimeMillis();
		Statement statement = new SimpleStatement(query);
		statement.setConsistencyLevel(ConsistencyLevel.ONE);
		ResultSet results = session.execute(statement);
		long end = System.currentTimeMillis();
		logger.debug("Time taken for actual get in cassandra:"+(end-start));
		return results;	
	}

	public ResultSet executeCriticalGet(String query){
		Statement statement = new SimpleStatement(query);
		logger.info("Executing critical get query:"+query);
		statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
		ResultSet results = session.execute(statement);
		return results;	
	}

	public DataType returnColumnDataType(String keyspace, String tableName, String columnName){
		KeyspaceMetadata ks = cluster.getMetadata().getKeyspace(keyspace);
		TableMetadata table = ks.getTable(tableName);
		return table.getColumn(columnName).getType();

	}

	public TableMetadata returnColumnMetadata(String keyspace, String tableName){
		KeyspaceMetadata ks = cluster.getMetadata().getKeyspace(keyspace);
		return ks.getTable(tableName);
	}

	public void atomicInsert() {
		
	}
	public void executePut(String query, String consistency){
		logger.debug("in data store handle, executing put:"+query);
		long start = System.currentTimeMillis();
		Statement statement = new SimpleStatement(query);
		if(consistency.equalsIgnoreCase("critical")){
		//	logger.info("Executing critical put query");
			statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
		}
		else if (consistency.equalsIgnoreCase("eventual")){
			logger.info("Executing normal put query..");
			statement.setConsistencyLevel(ConsistencyLevel.ONE);
		}		
		else if (consistency.equalsIgnoreCase("serial")){
		//	logger.info("Executing serial put query");
			statement.setConsistencyLevel(ConsistencyLevel.SERIAL);
		}
		else if (consistency.equalsIgnoreCase("local_serial")){
		//	logger.info("Executing local serial put query");
			statement.setConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
		}

		session.execute(statement); 
		long end = System.currentTimeMillis();
		logger.debug("Time taken for actual put in cassandra:"+(end-start));
	}


	public boolean doesRowSatisfyCondition(Row row, Map<String, Object> condition){
		ColumnDefinitions colInfo = row.getColumnDefinitions();
		
		for (Map.Entry<String, Object> entry : condition.entrySet()){
			String colName = entry.getKey();
			DataType colType = colInfo.getType(colName);
			Object columnValue = dataFormatter.getColValue(row, colName, colType);
			Object conditionValue = dataFormatter.convertToActualDataType(colType, entry.getValue());
			if(columnValue.equals(conditionValue) == false)
				return false;		
		}
		return true;	
	}


	
	
	public void createLockingTable(String keyspace, String table) {
		table = "locks_"+table; 
		String tabQuery = "CREATE TABLE IF NOT EXISTS "+keyspace+"."+table
				+ " (key text, lockReferenceUUID timeuuid, owner text,  metadata text, PRIMARY KEY ((key), lockReferenceUUID, owner) ) "
				+ "WITH CLUSTERING ORDER BY (lockReferenceUUID ASC);";
		System.out.println(tabQuery);
		
		executePut(tabQuery, "critical");
	}
	
	public UUID createLockReference(String keyspace, String table, String key) {
		table = "locks_"+table; 
		UUID timeBasedUuid = UUIDs.timeBased();
		String values = "('"+key+"',"+timeBasedUuid+",'user','no-lock-acquisition-status')";
		String insQuery = "INSERT INTO "+keyspace+"."+table+"(key, lockReferenceUUID, owner, metadata) VALUES"+values+" IF NOT EXISTS;";	
		executePut(insQuery, "critical");	
		return timeBasedUuid;
	}
	
	public boolean isTopOfLockQ(String keyspace, String table, String key, UUID lockReferenceUUID) {
		String topOfQ = whoIsTopOfLockQ(keyspace, table, key);
		return lockReferenceUUID.toString().equals(topOfQ);
	}
	
	
	public void releaseLockReference(String keyspace, String table, String key, UUID lockReferenceUUID) {
		table = "locks_"+table; 
		String deleteQuery = "delete from "+keyspace+"."+table+" where key='"+key+"' AND lockReferenceUUID ="+lockReferenceUUID+" IF EXISTS;";	
		executePut(deleteQuery, "critical");	
	}
	
	
	public void releaseLockHolderIfExpired(String keyspace, String table, String key) {
		//obtain row at top of lock queue for the key
		table = "locks_"+table; 
		
		//obtain both current lock holder and the one next in line 
		String selectQuery = "select * from "+keyspace+"."+table+" where key='"+key+"' and owner = 'user' LIMIT 2;";		
		ResultSet results = session.execute(selectQuery);
		
		//check if current lock holder has exceeded his lease
		UUID lockReferenceUUID = results.one().getUUID("lockReferenceUUID");
		long currentTime = UUIDs.timeBased().timestamp();//to compare meaningfully
		
		if((lockReferenceUUID.timestamp()-currentTime) < leasePeriod)
			return; 
		else {
			/* the current lock holder has exceeded the lease -- proceed with the activities to ensure
			 * that this lock holder does not corrupt a future critical section
			 */
			
			//get current epoch and increment it
			incrementEpoch(keyspace, table, key);
			
			
			//tell the next in line that it was a forceful acquisition 
			
		}
		


	}
	
	public class Epoch{
		int epochNumber;
		long epochStartTime;
		public Epoch(int epochNumber, long epochStartTime) {
			this.epochNumber = epochNumber;
			this.epochStartTime = epochStartTime;
		}
		
	}
	
	public int getCurrentEpoch(String keyspace, String table, String key) {
		int currentEpoch =0;
		String selectQuery = "select * from "+keyspace+"."+table+" where key='"+key+"' and owner = 'admin';";		
		ResultSet results = session.execute(selectQuery);
		if(results.all().size() !=0) {
			String metadata = results.one().getString("metadata");
			String[] splitMetaData = metadata.split("||");
			String[] epochInfo = splitMetaData[0].split(":");
			currentEpoch = Integer.parseInt(epochInfo[1]);
		}
		return currentEpoch;
	}

	public void incrementEpoch(String keyspace, String table, String key) {
		int newEpoch = getCurrentEpoch(keyspace, table, key)+ 1; 
		String updateQuery = "update "+keyspace+"."+table+" set where key='"+key+"' and owner = 'admin';";		

		
	}
	
	public String whoIsTopOfLockQ(String keyspace, String table, String key) {
		table = "locks_"+table; 
		String selectQuery = "select * from "+keyspace+"."+table+" where key='"+key+"' LIMIT 1;";	
		
		ResultSet results = session.execute(selectQuery);
		return results.one().getUUID("lockReferenceUUID")+"";
	}

	public  void deleteLock(String keyspace, String table, String key){
		table = "locks_"+table; 
		String deleteQuery = "delete from "+keyspace+"."+table+" where key='"+key+"';";	
		session.execute(deleteQuery);
	}


	
	public static void main(String[] args) {
		System.out.println(CassaStore.epochPeriod);
		System.out.println(60 * 60 * 1000);
		System.out.println(60 * 60 * 1000*365*24);
		
	/*	
		CassaStore ds = new CassaStore();
		String keyspace = "bmkeyspace";
		String table = "locktesttable";
		ds.createLockingTable(keyspace, table);
		
		UUID lockRefb1 = ds.createLockReference(keyspace, table, "bharath");
		UUID lockRefc1 = ds.createLockReference(keyspace, table, "cat");

		UUID lockRefb2 = ds.createLockReference(keyspace, table, "bharath");
		UUID lockRefc2 = ds.createLockReference(keyspace, table, "cat");

		System.out.println(ds.isTopOfLockQ(keyspace, table, "bharath", lockRefb1));
		
		System.out.println(ds.isTopOfLockQ(keyspace, table, "cat", lockRefc2));
		
		System.out.println(ds.isTopOfLockQ(keyspace, table, "bharath", lockRefb2));

		
		ds.releaseLockReference(keyspace, table, "cat", lockRefc1);

		System.out.println(ds.isTopOfLockQ(keyspace, table, "cat", lockRefc2));

		System.out.println(ds.isTopOfLockQ(keyspace, table, "cat", lockRefc1));
		*/
	}

}
