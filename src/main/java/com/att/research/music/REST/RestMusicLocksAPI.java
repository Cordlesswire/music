package com.att.research.music.REST;

import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.att.research.music.ecstore.jsonobjects.JsonLeasedLock;
import com.att.research.music.ecstore.jsonobjects.JsonLockResponse;
import com.att.research.music.main.Music;

import helpers.ResultType;
import helpers.WriteReturnType;


@Path("/locks/")
public class RestMusicLocksAPI {
	final static Logger logger = Logger.getLogger(RestMusicLocksAPI.class);
	/*	puts the requesting process in the q for this lock. The corresponding node will be
	created in zookeeper if it did not already exist*/
	@POST
	@Path("/create/keyspaces/{keyspace}/tables/{tablename}/key/{key}")
	@Produces(MediaType.TEXT_PLAIN)	
	public String createLockReference(@PathParam("keyspace") String keyspace, 
			@PathParam("tablename") String table, @PathParam("key")String key){
		return new Music().createLockReference(keyspace, table, key);
	}

	
	//checks if the node is in the top of the queue and hence acquires the lock
	@GET
	@Path("/acquire/keyspaces/{keyspace}/tables/{tablename}/key/{key}/lockref/{lockReference}")
	@Produces(MediaType.TEXT_PLAIN)	
	public String accquireLock(@PathParam("keyspace") String keyspace, @PathParam("tablename") String table, 
			@PathParam("key")String key,@PathParam("lockreference") String lockReference){
        WriteReturnType lockStatus = new Music().acquireLock(keyspace, table, key, lockReference);
        if(lockStatus.getResultType().equals(ResultType.SUCCESS))
        		return true+"";
        else
        		return false+""; 
    }
	
	@GET
	@Path("/enquire/keyspaces/{keyspace}/tables/{tablename}/key/{key}")
	@Produces(MediaType.TEXT_PLAIN)	
	public String currentLockHolder(@PathParam("keyspace") String keyspace, 
			@PathParam("tablename") String table, @PathParam("key")String key){
		return new Music().topOfLockQ(keyspace, table, key);
	}

	@DELETE
	@Path("/delete/keyspaces/{keyspace}/tables/{tablename}/key/{key}/lockref/{lockReference}")
	public void releaseLock(@PathParam("keyspace") String keyspace, @PathParam("tablename") String table, 
			@PathParam("key")String key,@PathParam("lockreference") String lockReference){
		new Music().releaseLock(keyspace, table, key, lockReference);
	}

	@DELETE
	@Path("/delete/keyspaces/{keyspace}/tables/{tablename}/key/{key}")
	public void deleteLock(@PathParam("keyspace") String keyspace, @PathParam("tablename") String table, 
			@PathParam("key")String key){
		new Music().deleteLock(keyspace, table, key);
	}

}
