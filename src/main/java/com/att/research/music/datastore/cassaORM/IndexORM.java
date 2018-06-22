package com.att.research.music.datastore.cassaORM;

public class IndexORM {

	private String keyspace;
	private String table;
	private String indexName;
	private String fieldName;
	
	public IndexORM(String keyspace, String table, String indexName, String fieldName) {
		this.keyspace = keyspace;
		this.table = table;
		this.indexName = indexName;
		this.fieldName = fieldName;
	}

	public String createIndexQuery() {
		String query = "Create index "+indexName+" if not exists on "+keyspace+"."+table+" ("+fieldName+");";
		return query;
	}

}
