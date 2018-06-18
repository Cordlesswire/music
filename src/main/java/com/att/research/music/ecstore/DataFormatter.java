package com.att.research.music.ecstore;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ColumnDefinitions.Definition;

public class DataFormatter {

	public Object getColValue(Row row, String colName, DataType colType){	
		switch(colType.getName()){
		case VARCHAR: 
			return row.getString(colName);
		case UUID: 
			return row.getUUID(colName);
		case VARINT: 
			return row.getVarint(colName);
		case BIGINT: 
			return row.getLong(colName);
		case INT: 
			return row.getInt(colName);
		case FLOAT: 
			return row.getFloat(colName);	
		case DOUBLE: 
			return row.getDouble(colName);
		case BOOLEAN: 
			return row.getBool(colName);
		case MAP: 
			return row.getMap(colName, String.class, String.class);
		default: 
			return null;
		}
	}
	
	public String convertToCQLDataType(DataType type,Object valueObj){
		String value ="";
		switch (type.getName()) {
		case UUID:
			value = valueObj+"";
			break;
		case TEXT: case VARCHAR:
			String valueString = valueObj+"";
			valueString = valueString.replace("'", "''");
			value = "'"+valueString+"'";
			break;
		case MAP:{
			Map<String,Object> otMap = (Map<String,Object>)valueObj;
			value = "{"+jsonMaptoSqlString(otMap, ",")+"}";
			break;
		}	
		default:
			value = valueObj+"";
			break;
		}
		return value;
	}
	
	public Object convertToActualDataType(DataType colType,Object valueObj){
		String valueObjString = valueObj+"";
		switch(colType.getName()){
		case UUID: 
			return UUID.fromString(valueObjString);
		case VARINT: 
			return BigInteger.valueOf(Long.parseLong(valueObjString));
		case BIGINT: 
			return Long.parseLong(valueObjString);
		case INT: 
			return Integer.parseInt(valueObjString);
		case FLOAT: 
			return Float.parseFloat(valueObjString);	
		case DOUBLE: 
			return Double.parseDouble(valueObjString);
		case BOOLEAN: 
			return Boolean.parseBoolean(valueObjString);
		case MAP: 
			return (Map<String,Object>)valueObj;
		default:
			return valueObjString;
		}
	}

	//utility function to parse json map into sql like string
	public String jsonMaptoSqlString(Map<String, Object> jMap, String lineDelimiter){
		String sqlString="";
		int counter =0;
		for (Map.Entry<String, Object> entry : jMap.entrySet())
		{
			Object ot = entry.getValue();
			String value = ot+"";
			if(ot instanceof String){
				value = "'"+value.replace("'", "''")+"'";
			}
			sqlString = sqlString+"'"+entry.getKey()+"':"+ value+"";
			if(counter!=jMap.size()-1)
				sqlString = sqlString+lineDelimiter;
			counter = counter +1;
		}	
		return sqlString;	
	}

	public Map<String, HashMap<String, Object>> marshalData(ResultSet results){
		Map<String, HashMap<String, Object>> resultMap = new HashMap<String, HashMap<String,Object>>();
		int counter =0;
		for (Row row : results) {
			ColumnDefinitions colInfo = row.getColumnDefinitions();
			HashMap<String,Object> resultOutput = new HashMap<String, Object>();
			for (Definition definition : colInfo) {
				resultOutput.put(definition.getName(), getColValue(row, definition.getName(), definition.getType()));
			}
			resultMap.put("row "+counter, resultOutput);
			counter++;
		}
		return resultMap;
	}

}
