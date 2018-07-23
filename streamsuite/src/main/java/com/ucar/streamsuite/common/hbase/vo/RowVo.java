
package com.ucar.streamsuite.common.hbase.vo;

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class RowVo implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private String rowKey ;

	private List<Column> columns;
	

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

	public Map<String,String> convertColumnDateToMap(){
		Map<String,String> columnDateToMap = Maps.newHashMap();
		try {
			List<Column> cList = this.getColumns();
			for (Column column : cList) {
				String columnName = new String(column.getQualifier(), "utf-8");
				String columnValue = Bytes.toString(column.getValue());
				columnDateToMap.put(columnName,columnValue);
			}
		} catch (Exception e) {
		}
		return columnDateToMap;
	}
}
