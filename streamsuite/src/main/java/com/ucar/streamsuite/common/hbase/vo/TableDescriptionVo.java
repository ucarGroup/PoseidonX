
package com.ucar.streamsuite.common.hbase.vo;

import java.io.Serializable;
import java.util.List;

/**
 *  描述表属性的vo
 * <br/> Created on 2016-1-6 下午4:13:46
 *
 * @since 4.1
 */
public class TableDescriptionVo implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String tableName ;
	
	private long fileSize ;
	
	private long memStoreSize ;

	private byte[][] splitKeys ;
	
	private List<ColumnDescriptor> listColumn ;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public long getMemStoreSize() {
		return memStoreSize;
	}

	public void setMemStoreSize(long memStoreSize) {
		this.memStoreSize = memStoreSize;
	}

	public List<ColumnDescriptor> getListColumn() {
		return listColumn;
	}

	public void setListColumn(List<ColumnDescriptor> listColumn) {
		this.listColumn = listColumn;
	}

	public byte[][] getSplitKeys() {
		return splitKeys;
	}

	public void setSplitKeys(byte[][] splitKeys) {
		this.splitKeys = splitKeys;
	}

}
