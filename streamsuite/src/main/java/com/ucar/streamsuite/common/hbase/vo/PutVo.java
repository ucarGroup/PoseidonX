/**
 * Description: PutVo.java
 * All Rights Reserved.
 * @version 1.0  2016-1-7 下午12:59:58    创建
 */
package com.ucar.streamsuite.common.hbase.vo;

import java.io.Serializable;
import java.util.List;

/**
 *  put 传输对象
 * <br/> Created on 2016-1-7 下午12:59:58
 *
 * @since 4.1
 */
public class PutVo extends CommonHbaseVo implements Serializable{

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	private byte[] rowKey ;

	private List<Column> columns;

	private String version;

	private boolean writeToWAL = true;

	public byte[] getRowKey() {
		return rowKey;
	}

	public void setRowKey(byte[] rowKey) {
		this.rowKey = rowKey;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public boolean isWriteToWAL() {
		return writeToWAL;
	}

	public void setWriteToWAL(boolean writeToWAL) {
		this.writeToWAL = writeToWAL;
	}
}
