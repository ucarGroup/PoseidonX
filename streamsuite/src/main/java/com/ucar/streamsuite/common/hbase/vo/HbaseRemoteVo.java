package com.ucar.streamsuite.common.hbase.vo;

import java.io.Serializable;
import java.util.List;

/**
 * 
 * Description:habse代理远程vo 
 * All Rights Reserved.
 * Created on 2016-1-7 下午3:45:10
 *
 */
public class HbaseRemoteVo extends CommonHbaseVo implements Serializable{

	/**
	 *
	 */
	private static final long serialVersionUID = -2929227417713321540L;

	private String tableName;

	private List<PutVo> putVos;

	private CommandsParameterVo cpvo;

	private TableDescriptionVo tableDescriptionVo;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<PutVo> getPutVos() {
		return putVos;
	}

	public void setPutVos(List<PutVo> putVos) {
		this.putVos = putVos;
	}

	public TableDescriptionVo getTableDescriptionVo() {
		return tableDescriptionVo;
	}

	public void setTableDescriptionVo(TableDescriptionVo tableDescriptionVo) {
		this.tableDescriptionVo = tableDescriptionVo;
	}

}
