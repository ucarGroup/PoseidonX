/**
 * Description: ScanVo.java
 * All Rights Reserved.
 * @version 4.0  2016-1-11 下午3:01:34    创建
 */
package com.ucar.streamsuite.common.hbase.vo;

import com.ucar.streamsuite.common.hbase.util.HBaseUtils;
import com.ucar.streamsuite.common.hbase.vo.ScanEnum.ScanComparatorEnum;
import com.ucar.streamsuite.common.hbase.vo.ScanEnum.ScanComparisonEnum;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *  scan 参数
 *  
 * <br/> Created on 2016-1-11 下午3:01:34
 *
 * @since 4.1
 */
public class ScanVo extends CommonHbaseVo implements Serializable{

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	private String familyName = HBaseUtils.DEFAULT_FAMILIY_NAME;

	private String tableName ;

	private Date date;

	private int cacheCount = 200 ;

	private String startKey ;

	private String endKey ;

	/**
	 * 反向查询，默认为正向
	 */
	private boolean reversed = false;

	/**
	 * 限制返回行数
	 */
	private int limit = 200;

	/**
	 * 返回指定扫描的列族及列
	 * 如果 value＝null ，将返回列族下所有的列
	 */
	private Map<String,List<String>> scanFamilyMap ;

	private FiltersOperatorVo.Operator operator = FiltersOperatorVo.Operator.MUST_PASS_ALL;

	private List<ScanComparisonVo> scanComparisons ;

    private CommandsParameterVo cpvo;


	public String getStartKey() {
		return startKey;
	}

	public void setStartKey(String startKey) {
		this.startKey = startKey;
	}

	public String getEndKey() {
		return endKey;
	}

	public void setEndKey(String endKey) {
		this.endKey = endKey;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getCacheCount() {
		return cacheCount;
	}

	public void setCacheCount(int cacheCount) {
		this.cacheCount = cacheCount;
	}

	public Map<String, List<String>> getScanFamilyMap() {
		return scanFamilyMap;
	}

	public void setScanFamilyMap(Map<String, List<String>> scanFamilyMap) {
		this.scanFamilyMap = scanFamilyMap;
	}

	public List<ScanComparisonVo> getScanComparisons() {
		return scanComparisons;
	}

	public void setScanComparisons(List<ScanComparisonVo> scanComparisons) {
		this.scanComparisons = scanComparisons;
	}

	public void addScanComparion(String family, String qualifier, Object value, ScanComparisonEnum scanComparisonEnum, ScanComparatorEnum scanComparatorEnum) {
		addScanComparion(new ScanComparisonVo(family, qualifier, value, scanComparisonEnum, scanComparatorEnum));
	}

	public void addScanComparion(ScanComparisonVo scanComparisonVo) {
		if(scanComparisons == null) {
			scanComparisons = new ArrayList<ScanComparisonVo>();
		}
		if (scanComparisonVo != null) {
			scanComparisons.add(scanComparisonVo);
		}
	}

	public CommandsParameterVo getCpvo() {
		return cpvo;
	}

	public void setCpvo(CommandsParameterVo cpvo) {
		this.cpvo = cpvo;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public boolean isReversed() {
		return reversed;
	}

	public void setReversed(boolean reversed) {
		this.reversed = reversed;
	}

	public FiltersOperatorVo.Operator getOperator() {
		return operator;
	}

	public void setOperator(FiltersOperatorVo.Operator operator) {
		this.operator = operator;
	}

	public String getFamilyName() {
		return familyName;
	}

	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}
}
