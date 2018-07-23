package com.ucar.streamsuite.common.hbase.vo;

import com.ucar.streamsuite.common.hbase.vo.ScanEnum.ScanComparatorEnum;
import com.ucar.streamsuite.common.hbase.vo.ScanEnum.ScanComparisonEnum;

import java.io.Serializable;

public class ScanComparisonVo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -444434300134655972L;
	
	private String family;
	
	private String qualifier;
	
	private Object value;
	
	private ScanComparisonEnum scanComparisonEnum;
	
	private ScanComparatorEnum scanComparatorEnum;

	private boolean isRowFilter = false;

	public ScanComparisonVo(String family, String qualifier, Object value,
			ScanComparisonEnum scanComparisonEnum, ScanComparatorEnum scanComparatorEnum) {
		this(family,qualifier,value, scanComparisonEnum, scanComparatorEnum, false);
	}

	public ScanComparisonVo(String family, String qualifier, Object value,
							ScanComparisonEnum scanComparisonEnum,
							ScanComparatorEnum scanComparatorEnum,
							boolean isRowFilter) {
		this.family = family;
		this.qualifier = qualifier;
		this.value = value;
		this.scanComparisonEnum = scanComparisonEnum;
		this.scanComparatorEnum = scanComparatorEnum;
		this.isRowFilter = isRowFilter;
	}

	public ScanComparisonVo(Object value,
							ScanComparisonEnum scanComparisonEnum,
							ScanComparatorEnum scanComparatorEnum) {
		this.value = value;
		this.scanComparisonEnum = scanComparisonEnum;
		this.scanComparatorEnum = scanComparatorEnum;
		this.isRowFilter = true;
	}
	
	public ScanComparisonVo() {	}

	public String getFamily() {
		return family;
	}

	public void setFamily(String family) {
		this.family = family;
	}

	public String getQualifier() {
		return qualifier;
	}

	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public ScanComparisonEnum getScanComparisonEnum() {
		return scanComparisonEnum;
	}

	public void setScanComparisonEnum(ScanComparisonEnum scanComparisonEnum) {
		this.scanComparisonEnum = scanComparisonEnum;
	}

	public ScanComparatorEnum getScanComparatorEnum() {
		return scanComparatorEnum;
	}

	public void setScanComparatorEnum(ScanComparatorEnum scanComparatorEnum) {
		this.scanComparatorEnum = scanComparatorEnum;
	}


	public boolean isRowFilter() {
		return isRowFilter;
	}

	public void setIsRowFilter(boolean isRowFilter) {
		this.isRowFilter = isRowFilter;
	}
}
