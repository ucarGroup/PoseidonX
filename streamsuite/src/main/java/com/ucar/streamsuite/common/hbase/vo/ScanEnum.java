package com.ucar.streamsuite.common.hbase.vo;

import java.io.Serializable;

public class ScanEnum implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2243472729680037311L;


	public enum ScanComparisonEnum{
	    /** less than */
	    LESS,
	    /** less than or equal to */
	    LESS_OR_EQUAL,
	    /** equals */
	    EQUAL,
	    /** not equal */
	    NOT_EQUAL,
	    /** greater than or equal to */
	    GREATER_OR_EQUAL,
	    /** greater than */
	    GREATER,
	    /** no operation */
	    NO_OP
	}
	
	
	public enum ScanComparatorEnum{
	    /** 字符串全匹配 */
	    EXACTSTRING,
	    /** 字符串模糊匹配 */
	    SUBSTRING,
	    /** 表达式匹配 */
	    REGEXSTRING,
	    /** long型匹配 */
	    LONG,
	    /** null匹配 */
	    NULL,
	    /** 字节匹配 */
	    BINARY,
	    /** 字节前缀 */
	    BINARYPREFIX
	}
}
