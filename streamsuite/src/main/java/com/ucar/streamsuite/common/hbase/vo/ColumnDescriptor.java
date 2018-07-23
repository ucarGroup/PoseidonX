/**
 * Description: ColumnDescriptor.java
 * All Rights Reserved.
 * @version 1.0  2016-1-7 上午8:24:47    创建
 */
package com.ucar.streamsuite.common.hbase.vo;

import java.io.Serializable;

/**
 *  列描述
 * <br/> Created on 2016-1-7 上午8:24:47
 *
 * @since 4.1
 */
public class ColumnDescriptor implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -7644172631042291495L;

	public ColumnDescriptor() {}

	public ColumnDescriptor(String familyName,  boolean isCompress) {
		this.familyName = familyName;
		this.isCompress = isCompress;
	}

	private String familyName ;
	
	private String algorithm ;
	
	private boolean isCompress ;

	public boolean isCompress() {
		return isCompress;
	}

	public void setCompress(boolean isCompress) {
		this.isCompress = isCompress;
	}

	public String getFamilyName() {
		return familyName;
	}

	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}

	public String getAlgorithm() {
		return algorithm;
	}

	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}
	
	
}
