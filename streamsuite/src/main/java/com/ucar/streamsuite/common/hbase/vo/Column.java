package com.ucar.streamsuite.common.hbase.vo;

import java.io.Serializable;

public class Column implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7715215468344916342L;

	private byte[] family ;
	
	private byte[] qualifier ;
	
	private byte[] value ;
	
	private long ts ;

    public Column(){

    }

    public Column(byte[] family,byte[] qualifier,byte[] value){
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
    }


	public byte[] getFamily() {
		return family;
	}

	public void setFamily(byte[] family) {
		this.family = family;
	}

	public byte[] getQualifier() {
		return qualifier;
	}

	public void setQualifier(byte[] qualifier) {
		this.qualifier = qualifier;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}
	
	
}
