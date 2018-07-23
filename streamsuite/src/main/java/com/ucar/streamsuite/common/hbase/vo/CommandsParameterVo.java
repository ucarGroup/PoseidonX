
package com.ucar.streamsuite.common.hbase.vo;

import java.io.Serializable;

public class CommandsParameterVo implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private boolean autoFlush ;
	
	private boolean clearBufferOnFail ;

	public boolean isAutoFlush() {
		return autoFlush;
	}

	public void setAutoFlush(boolean autoFlush) {
		this.autoFlush = autoFlush;
	}

	public boolean isClearBufferOnFail() {
		return clearBufferOnFail;
	}

	public void setClearBufferOnFail(boolean clearBufferOnFail) {
		this.clearBufferOnFail = clearBufferOnFail;
	}
	
}
