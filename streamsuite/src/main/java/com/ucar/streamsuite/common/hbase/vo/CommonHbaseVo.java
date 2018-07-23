
package com.ucar.streamsuite.common.hbase.vo;

public class CommonHbaseVo {
	
	private String hostName ;

	private String localIp ;
	
	public CommonHbaseVo(){
		hostName = LocalUtil.HOST_NAME;
		localIp = LocalUtil.LOCAL_IP;
	}
	
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public String getHostName(){
		
		return this.hostName ;
	}

	public String getLocalIp() {
		return localIp;
	}

	public void setLocalIp(String localIp) {
		this.localIp = localIp;
	}
}
