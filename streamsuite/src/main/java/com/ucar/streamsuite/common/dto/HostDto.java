
package com.ucar.streamsuite.common.dto;

import java.io.Serializable;

public class HostDto implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;

    private String host;
    private Integer port;

    public HostDto(String host,Integer port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HostDto hostDto = (HostDto) o;

        if (!host.equals(hostDto.host)) return false;
        return port.equals(hostDto.port);
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port.hashCode();
        return result;
    }
}
