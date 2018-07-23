package com.ucar.streamsuite.common.dto;

import java.io.Serializable;

/**
 * Description: 分页查询条件
 * Created on 2018/1/31 下午4:12
 *
 */
public class PageQueryDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;

    private Integer pageNum;
    private Integer pageSize;

    private Integer startIndex;

    public PageQueryDTO(){}

    public PageQueryDTO(Integer pageNum, Integer pageSize) {
        this.pageNum = pageNum;
        this.pageSize = pageSize;
    }



    public Integer getPageNum() {
        return pageNum;
    }

    public void setPageNum(Integer pageNum) {
        this.pageNum = pageNum;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public Integer getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(Integer startIndex) {
        this.startIndex = startIndex;
    }

}
