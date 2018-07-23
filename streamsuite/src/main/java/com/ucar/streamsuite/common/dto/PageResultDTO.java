package com.ucar.streamsuite.common.dto;

import java.io.Serializable;
import java.util.List;

/**
 * Description: 用于分页列表返回结果的dto
 * Created on 2018/1/31 下午4:08
 *
 */
public class PageResultDTO<T> implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;

    private List<T> list;
    private int currentPage; //当前页数
    private int count; //总数

    public List<T> getList() {
        return list;
    }

    public void setList(List<T> list) {
        this.list = list;
    }

    public int getCurrentPage() {
        return currentPage;
    }

    public void setCurrentPage(int currentPage) {
        this.currentPage = currentPage;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
