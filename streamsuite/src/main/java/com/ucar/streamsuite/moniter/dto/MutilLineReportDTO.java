package com.ucar.streamsuite.moniter.dto;

import org.assertj.core.util.Lists;

import java.io.Serializable;
import java.util.List;

/**
 * Description: 复合的曲线报表
 * Created on 2018/1/18 下午4:33
 *
 */
public class MutilLineReportDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315860L;

    /**
     * tab标题
     */
    private String title;

    /**
     * 多个报表
     */
    private List<LineReportDTO> lineReports = Lists.newArrayList();

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<LineReportDTO> getLineReports() {
        return lineReports;
    }

    public void setLineReports(List<LineReportDTO> lineReports) {
        this.lineReports = lineReports;
    }
}
