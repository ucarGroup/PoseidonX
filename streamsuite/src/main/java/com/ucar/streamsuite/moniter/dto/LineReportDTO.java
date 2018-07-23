package com.ucar.streamsuite.moniter.dto;

import com.ucar.streamsuite.common.util.LruHashMap;

import java.io.Serializable;
import java.util.Map;

/**
 * Description: 基于线的报表的dto
 * Created on 2018/1/18 下午4:33
 *
 */
public class LineReportDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315860L;

    /**
     * 曲线的标题
     */
    private String title;

    /**
     * 时间线数据
     */
    private Map<String,LineReportTimelineDTO> groupToTimeline = new LruHashMap<>(50);

    public LineReportDTO() {
    }

    public LineReportDTO(String title) {
        this.title = title;
    }

    public void addTimeLine(LineReportTimelineDTO timeline){
        groupToTimeline.put(timeline.getGroup(),timeline);
    }

    public LineReportTimelineDTO getTimeLineByGroup(String group){
        return groupToTimeline.get(group);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LineReportDTO that = (LineReportDTO) o;

        return title != null ? title.equals(that.title) : that.title == null;
    }

    @Override
    public int hashCode() {
        return title != null ? title.hashCode() : 0;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Map<String, LineReportTimelineDTO> getGroupToTimeline() {
        return groupToTimeline;
    }

    public void setGroupToTimeline(Map<String, LineReportTimelineDTO> groupToTimeline) {
        this.groupToTimeline = groupToTimeline;
    }
}
