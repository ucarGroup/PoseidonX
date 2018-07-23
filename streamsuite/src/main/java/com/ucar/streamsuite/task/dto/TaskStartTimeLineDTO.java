package com.ucar.streamsuite.task.dto;

import com.google.common.collect.Lists;
import com.ucar.streamsuite.common.util.DateUtil;
import org.apache.commons.lang.StringUtils;

import java.util.Date;
import java.util.List;

/**
 * Description: 任务开始过程的时间线的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class TaskStartTimeLineDTO {

    private static final long serialVersionUID = -5729044331250315890L;

    private String taskName = "";

    private String pending = "";

    private List<TaskStartTimeLineItemDTO> timelineItemList = Lists.newArrayListWithCapacity(15);

    public TaskStartTimeLineDTO(){
    }

    public TaskStartTimeLineDTO(String taskName,String beginPending){
        startTimeline(taskName,beginPending);
    }

    public void setPending(String pending) {
        this.pending = pending;
    }

    public String getPending() {
        return pending;
    }

    public List<TaskStartTimeLineItemDTO> getTimelineItemList() {
        return timelineItemList;
    }

    public void setTimelineItemList(List<TaskStartTimeLineItemDTO> timelineItemList) {
        this.timelineItemList = timelineItemList;
    }

    private void addTimelineItem(String content, String time, String status){
        TaskStartTimeLineItemDTO timelineItem = new TaskStartTimeLineItemDTO();
        timelineItem.setContent(content);
        timelineItem.setTime(time);
        timelineItem.setStatus(status);
        timelineItemList.add(timelineItem);
    }

    private void startTimeline(String taskName,String beginPending){
        this.taskName = taskName;
        this.pending = beginPending;
        this.timelineItemList = Lists.newArrayListWithCapacity(15);
    }


    public TaskStartTimeLineDTO goToEnd(String currentStatus, String endContnet, String endStatus){
        String time = DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime());
        addTimelineItem(pending, time, currentStatus);
        addTimelineItem(endContnet,time,  endStatus);
        this.pending = "";
        return this;
    }

    public TaskStartTimeLineDTO goToNext(String currentStatus, String nextPending){
        if(StringUtils.isNotBlank(pending)){
            String time = DateUtil.yyyymmddhhmmss(DateUtil.getCurrentDateTime());
            addTimelineItem(pending, time, currentStatus);
        }
        if(nextPending!=null){
            pending = nextPending;
        }
        return this;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }
}
