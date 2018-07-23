package com.ucar.streamsuite.task.dto;

import com.ucar.streamsuite.common.dto.PageQueryDTO;

/**
 * Description:
 * Created on 2018/2/9 上午9:10
 *
 */
public class TaskArchivePageQueryDTO extends PageQueryDTO{

    private static final long serialVersionUID = -5729044331250315760L;

    private String createUser;
    private String taskArchiveName;


    public TaskArchivePageQueryDTO(){}

    public TaskArchivePageQueryDTO(Integer pageNum, Integer pageSize) {
       super(pageNum,pageSize);
    }

    public TaskArchivePageQueryDTO(Integer pageNum, Integer pageSize,String createUser,String archiveName) {
        super(pageNum,pageSize);

        this.createUser = createUser;
        this.taskArchiveName = archiveName;

    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public String getTaskArchiveName() {
        return taskArchiveName;
    }

    public void setTaskArchiveName(String taskArchiveName) {
        this.taskArchiveName = taskArchiveName;
    }
}
