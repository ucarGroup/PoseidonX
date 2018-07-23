package com.ucar.streamsuite.common.util;

import com.ucar.streamsuite.common.dto.PageQueryDTO;

/**
 * Description: 一些零碎的工具类
 * Created on 2018/1/31 下午5:38
 *
 */
public class StreamUtil {

    /**
     * 处理分页查询的 dto ,根据页数生成对应的startIndex 和 pageIndex
     *
     * @param pageQueryDTO
     * @return
     */
    public static  PageQueryDTO dealPageQuery(PageQueryDTO pageQueryDTO){

        Integer pageNum = pageQueryDTO.getPageNum();
        Integer pageSize = pageQueryDTO.getPageSize();


        Integer startIndex = (pageNum-1) * pageSize  ;

        if(startIndex < 0){
            startIndex = 0;
        }

        pageQueryDTO.setStartIndex(startIndex);

        return pageQueryDTO;
    }

}
