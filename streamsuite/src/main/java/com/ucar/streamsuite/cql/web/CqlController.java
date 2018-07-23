package com.ucar.streamsuite.cql.web;

import com.ucar.streamsuite.common.constant.UserRoleEnum;
import com.ucar.streamsuite.common.dto.OperResultDTO;
import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.common.filter.WebContextHolder;
import com.ucar.streamsuite.cql.business.CQLAnalyzerBusiness;
import com.ucar.streamsuite.cql.dto.CqlDTO;
import com.ucar.streamsuite.cql.po.CqlPO;
import com.ucar.streamsuite.cql.service.CqlService;

import com.ucar.streamsuite.user.po.UserPO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Description:
 * Created on 2018/3/12 下午1:53
 *
 */
@Controller
@RequestMapping("/cql")
public class CqlController {

    private static final Logger LOGGER = LoggerFactory.getLogger(CqlController.class);

    @Autowired
    private CqlService cqlService;

    @ResponseBody
    @RequestMapping(value = "/list", method = RequestMethod.POST)
    public PageResultDTO list(HttpServletRequest request, HttpServletResponse response) {

        Integer pageNum = Integer.valueOf(request.getParameter("pageNum"));
        Integer pageSize = Integer.valueOf(request.getParameter("pageSize"));

        PageQueryDTO pageQueryDTO = new PageQueryDTO(pageNum,pageSize);
        return  cqlService.pageQuery(pageQueryDTO);
    }

    @ResponseBody
    @RequestMapping(value = "/queryById", method = RequestMethod.POST)
    public CqlDTO queryById(HttpServletRequest request, HttpServletResponse response) {

        Integer id = Integer.valueOf(request.getParameter("id"));

        CqlPO cqlPO = cqlService.getCqlById(id);

        CqlDTO cqlDTO = new CqlDTO();
        cqlDTO.setId(cqlPO.getId());
        cqlDTO.setCqlName(cqlPO.getCqlName());
        cqlDTO.setCqlType(cqlPO.getCqlType());
        cqlDTO.setCqlText(cqlPO.getCqlText());
        cqlDTO.setCqlRemark(cqlPO.getCqlRemark());
        cqlDTO.setCqlStatue(cqlPO.getCqlStatue());
        cqlDTO.setCreatorUserName(cqlPO.getCreatorUserName());
        cqlDTO.setModifyUserName(cqlPO.getModifyUserName());
        cqlDTO.setCreateTime(cqlPO.getCreateTime());
        cqlDTO.setModifyTime(cqlPO.getModifyTime());
        cqlDTO.setUserGroupId(cqlPO.getUserGroupId());

        return cqlDTO;
    }


    @ResponseBody
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public OperResultDTO save(HttpServletRequest request, HttpServletResponse response) throws Exception {

        UserPO userPO = WebContextHolder.getLoginUser();
        if(userPO == null){
            throw new Exception("保存失败，用户会话超时，请重新登录再操作！");
        }

        String id = request.getParameter("id");
        String cqlName = request.getParameter("cqlName");
        String cqlType = request.getParameter("cqlType");
        String cqlText = request.getParameter("cqlText");
        String cqlRemark = request.getParameter("cqlRemark");
        String userGropuId = request.getParameter("userGroupId");


        CqlPO cqlPO = new CqlPO();
        cqlPO.setCqlName(cqlName);
        cqlPO.setCqlType(Integer.valueOf(cqlType));
        cqlPO.setCqlText(cqlText);
        cqlPO.setCqlRemark(cqlRemark);
        cqlPO.setUserGroupId(Integer.valueOf(userGropuId));
        cqlPO.setCreatorUserName(userPO.getUserName());
        cqlPO.setCreateTime(new Date());

        OperResultDTO operResultDTO = new OperResultDTO();
        operResultDTO.setResult(true);

        if (operResultDTO.isResult()) {
            //如果id为-1 则表示为新配置插入,否则为更新已经存在的用户
            if (id == null || id.equals("-1")) {

                cqlPO.setCreatorUserName(userPO.getUserName());
                cqlPO.setCreateTime(new Date());

                boolean result = cqlService.insertCql(cqlPO);
                operResultDTO.setResult(result);

            } else {

                cqlPO.setModifyUserName(userPO.getUserName());
                cqlPO.setModifyTime(new Date());

                cqlPO.setId(Integer.valueOf(id));
                cqlService.updateCql(cqlPO);
            }
        }
        return operResultDTO;
    }

    /***
     * 校验语法
     * @param request
     * @return
     */
    @ResponseBody
    @RequestMapping(value="checkCQL",method = RequestMethod.POST)
    public String checkCQL(HttpServletRequest request) {
        return CQLAnalyzerBusiness.analyze((request.getParameter("cqlText")));
    }

    @ResponseBody
    @RequestMapping(value = "/getCqlByUser", method = RequestMethod.POST)
    public List<CqlDTO> getCqlByUser(String cqlType) {
        UserPO userPO = WebContextHolder.getLoginUser();
        String queryUserName = "";
        if(userPO.getUserRole() != UserRoleEnum.SUPER_ADMIN.ordinal()){
            queryUserName = userPO.getUserName();
        }
        List<CqlPO> poList = cqlService.getCqlByUser(queryUserName,cqlType);
        List<CqlDTO> rsDtoList = new ArrayList<CqlDTO>();
        for(CqlPO po : poList){
            CqlDTO dto = new CqlDTO();
            dto.setId(po.getId());
            dto.setCqlName(po.getCqlName());
            dto.setCqlType(po.getCqlType());
            dto.setCqlRemark(po.getCqlRemark());
            rsDtoList.add(dto);
        }
        return rsDtoList;
    }

}
