package com.ucar.streamsuite.config.web;

import com.ucar.streamsuite.common.dto.OperResultDTO;
import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.config.dto.ConfigDTO;
import com.ucar.streamsuite.config.po.ConfigPO;
import com.ucar.streamsuite.config.service.ConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Description: 配置管理模块的控制器
 * Created on 2018/2/1 上午9:53
 *
 */
@Controller
@RequestMapping("/config")
public class ConfigController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigController.class);

    @Autowired
    private ConfigService configService;


    @ResponseBody
    @RequestMapping(value = "/list", method = RequestMethod.POST)
    public PageResultDTO list(HttpServletRequest request, HttpServletResponse response) {

        Integer pageNum = Integer.valueOf(request.getParameter("pageNum"));
        Integer pageSize = Integer.valueOf(request.getParameter("pageSize"));

        PageQueryDTO pageQueryDTO = new PageQueryDTO(pageNum,pageSize);
        return  configService.pageQuery(pageQueryDTO);
    }

    @ResponseBody
    @RequestMapping(value = "/queryById", method = RequestMethod.POST)
    public ConfigDTO queryById(HttpServletRequest request, HttpServletResponse response) {

        Integer configId = Integer.valueOf(request.getParameter("id"));

        ConfigPO configPO = configService.getConfigById(configId);

        ConfigDTO configDTO = new ConfigDTO();
        configDTO.setId(configPO.getId());
        configDTO.setConfigName(configPO.getConfigName());
        configDTO.setConfigValue(configPO.getConfigValue());
        configDTO.setConfigRemark(configPO.getConfigRemark());
        configDTO.setCreateTime(configPO.getCreateTime());
        configDTO.setModifyTime(configPO.getModifyTime());

        return configDTO;
    }


    @ResponseBody
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public OperResultDTO save(HttpServletRequest request, HttpServletResponse response) {

        String id = request.getParameter("id");
        String configName = request.getParameter("configName");
        String configValue = request.getParameter("configValue");
        String configRemark = request.getParameter("configRemark");

        ConfigPO configPO = new ConfigPO();
        configPO.setConfigName(configName);
        configPO.setConfigValue(configValue);
        configPO.setConfigRemark(configRemark);

        OperResultDTO operResultDTO = new OperResultDTO();
        operResultDTO.setResult(true);

        if (operResultDTO.isResult()) {
            //如果id为-1 则表示为新配置插入,否则为更新已经存在的用户
            if (id == null || id.equals("-1")) {
                boolean result = configService.insertConfig(configPO);
                operResultDTO.setResult(result);

                if (!operResultDTO.isResult()) {
                    operResultDTO.setErrMsg("请检查配置是否已经存在!");
                }
            } else {
                configPO.setId(Integer.valueOf(id));
                configService.updateConfig(configPO);
            }

        }
        return operResultDTO;
    }

}
