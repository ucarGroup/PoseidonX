import React from 'react';
import {Form} from "antd";
import { qs, util, config, auth } from "libs";
import moment from 'moment';


class TaskInfoForm extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            taskId: "",
            engineType: "",
        }
    }

    //组件加载时执行查询
    componentWillMount() {

        if(this.props.currentTab == 'taskInfo' || this.props.currentTab == 'flinkTaskInfo'){
            qs.form("/streamsuite/task/task/showById", {id: this.props.taskId}).then((data) => {

                this.setState({
                    taskName: data.taskName,
                    auditTime: data.auditTime,
                    auditUserName: data.auditUserName,
                    taskStartTime: data.taskStartTime,
                    taskStopTime: data.taskStopTime,
                    creatorUserName: data.creatorUserName,
                    createTime: data.createTime,
                    modifyUserName: data.modifyUserName,
                    modifyTime: data.modifyTime,
                    engineType:data.engineType,
                    classPath: data.classPath,
                    workerNum: data.workerNum,
                    workerMem: data.workerMem,
                    taskStatus: data.taskStatusShow,
                    taskType:data.taskTypeShow,
                    archive: data.archiveShow,
                    archiveVersion: data.archiveVersionShow,
                    jstormEngineVersion: data.jstormEngineVersionShow,
                    yarnAmEngineVersion: data.yarnAmEngineVersionShow,
                    zkAddress:data.zkAddressShow,
                    taskCql:data.taskCqlShow,
                    isCQL:data.isCql,
                    yarnAddress:data.yarnAddress,
                    customParams:data.customParams,
                    slots:data.slots,
                    parallelism:data.parallelism,
                })
            });
        }
    }

    render() {
        let createTime = moment(this.state.createTime).format("YYYY-MM-DD HH:mm:ss")
        let auditTime = ''
        let taskStartTime =  ''
        let taskStopTime =  ''
        let modifyTime =  ''

        if(this.state.auditTime!= null){
            auditTime = moment(this.state.auditTime).format("YYYY-MM-DD HH:mm:ss")
        }
        if(this.state.taskStartTime != null){
            taskStartTime = moment(this.state.taskStartTime).format("YYYY-MM-DD HH:mm:ss")
        }
        if(this.state.taskStopTime != null){
            taskStopTime = moment(this.state.taskStopTime).format("YYYY-MM-DD HH:mm:ss")
        }
        if(this.state.modifyTime != null){
            modifyTime = moment(this.state.modifyTime).format("YYYY-MM-DD HH:mm:ss")
        }
        let isCQL = (this.state.isCQL == 1);

        let isJstorm = (this.state.engineType == '0');
        let isFlink = (this.state.engineType == '1');

        if(isJstorm){
            return (
                <div>

                    <div style={{display:!isCQL?'inline':'none'}}>
                        <table>
                            <tbody><tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">任务名称：</td>
                                <td width='30%'>{this.state.taskName}</td>
                                <td width='20%' bgcolor="#EDEDED">任务类型：</td>
                                <td width='30%'>{this.state.taskType}</td>
                            </tr>

                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">创建人：</td>
                                <td >{this.state.creatorUserName}</td>
                                <td  bgcolor="#EDEDED">创建时间：</td>
                                <td >{createTime}</td>
                            </tr>
                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">审核人：</td>
                                <td >{this.state.auditUserName}</td>
                                <td  bgcolor="#EDEDED">审核时间：</td>
                                <td >{auditTime}</td>
                            </tr>
                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">开始时间：</td>
                                <td >{taskStartTime}</td>
                                <td  bgcolor="#EDEDED">停止时间：</td>
                                <td >{taskStopTime}</td>
                            </tr>

                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">最后修改人：</td>
                                <td >{this.state.modifyUserName}</td>
                                <td  bgcolor="#EDEDED">最后修改时间：</td>
                                <td >{modifyTime}</td>
                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">任务状态：</td>
                                <td width='80%' colSpan='3'>{this.state.taskStatus}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">任务运行文件：</td>
                                <td width='80%' colSpan='3'>{this.state.archive}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">任务运行文件版本：</td>
                                <td width='80%' colSpan='3'>{this.state.archiveVersion}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">任务main类路径：</td>
                                <td width='80%' colSpan='3'>{this.state.classPath}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">zk地址信息：</td>
                                <td width='80%' colSpan='3'>{this.state.zkAddress}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">worker数：</td>
                                <td width='80%' colSpan='3'>{this.state.workerNum}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">每个worker内存大小：</td>
                                <td width='80%' colSpan='3'>{this.state.workerMem}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">Jstorm 版本包：</td>
                                <td width='80%' colSpan='3'>{this.state.jstormEngineVersion}</td>

                            </tr>
                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">Jstorm_AM 版本包：</td>
                                <td width='80%' colSpan='3'>{this.state.yarnAmEngineVersion}</td>

                            </tr></tbody>

                        </table>
                    </div>



                    <div style={{display:!isCQL?'none':'inline'}}>
                        <table>
                            <tbody><tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">任务名称：</td>
                                <td width='30%'>{this.state.taskName}</td>
                                <td width='20%' bgcolor="#EDEDED">任务类型：</td>
                                <td width='30%'>{this.state.taskType}</td>
                            </tr>

                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">创建人：</td>
                                <td >{this.state.creatorUserName}</td>
                                <td  bgcolor="#EDEDED">创建时间：</td>
                                <td >{createTime}</td>
                            </tr>
                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">审核人：</td>
                                <td >{this.state.auditUserName}</td>
                                <td  bgcolor="#EDEDED">审核时间：</td>
                                <td >{auditTime}</td>
                            </tr>
                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">开始时间：</td>
                                <td >{taskStartTime}</td>
                                <td  bgcolor="#EDEDED">停止时间：</td>
                                <td >{taskStopTime}</td>
                            </tr>

                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">最后修改人：</td>
                                <td >{this.state.modifyUserName}</td>
                                <td  bgcolor="#EDEDED">最后修改时间：</td>
                                <td >{modifyTime}</td>
                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">任务状态：</td>
                                <td width='80%' colSpan='3'>{this.state.taskStatus}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">CQL脚本：</td>
                                <td width='80%' colSpan='3'>{this.state.taskCql}</td>
                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">zk地址信息：</td>
                                <td width='80%' colSpan='3'>{this.state.zkAddress}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">worker数：</td>
                                <td width='80%' colSpan='3'>{this.state.workerNum}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">每个worker内存大小：</td>
                                <td width='80%' colSpan='3'>{this.state.workerMem}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">Jstorm 版本包：</td>
                                <td width='80%' colSpan='3'>{this.state.jstormEngineVersion}</td>

                            </tr>
                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">Jstorm_AM 版本包：</td>
                                <td width='80%' colSpan='3'>{this.state.yarnAmEngineVersion}</td>

                            </tr></tbody>

                        </table>
                    </div>
                </div>
            )
        }else if(isFlink){
            return (
                <div>
                    <div style={{display:!isCQL?'inline':'none'}}>
                        <table>
                            <tbody><tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">任务名称：</td>
                                <td width='30%'>{this.state.taskName}</td>
                                <td width='20%' bgcolor="#EDEDED">任务类型：</td>
                                <td width='30%'>{this.state.taskType}</td>
                            </tr>

                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">创建人：</td>
                                <td >{this.state.creatorUserName}</td>
                                <td  bgcolor="#EDEDED">创建时间：</td>
                                <td >{createTime}</td>
                            </tr>
                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">审核人：</td>
                                <td >{this.state.auditUserName}</td>
                                <td  bgcolor="#EDEDED">审核时间：</td>
                                <td >{auditTime}</td>
                            </tr>
                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">开始时间：</td>
                                <td >{taskStartTime}</td>
                                <td  bgcolor="#EDEDED">停止时间：</td>
                                <td >{taskStopTime}</td>
                            </tr>

                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">最后修改人：</td>
                                <td >{this.state.modifyUserName}</td>
                                <td  bgcolor="#EDEDED">最后修改时间：</td>
                                <td >{modifyTime}</td>
                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">任务状态：</td>
                                <td width='80%' colSpan='3'>{this.state.taskStatus}&nbsp;&nbsp;
                                    <a href={this.state.yarnAddress} target="_blank">查看APP控制台</a>
                                </td>
                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">任务运行文件：</td>
                                <td width='80%' colSpan='3'>{this.state.archive}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">任务运行文件版本：</td>
                                <td width='80%' colSpan='3'>{this.state.archiveVersion}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">任务main类路径：</td>
                                <td width='80%' colSpan='3'>{this.state.classPath}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">jobmanager数：</td>
                                <td width='80%' colSpan='3'>{this.state.workerNum}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">每个jobmanager内存大小：</td>
                                <td width='80%' colSpan='3'>{this.state.workerMem}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">Slot数：</td>
                                <td width='80%' colSpan='3'>{this.state.slots}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">并发度parallelism：</td>
                                <td width='80%' colSpan='3'>{this.state.parallelism}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">用户参数：</td>
                                <td width='80%' colSpan='3'>{this.state.customParams}</td>

                            </tr>

                            </tbody>
                        </table>
                    </div>


                    <div style={{display:!isCQL?'none':'inline'}}>
                        <table>
                            <tbody><tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">任务名称：</td>
                                <td width='30%'>{this.state.taskName}</td>
                                <td width='20%' bgcolor="#EDEDED">任务类型：</td>
                                <td width='30%'>{this.state.taskType}</td>
                            </tr>

                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">创建人：</td>
                                <td >{this.state.creatorUserName}</td>
                                <td  bgcolor="#EDEDED">创建时间：</td>
                                <td >{createTime}</td>
                            </tr>
                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">审核人：</td>
                                <td >{this.state.auditUserName}</td>
                                <td  bgcolor="#EDEDED">审核时间：</td>
                                <td >{auditTime}</td>
                            </tr>
                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">开始时间：</td>
                                <td >{taskStartTime}</td>
                                <td  bgcolor="#EDEDED">停止时间：</td>
                                <td >{taskStopTime}</td>
                            </tr>

                            <tr height='50px'>
                                <td  bgcolor="#EDEDED">最后修改人：</td>
                                <td >{this.state.modifyUserName}</td>
                                <td  bgcolor="#EDEDED">最后修改时间：</td>
                                <td >{modifyTime}</td>
                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">任务状态：</td>
                                <td width='80%' colSpan='3'>{this.state.taskStatus}&nbsp;&nbsp;
                                    <a href={this.state.yarnAddress} target="_blank">查看APP控制台</a>
                                </td>
                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">CQL脚本：</td>
                                <td width='80%' colSpan='3'>{this.state.taskCql}</td>
                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">jobmanager数：</td>
                                <td width='80%' colSpan='3'>{this.state.workerNum}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">每个jobmanager内存大小：</td>
                                <td width='80%' colSpan='3'>{this.state.workerMem}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">Slot数：</td>
                                <td width='80%' colSpan='3'>{this.state.slots}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">并发度parallelism：</td>
                                <td width='80%' colSpan='3'>{this.state.parallelism}</td>

                            </tr>

                            <tr height='50px'>
                                <td width='20%' bgcolor="#EDEDED">用户参数：</td>
                                <td width='80%' colSpan='3'>{this.state.customParams}</td>

                            </tr>

                            </tbody>
                        </table>
                    </div>
                </div>
            )
        } else{
                return (
                    <div></div>
                )
        }

    }
}

export default Form.create()(TaskInfoForm);