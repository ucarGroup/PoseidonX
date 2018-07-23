import React from 'react';
import {auth, qs, util} from 'libs';
import '../../styles/glob.less'
import {inject} from "mobx-react";
import moment from 'moment';
import TaskForm from './taskForm';
import TaskInfoTabsModal from './taskInfoTabsModal';
import TaskTimeLineModal from './taskTimeLineModal';
import TaskHistoryModal from './taskHistoryModal';

import {Button,Modal,Table,notification,List,Popover, Form, Input,Select,Row, Col} from "antd";
import {observer} from "mobx-react/index";

const FormItem = Form.Item;

@inject("appStore")
@observer
class TaskList extends React.Component {
    constructor(props, context) {
        super(props, context);
        this.state = {
            dataSource: [],
            loading: false,
            btnText: '停止',
            showModal: false,
            showInfoModal: false,
            showTimeLineModal: false,
            showHistoryModal: false,
            engineTypeCondition: '',
            auditStatusCondition : '',
            taskStatusCondition: '',
            pageInfo: {
                pageNum: 1,
                pageSize: 10,
                current: 1,
                showSizeChanger: true,
                total: 0,
                onChange: (page, pageSize) => {
                    this.changePage(page, pageSize);
                },
                onShowSizeChange: (page, pageSize) => {
                    this.changePage(page, pageSize);
                }
            }
        }
    }

    //组件加载时执行查询
    componentWillMount() {
        this.query();
    }

    changePage(page, pageSize) {
        this.setState({
            pageInfo: {
                ...this.state.pageInfo,
                pageNum: page,
                pageSize: pageSize
            }
        }, () => {
            this.query();
        });
    }

    /**
     * [refresh 刷新表格]
     * @param  {[type]} params    [description]
     * @param  {[type]} firstPage [是否到第一页]
     * @return {[type]}           [description]
     */
    refresh(firstPage) {
        if (firstPage) {
            this.setState({
                ...this.state.pageInfo,
                pageNum: 1,
                showModal: false,
                showInfoModal: false,
                showTimeLineModal: false,
                showHistoryModal: false,
            });
        }
        this.query();
    }

    getQueryString() {
        const pageInfo = this.state.pageInfo;
        let params = {
            engineTypeCondition:  this.state.engineTypeCondition,
            auditStatusCondition :  this.state.auditStatusCondition,
            taskStatusCondition:  this.state.taskStatusCondition,
            pageNum: pageInfo.pageNum,
            pageSize: pageInfo.pageSize
        }
        return params;
    }

    query(outParams = {}) {
        let params = this.getQueryString();
        Object.assign(params, outParams);
        this.setState({
            loading: true
        });
        qs.post("/streamsuite/task/task/list", params).then(data => {
            this.setState({
                loading: false,
                dataSource: data.list,
                pageInfo: {
                    ...this.state.pageInfo,
                    pageNum: data.currentPage,
                    total: data.count,
                    current: data.currentPage
                }
            });
        });
    }

    handerAddTask = (isCQL) => {
        this.setState({
            showModal: true,
            showInfoModal: false,
            showTimeLineModal: false,
            showHistoryModal: false,
            isNew: true,
            taskFormTitle: "添加任务信息",
            taskId: '-1',
            taskName: '',
            remark: '',
            classPath: '',
            workerNum: '',
            workerMem: '',
            blotNum: '',
            spoutNum: '',
            taskType:'',
            engineType:'',
            archiveId: '',
            archiveVersionId:'',
            jstormEngineVersionId: '',
            yarnAmEngineVersionId: '',
            isCQL: isCQL,
            taskCqlId: '',
            customParams: '',
            slots: '',
            parallelism: '',
        })
    }

    deleteTask = (e)=> {
        let postData = {
            id: e
        };

        qs.form("/streamsuite/task/task/delete", postData).then((data) => {
            //删除失败
            if (!data.result) {
                notification.error({
                    message:  "删除失败",
                    description: data.errMsg
                });
            }
            //删除成功
            else {
                notification.info({
                    message: "删除成功",
                });
                this.refresh()
                return;
            }
        });
    }

    auditTask = (id,aduitStatus)=> {
        let postData = {
            id: id,
            aduitStatus: aduitStatus
        };

        qs.form("/streamsuite/task/task/aduit", postData).then((data) => {
            //删除失败
            if (!data.result) {
                notification.error({
                    message:  "操作失败",
                    description: data.errMsg
                });
            }
            //删除成功
            else {
                notification.info({
                    message: "操作成功",
                });
                this.refresh()
                return;
            }
        });
    }

    stopTask = (e)=> {
        let postData = {
            id: e
        };

        qs.form("/streamsuite/task/task/stop", postData).then((data) => {
            if (!data.result) {
                notification.error({
                    message:  "任务停止失败",
                    description: data.errMsg
                });
            }
            else {
                notification.info({
                    message: "任务停止成功",
                });
                this.refresh()
                return;
            }
        });
    }

    startTask = (e)=> {
        let postData = {
            id: e
        };

        qs.form("/streamsuite/task/task/start", postData).then((data) => {
            if (!data.result) {
                notification.error({
                    message:  "任务开始失败",
                    description: data.errMsg
                });
            }
            else {
                this.setState({
                    showTimeLineModal: true,
                    timeLineTaskId:e,
                })
                return;
            }
        });
    }

    handleSearch = (e) => {
        this.props.form.validateFields((err, params) => {
            if (!err) {
                this.setState({
                    engineTypeCondition: params.engineTypeCondition,
                    auditStatusCondition : params.auditStatusCondition,
                    taskStatusCondition : params.taskStatusCondition,
                });
                this.changePage(1, 10);
            }
        });
    }

    handleReset = () => {
        this.props.form.resetFields();
    }

    render() {
        var list = (data,length) => {
            var res = [];
            for(var i = 0; i < length; i++) {
                res.push(<h6>{data[i]}</h6>);
            }
            return res
        }

        const state = this.state;
        const columns = [{
            title: '任务名',
            dataIndex: 'taskName',
            key: 'taskName',
            width: 200,
            render: (text, record) => {
                return (
                    <div>
                            <div style={{color:'blue'}} onClick={(e) => {

                                this.setState({
                                    showModal: false,
                                    showInfoModal: true,
                                    showTimeLineModal: false,
                                    showHistoryModal: false,
                                    show_taskId: record.id,
                                    show_engineType: record.engineType,
                                })

                           }}><u>{record.taskName}</u></div>
                    </div>
                )
            }
        }, {
            title: '任务类型',
            dataIndex: 'isCql',
            key: 'isCql',
            width: 200,
            render: (text) => {
                let taskType = ""
                if (text == "0") {
                    taskType = "API任务"
                }
                if (text == "1") {
                    taskType = "CQL任务"
                }
                return (<font>{taskType}</font>)
            }
        }, {
            title: '引擎类型',
            dataIndex: 'engineType',
            key: 'engineType',
            width: 200,
            render: (text) => {
                let engineType = ""
                if (text == "0") {
                    engineType = "Jstorm"
                }
                if (text == "1") {
                    engineType = "Flink"
                }
                return (<font>{engineType}</font>)
            }
        }, {
            title: '任务状态',
            dataIndex: 'taskStatus',
            key: 'taskStatus',
            width: 100,
            render: (text) => {
                let name = "未开始"
                let color = "blue"
                if (text == "1") {
                    name = "执行中"
                    color = "green"
                }
                if (text == "2") {
                    name = "异常中止"
                    color = "red"
                }
                if (text == "3") {
                    name = "暂停执行"
                    color = "blue"
                }
                return (<font color={color}>{name}</font>)
            }
        }, {
            title: '审核状态',
            dataIndex: 'auditStatus',
            key: 'auditStatus',
            width: 100,
            render: (text) => {
                let name = "未审核"
                let color = "blue"
                if (text == "1") {
                    name = "审核通过"
                    color = "green"
                }
                if (text == "2") {
                    name = "审核驳回"
                    color = "red"
                }
                return (<font color={color}>{name}</font>)
            }
        }, {
            title: '开始时间',
            dataIndex: 'taskStartTime',
            key: 'taskStartTime',
            width: 200,
            render: (text) => {
                if (text != null) {
                    return moment(text).format("YYYY-MM-DD HH:mm:ss")
                }
            }
        }, {
            title: '创建人',
            dataIndex: 'creatorUserName',
            key: 'creatorUserName',
            width: 200
        }, {
            title: '创建时间',
            dataIndex: 'createTime',
            key: 'createTime',
            width: 200,
            render: (text) => moment(text).format("YYYY-MM-DD HH:mm:ss")
        }, {
            title: 'ErrorInfo',
            dataIndex: 'errorInfo',
            key: 'errorInfo',
            width: 200,
            render: (text) => {
                if (text != null && text!='') {
                    var jsonObj =  JSON.parse(text);

                    const renderContent = (
                        <div className="auto-overflow-container">
                            {list(jsonObj,jsonObj.length)}
                        </div>
                    );

                    return (
                        <Popover placement="left" content={renderContent} title="ErrorInfo">
                            <div style={{color:'red'}}><u>error</u></div>
                        </Popover>
                    )
                }
            }
        }, {
            title: '操作',
            width: 250,
            render: (text, record) => {

                let disableDelete = !((auth.isSuperManager()) && (record.taskStatus == "0" || record.taskStatus == "3"));
                let disableAudit = !((auth.isSuperManager()) && (record.auditStatus == "0"));


                let isAdmin = auth.isSuperManager();

                let currentUserName = auth.getUserName();

                let disableBegin = true;
                let disableStop = true;

                if(record.auditStatus == "1"){
                    disableBegin = !(((auth.isSuperManager()||record.creatorUserName == currentUserName) && (record.taskStatus == "0" || record.taskStatus == "3")));
                    disableStop =  !(((auth.isSuperManager()||record.creatorUserName == currentUserName) && (record.taskStatus == "1" || record.taskStatus == "2")));
                }

                let disableEdit = !(((auth.isSuperManager()||record.creatorUserName == currentUserName) && (record.taskStatus == "0" || record.taskStatus == "3")));

                if (isAdmin) {
                    return (
                        <span>

                            <div style={{display:!disableDelete?'inline':'none'}}>
                            <Button size='small' type="primary" disabled={disableDelete} onClick={(e) => {

                                Modal.confirm({
                                    title: '确认删除',
                                    content: '是否确定删除该任务:'+record.taskName,
                                    okText: '确认',
                                    cancelText: '取消',
                                    onOk:()=>this.deleteTask(record.id)
                                });


                            }}>作废</Button>&nbsp;
                            </div>

                            <div style={{'display':!disableAudit?'inline':'none'}}>
                            <Button size='small' type="primary" disabled={disableAudit}  onClick={(e) => {

                                Modal.confirm({
                                    title: '审核',
                                    content: '是否确定审核通过该任务:'+record.taskName,
                                    okText: '确认',
                                    cancelText: '取消',
                                    onOk:()=>this.auditTask(record.id,1)
                                });


                            }}>审核通过</Button>&nbsp;
                            </div>

                            <div style={{'display':!disableAudit?'inline':'none'}}>
                            <Button size='small' type="primary" disabled={disableAudit} onClick={(e) => {

                                Modal.confirm({
                                    title: '审核',
                                    content: '是否确定审核驳回该任务:'+record.taskName,
                                    okText: '确认',
                                    cancelText: '取消',
                                    onOk:()=>this.auditTask(record.id,2)
                                });

                            }}>审核驳回</Button> &nbsp;
                            </div>

                            <div style={{'display':!disableEdit?'inline':'none'}}>
                            <Button size='small' type="primary" disabled={disableEdit} onClick={(e) => {

                                qs.form("/streamsuite/task/task/queryById", {id: record.id}).then((data) => {

                                    this.setState({
                                        showModal: true,
                                        showInfoModal: false,
                                        showTimeLineModal: false,
                                        showHistoryModal: false,
                                        isNew: false,
                                        taskFormTitle: "编辑任务信息",
                                        taskName: data.taskName,
                                        taskId: data.id,
                                        taskType:data.taskType,
                                        engineType:data.engineType,
                                        remark: data.remark,
                                        archiveId: data.archiveId,
                                        archiveVersionId: data.archiveVersionId,
                                        classPath: data.classPath,
                                        workerNum: data.workerNum,
                                        workerMem: data.workerMem,
                                        blotNum: data.blotNum,
                                        spoutNum: data.spoutNum,
                                        jstormEngineVersionId: data.jstormEngineVersionId,
                                        yarnAmEngineVersionId: data.yarnAmEngineVersionId,
                                        isCQL: data.isCql,
                                        taskCqlId: data.taskCqlId,
                                        customParams: data.customParams,
                                        slots: data.slots,
                                        parallelism: data.parallelism,
                                    })
                                });

                            }}>修改</Button>&nbsp;
                            </div>

                            <div style={{'display':!disableBegin?'inline':'none'}}>
                            <Button size='small' type="primary" disabled={disableBegin} onClick={(e) => {

                                Modal.confirm({
                                    title: '确认开始',
                                    content: '是否确定开始该任务:'+record.taskName,
                                    okText: '确认',
                                    cancelText: '取消',
                                    onOk:()=>this.startTask(record.id)
                                });


                            }}>开始</Button>&nbsp;
                            </div>

                            <div style={{'display':!disableStop?'inline':'none'}}>
                            <Button size='small' type="primary" disabled={disableStop} onClick={(e) => {
                                Modal.confirm({
                                    title: '确认停止',
                                    content: '是否确定停止该任务:'+record.taskName,
                                    okText: '确认',
                                    cancelText: '取消',
                                    onOk:()=>this.stopTask(record.id)
                                });

                            }}>停止</Button>&nbsp;
                            </div>

                            <Button size='small' type="primary" onClick={(e) => {
                                this.setState({
                                    showModal: false,
                                    showInfoModal: false,
                                    showTimeLineModal: false,
                                    showHistoryModal: true,
                                    taskFormTitle: "任务执行历史",
                                    taskId: record.id,
                                    engineType: record.engineType,
                                })
                            }}>执行历史</Button>&nbsp;
                        </span>
                    )
                }else{
                    return (
                        <span>
                            <div style={{'display':!disableEdit?'inline':'none'}}>
                            <Button size='small' type="primary" disabled={disableEdit} onClick={(e) => {

                                qs.form("/streamsuite/task/task/queryById", {id: record.id}).then((data) => {

                                    this.setState({
                                        showModal: true,
                                        showInfoModal: false,
                                        showTimeLineModal: false,
                                        showHistoryModal: false,
                                        isNew: false,
                                        taskFormTitle: "编辑任务信息",
                                        taskName: data.taskName,
                                        taskId: data.id,
                                        taskType:data.taskType,
                                        engineType:data.engineType,
                                        remark: data.remark,
                                        archiveId: data.archiveId,
                                        archiveVersionId: data.archiveVersionId,
                                        classPath: data.classPath,
                                        workerNum: data.workerNum,
                                        workerMem: data.workerMem,
                                        blotNum: data.blotNum,
                                        spoutNum: data.spoutNum,
                                        jstormEngineVersionId: data.jstormEngineVersionId,
                                        yarnAmEngineVersionId: data.yarnAmEngineVersionId,
                                        isCQL: data.isCql,
                                        taskCqlId: data.taskCqlId,
                                        customParams: data.customParams,
                                        slots: data.slots,
                                        parallelism: data.parallelism,
                                    })
                                });

                            }}>修改</Button>&nbsp;
                            </div>

                            <div style={{'display':!disableBegin?'inline':'none'}}>
                            <Button size='small' type="primary" disabled={disableBegin} onClick={(e) => {

                                Modal.confirm({
                                    title: '确认开始',
                                    content: '是否确定开始该任务:'+record.taskName,
                                    okText: '确认',
                                    cancelText: '取消',
                                    onOk:()=>this.startTask(record.id)
                                });


                            }}>开始</Button>&nbsp;
                            </div>

                            <div style={{'display':!disableStop?'inline':'none'}}>
                            <Button size='small' type="primary" disabled={disableStop} onClick={(e) => {

                                Modal.confirm({
                                    title: '确认停止',
                                    content: '是否确定停止该任务:'+record.taskName,
                                    okText: '确认',
                                    cancelText: '取消',
                                    onOk:()=>this.stopTask(record.id)
                                });

                            }}>停止</Button>
                            </div>

                             <Button size='small' type="primary" onClick={(e) => {
                                 this.setState({
                                     showModal: false,
                                     showInfoModal: false,
                                     showTimeLineModal: false,
                                     showHistoryModal: true,
                                     taskFormTitle: "任务执行历史",
                                     taskId: record.id,
                                     engineType: record.engineType,
                                 })
                             }}>执行历史</Button>&nbsp;
                        </span>
                    )
                }
            }
        }]

        const taskFormProps = {
            visible: this.state.showModal,
            taskFormTitle: this.state.taskFormTitle,
            taskName: this.state.taskName,
            taskId: this.state.taskId,
            isNew: this.state.isNew,
            taskType:this.state.taskType,
            engineType:this.state.engineType,
            remark: this.state.remark,
            archiveId: this.state.archiveId,
            archiveVersionId: this.state.archiveVersionId,
            classPath: this.state.classPath,
            workerNum: this.state.workerNum,
            workerMem: this.state.workerMem,
            blotNum: this.state.blotNum,
            spoutNum: this.state.spoutNum,
            jstormEngineVersionId: this.state.jstormEngineVersionId,
            yarnAmEngineVersionId: this.state.yarnAmEngineVersionId,
            isCQL: this.state.isCQL,
            taskCqlId: this.state.taskCqlId,
            customParams: this.state.customParams,
            slots: this.state.slots,
            parallelism: this.state.parallelism,
            onRefresh: (e) => {
                this.setState({
                    showModal: false,
                    showInfoModal: false,
                    showTimeLineModal: false,
                    showHistoryModal: false,
                })
                this.refresh()
            }
        }

        const taskInfoTabsModalProps = {
            visible: this.state.showInfoModal,
            taskId: this.state.show_taskId,
            engineType: this.state.show_engineType,
            onRefresh: (e) => {
                this.setState({
                    showModal: false,
                    showInfoModal: false,
                    showTimeLineModal: false,
                    showHistoryModal: false,
                })
                this.refresh()
            }
        }

        const taskTimeLineModalProps = {
            showTimeLineModal: this.state.showTimeLineModal,
            taskId: this.state.timeLineTaskId,
            onRefresh: (e) => {
                this.setState({
                    showModal: false,
                    showInfoModal: false,
                    showTimeLineModal: false,
                    showHistoryModal: false,
                })
                this.refresh()
            }
        }

        const taskHistoryModalProps = {
            showHistoryModal: this.state.showHistoryModal,
            taskId: this.state.taskId,
            engineType: this.state.engineType,
            onRefresh: (e) => {
                this.setState({
                    showModal: false,
                    showInfoModal: false,
                    showTimeLineModal: false,
                    showHistoryModal: false,
                })
                this.refresh()
            }
        }

        const {getFieldDecorator} = this.props.form;

        return (
            <div className="listPage">

                <div className="ant-advanced-search-form" >

                    <Form>
                        <Row gutter={24}>
                            <Col span={8}  >
                                <FormItem label="引擎类型：">
                                    {getFieldDecorator('engineTypeCondition', {
                                        initialValue: this.state.engineTypeCondition + ''
                                    })(
                                        <Select
                                            showSearch
                                            optionFilterProp="children"
                                        >
                                            <Select.Option value="0">Jstorm</Select.Option>
                                            <Select.Option value="1">Flink</Select.Option>
                                        </Select>
                                    )}
                                </FormItem>
                            </Col>
                            <Col span={8}  >
                                <FormItem label="任务状态：">
                                    {getFieldDecorator('taskStatusCondition', {
                                        initialValue: this.state.taskStatusCondition + ''
                                    })(
                                        <Select
                                            showSearch
                                            optionFilterProp="children"
                                        >
                                            <Select.Option value="0">未开始</Select.Option>
                                            <Select.Option value="1">运行中</Select.Option>
                                            <Select.Option value="2">异常中止</Select.Option>
                                            <Select.Option value="3">暂停执行</Select.Option>
                                        </Select>
                                    )}
                                </FormItem>
                            </Col>
                            <Col span={8}  >
                                <FormItem label="审核状态：">
                                    {getFieldDecorator('auditStatusCondition', {
                                        initialValue: this.state.auditStatusCondition + ''
                                    })(
                                        <Select
                                            showSearch
                                            optionFilterProp="children"
                                        >
                                            <Select.Option value="0">未审核</Select.Option>
                                            <Select.Option value="1">审核通过</Select.Option>
                                            <Select.Option value="2">审核驳回</Select.Option>
                                        </Select>
                                    )}
                                </FormItem>
                            </Col>
                        </Row>
                        <Row>
                            <Col span={8}  >
                                <Button type="primary" onClick={()=>this.handerAddTask('0')}>添加API任务</Button>&nbsp;&nbsp;
                                <Button type="primary" onClick={()=>this.handerAddTask('1')}>添加CQL任务</Button>&nbsp;&nbsp;
                                <Button type="primary" htmlType="button" onClick={this.handleSearch}>查询</Button>&nbsp;&nbsp;
                                <Button style={{ marginLeft: 8 }} onClick={this.handleReset}>清空</Button>
                            </Col>
                            <Col span={16}></Col>
                        </Row>
                    </Form>
                </div>

                <div className="table-wrapper">
                    <Table rowKey="id" pagination={state.pageInfo} columns={columns} dataSource={state.dataSource}></Table>
                </div>

                <TaskForm {...taskFormProps}/>

                <TaskInfoTabsModal {...taskInfoTabsModalProps}/>

                <TaskTimeLineModal {...taskTimeLineModalProps}/>

                <TaskHistoryModal {...taskHistoryModalProps}/>

            </div>
        )
    }
}

export default Form.create()(TaskList);