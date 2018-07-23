import React from 'react';
import PropTypes from 'prop-types';
import {Form,Modal,Table,List,Popover} from "antd";
import { qs, util, config, auth } from "libs";
import moment from 'moment';
import LinesEllipsis from 'react-lines-ellipsis';
import AceEditor from 'react-ace';

class TaskHistoryModal extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            dataSource: [],
            loading: false,
            showModal: this.props.visible,
            taskId:"",
            engineType:"",
            pageInfo: {
                pageNum: 1,
                pageSize: 5,
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

    static propTypes = {
        visible: PropTypes.bool,
        onRefresh: PropTypes.func
    }

    static defaultProps = {
        visible: false,
        onRefresh: () => {
        }
    }

    componentWillReceiveProps(nextProps) {
        if ('showHistoryModal' in nextProps && nextProps.showHistoryModal) {
            this.setState({
                showModal: nextProps.showHistoryModal,
                taskId: nextProps.taskId,
                engineType: nextProps.engineType,
            })
            this.query(nextProps.taskId,nextProps.engineType,1,this.state.pageInfo.pageSize);
        }
    }

    query(taskId,engineType,pageNum,pageSize,outParams = {}) {
        let params = {
            pageNum: pageNum,
            pageSize: pageSize,
            taskId: taskId
        }
        Object.assign(params, outParams);
        this.setState({
            loading: true
        });
        let qurl = '';
        if(engineType == '0'){
            qurl = "/streamsuite/jstormEngine/listProcessByTaskId";
        }
        if(engineType == '1'){
            qurl = "/streamsuite/flinkEngine/listProcessByTaskId";
        }

        qs.post(qurl, params).then(data => {
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

    changePage(page, pageSize) {
        this.setState({
            pageInfo: {
                ...this.state.pageInfo,
                pageNum: page,
                pageSize: pageSize
            }
        }, () => {
            this.query(this.state.taskId,this.state.engineType,this.state.pageInfo.pageNum,this.state.pageInfo.pageSize);
        });
    }

    handleCancel = (e) => {
        this.setState({
            showScriptModal: false,
        })
    }

    render() {
        const modalProps = {
            destroyOnClose:true,
            visible: this.state.showModal,
            title: "任务执行历史",
            footer: null,
            onCancel: (e) => {
                this.setState({showModal: false});
                this.props.onRefresh();
            }
        }

        var list = (data,length) => {
            var res = [];
            for(var i = 0; i < length; i++) {
                res.push(<h6>{data[i]}</h6>);
            }
            return res
        }

        const state = this.state;

        let columns;
        if(this.state.engineType == '2'){
            columns = [{
                title: '执行时间',
                dataIndex: 'startTime',
                key: 'startTime',
                width: 180,
                render: (text) => moment(text).format("YYYY-MM-DD HH:mm:ss")
            },{
                title: '执行结果',
                dataIndex: 'result',
                key: 'result',
                width: 100,
                render: (text) => {
                    let name = ""
                    let color = ""
                    if (text == "1") {
                        name = "成功"
                        color = "green"
                    }
                    if (text == "0") {
                        name = "失败"
                        color = "red"
                    }
                    return (<font color={color}>{name}</font>)
                }
            },{
                title: '提交参数',
                dataIndex: 'taskConfig',
                key: 'taskConfig',
                width: 900,
                render: (text) => {
                    if (text != null && text!='') {
                        if(text[0].indexOf("运行任务文件") != -1){
                            const renderContent = (
                                <List
                                    bordered
                                    dataSource={text}
                                    size = 'small'
                                    renderItem={item => (
                                    <List.Item>
                                        <div>{item}</div>
                                    </List.Item>
                                )}
                                />
                            );
                            return (
                                <div>{renderContent}</div>
                            )
                        }else{
                            return (
                                <div>
                                    <div style={{color:'blue'}} onClick={(e) => {
                                        this.setState({
                                            showScriptModal: true,
                                            scriptText:text[0],
                                        });
                                    }}><u><pre><LinesEllipsis text={text[0]} maxLine={3} /></pre></u>
                                    </div>
                                    <Modal
                                        title="远程提交脚本"
                                        visible={this.state.showScriptModal}
                                        footer={null}
                                        maskClosable={false}
                                        onCancel={this.handleCancel}
                                        width={1200}
                                    >
                                        <AceEditor
                                            readOnly={true}
                                            showPrintMargin={true}
                                            showGutter={true}
                                            highlightActiveLine={true}
                                            editorProps={{$blockScrolling: true}}
                                            value={this.state.scriptText}
                                            width={950}
                                            height={650}
                                            setOptions={{
                                                enableBasicAutocompletion: true,
                                                enableLiveAutocompletion: true,
                                                enableSnippets: false,
                                                showLineNumbers: true,
                                                tabSize: 2,
                                            }}
                                        />
                                    </Modal>
                                </div>
                            )
                        }
                    }
                }
            },{
                title: 'yarnAppId',
                dataIndex: 'yarnAppId',
                key: 'yarnAppId',
                width: 150
            },{
                title: '提交日志',
                dataIndex: 'logMessage',
                key: 'logMessage',
                width: 100,
                render: (text) => {
                    if (text != null && text!='') {
                        const renderContent = (
                            <div style={{width:1500,height:500}} className="auto-overflow-container">
                                {list(text,text.length)}
                            </div>
                        );
                        return (
                            <div>
                                <Popover placement="left" content={renderContent} title="" >
                                    <div><u>查看</u></div>
                                </Popover>
                            </div>
                        )
                    }
                }
            }]
        }

        if(this.state.engineType == '1'){
            columns = [{
                title: '执行时间',
                dataIndex: 'startTime',
                key: 'startTime',
                width: 180,
                render: (text) => moment(text).format("YYYY-MM-DD HH:mm:ss")
            },{
                title: '执行结果',
                dataIndex: 'result',
                key: 'result',
                width: 100,
                render: (text) => {
                    let name = ""
                    let color = ""
                    if (text == "1") {
                        name = "成功"
                        color = "green"
                    }
                    if (text == "0") {
                        name = "失败"
                        color = "red"
                    }
                    return (<font color={color}>{name}</font>)
                }
            }, {
                title: '执行类型',
                dataIndex: 'type',
                key: 'type',
                width: 100,
            },{
                title: '提交参数',
                dataIndex: 'taskConfig',
                key: 'taskConfig',
                width: 900,
                render: (text) => {
                    if (text != null && text!='') {
                        const renderContent = (
                            <List
                                bordered
                                dataSource={text}
                                size = 'small'
                                renderItem={item => (
                                    <List.Item>
                                        <div>{item}</div>
                                    </List.Item>
                                )}
                            />
                        );
                        return (
                            <div>{renderContent}</div>
                        )
                    }
                }
            },{
                title: 'yarnAppId',
                dataIndex: 'yarnAppId',
                key: 'yarnAppId',
                width: 150
            },{
                title: '提交日志',
                dataIndex: 'logMessage',
                key: 'logMessage',
                width: 100,
                render: (text) => {
                    if (text != null && text!='') {
                        const renderContent = (
                            <div style={{width:1500,height:500}} className="auto-overflow-container">
                                {list(text,text.length)}
                            </div>
                        );
                        return (
                            <div>
                                <Popover placement="left" content={renderContent} title="" >
                                    <div><u>查看</u></div>
                                </Popover>
                            </div>
                        )
                    }
                }
            }]
        }

        if(this.state.engineType == '0'){
            columns = [{
                title: '执行时间',
                dataIndex: 'startTime',
                key: 'startTime',
                width: 180,
                render: (text) => moment(text).format("YYYY-MM-DD HH:mm:ss")
            }, {
                title: '执行结果',
                dataIndex: 'result',
                key: 'result',
                width: 100,
                render: (text) => {
                    let name = ""
                    let color = ""
                    if (text == "1") {
                        name = "成功"
                        color = "green"
                    }
                    if (text == "0") {
                        name = "失败"
                        color = "red"
                    }
                    return (<font color={color}>{name}</font>)
                }
            }, {
                title: '执行类型',
                dataIndex: 'type',
                key: 'type',
                width: 100,
            }, {
                title: 'yarnAppId',
                dataIndex: 'yarnAppId',
                key: 'yarnAppId',
                width: 150
            }, {
                title: '提交参数',
                dataIndex: 'taskConfig',
                key: 'taskConfig',
                width: 900,
                render: (text) => {
                    if (text != null && text!='') {
                        const renderContent = (
                            <List
                                bordered
                                dataSource={text}
                                size = 'small'
                                renderItem={item => (
                                    <List.Item>
                                        <Popover content={item} title="">
                                            <div>{item}</div>
                                        </Popover>
                                    </List.Item>
                                )}
                            />
                        );
                        return (
                            <div>{renderContent}</div>
                        )
                    }
                }
            }]
        }

        return (
            <Modal {...modalProps} width='80%'>
                <div className="listPage">
                    <div className="table-wrapper">
                        <Table rowKey="rowId" pagination={state.pageInfo} columns={columns} dataSource={state.dataSource}></Table>
                    </div>
                </div>
            </Modal>
        )
    }
}

export default Form.create()(TaskHistoryModal);