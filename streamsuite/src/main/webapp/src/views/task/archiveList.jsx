import React from 'react';
import {auth, qs, util} from 'libs';
import '../../styles/glob.less'
import {inject} from "mobx-react";
import moment from 'moment';

import {Button, Table,Tag} from "antd";

import ArchiveForm from "./archiveForm"
import ArchiveVersionForm from "./archiveVersionForm"
import ArchiveVersionList from "./archiveVersionList"

@inject("appStore")
export default class ArchiveList extends React.Component {


    constructor(props, context) {
        super(props, context);
        this.state = {
            dataSource: [],
            loading: false,
            showArchiveFormModal: false,
            showArchiveVersionFormModal: false,
            showArchiveVersionListModal: false,
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
                showArchiveFormModal: false,
                showArchiveVersionFormModal: false,
                showArchiveVersionListModal: false,
            });
        }
        this.query();
    }


    getQueryString() {
        const pageInfo = this.state.pageInfo;
        let params = {
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
        qs.post("/streamsuite/task/archive/list", params).then(data => {
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

    handerAdd = (e) => {
        this.setState({
            showArchiveFormModal: true,
            showArchiveVersionFormModal: false,
            showArchiveVersionListModal: false,
            isNew: true,
            formTitle: "添加任务文件信息",
            taskArchiveName: '',
            taskArchiveRemark: '',
            userGroupId: undefined,
            id: '-1'
        })
    }


    render() {

        const state = this.state;
        const columns = [{
            title: '任务文件名称',
            dataIndex: 'taskArchiveName',
            key: 'taskArchiveName',
            width: 200
        }, {
            title: '任务文件说明',
            dataIndex: 'taskArchiveRemark',
            key: 'taskArchiveRemark',
            width: 400
        }, {
            title: '版本数目',
            dataIndex: 'taskArchiveCount',
            key: 'taskArchiveCount',
            width: 200,
            render: (text, record) => {
                const {router} = this.props;

                let enableEdit = !(auth.isSuperManager());

                return (
                    <div>
                        <Tag color="geekblue"  onClick={(e) => {

                            let params = {
                                archiveId: record.id
                            }

                            qs.post("/streamsuite/task/archive/getArchiveVersionByArchiveId", params).then(data => {

                                this.setState({
                                    dataSourceForVersion: data,
                                    showArchiveFormModal: false,
                                    showArchiveVersionFormModal: false,
                                    showArchiveVersionListModal: true,
                                    isNew: false,
                                    formTitle: "任务 " + record.taskArchiveName + " 的版本列表",
                                    id: record.id,
                                    archiveName: record.taskArchiveName,
                                });
                            });


                        }}>{text}个</Tag>

                    </div>
                )
            }
        }, {
            title: '创建用户',
            dataIndex: 'createUser',
            key: 'createUser',
            width: 250
        }, {
            title: '所属用户组',
            dataIndex: 'userGroupName',
            key: 'userGroupName',
            width: 200
        }, {
            title: '创建日期',
            dataIndex: 'createTime',
            key: 'createTime',
            width: 200,
            render: (text) => moment(text).format("YYYY-MM-DD HH:mm:ss")
        }, {
            title: '操作',
            width: 100,
            render: (text, record) => {
                const {router} = this.props;

                let enableEdit = !(auth.isSuperManager());

                return (
                    <div>
                        <Button size='small' type="primary" disabled={enableEdit} onClick={(e) => {

                            this.setState({
                                showArchiveFormModal: false,
                                showArchiveVersionFormModal: true,
                                showArchiveVersionListModal: false,
                                isNew: false,
                                formTitle: "上传 " + record.taskArchiveName + " 新版本文件",
                                id: record.id,
                                archiveName: record.taskArchiveName,
                            })


                        }}>上传文件</Button>

                        <Button size='small' type="primary" disabled={enableEdit} onClick={(e) => {
                            qs.form("/streamsuite/task/archive/queryById", {id: record.id}).then((data) => {
                                this.setState({
                                    showArchiveFormModal: true,
                                    showArchiveVersionFormModal: false,
                                    showArchiveVersionListModal: false,
                                    isNew: false,
                                    id: record.id,
                                    taskArchiveName: data.taskArchiveName,
                                    taskArchiveRemark:data.taskArchiveRemark,
                                    userGroupId:data.userGroupId,
                                })
                            });
                        }}>修改</Button>

                    </div>
                )
            }
        }]


        const archiveFormProps = {
            //配置表单标题,是 "编辑配置项" 还是 "添加配置项"
            formTitle: this.state.formTitle,
            visible: this.state.showArchiveFormModal,
            taskArchiveName: this.state.taskArchiveName,
            taskArchiveRemark: this.state.taskArchiveRemark,
            userGroupId: this.state.userGroupId==null?undefined:this.state.userGroupId,
            id: this.state.id,
            isNew: this.state.isNew,
            onRefresh: (e) => {
                this.setState({
                    showArchiveFormModal: false,
                    showArchiveVersionFormModal: false,
                    showArchiveVersionListModal: false,
                })
                this.refresh()
            }
        }


        const archiveVersionFormProps = {
            //配置表单标题,是 "编辑配置项" 还是 "添加配置项"
            formTitle: this.state.formTitle,
            visible: this.state.showArchiveVersionFormModal,
            id: this.state.id,
            isNew: this.state.isNew,
            onRefresh: (e) => {
                this.setState({
                    showArchiveFormModal: false,
                    showArchiveVersionFormModal: false,
                    showArchiveVersionListModal: false,
                })
                this.refresh()
            }
        }

        const archiveVersionListProps = {
            //配置表单标题,是 "编辑配置项" 还是 "添加配置项"
            formTitle: this.state.formTitle,
            visible: this.state.showArchiveVersionListModal,
            id: this.state.id,
            archiveName: this.state.archiveName,
            dataSourceForVersion: this.state.dataSourceForVersion,
            onRefresh: (e) => {
                this.setState({
                    showArchiveFormModal: false,
                    showArchiveVersionFormModal: false,
                    showArchiveVersionListModal: false,
                })
                this.refresh()
            }
        }

        return (
            <div className="listPage">
                <div className="oper-panel">
                    <Button type="primary" onClick={this.handerAdd}>添加任务文件</Button>
                </div>
                <div className="table-wrapper">
                    <Table rowKey="id" pagination={state.pageInfo} columns={columns}
                           dataSource={state.dataSource}></Table>
                </div>

                <ArchiveForm {...archiveFormProps}/>
                <ArchiveVersionForm {...archiveVersionFormProps}/>
                <ArchiveVersionList {...archiveVersionListProps}/>
            </div>
        )
    }


}