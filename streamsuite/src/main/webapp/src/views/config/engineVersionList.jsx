import React from 'react';
import {auth, qs, util} from 'libs';
import '../../styles/glob.less'
import {inject} from "mobx-react";
import EngineVersionForm from './engineVersionForm';
import moment from 'moment';

import {Button, Modal, Table} from "antd";


@inject("appStore")
export default class EngineVersionList extends React.Component {


    constructor(props, context) {
        super(props, context);
        this.state = {
            dataSource: [],
            loading: false,
            showModal: false,
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
                showModal: false
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
        qs.post("/streamsuite/engineVersion/list", params).then(data => {
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

    handerAddJstorm = (e) =>{
        this.setState({
            showModal: true,
            isNew: true,
            formTitle: "添加引擎版本",
            versionName: '',
            versionType: "JSTORM",
            versionRemark: '',
            versionUrl: '',
            id: '-1'
        })
    }

    handerAddJstormAM = (e) =>{
        this.setState({
            showModal: true,
            isNew: true,
            formTitle: "添加引擎版本",
            versionName: '',
            versionType: "JSTORM_AM",
            versionRemark: '',
            versionUrl: '',
            id: '-1'
        })
    }


    deleteVersion = (e)=> {
        let postData = {
            id: e
        };

        qs.form("/streamsuite/engineVersion/disable", postData).then((data) => {
            this.refresh()
        });
    }


    render() {

        const state = this.state;
        const columns = [{
            title: '版本名称',
            dataIndex: 'versionName',
            key: 'versionName',
            width: 200
        }, {
            title: '版本类型',
            dataIndex: 'versionType',
            key: 'versionType',
            width: 200,
        }, {
            title: '版本说明',
            dataIndex: 'versionRemark',
            key: 'versionRemark',
            width: 300
        }, {
            title: '版本路径',
            dataIndex: 'versionUrl',
            key: 'versionUrl',
            width: 300
        }, {
            title: '状态',
            dataIndex: 'status',
            key: 'status',
            render: (text) => {
                let name = "可用"
                let color = "#000"
                if (text == "1") {
                    name = "禁用"
                    color = "red"
                }
                return (<font color={color}>{name}</font>)
            }
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

                let enableEdit = !((auth.isSuperManager()) && (record.status == "0"));

                return (
                    <div>
                        {/* 只有超级管理员可以修改 */}
                        <Button size='small' type="primary" disabled={enableEdit} onClick={(e) => {

                            Modal.confirm({
                                title: '确认删除',
                                content: '请问是否确认禁用该引擎版本:'+record.versionName,
                                okText: '确认',
                                cancelText: '取消',
                                onOk:()=>this.deleteVersion(record.id)
                            });


                        }}>废弃</Button>
                    </div>
                )
            }
        }]

        const formProps = {
            formTitle: this.state.formTitle,
            visible: this.state.showModal,
            versionName: this.state.versionName,
            versionType: this.state.versionType,
            versionRemark: this.state.versionRemark,
            versionUrl: this.state.versionUrl,
            id: this.state.id,
            isNew: this.state.isNew,
            onRefresh: (e) => {
                this.setState({
                    showModal: false
                })
                this.refresh()
            }
        }

        return (
            <div className="listPage">
                <div className="oper-panel">
                    <Button type="primary" onClick={this.handerAddJstorm} disabled={!(auth.isSuperManager())}>添加JSTORM</Button>
                    &nbsp;&nbsp;
                    <Button type="primary" onClick={this.handerAddJstormAM} disabled={!(auth.isSuperManager())}>添加JSTORM_AM</Button>

                </div>
                <div className="table-wrapper">
                    <Table rowKey="id" pagination={state.pageInfo} columns={columns}
                           dataSource={state.dataSource}></Table>
                </div>

                <EngineVersionForm {...formProps}/>

            </div>
        )
    }


}