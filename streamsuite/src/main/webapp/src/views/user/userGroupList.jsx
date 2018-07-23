import React from 'react';
import {auth, qs, util} from 'libs';
import '../../styles/glob.less'
import {inject} from "mobx-react";
import UserGroupForm from './userGroupForm';
import moment from 'moment';

import {Button, Table} from "antd";


@inject("appStore")
export default class UserGroupList extends React.Component {


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
        qs.post("/streamsuite/usergroup/list", params).then(data => {
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

    handerAddUserGroup = (e) => {
        this.setState({
            showModal: true,
            isNew: true,
            formTitle: "添加用户组",
            name: '',
            members: '',
            id: '-1'
        })
    }


    render() {

        const state = this.state;
        const columns = [{
            title: '用户组名',
            dataIndex: 'name',
            key: 'name',
            width:150,
        }, {
            title: '创建日期',
            dataIndex: 'createTime',
            key: 'createTime',
            width: 200,
            render: (text) => moment(text).format("YYYY-MM-DD HH:mm:ss")
        }, {
            title: '修改日期',
            dataIndex: 'modifyTime',
            key: 'modifyTime',
            width: 200,
            render: (text) => {
                if (text != null) {
                    return moment(text).format("YYYY-MM-DD HH:mm:ss")
                }
            }
        }, {
            title: '操作',
            width: 50,
            render: (text, record) => {
                const {router} = this.props;

                let enableEdit = !(auth.isSuperManager());

                return (
                    <div>
                        {/* 只有超级管理员可以修改 */}
                        <Button size='small' type="primary" disabled={enableEdit} onClick={(e) => {

                            qs.form("/streamsuite/usergroup/queryById", {id: record.id}).then((data) => {

                                this.setState({
                                    showModal: true,
                                    isNew: false,
                                    formTitle: "编辑用户组",
                                    name: data.name,
                                    members: data.members,
                                    id: data.id
                                })
                            });


                        }}>修改</Button>

                    </div>
                )
            }
        }]

        const userGroupFormProps = {
            //用户组表单标题,是 "编辑用户组" 还是 "添加用户组"
            formTitle: this.state.formTitle,
            visible: this.state.showModal,
            name: this.state.name,
            members: this.state.members,
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
                    <Button type="primary" onClick={this.handerAddUserGroup} disabled={!(auth.isSuperManager())}>添加用户组</Button>
                </div>
                <div className="table-wrapper">
                    <Table rowKey="id" pagination={state.pageInfo} columns={columns}  dataSource={state.dataSource}></Table>
                </div>

                <UserGroupForm {...userGroupFormProps}/>

            </div>
        )
    }


}