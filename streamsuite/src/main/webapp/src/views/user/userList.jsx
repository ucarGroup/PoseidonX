import React from 'react';
import {auth, qs, util} from 'libs';
import '../../styles/glob.less'
import {inject} from "mobx-react";
import UserForm from './userForm';
import moment from 'moment';

import {Button, Table} from "antd";
import {observer} from "mobx-react/index";

const USER_ROLE_MANAGER = "0";
const USER_ROLE_NORMAL = "1";
const USER_STATE_ENABLE = "0";
const USER_STATE_DISABLE = "1";

@inject("appStore")
@observer
export default class UserList extends React.Component {


    constructor(props, context) {
        super(props, context);
        this.state = {
            userName: '',
            password: '',
            mobile: '',
            userRole: USER_ROLE_NORMAL,
            userStatus: USER_STATE_ENABLE,
            userId: '-1',
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
        qs.post("/streamsuite/user/list", params).then(data => {
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

    handerAddUser = (e) => {
        this.setState({
            showModal: true,
            isNew: true,
            userFormTitle: "添加用户",
            userName: '',
            password: '',
            mobile: '',
            userRole: USER_ROLE_NORMAL,
            userStatus: USER_STATE_ENABLE,
            userId: '-1'
        })
    }


    render() {

        const state = this.state;
        const columns = [{
            title: '用户邮箱',
            dataIndex: 'userName',
            key: 'userName'
        }, {
            title: '用户手机',
            dataIndex: 'mobile',
            key: 'mobile',
        }, {
            title: '用户类型',
            dataIndex: 'userRole',
            key: 'userRole',
            render: (text) => {
                let name = "普通用户"
                let color = "#000"
                if (text == "0") {
                    name = "超级管理员"
                    color = "#a52a2a"
                }
                return (<font color={color}>{name}</font>)
            }
        }, {
            title: '用户状态',
            dataIndex: 'userStatus',
            key: 'userStatus',
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
            width: 100,
            render: (text, record) => {
                const {router} = this.props;

                let enableEdit = !(auth.isSuperManager() || record.userName == auth.getUserName());

                return (
                    <div>
                        {/* 只有超级管理员和当前用户可以修改 */}
                        <Button size='small' type="primary" disabled={enableEdit} onClick={(e) => {


                            qs.form("/streamsuite/user/queryById", {id: record.id}).then((data) => {

                                this.setState({
                                    showModal: true,
                                    isNew: false,
                                    userFormTitle: "编辑用户",
                                    userName: data.userName,
                                    password: data.password,
                                    mobile: data.mobile,
                                    userRole: data.userRole,
                                    userStatus: data.userStatus,
                                    userId: data.id
                                })
                            });

                        }}>修改</Button>

                    </div>
                )
            }
        }]

        const userFormProps = {
            //用户表单标题,是 "编辑用户" 还是 "添加用户"
            userFormTitle: this.state.userFormTitle,
            visible: this.state.showModal,
            userName: this.state.userName,
            password: this.state.password,
            mobile: this.state.mobile,
            userRole: this.state.userRole,
            userStatus: this.state.userStatus,
            userId: this.state.userId,
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
                    <Button type="primary" onClick={this.handerAddUser} disabled={!(auth.isSuperManager())}>添加用户</Button>
                </div>
                <div className="table-wrapper">
                    <Table rowKey="id" pagination={state.pageInfo} columns={columns} dataSource={state.dataSource}></Table>
                </div>

                <UserForm {...userFormProps}/>

            </div>
        )
    }


}