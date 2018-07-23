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
export default class UserLoginHistoryList extends React.Component {


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
        qs.post("/streamsuite/user/loginhistorylist", params).then(data => {
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



    render() {

        const state = this.state;
        const columns = [{
            title: '用户邮箱',
            dataIndex: 'userName',
            key: 'userName'
        }, {
            title: '用户类型',
            dataIndex: 'userRole',
            key: 'userRole',
            render: (text) => {
                let color = "#000"
                if (text == "超级管理员") {
                    color = "#a52a2a"
                }
                return (<font color={color}>{text}</font>)
            }
        }, {
            title: '登录ip',
            dataIndex: 'loginIp',
            key: 'loginIp'
        }, {
            title: '登录时间',
            dataIndex: 'loginTime',
            key: 'loginTime',
            width: 200,
            render: (text) => moment(text).format("YYYY-MM-DD HH:mm:ss")
        }]

        return (
            <div className="listPage">
                <div className="table-wrapper">
                    <Table rowKey="id" pagination={state.pageInfo} columns={columns} dataSource={state.dataSource}></Table>
                </div>
            </div>
        )
    }


}