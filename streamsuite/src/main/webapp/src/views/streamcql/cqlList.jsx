import React from 'react';
import {auth, qs, util} from 'libs';
import '../../styles/glob.less'
import {inject} from "mobx-react";
import CqlForm from './cqlForm';
import moment from 'moment';
import LinesEllipsis from 'react-lines-ellipsis';

import {Button, Table,Modal} from "antd";

import brace from 'brace'
import AceEditor from 'react-ace';

import './mode-cql';
import 'brace/theme/sqlserver';
import 'brace/ext/language_tools';
import 'brace/ext/searchbox';

@inject("appStore")
export default class CqlList extends React.Component {


    constructor(props, context) {
        super(props, context);
        this.state = {
            dataSource: [],
            loading: false,
            showModal: false,
            showScriptModal:false,
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
        qs.post("/streamsuite/cql/list", params).then(data => {
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

    handerAddJstorm = (e) => {
        this.setState({
            showModal: true,
            isNew: true,
            formTitle: "添加 JSTORM CQL 脚本",
            cqlName: '',
            cqlText: '',
            cqlType: '0',
            cqlRemark: '',
            id: '-1',
            userGroupId:undefined,
        })
    }

    handerAddFlink = (e) => {
        this.setState({
            showModal: true,
            isNew: true,
            formTitle: "添加 FLINK CQL 脚本",
            cqlName: '',
            cqlText: '',
            cqlType: '1',
            cqlRemark: '',
            id: '-1',
            userGroupId: undefined,
        })
    }



    handleCancel = (e) => {
        this.setState({
            showScriptModal: false,
        })
    }

    render() {

        const state = this.state;
        const columns = [{
            title: 'CQL名称',
            dataIndex: 'cqlName',
            key: 'cqlName',
            width: 200
        }, {
            title: '类型',
            dataIndex: 'cqlType',
            key: 'cqlType',
            width: 100,
            render: (text) => {
                let name = "JSTORM"
                if (text == "1") {
                    name = "FLINK"
                }
                return (<font>{name}</font>)
            }
        },
            {
            title: 'CQL内容',
            dataIndex: 'cqlText',
            key: 'cqlText',
            render: (text,record) => {

                return (
                    <div>
                        <div style={{color:'blue'}} onClick={(e) => {

                            this.setState({
                                showScriptModal: true,
                                scriptHeader:record.cqlName,
                                scriptText:record.cqlText,
                            })

                        }}><u><pre><LinesEllipsis text={text} maxLine={3} /></pre></u></div>
                        <Modal
                            title={this.state.scriptHeader}
                            visible={this.state.showScriptModal}
                            footer={null}
                            maskClosable={false}
                            onCancel={this.handleCancel}
                            width={1200}
                        >
                            <AceEditor
                                readOnly={true}
                                mode="cql"
                                theme="sqlserver"
                                name="cqlTextView"
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
            },

        }, {
            title: 'CQL说明',
            dataIndex: 'cqlRemark',
            key: 'cqlRemark',
            width: 200
        }, {
            title: '创建日期',
            dataIndex: 'createTime',
            key: 'createTime',
            width: 200,
            render: (text) => moment(text).format("YYYY-MM-DD HH:mm:ss")
        }, {
            title: '创建用户',
            dataIndex: 'creatorUserName',
            key: 'creatorUserName',
            width: 155
            },{
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


                return (
                    <div>

                        <Button size='small' type="primary" onClick={(e) => {

                            qs.form("/streamsuite/cql/queryById", {id: record.id}).then((data) => {

                                this.setState({
                                    showModal: true,
                                    isNew: false,
                                    formTitle: "编辑CQL",
                                    cqlName: data.cqlName,
                                    cqlType: data.cqlType,
                                    cqlText: data.cqlText,
                                    cqlRemark: data.cqlRemark,
                                    id: data.id,
                                    userGroupId:data.userGroupId,
                                })
                            });


                        }}>修改</Button>

                    </div>
                )
            }
        }]

        const formProps = {
            //表单标题,是 "编辑" 还是 "添加"
            formTitle: this.state.formTitle,
            visible: this.state.showModal,
            cqlName: this.state.cqlName,
            cqlText: this.state.cqlText,
            cqlType: this.state.cqlType,
            cqlRemark: this.state.cqlRemark,
            id: this.state.id,
            isNew: this.state.isNew,
            userGroupId: this.state.userGroupId,
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
                    <Button type="primary" onClick={this.handerAddJstorm}>添加 JSTORM CQL脚本</Button>
                    &nbsp;&nbsp;
                    <Button type="primary" onClick={this.handerAddFlink}>添加 FLINK SQL脚本</Button>
                </div>
                <div className="table-wrapper">
                    <Table rowKey="id" pagination={state.pageInfo} columns={columns}
                           dataSource={state.dataSource}></Table>
                </div>

                <CqlForm {...formProps}/>

            </div>
        )
    }


}