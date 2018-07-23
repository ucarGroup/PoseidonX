import React from 'react';
import {auth, qs, util} from 'libs';
import '../../styles/glob.less'
import {inject} from "mobx-react";

import {Table} from "antd";


@inject("appStore")
export default class HadoopConfigTabItem extends React.Component {


    constructor(props, context) {
        super(props, context);
        this.state = {
            dataSource: [],
            loading: false,
            configType:props.configType
        }
    }

    componentWillReceiveProps(nextProps) {

        const {appStore} = this.props;
        this.setState({
            configType: appStore.conftab,
        });
        this.query();
    }

    //组件加载时执行查询
    componentWillMount() {
        this.query();
    }


    getQueryString() {
        let params = {
            configType: this.state.configType
        }
        return params;
    }


    query(outParams = {}) {
        let params = this.getQueryString();
        qs.post("/streamsuite/config/hadoop/view", params).then(data => {

            this.setState({
                dataSource: data,
            });
        });
    }


    render() {
        const state = this.state;
        const columns = [{
            title: '配置项名称',
            dataIndex: 'configName',
            key: 'configName'
        }, {
            title: '配置项值',
            dataIndex: 'configValue',
            key: 'configValue',
        }]


        return (
            <div className="listPage">
                <div className="table-wrapper">
                    <Table rowKey="configName" pagination={false} columns={columns}  dataSource={state.dataSource}></Table>
                </div>
            </div>
        )
    }


}