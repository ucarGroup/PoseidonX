import React from 'react';
import {Form,Table,List} from "antd";
import { qs, util, config, auth } from "libs";

class JstormTopologyInfoForm extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            taskId: "",
            topDataSource: [],
            compenentDataSource: []
        }
    }

    //组件加载时执行查询
    componentWillMount() {
        if(this.props.currentTab == 'topologyInfo'){
            qs.form("/streamsuite/jstormEngine/getTopologyDetailByTaskId", {taskId: this.props.taskId}).then((data) => {
                this.setState({
                    topDataSource:data,
                    compenentDataSource:data[0].compenentInfos,
                });
            });
        }
    }

    render() {
        var list = (data,length) => {
            var res = [];
            for(var i = 0; i < length; i++) {
                var host = data[i].host;
                var post = data[i].port;
                res.push(<h6>{host + ":" + post}</h6>);
            }
            return res
        }

        var listError = (data,length) => {
            var res = [];
            for(var i = 0; i < length; i++) {
                res.push(<h6>{data[i]}</h6>);
            }
            return res
        }

        const state = this.state;

        const topDataColumns = [{
            title: 'Top Id',
            dataIndex: 'topId',
            key: 'topId'
        },{
            title: 'Top Name',
            dataIndex: 'name',
            key: 'name'
        },{
            title: 'Status',
            dataIndex: 'status',
            key: 'status'
        },{
            title: 'Worker',
            dataIndex: 'workersTotal',
            key: 'workersTotal'
        },{
            title: 'Uptime',
            dataIndex: 'uptime',
            key: 'uptime'
        }]

        const compenentColumns = [{
            title: 'Compenent name',
            dataIndex: 'name',
            key: 'name'
        },{
            title: 'Compenent type',
            width: 200,
            dataIndex: 'type',
            key: 'type'
        },{
            title: 'Task thread（starting/active/total）',
            dataIndex: 'taskInfo',
            key: 'taskInfo'
        }, {
            title: 'Worker',
            width: 200,
            render: (text, record) => {
                let workerData = record.workers;
                return (
                    <div>
                        {list(workerData,workerData.length)}
                    </div>
                )
            }
        },{
            title: 'ErrorMessage',
            width: 500,
            dataIndex: 'errorMessage',
            key: 'errorMessage',
            render: (text) => {
                if (text != null && text!='') {
                    var jsonObj =  JSON.parse(text);
                    const renderContent = (
                        <div className="auto-overflow-container">
                            {listError(jsonObj,jsonObj.length)}
                        </div>
                    );
                    return (
                        <div>{renderContent}</div>
                    )
                }
            }
        }]

        return (
            <div className="listPage">
                <div className="table-wrapper">
                    <Table rowKey="topId" pagination={false} columns={topDataColumns}  dataSource={state.topDataSource}></Table><br></br>

                    <strong>Compenent</strong>
                    <Table rowKey="name" pagination={false} columns={compenentColumns}  dataSource={state.compenentDataSource}></Table><br></br>

                </div>
            </div>
        )
    }
}

export default Form.create()(JstormTopologyInfoForm);