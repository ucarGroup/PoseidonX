import React from 'react';
import {Form,Table} from "antd";
import { qs, util, config, auth, } from "libs";

class FlinkJobInfoForm extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            taskId: "",
            jobDataSource: [],
            compenentDataSource: []
        }
    }

    //组件加载时执行查询
    componentWillMount() {
        if(this.props.currentTab == 'flinkJobDetail'){
            qs.form("/streamsuite/flinkEngine/getJobDetailByTaskId", {taskId: this.props.taskId}).then((data) => {
                this.setState({
                    jobDataSource:data,
                    compenentDataSource:data[0].vertices,
                });
            });
        }
    }

    render() {
        const state = this.state;

        const jobDataColumns = [{
            title: 'App Id',
            dataIndex: 'appId',
            key: 'appId'
        },{
            title: 'App Status',
            dataIndex: 'appStatus',
            key: 'appStatus'
        },{
            title: 'Slots Total',
            dataIndex: 'slotsTotal',
            key: 'slotsTotal'
        },{
            title: 'Slots Available',
            dataIndex: 'slotsAvailable',
            key: 'slotsAvailable'
        },{
            title: 'Flink Version',
            dataIndex: 'flinkVersion',
            key: 'flinkVersion'
        },{
            title: 'Job Id',
            dataIndex: 'jid',
            key: 'jid'
        },{
            title: 'Job State',
            dataIndex: 'state',
            key: 'state'
        },{
            title: 'Start Time',
            dataIndex: 'startTimeShow',
            key: 'startTimeShow'
        },{
            title: 'Uptime',
            dataIndex: 'uptime',
            key: 'uptime'
        }]

        const compenentColumns = [{
            title: 'Name',
            dataIndex: 'name',
            key: 'name',
            width: 1000,
        },{
            title: 'Parallelism',
            dataIndex: 'parallelism',
            key: 'parallelism'
        },{
            title: 'Status',
            dataIndex: 'status',
            key: 'status'
        },{
            title: 'StartTime',
            dataIndex: 'startTimeShow',
            key: 'startTimeShow'
        },{
            title: 'Uptime',
            dataIndex: 'uptime',
            key: 'uptime'
        }]

        return (
            <div className="listPage">
                <div className="table-wrapper">
                    <br></br>
                    <Table rowKey="appId" pagination={false} columns={jobDataColumns}  dataSource={state.jobDataSource}></Table><br></br>

                    Vertice Info
                    <Table rowKey="id" pagination={false} columns={compenentColumns}  dataSource={state.compenentDataSource}></Table><br></br>
                </div>
            </div>
        )
    }
}

export default Form.create()(FlinkJobInfoForm);