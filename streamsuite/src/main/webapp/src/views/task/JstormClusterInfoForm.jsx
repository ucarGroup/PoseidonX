import React from 'react';
import {Form,Table} from "antd";
import { qs, util, config, auth, } from "libs";

class JstormClusterInfoForm extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            taskId: "",
            amDataSource: [],
            clusterDataSource: [],
            nimbusDataSource: [],
            supervisorDataSource: [],
        }
    }

    //组件加载时执行查询
    componentWillMount() {
        if(this.props.currentTab == 'clusterInfo'){
            qs.form("/streamsuite/jstormEngine/getClusterDetailByTaskId", {taskId: this.props.taskId}).then((data) => {
                this.setState({
                    amDataSource:data,
                    clusterDataSource:data,
                    nimbusDataSource:data[0].nimbusInfos,
                    supervisorDataSource:data[0].supervisorInfos,
                });
            });
        }
    }

    render() {
        const state = this.state;

        const amDataColumns = [{
            title: 'App Id',
            dataIndex: 'appId',
            key: 'appId'
        },{
            title: 'Host',
            dataIndex: 'appAmHost',
            key: 'appAmHost'
        },{
            title: 'RpcPort',
            dataIndex: 'appAmPort',
            key: 'appAmPort'
        },{
            title: 'StartTime',
            dataIndex: 'appStartTime',
            key: 'appStartTime'
        },{
            title: 'Status',
            dataIndex: 'appState',
            key: 'appState'
        }]

        const clusterDataColumns = [{
            title: 'ZkRoot',
            dataIndex: 'clusterZkRoot',
            key: 'clusterZkRoot'
        },{
            title: 'ZkHost(Port)',
            dataIndex: 'clusterZkHost',
            key: 'clusterZkHost'
        },{
            title: 'Supervisor',
            dataIndex: 'clusterSupervisorSize',
            key: 'clusterSupervisorSize'
        },{
            title: 'Ports Usage(Worker)',
            dataIndex: 'clusterSlots',
            key: 'clusterSlots'
        }
]

        const nimbusTableColumns = [{
            title: 'Host',
            dataIndex: 'host',
            key: 'host'
        },{
            title: ' Uptime',
            dataIndex: 'uptime',
            key: 'uptime'
        },{
            title: ' ContainerId',
            dataIndex: 'containerId',
            key: 'containerId'
        },{
            title: ' ContainerStats',
            dataIndex: 'containerStats',
            key: 'containerStats'
        }]

        const supervisorTableColumns = [{
            title: 'Host',
            dataIndex: 'host',
            key: 'host'
        },{
            title: ' Uptime',
            dataIndex: 'uptime',
            key: 'uptime'
        },{
            title: ' Worker PortsList',
            dataIndex: 'portsList',
            key: 'portsList'
        },{
            title: ' ContainerId',
            dataIndex: 'containerId',
            key: 'containerId'
        },{
            title: ' ContainerStats',
            dataIndex: 'containerStats',
            key: 'containerStats'
        }]

        return (
            <div className="listPage">
                <div className="table-wrapper">
                    <strong>Yarn Application Master</strong>
                    <Table rowKey="appId" pagination={false} columns={amDataColumns}  dataSource={state.amDataSource}></Table><br></br>

                    <strong>Jstorm Cluster</strong>
                    <Table rowKey="clusterZkRoot" pagination={false} columns={clusterDataColumns}  dataSource={state.clusterDataSource}></Table><br></br>

                    <strong>Jstorm Cluster Nimbus</strong>
                    <Table rowKey="host" pagination={false} columns={nimbusTableColumns}  dataSource={state.nimbusDataSource}></Table><br></br>

                    <strong>Jstorm Cluster Supervisor</strong>
                    <Table rowKey="host" pagination={false} columns={supervisorTableColumns}  dataSource={state.supervisorDataSource}></Table>
                </div>
            </div>
        )
    }
}

export default Form.create()(JstormClusterInfoForm);