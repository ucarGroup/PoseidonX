import React from 'react';
import {Form,Table} from "antd";
import { qs, util, config, auth, } from "libs";

class FlinkTaskManagerForm extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            taskId: "",
            taskManagers: [],
        }
    }

    //组件加载时执行查询
    componentWillMount() {
        if(this.props.currentTab == 'flinkTaskManagers'){
            qs.form("/streamsuite/flinkEngine/getTaskManagersByTaskId", {taskId: this.props.taskId}).then((data) => {
                if(data!=null){
                    this.setState({
                        taskManagers: data,
                    });
                }
            });
        }
    }

    render() {

        const state = this.state;

        const taskManagersColumns = [{
            title: 'Path',
            dataIndex: 'path',
            key: 'path',
            width: 200,
        },{
            title: 'Data Port',
            dataIndex: 'dataPort',
            key: 'dataPort',
            width: 80,
        },{
            title: 'Last Heartbeat',
            dataIndex: 'lastHeartbeat',
            key: 'lastHeartbeat',
            width: 100,
        },{
            title: 'All Slots',
            dataIndex: 'slotsNumber',
            key: 'slotsNumber',
            width: 100,
        },{
            title: 'Free Slots',
            dataIndex: 'freeSlots',
            key: 'freeSlots',
            width: 100,
        },{
            title: 'CPU Cores',
            dataIndex: 'cpuCores',
            key: 'cpuCores',
            width: 100,
        },{
            title: 'Physical Memory',
            dataIndex: 'physicalMemoryShow',
            key: 'physicalMemoryShow',
            width: 100,
        },{
            title: 'JVM Heap Size',
            dataIndex: 'freeMemoryShow',
            key: 'freeMemoryShow',
            width: 100,
        },{
            title: 'Flink Managed Memory',
            dataIndex: 'managedMemoryShow',
            key: 'managedMemoryShow',
            width: 150
        }]

        return (
            <div className="listPage">
                <div className="table-wrapper">
                    <br></br>
                    <Table rowKey="path" pagination={false} columns={taskManagersColumns}  dataSource={state.taskManagers}></Table>
                </div>
            </div>
        )
    }
}

export default Form.create()(FlinkTaskManagerForm);