import React from 'react';
import {Form,Table} from "antd";
import { qs, util, config, auth, } from "libs";

class FlinkJobManagerForm extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            taskId: "",
        }
    }

    //组件加载时执行查询
    componentWillMount() {
        if(this.props.currentTab == 'flinkJobManagerConfig'){
            qs.form("/streamsuite/flinkEngine/getJobManagerConfigByTaskId", {taskId: this.props.taskId}).then((data) => {

                this.setState({
                    dataSource: data,
                });
            });
        }
    }

    render() {
        const state = this.state;
        const columns = [{
            title: '配置项名称',
            dataIndex: 'key',
            key: 'key'
        }, {
            title: '配置项值',
            dataIndex: 'value',
            key: 'value',
        }]

        return (
            <div className="listPage">
                <div className="table-wrapper">
                    <br></br>
                    <Table rowKey="configName" pagination={false} columns={columns}  dataSource={state.dataSource}></Table>
                </div>
            </div>
        )
    }
}

export default Form.create()(FlinkJobManagerForm);