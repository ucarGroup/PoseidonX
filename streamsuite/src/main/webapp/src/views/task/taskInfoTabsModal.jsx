import React from 'react';
import PropTypes from 'prop-types';
import {Form, Tabs , Modal} from "antd";
import { qs, util, config, auth } from "libs";
import TaskInfoForm from './taskInfoForm';
import JstormClusterInfoForm from './jstormClusterInfoForm';
import JstormTopologyInfoForm from './jstormTopologyInfoForm'
import JstormWorkerExceptionForm from './jstormWorkerExceptionForm';
import FlinkJobInfoForm from './flinkJobInfoForm';
import FlinkJobManagerForm from './flinkJobManagerForm';
import FlinkTaskManagerForm from './flinkTaskManagerForm';
import FlinkJobExceptionForm from './flinkJobExceptionForm';


class TaskInfoTabsModal extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            showModal: this.props.visible,
            currentTab: "taskInfo",
            engineType: "",
        }
    }

    static propTypes = {
        visible: PropTypes.bool,
        onRefresh: PropTypes.func
    }

    static defaultProps = {
        visible: false,
        onRefresh: () => {
        }
    }

    componentWillReceiveProps(nextProps) {
        if ('visible' in nextProps && nextProps.visible) {

            this.setState({
                showModal: nextProps.visible,
                taskId: nextProps.taskId,
                engineType: nextProps.engineType,
            })
        }
    }

    changeTab = (e) => {
        this.setState({
            currentTab: e,
        })
    }

    render() {
        const modalProps = {
            destroyOnClose:true,
            visible: this.state.showModal,
            title: "",
            footer: null,
            onCancel: (e) => {
                this.setState({showModal: false,currentTab: "taskInfo"});
                this.props.onRefresh();
            }
        }

        const tabItemProps = {
            taskId: this.state.taskId,
            engineType: this.state.engineType,
            currentTab: this.state.currentTab,
        }

        const TabPane = Tabs.TabPane;

        let isJstorm = (this.state.engineType == '0');
        let isFlink = (this.state.engineType == '1');

        return (
            <Modal {...modalProps} width='80%'>

                <div style={{'display':isJstorm?'inline':'none'}}>
                    <Tabs defaultActiveKey="taskInfo" onChange={this.changeTab}>
                        <TabPane tab="任务信息" key="taskInfo"><TaskInfoForm {...tabItemProps}/></TabPane>
                        <TabPane tab="集群信息" key="clusterInfo"><JstormClusterInfoForm {...tabItemProps}/></TabPane>
                        <TabPane tab="拓扑信息" key="topologyInfo"><JstormTopologyInfoForm {...tabItemProps}/></TabPane>
                        <TabPane tab="Exceptions" key="workerException"><JstormWorkerExceptionForm {...tabItemProps}/></TabPane>
                    </Tabs>
                </div>
                <div style={{'display':isFlink?'inline':'none'}}>
                    <Tabs defaultActiveKey="flinkTaskInfo" onChange={this.changeTab}>
                        <TabPane tab="任务信息" key="flinkTaskInfo"><TaskInfoForm {...tabItemProps}/></TabPane>
                        <TabPane tab="Job信息" key="flinkJobDetail"><FlinkJobInfoForm {...tabItemProps}/></TabPane>
                        <TabPane tab="Jobmanager配置信息" key="flinkJobManagerConfig"><FlinkJobManagerForm {...tabItemProps}/></TabPane>
                        <TabPane tab="TaskManager信息" key="flinkTaskManagers"><FlinkTaskManagerForm {...tabItemProps}/></TabPane>
                        <TabPane tab="Exception" key="flinkJobException"><FlinkJobExceptionForm {...tabItemProps}/></TabPane>
                    </Tabs>
                </div>
            </Modal>
        )
    }
}

export default Form.create()(TaskInfoTabsModal);