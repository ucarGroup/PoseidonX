import React from 'react';
import {Form,Table} from "antd";
import { qs, util, config, auth, } from "libs";
import '../../styles/glob.less'

class FlinkJobExceptionForm extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            taskId: "",
            rootExceptionShow: [],
            allExceptions: [],
        }
    }

    //组件加载时执行查询
    componentWillMount() {
        if(this.props.currentTab == 'flinkJobException'){
            qs.form("/streamsuite/flinkEngine/getJobExceptionByTaskId", {taskId: this.props.taskId}).then((data) => {
                if(data!=null){
                    this.setState({
                        rootExceptionShow: data.rootExceptionShow,
                        allExceptions: data.allExceptions,
                    });
                }
            });
        }
    }

    render() {
        var list = (data,length) => {
            var res = [];
            for(var i = 0; i < length; i++) {
                res.push(<h6>{data[i]}</h6>);
            }
            return res
        }

        const state = this.state;

        const allExceptionsColumns = [{
            title: 'Location',
            dataIndex: 'location',
            key: 'location',
            width: 150,
        },{
            title: 'Task',
            dataIndex: 'task',
            key: 'task',
            width: 200,
        },{
            title: 'Exception',
            width: 500,
            render: (text, record) => {
                let exceptionShow = record.exceptionShow;
                return (
                    <div className="auto-overflow-container">
                        {list(exceptionShow,exceptionShow.length)}
                    </div>
                )
            }
        }]

        return (
            <div className="listPage">
                <div className="table-wrapper">

                    RootException
                    <div className="auto-overflow-container">
                      {list(state.rootExceptionShow,state.rootExceptionShow.length)}
                    </div>
                    AllException
                    <Table rowKey="id" pagination={false} columns={allExceptionsColumns}  dataSource={state.allExceptions}></Table>
                </div>
            </div>
        )
    }
}

export default Form.create()(FlinkJobExceptionForm);