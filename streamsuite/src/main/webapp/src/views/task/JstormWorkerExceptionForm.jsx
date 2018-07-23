import React from 'react';
import {Form,Table} from "antd";
import { qs, util, config, auth, } from "libs";
import '../../styles/glob.less'

class JstormWorkerExceptionForm extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            taskId: "",
            rootException: [],
            workerExceptions: [],
        }
    }

    //组件加载时执行查询
    componentWillMount() {
        if(this.props.currentTab == 'workerException'){
            qs.form("/streamsuite/jstormEngine/getTopologyExceptionByTaskId", {taskId: this.props.taskId}).then((data) => {
                if(data!=null){
                    this.setState({
                        rootException: data.rootException,
                        workerExceptions: data.workerExceptions,
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

        return (
            <div className="listPage">
                <div className="table-wrapper">

                    Root Exception
                    <div className="auto-overflow-container">
                        {list(state.rootException,state.rootException.length)}
                    </div>
                    Worker Exception (The last week)
                    <div className="auto-overflow-container">
                        {list(state.workerExceptions,state.workerExceptions.length)}
                    </div>
                </div>
            </div>
        )
    }
}

export default Form.create()(JstormWorkerExceptionForm);