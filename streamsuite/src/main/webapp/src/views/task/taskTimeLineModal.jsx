import React from 'react';
import PropTypes from 'prop-types';
import { qs, util, config, auth } from "libs";
import {Form,Timeline ,Modal} from 'antd';

class TaskTimeLineModal extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            taskId:"",
            timeLinePending:"",
            timeLineItemData:[],
            showModal: this.props.showTimeLineModal,
            getStartTimeLineBind: null,
        }
    }

    static propTypes = {
        showTimeLineModal: PropTypes.bool,
        onRefresh: PropTypes.func
    }

    static defaultProps = {
        visible: false,
        onRefresh: () => {
        }
    }

    getStartTimeLine() {
        let postData = {
            id: this.state.taskId
        };
        qs.form("/streamsuite/task/task/getStartTimeLine", postData).then((data) => {
            this.setState({
                timeLinePending:data.pending,
                timeLineItemData:data.timelineItemList,
            });
        });
    }

    componentWillReceiveProps(nextProps) {
        if ('showTimeLineModal' in nextProps && nextProps.showTimeLineModal) {

            this.setState({
                timeLinePending:"",
                timeLineItemData:[],
                taskId: nextProps.taskId,
                showModal: nextProps.showTimeLineModal,
            });

            this.state.getStartTimeLineBind = window.setInterval(this.getStartTimeLine.bind(this),3000);
        }
    }

    render() {
        const {timeLinePending,timeLineItemData,getStartTimeLineBind} = this.state;

        const modalProps = {
            destroyOnClose:true,
            maskClosable: false,
            visible: this.state.showModal,
            title: "任务正在开始请耐心等待......",
            footer: null,
            onCancel: (e) => {
                this.setState({showModal: false});
                this.props.onRefresh();
                window.clearInterval(getStartTimeLineBind);
            }
        }

        return (
            <Modal {...modalProps}>
                <Timeline pending={timeLinePending}>
                    {timeLineItemData.map(d => <Timeline.Item color={d.status}>{d.content}</Timeline.Item>)}
                </Timeline>
            </Modal>
        )
    }
}

export default Form.create()(TaskTimeLineModal);
