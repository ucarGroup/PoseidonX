import React from 'react';
import PropTypes from 'prop-types';
import {Form, Modal,  RadioGroup, Table, Button, notification} from "antd";
import {auth, config, qs, util} from "libs";
import {inject} from "mobx-react/index";
import moment from 'moment';

const FormItem = Form.Item;

@inject("appStore")
class ArchiveVersionList extends React.Component {
    constructor(props, context) {
        super(props, context);
        this.state = {
            dataSource: [],
            loading: false,
            showModal: this.props.visible,
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
        if ('visible' in nextProps) {

            this.setState({
                showModal: nextProps.visible,
                isNew: nextProps.isNew,
                isSuperManager: auth.isSuperManager(),
                archiveId: nextProps.id,
                archiveName: nextProps.archiveName,
                dataSource:nextProps.dataSourceForVersion,
            });

        }
    }
    //组件加载时执行查询
    componentWillMount() {
    }



    render() {
        const modalProps = {
            destroyOnClose: true,
            maskClosable: false,
            visible: this.state.showModal,
            title: this.props.formTitle,
            footer: null,
            width:1000,
            onCancel: (e) => {
                this.setState({
                    showModal: false,
                    uploadDisabled: false,
                });
                this.props.onRefresh();
            }
        }


        const columns = [{
            title: '文件hdfs地址',
            dataIndex: 'taskArchiveVersionUrl',
            key: 'taskArchiveVersionUrl',
            width: 350,
            render: (text,record) => {
                return (
                    <div onClick={(e) => {
                        qs.get("/streamsuite/task/archive/download/", {id: record.id}).then((data) => {
                            //下载失败
                            //if (!data) {
                            //    notification.error({
                            //        message:  "任务文件保存失败",
                            //    });
                            //
                            //}
                        });
                    }}>
                        {text}
                    </div>
                )

            }
        }, {
            title: '说明',
            dataIndex: 'taskArchiveVersionRemark',
            key: 'taskArchiveVersionRemark',
            width: 120
        },  {
            title: '上传用户',
            dataIndex: 'createUser',
            key: 'createUser',
            width: 200
        },  {
            title: '创建日期',
            dataIndex: 'createTime',
            key: 'createTime',
            width: 200,
            render: (text) => moment(text).format("YYYY-MM-DD HH:mm:ss")
        }]


        return (
            <Modal {...modalProps}>
                <div className="listPage">
                    <div className="table-wrapper">
                        <Table rowKey="id" pagination={false} columns={columns}  dataSource={this.state.dataSource}></Table>
                    </div>
                </div>
            </Modal>
        )
    }
}

export default Form.create()(ArchiveVersionList);