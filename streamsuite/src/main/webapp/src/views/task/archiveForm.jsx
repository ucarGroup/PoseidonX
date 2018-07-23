import React from 'react';
import PropTypes from 'prop-types';
import {Button, Form, Icon, Input, Modal, RadioGroup,notification,Select} from "antd";
import { qs, util, config, auth } from "libs";

const FormItem = Form.Item;

class ArchiveForm extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            userGroupData:[],
            loading: false,
            btnText: '保存',
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

            //查询用户组
            qs.form("/streamsuite/usergroup/listAll", null).then((data) => {
                this.setState({
                    userGroupData:data
                });

            });

            this.setState({
                showModal: nextProps.visible,
                isNew:nextProps.isNew,
                isSuperManager: auth.isSuperManager(),
                taskArchiveName: nextProps.taskArchiveName,
                taskArchiveRemark: nextProps.taskArchiveRemark,
                userGroupId: nextProps.userGroupId,
                id:nextProps.id,
            });
        }
    }


    handleSubmit = (e) => {
        e.preventDefault();
        this.props.form.validateFields((err, params) => {
            if (!err) {
                this.postConfig(params);
            }
        });
    }

    //提交配置信息
    postConfig(params) {

        this.setState({
            loading: true,
            btnText: "保存中..."
        });

        let { appStore, router } = this.props;

        let postData = {
                ...params,
                id:this.props.id
         };
        let url = "";
        if(this.state.isNew){
            url = "/streamsuite/task/archive/save"
        }else{
            url = "/streamsuite/task/archive/update";
        }

        qs.form(url, postData).then((data) => {

            this.setState({
                loading: false,
                btnText: "保存"
            });

            //保存失败
            if (!data.result) {
                notification.error({
                    message:  "任务文件保存失败",
                    description: data.errMsg
                });

            }
            //保存成功
            else {

                notification.info({
                    message: "任务文件保存成功",
                });

                this.props.onRefresh();

                return;
            }
        });

    }



    render() {
        const {getFieldDecorator} = this.props.form;
        const modalProps = {
            destroyOnClose:true,
            visible: this.state.showModal,
            title: this.props.formTitle,
            footer: null,
            onCancel: (e) => {
                this.setState({showModal: false});
                this.props.onRefresh();
            }
        }
        const { TextArea } = Input;

        const {userGroupData } = this.state;

        return (
            <Modal {...modalProps}>
                <Form onSubmit={this.handleSubmit}>
                    <FormItem hasFeedback>
                        {getFieldDecorator('taskArchiveName', {
                            initialValue: this.state.taskArchiveName,
                            rules: [{required: true, message: '请输入任务文件名称!'}],
                        })(
                            <Input prefix={<Icon type="tag" style={{fontSize: 13}}/>} placeholder="任务文件名称"/>
                        )}
                    </FormItem>

                    <FormItem hasFeedback>
                        {getFieldDecorator('taskArchiveRemark', {
                            initialValue: this.state.taskArchiveRemark,
                            rules: [{required: true, message: '请输入备注信息!'}],
                        })(
                            <Input prefix={<Icon type="form" style={{fontSize: 13}}/>} placeholder="备注信息" />
                        )}
                    </FormItem>

                    <FormItem hasFeedback>
                        {getFieldDecorator('userGroupId', {
                            initialValue: this.state.userGroupId,
                            rules: [{required: true, message: '请选择用户组!'}],
                        })(
                            <Select
                                showSearch
                                style={{ width: 200 }}
                                placeholder="请选择用户组"
                                optionFilterProp="children"
                                filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}
                            >
                                {userGroupData.map(d => <Select.Option value={d.id} key={d.id}>{d.name}</Select.Option>)}

                            </Select>
                        )}
                    </FormItem>


                    <Button type="primary"
                            htmlType="submit"
                            loading={this.state.loading}
                            className="login-form-button"
                            style={{width: '100%'}}>{this.state.btnText}
                    </Button>
                </Form>
            </Modal>
        )
    }
}

export default Form.create()(ArchiveForm);