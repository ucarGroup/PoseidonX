import React from 'react';
import PropTypes from 'prop-types';
import {Button, Form, Icon, Input, Modal, Radio, RadioGroup,notification} from "antd";
import { qs, util, config, auth } from "libs";

const FormItem = Form.Item;

class UserForm extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
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

            this.setState({
                showModal: nextProps.visible,
                isNew:nextProps.isNew,
                isSuperManager: auth.isSuperManager(),
                userName: nextProps.userName,
                password: nextProps.password,
                mobile: nextProps.mobile,
                userRole: nextProps.userRole,
                userStatus: nextProps.userStatus,
                userId:nextProps.userId,
            });

        }
    }


    handleSubmit = (e) => {
        e.preventDefault();
        this.props.form.validateFields((err, params) => {
            if (!err) {
                this.postUser(params);
            }
        });
    }

    //提交用户信息
    postUser(params) {

        this.setState({
            loading: true,
            btnText: "保存中..."
        });

        let { appStore, router } = this.props;

        let postData = {
                ...params,
                userId:this.props.userId
         };


        qs.form("/streamsuite/user/save", postData).then((data) => {

            this.setState({
                loading: false,
                btnText: "保存"
            });

            //保存失败
            if (!data.result) {
                notification.error({
                    message:  "用户保存失败",
                    description: data.errMsg
                });

            }
            //保存成功
            else {

                notification.info({
                    message: "用户保存成功",
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
            title: this.props.userFormTitle,
            footer: null,
            onCancel: (e) => {
                this.setState({showModal: false});
                this.props.onRefresh();
            }
        }

        const RadioGroup = Radio.Group;
        return (
            <Modal {...modalProps}>
                <Form onSubmit={this.handleSubmit}>
                    <FormItem hasFeedback>
                        {getFieldDecorator('userName', {
                            initialValue: this.state.userName,
                            rules: [{required: true, message: '请输入用户邮箱!'}],
                        })(
                            <Input prefix={<Icon type="user" style={{fontSize: 13}}/>} placeholder="用户邮箱" readOnly={!this.state.isNew}/>
                        )}
                    </FormItem>

                    <FormItem hasFeedback>
                        {getFieldDecorator('password', {
                            initialValue: this.state.password,
                            rules: [{required: true, message: '请输入用户密码!'}],
                        })(
                            <Input placeholder="用户密码"/>
                        )}
                    </FormItem>

                    <FormItem hasFeedback>
                        {getFieldDecorator('mobile', {
                            initialValue: this.state.mobile,
                        })(
                            <Input prefix={<Icon type="mobile" style={{fontSize: 13}}/>} placeholder="手机号"/>
                        )}
                    </FormItem>

                    <FormItem hasFeedback>
                        {getFieldDecorator('userRole', {initialValue: this.state.userRole })(
                            <RadioGroup disabled={!this.state.isSuperManager}>
                                <Radio value="0">超级管理员</Radio>
                                <Radio value="1">普通用户</Radio>
                            </RadioGroup>
                        )}
                    </FormItem>

                    <FormItem hasFeedback>
                        {getFieldDecorator('userStatus', {initialValue: this.state.userStatus})(
                            <RadioGroup disabled={!this.state.isSuperManager}>
                                <Radio value="0">可用</Radio>
                                <Radio value="1">禁用</Radio>
                            </RadioGroup>
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

export default Form.create()(UserForm);