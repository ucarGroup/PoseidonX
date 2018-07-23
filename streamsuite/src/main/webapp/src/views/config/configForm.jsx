import React from 'react';
import PropTypes from 'prop-types';
import {Button, Form, Icon, Input, Modal, RadioGroup,notification} from "antd";
import { qs, util, config, auth } from "libs";

const FormItem = Form.Item;

class ConfigForm extends React.Component {

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
                configName: nextProps.configName,
                configValue: nextProps.configValue,
                configRemark: nextProps.configRemark,
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


        qs.form("/streamsuite/config/save", postData).then((data) => {

            this.setState({
                loading: false,
                btnText: "保存"
            });

            //保存失败
            if (!data.result) {
                notification.error({
                    message:  "配置项保存失败",
                    description: data.errMsg
                });

            }
            //保存成功
            else {

                notification.info({
                    message: "配置项保存成功",
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
        return (
            <Modal {...modalProps}>
                <Form onSubmit={this.handleSubmit}>
                    <FormItem hasFeedback>
                        {getFieldDecorator('configName', {
                            initialValue: this.state.configName,
                            rules: [{required: true, message: '请输入配置项名称!'}],
                        })(
                            <Input prefix={<Icon type="tag" style={{fontSize: 13}}/>} placeholder="配置项名称" readOnly={!this.state.isNew}/>
                        )}
                    </FormItem>

                    <FormItem hasFeedback>
                        {getFieldDecorator('configValue', {
                            initialValue: this.state.configValue,
                            rules: [{required: true, message: '请输入配置项值!'}],
                        })(
                            <Input prefix={<Icon type="form" style={{fontSize: 13}}/>} placeholder="配置项值" />
                        )}
                    </FormItem>

                    <FormItem hasFeedback>
                        {getFieldDecorator('configRemark', {
                            initialValue: this.state.configRemark,
                        })(
                            <TextArea prefix={<Icon type="profile" style={{fontSize: 13}}/>} placeholder="配置项备注"/>
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

export default Form.create()(ConfigForm);