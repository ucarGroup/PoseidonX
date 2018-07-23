import React from 'react';
import PropTypes from 'prop-types';
import {Button, Form, Icon, Input, Modal, notification, Radio, RadioGroup, Upload} from "antd";
import {auth, config, qs, util} from "libs";

const FormItem = Form.Item;

class EngineVersionForm extends React.Component {
    constructor(props, context) {
        super(props, context);
        this.state = {
            loading: false,
            btnText: '保存',
            showModal: this.props.visible,
            fileList:[]
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
                versionName: nextProps.versionName,
                versionType: nextProps.versionType,
                versionRemark: nextProps.versionRemark,
                versionUrl: nextProps.versionUrl,
                id: nextProps.id,
            });

        }
    }


    handleSubmit = (e) => {
        e.preventDefault();

        this.setState({
            versionUrlName: "上传文件",
            uploadDisabled: false,
        });

        this.props.form.validateFields((err, params) => {
            if (!err) {
                this.postConfig(params);
            }
        });
    }

    //提交表单信息
    postConfig(params) {

        this.setState({
            loading: true,
            btnText: "保存中..."
        });

        let {appStore, router} = this.props;

        let postData = {
            ...params,
            id: this.props.id
        };

        qs.form("/streamsuite/engineVersion/save", postData).then((data) => {

            this.setState({
                loading: false,
                btnText: "保存"
            });

            //保存失败
            if (!data.result) {
                notification.error({
                    message: "引擎版本保存失败",
                    description: data.errMsg
                });

            }
            //保存成功
            else {
                notification.info({
                    message: "引擎版本保存成功",
                });

                this.props.onRefresh();

                return;
            }
        });

    }

    //组件将被卸载
    componentWillUnmount() {
        //重写组件的setState方法，直接返回空
        this.setState = (state, callback) => {
            return;
        };
    }

    normFile = (e) => {
        if (e.fileList.length == 0) {
            this.setState({
                uploadDisabled: false,
            })
        }
        let fileUploadStatus = e.file.status;
        //上传中
        if (fileUploadStatus == 'uploading') {
            this.setState({
                uploadDisabled: true,
            });

            this.setState({ fileList:e.fileList });
            return null;
        }
        else if (fileUploadStatus == 'done') {

            //上传完成 fileUploadStatus == 'done'
            let uploadResponse = e.file.response;
            if (uploadResponse != undefined) {

                let status = e.file.response.status;

                if (status == "success") {
                    let fileName = e.file.response.fileName;

                    this.setState({
                        versionUrlName: fileName,
                        uploadDisabled: true,
                    });
                    this.setState({ fileList:e.fileList });
                    return fileName;
                }
                else {
                    console.log("1")
                    notification.error({
                        message: "文件上传失败",
                        description:  e.file.response.errorString
                    });

                    this.setState({
                        versionUrlName: "上传文件",
                        uploadDisabled: false,
                        fileList:[],
                    });

                    return null;
                }

            }
        }


    }


    render() {
        const {getFieldDecorator} = this.props.form;
        const modalProps = {
            destroyOnClose: true,
            maskClosable: false,
            visible: this.state.showModal,
            title: this.props.formTitle,
            footer: null,
            onCancel: (e) => {
                this.setState({
                    showModal: false,
                    versionUrlName: "上传文件",
                    uploadDisabled: false,
                    fileList:[],
                });
                this.props.onRefresh();
            }
        }
        const RadioGroup = Radio.Group;
        const {TextArea} = Input;
        return (
            <Modal {...modalProps}>
                <Form onSubmit={this.handleSubmit}>
                    <FormItem hasFeedback>
                        {getFieldDecorator('versionName', {
                            initialValue: this.state.versionName,
                            rules: [{required: true, message: '请输入引擎版本名称!'}],
                        })(
                            <Input prefix={<Icon type="tag" style={{fontSize: 13}}/>} placeholder="引擎版本名称"/>
                        )}
                    </FormItem>

                    <FormItem hasFeedback>
                        {getFieldDecorator('versionType', {initialValue: this.state.versionType})(
                            <RadioGroup disabled={true}>
                                <Radio value="JSTORM">JSTORM引擎</Radio>
                                <Radio value="JSTORM_AM">JSTORM_AM</Radio>
                            </RadioGroup>
                        )}
                    </FormItem>


                    <FormItem hasFeedback>
                        {getFieldDecorator('versionUrl', {
                                valuePropName: 'versionUrl',
                                getValueFromEvent: this.normFile,
                                rules: [{required: true, message: '请选择引擎文件!'}]
                            },
                        )(
                            <Upload name="file"
                                    action={"/streamsuite/engineVersion/upload?versionType=" + this.state.versionType}
                                    fileList={this.state.fileList}
                                    listType="text" disabled={this.state.uploadDisabled}
                                    >
                                <Button>
                                    <Icon type="upload"/> {this.state.versionUrlName || "上传文件"}
                                </Button>
                            </Upload>
                        )}
                    </FormItem>


                    <FormItem hasFeedback>
                        {getFieldDecorator('versionRemark', {
                            initialValue: this.state.versionRemark,
                        })(
                            <TextArea prefix={<Icon type="profile" style={{fontSize: 13}}/>} placeholder="引擎版本备注"/>
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

export default Form.create()(EngineVersionForm);