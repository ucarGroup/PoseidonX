import React from 'react';
import PropTypes from 'prop-types';
import {Button, Form, Icon, Input, Modal, notification, RadioGroup,Select} from "antd";
import {auth, config, qs, util} from "libs";

import brace from 'brace'
import AceEditor from 'react-ace';

import './mode-cql';
import 'brace/theme/sqlserver';
import 'brace/ext/language_tools';
import 'brace/ext/searchbox';


const FormItem = Form.Item;

class CqlForm extends React.Component {

    constructor(props, context) {
        super(props, context);

        this.state = {
            loading: false,
            btnText: '保存',
            checkBtnText:'检查脚本',
            showModal: this.props.visible,
            userGroupData: []
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


            let btnWidthVal = '50%';
            let checkBtnDisplayVal = 'inline';
            display:
            if(nextProps.cqlType == 1){
                btnWidthVal = '100%'
                checkBtnDisplayVal = ' none'
            }

            //查询用户组
            qs.form("/streamsuite/usergroup/listAll", null).then((data) => {
                this.setState({
                    userGroupData:data
                });

            });

            this.setState({
                showModal: nextProps.visible,
                isNew:nextProps.isNew,
                cqlName: nextProps.cqlName,
                cqlType: nextProps.cqlType,
                cqlText: nextProps.cqlText,
                cqlRemark: nextProps.cqlRemark,
                id:nextProps.id,
                btnWidth:btnWidthVal,
                checkBtnDisplay:checkBtnDisplayVal,
                userGroupId:nextProps.userGroupId,
            });

        }
    }


    handleSubmit = (e) => {
        e.preventDefault();
        this.props.form.validateFields((err, params) => {
            if (!err) {
                this.postCql(params);
            }
        });
    }

    handleCheck= (e) => {
        e.preventDefault();
        this.props.form.validateFields((err, params) => {
            if (!err) {
                this.checkCql(params);
            }
        });
    }

    //提交配置信息
    checkCql(params) {

        this.setState({
            loading: true,
            checkBtnText: "检查中..."
        });

        let { appStore, router } = this.props;

        let postData = {
            ...params,
            id:this.props.id
        };


        qs.form("/streamsuite/cql/checkCQL", postData).then((data) => {

            this.setState({
                loading: false,
                checkBtnText: "检查脚本"
            });

            Modal.info({
                title:  "CQL检查结果",
                width:1200,
                content: data
            });

        });

    }



    //提交配置信息
    postCql(params) {

        this.setState({
            loading: true,
            btnText: "保存中..."
        });

        let { appStore, router } = this.props;

        let postData = {
                ...params,
                id:this.props.id,
                cqlType:this.props.cqlType,
         };


        qs.form("/streamsuite/cql/save", postData).then((data) => {

            this.setState({
                loading: false,
                btnText: "保存"
            });

            //保存失败
            if (!data.result) {
                notification.error({
                    message:  "CQL保存失败",
                    description: data.errMsg
                });

            }
            //保存成功
            else {

                notification.info({
                    message: "CQL保存成功",
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
            style:{ top: 1 },
            width:1200,
            onCancel: (e) => {
                this.setState({showModal: false});
                this.props.onRefresh();
            }
        }
        const { TextArea } = Input;
        const {userGroupData} = this.state;
        return (
            <Modal {...modalProps}>
                <Form onSubmit={this.handleSubmit}>
                    <FormItem hasFeedback>
                        {getFieldDecorator('cqlName', {
                            initialValue: this.state.cqlName,
                            rules: [{required: true, message: '请输入CQL名称!'}],
                        })(
                            <Input prefix={<Icon type="tag" style={{fontSize: 13}}/>} placeholder="CQL名称"/>
                        )}
                    </FormItem>



                    <FormItem hasFeedback>
                        {getFieldDecorator('cqlRemark', {
                            initialValue: this.state.cqlRemark,
                        })(
                            <Input prefix={<Icon type="profile" style={{fontSize: 13}}/>} placeholder="CQL备注"/>
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
                                {userGroupData.map(d => <Select.Option value= {d.id} key= {d.id}>{d.name}</Select.Option>)}

                            </Select>
                        )}
                    </FormItem>

                    <FormItem hasFeedback>
                        {getFieldDecorator('cqlText', {
                            initialValue: this.state.cqlText,
                            rules: [{required: true, message: '请输入CQL脚本!'}],
                        })(
                            <AceEditor
                                mode="cql"
                                theme="sqlserver"
                                name="cqlTextEdit"
                                showPrintMargin={true}
                                showGutter={true}
                                highlightActiveLine={true}
                                editorProps={{$blockScrolling: true}}
                                width={1100}
                                height={650}
                                setOptions={{
                                    enableBasicAutocompletion: true,
                                    enableLiveAutocompletion: true,
                                    enableSnippets: false,
                                    showLineNumbers: true,
                                    tabSize: 2,
                                }}
                            />

                        )}
                    </FormItem>

                    <Button type="primary"
                            htmlType="submit"
                            loading={this.state.loading}
                            className="login-form-button"
                            style={{width: this.state.btnWidth}}>{this.state.btnText}
                    </Button>

                    <Button type="danger"
                            htmlType="button"
                            loading={this.state.loading}
                            className="login-form-button"
                            style={{width: "50%", display:this.state.checkBtnDisplay}}
                            onClick={this.handleCheck}
                            vis
                    > {this.state.checkBtnText}
                    </Button>

                </Form>
            </Modal>
        )
    }
}

export default Form.create()(CqlForm);