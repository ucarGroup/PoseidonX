import React from 'react';
import PropTypes from 'prop-types';
import {Button, Form, Icon, Input, Modal, Radio, RadioGroup,notification,Select} from "antd";
import { qs, util, config, auth } from "libs";

const FormItem = Form.Item;
const children = [];

class UserGroupForm extends React.Component {

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
            let arr = [];
            if(nextProps.members!=null&&nextProps.members!="") {
                arr = nextProps.members.split(",");
            }
            this.setState({
                showModal: nextProps.visible,
                isNew:nextProps.isNew,
                isSuperManager: auth.isSuperManager(),
                name: nextProps.name,
                members: arr,
                id:nextProps.id,
            });

        }
    }

    componentDidMount(){
        let postDate = {
            pageNum:0,
            pageSize:10000
        }
        qs.form("/streamsuite/user/list", postDate).then((data) => {
            if(data!=null) {
                let arr = data.list;
                for(let i=0; i<arr.length; i++){
                    children.push(<Option key={arr[i].id}>{arr[i].userName}</Option>);
                }
            }
        });
    }

    handleSubmit = (e) => {
        e.preventDefault();
        this.props.form.validateFields((err, params) => {
            if (!err) {
                this.postUserGroup(params);
            }
        });
    }

    //提交用户组信息
    postUserGroup(params) {

        this.setState({
            loading: true,
            btnText: "保存中..."
        });

        let { appStore, router } = this.props;

        let postData = {
                ...params,
                id:this.props.id
         };

        qs.form("/streamsuite/usergroup/save", postData).then((data) => {

            this.setState({
                loading: false,
                btnText: "保存"
            });

            //保存失败
            if (!data.result) {
                notification.error({
                    message:  "用户组保存失败",
                    description: data.errMsg
                });

            }
            //保存成功
            else {

                notification.info({
                    message: "用户组保存成功",
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
        const RadioGroup = Radio.Group;
        return (
            <Modal {...modalProps}>
                <Form onSubmit={this.handleSubmit}>
                    <FormItem hasFeedback>
                        {getFieldDecorator('name', {
                            initialValue: this.state.name,
                            rules: [{required: true, message: '请输入用户组名称!'}],
                        })(
                            <Input prefix={<Icon type="team" style={{fontSize: 13}}/>} placeholder="用户组名称" readOnly={!this.state.isNew}/>
                        )}
                    </FormItem>

                    <FormItem hasFeedback>
                        {getFieldDecorator('members', {
                            initialValue: this.state.members,
                        })(
                        <Select mode="multiple"
                                placeholder="Please select"
                                style={{ width: '100%' }}
                                filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}
                        >
                            {children}
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

export default Form.create()(UserGroupForm);