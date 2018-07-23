import React from 'react';
import classnames from 'classnames'
import ReactDOM from 'react-dom';
import { Form, Icon, Button, Input, notification } from "antd";
import { qs, util, auth, config } from "libs";
import { observer, inject } from "mobx-react";

const FormItem = Form.Item;

import "styles/login.less";

@inject("appStore")
class LoginForm extends React.Component {

  state = {
    loading: false,
    validateComplate: false,
    btnText: "登 录"
  }

  constructor(props, context) {
    super(props, context);
  }

  componentWillMount() {

      let { appStore, router } = this.props;

      qs.post("/streamsuite/unionPlatform/login").then((data) => {
          if(data.result){
              console.log("unionPlatform is ok ");
              appStore.userInfo = {
                  name: data.msg
              }
              router.push({
                  pathname: '/index'
              });
          }
      });
  }

  handleSubmit = (e) => {
    e.preventDefault();
    this.props.form.validateFields((err, params) => {
      if (!err) {
        this.postLogin(params);
      }
    });
  }

  /**
   * [postLogin 登录方法]
   * @param  {[type]} params [description]
   * @return {[type]}        [description]
   */
  postLogin(params) {
    let { appStore, router } = this.props,
      location = router.location,
      query = location.query,
      redirectUrl = query.redirectUrl,
      postData = {
        ...params
      };

    this.setState({
      loading: true,
      btnText: "登录中..."
    });

    qs.form("/streamsuite/user/login", postData).then((data) => {

        this.setState({
            loading: false,
            btnText: "登 录"
        });

        //登录失败
      if (!data.result) {

        notification.error({
          message: "登录失败",
          description: "登录失败，请检查用户邮箱或密码是否正确"
        });
      }
      //登录成功
      else {
        if (redirectUrl && redirectUrl.indexOf("login") <= 0) {
          top.location.href = decodeURIComponent(redirectUrl);
        } else {
          router.push({
            pathname: '/index'
          });
        }
        appStore.userInfo = {
              name: data.msg
          }

        return;
      }
    });
  }
  render() {
      const { getFieldDecorator } = this.props.form;

      return (
          <div className="login_wrapper">
              <div className="login__box">
                  <span><img src={require('../images/poseidon_logo1.png')} width={250}/></span>
                  <Form onSubmit={this.handleSubmit}>

                      <FormItem hasFeedback>
                          {getFieldDecorator('username', {
                              rules: [{ required: true, message: '请输入用户邮箱!' }],
                          })(
                              <Input prefix={<Icon type="user" style={{ fontSize: 13 }} />} placeholder="用户邮箱" />
                          )}
                      </FormItem>

                      <FormItem hasFeedback>
                          {getFieldDecorator('password', {
                              rules: [{ required: true, message: '请输入密码!' }],
                          })(
                              <Input prefix={<Icon type="lock" style={{ fontSize: 13 }} />} type="password" placeholder="密码" />
                          )}
                      </FormItem>


                      <Button type="primary"
                              htmlType="submit"
                              loading={this.state.loading}
                              className="login-form-button"
                              style={{width: '100%'}}>{this.state.btnText}
                      </Button>
                  </Form>
              </div>
          </div>
      );
  }
};

export default Form.create()(LoginForm);