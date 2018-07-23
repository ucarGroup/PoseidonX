import React from 'react';
import classnames from 'classnames'
import ReactDOM from 'react-dom';
import { action, observable, } from 'mobx';
import { observer, inject } from "mobx-react";
import { Icon, Menu, Badge } from "antd";
import PropTypes from 'prop-types';
import { qs, util, config, auth } from "libs";

const SubMenu = Menu.SubMenu;


@inject("appStore")
@observer
export default class Header extends React.Component {

  state = {
    current: '',
  }

  static propTypes = {
    router: PropTypes.object.isRequired
  }

  constructor(props, context) {
    super(props, context);
  }

  componentWillMount() {
    const { appStore } = this.props;
      appStore.userInfo = {
          name: auth.getUserName(),
          role: auth.getUserRole(),
      }
      console.log(appStore)
  }

  //处理用户注销操作
  handerlogout = (e) => {
     auth.toLogOut();
  }



  render() {
    const { appStore } = this.props;

    const sysInfo = appStore.sysInfo;
    const userInfo = appStore.userInfo;
    const selectedKeys = [this.state.current];
    const username = userInfo.name;


    return (
      <div className="header" >

          <div className="siderbutton" key="switchSider" onClick={()=>{appStore.switchNavBar()}}>
            <Icon type={sysInfo.collapsed ? 'menu-unfold' : 'menu-fold'}/>
          </div>
          <img src={require('../images/poseidon_logo2.png')}   height={45}/>&nbsp;
          <Menu className='right-menu' mode='horizontal' onClick={this.handleClickMenu}>
              <Menu.SubMenu style={{
                  float: 'right'
              }} title={< span> <Icon type='user'/> {username ? username:'请登录'} </span>}>
                  <Menu.Item key='logout'>
                      <a  onClick={this.handerlogout}>注销</a>
                  </Menu.Item>
              </Menu.SubMenu>
          </Menu>
      </div>
    )
  }
};