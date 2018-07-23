import React from 'react';
import {inject, observer} from "mobx-react";
import {Link} from "react-router";
import 'styles/app.less';
import 'styles/index.less';
import {Icon, Layout, Menu, message} from 'antd';
import SysHeader from './layout/header';
import {config} from "libs";
import DragSide from "components/DragSide";
import classNames from 'classnames';

const {Header, Footer, Sider, Content} = Layout;

const SubMenu = Menu.SubMenu;

@inject("appStore")
@observer
export default class App extends React.Component {

    constructor(props, context) {
        super(props, context);
        message.config({
            top: 100,
            duration: 1
        });
    }


    componentWillMount() {
        const {appStore, router} = this.props;
        window.router = router;
    }

    handleClick(e) {
        console.log("##["+e.key+"]")
        const {appStore} = this.props;
        appStore.headerInfo = e.key;
    }


    render() {
        const {appStore} = this.props;
        const sysInfo = appStore.sysInfo;
        const backdrop = appStore.backdrop;
        const isDraging = appStore.isDraging;

        const headerProps = {
            router: this.props.router
        }
        const classes = classNames('app_index', {
            [`backdrop_${backdrop}`]: backdrop,
            [`draging`]: isDraging
        });
        return (
            <div className={classes}>
                <Layout className={sysInfo.collapsed ? 'ant-layout-collapsed' : ''}>
                    <Sider trigger={null} width={sysInfo.navBarWidth} collapsible collapsed={sysInfo.collapsed}
                           className='ant-layout-dark'>
                        <div className='logo'><span>POSEIDON</span></div>
                        <Menu theme='dark' defaultOpenKeys={['app']} mode="inline"  onClick={this.handleClick.bind(this)}>
                            {/* 用户模块菜单 */}
                            <SubMenu key="user" title={<span><Icon type="user"/><span>用户管理</span></span>}>
                                <Menu.Item key="用户管理 >> 用户列表">
                                    <Link to="/user/list" >用户列表</Link>
                                </Menu.Item>
                                <Menu.Item key="用户管理 >> 用户组列表">
                                    <Link to="/usergroup/list">用户组列表</Link>
                                </Menu.Item>
                            </SubMenu>

                            {/* 配置模块菜单 */}
                            <SubMenu key="config" title={<span><Icon type="setting"/><span>配置管理</span></span>}>
                                <Menu.Item key="配置管理 >> 通用配置信息">
                                    <Link to="/config/list">通用配置信息</Link>
                                </Menu.Item>
                                <Menu.Item key="配置管理 >> HADOOP配置信息">
                                    <Link to="/config/hadoop/list">HADOOP配置信息</Link>
                                </Menu.Item>
                                <Menu.Item key="配置管理 >> 实时引擎版本配置">
                                    <Link to="/config/engineVersion/list">实时引擎版本配置</Link>
                                </Menu.Item>
                            </SubMenu>

                            {/* 任务模块菜单 */}
                            <SubMenu key="task" title={<span><Icon type="fork"/><span>任务管理</span></span>}>
                                <Menu.Item key="任务管理 >> 任务文件管理">
                                    <Link to="/task/archive/list">任务文件管理</Link>
                                </Menu.Item>
                                <Menu.Item key="任务管理 >> 任务实例管理">
                                    <Link to="/task/task/list">任务实例管理</Link>
                                </Menu.Item>
                            </SubMenu>

                            {/* StreamCQL模块菜单 */}
                            <SubMenu key="cql" title={<span><Icon type="desktop"/><span>StreamCQL</span></span>}>
                                <Menu.Item key="StreamCQL >> CQL脚本列表">
                                    <Link to="/streamcql/list">CQL脚本列表</Link>
                                </Menu.Item>
                            </SubMenu>

                            {/* 历史记录管理 */}
                            <SubMenu key="history" title={<span><Icon type="table"/><span>历史记录管理</span></span>}>
                                <Menu.Item key="history >> 历史记录管理">
                                    <Link to="/user/userLoginHistoryList">用户登录历史</Link>
                                </Menu.Item>
                            </SubMenu>

                            {/* 监控 */}
                            <SubMenu key="moniter" title={<span><Icon type="line-chart"/><span>监控管理</span></span>}>
                                <Menu.Item key="moniter >> jstorm任务监控">
                                    <Link to="/moniter/jstormTaskMoniter">jstorm任务监控</Link>
                                </Menu.Item>
                                <Menu.Item key="moniter >> flink任务监控">
                                    <Link to="/moniter/flinkTaskMoniter">flink任务监控</Link>
                                </Menu.Item>
                            </SubMenu>

                        </Menu>
                    </Sider>
                    <Layout>
                        <div className="main" style={{'marginLeft': sysInfo.navBarWidth + 'px'}}>
                            <Header>
                                <SysHeader {...headerProps}></SysHeader>
                            </Header>
                            <Content>
                                <div className="drag-mask"></div>
                                <div className="container">
                                    {this.props.children}
                                </div>
                            </Content>
                        </div>
                        <Footer>版权所有 © 2017 </Footer>
                    </Layout>
                </Layout>
            </div>
        );
    }
};