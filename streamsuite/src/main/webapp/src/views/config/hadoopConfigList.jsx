import React from 'react';
import {auth, qs, util} from 'libs';
import '../../styles/glob.less'
import {inject} from "mobx-react";
import HadoopConfigTabItem from './hadoopConfigTabItem';

import {Tabs} from 'antd';
import {observer} from "mobx-react/index";


@inject("appStore")
@observer
export default class HadoopConfigList extends React.Component {

    constructor(props, context) {
        super(props, context);
    }

    //组件加载时执行查询
    componentWillMount() {
    }

    changeTab = (e) => {
        console.log(e);
        const {appStore} = this.props;

        appStore.conftab = e;

        console.log(appStore);
    }

    render() {

        const TabPane = Tabs.TabPane;

        return (
            <div>
                <Tabs defaultActiveKey="core" onChange={this.changeTab}>
                    <TabPane tab="CORE" key="core"><HadoopConfigTabItem configType={"core"}/></TabPane>
                    <TabPane tab="HDFS" key="hdfs"><HadoopConfigTabItem configType={"hdfs"}/></TabPane>
                    <TabPane tab="YARN" key="yarn"><HadoopConfigTabItem configType={"yarn"}/></TabPane>
                </Tabs>
            </div>
        )
    }

}
