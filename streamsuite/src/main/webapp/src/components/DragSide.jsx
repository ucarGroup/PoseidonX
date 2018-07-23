import React from "react";
import PropTypes from "prop-types";
import { action, observable, autorun } from 'mobx';
import { observer, inject } from "mobx-react";
import Rnd from "react-rnd";

class DragStore {
  @observable opt = {
    x: 200,
    y: 0
  };
}

const d = new DragStore();

@inject("appStore")
@observer
export default class DragSide extends React.Component {

  componentDidMount() {
    const { appStore } = this.props;
    const sysInfo = appStore.sysInfo;
    d.opt.x = sysInfo.lastNavBarWidth;
    d.opt.x += sysInfo.barOffset;
    autorun(() => {
      let collapsed = sysInfo.collapsed;
      if (collapsed) {
        d.opt.x = sysInfo.lastNavBarWidth;
      }
    });
  }

  handleDrag = (e, data) => {
    e.preventDefault();
  }

  handleDragStart = (e, data) => {
    const { appStore } = this.props;
    appStore.isDraging = true;
  }

  handleDragStop = (e, data) => {
    const { appStore } = this.props;
    const sysInfo = appStore.sysInfo;
    let x = data.x,
      minWidth = sysInfo.defaultNavBarWidth;
    if (data.x > sysInfo.maxBarWidth) {
      x = sysInfo.maxBarWidth;
    }
    if (data.x < minWidth) {
      x = minWidth;
    }
    d.opt.x = x + sysInfo.barOffset;
    appStore.updateNavBarWidth(x);
    appStore.isDraging = false;
  }

  render() {
    const { appStore } = this.props;
    const size = { width: 0, height: 400 };
    return (
      <Rnd 
        dragAxis='x' 
        size={size} 
        position = {d.opt} 
        onDragStart={this.handleDragStart} 
        onDrag={this.handleDrag} 
        onDragStop={this.handleDragStop}
        enableResizing={false}>
            <i className="antd-cust-icon ant-cust-icon-drag">&#xe621;</i>
       </Rnd>
    )
  }
}