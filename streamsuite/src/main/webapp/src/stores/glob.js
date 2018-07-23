import { action, observable, } from 'mobx';
import { observer, inject } from "mobx-react";
import { qs, config, util, auth } from "libs";

class appStore {
  @observable userInfo = {};
  @observable isDraging = false;
  @observable sysInfo = {
    isNavbar: false,
    darkTheme: true,
    collapsed: false,
    barOffset: 7,
    collapsedWidth: 82,
    defaultNavBarWidth: 200,
    lastNavBarWidth: 200,
    navBarWidth: 200,
    maxBarWidth: 400,
    openKeys: []
  };
  @action
  updateNavBarWidth(w) {
    this.sysInfo.navBarWidth = w;
    if (!this.sysInfo.collapsed) {
      this.sysInfo.lastNavBarWidth = w;
    }
  }
  @action
  switchNavBar() {
    this.sysInfo.collapsed = !this.sysInfo.collapsed;
    let navBarWidth = this.sysInfo.collapsed ? this.sysInfo.collapsedWidth : this.sysInfo.lastNavBarWidth;
    this.updateNavBarWidth(navBarWidth);
  }
}

const _appStore = new appStore();
export default _appStore;