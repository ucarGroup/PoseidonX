import React from "react";
import PropTypes from "prop-types";
import { action, observable, } from 'mobx';
import { observer, inject } from "mobx-react";
import { util, qs } from "libs";
import { Spin, Table } from "antd";

class TableStore {
  @observable dataSource = [];
  @observable loading = false;
  @observable pageInfo = {
    pageNum: 1,
    pageSize: 10,
    current: 1,
    showSizeChanger: true,
    total: 0
  }
}

const store = new TableStore();

@observer
export default class TableExtend extends React.Component {

  static propTypes = {
    tableConfig: PropTypes.object
  }

  static queryParams = {}

  componentWillMount() {
    store.pageInfo = {
      ...store.pageInfo,
      onChange: (page, pageSize) => {
        this.changePage(page, pageSize);
      },
      onShowSizeChange: (page, pageSize) => {
        this.changePage(page, pageSize);
      }
    }
    this.query();
  }

  changePage(page, pageSize) {
    store.pageInfo = {
      ...store.pageInfo,
      pageNum: page,
      pageSize: pageSize
    }
    this.query();
  }

  /**
   * [refresh 刷新表格]
   * @param  {[type]} params    [description]
   * @param  {[type]} firstPage [是否到第一页]
   * @return {[type]}           [description]
   */
  refresh(params = {}, firstPage = {}) {
    if (firstPage) {
      store.pageInfo.page = 1;
    }
    this.queryParams = params;
    this.query();
  }

  getQueryString() {
    const { tableConfig } = this.props;
    const pageInfo = store.pageInfo;
    let params = {};
    if (tableConfig.showPage) {
      params = {
        pageNum: pageInfo.pageNum,
        pageSize: pageInfo.pageSize,
        ...this.props.tableConfig.params,
        ...this.queryParams
      }
    } else {
      params = {
        ...this.props.tableConfig.params,
        ...this.queryParams
      }
    }
    return params;
  }

  async query() {
    const { tableConfig } = this.props;
    let params = this.getQueryString();
    let response = await qs.get(tableConfig.url, params);
    store.loading = true;
    if (response.success) {
      store.dataSource = response.data.data;
      store.pageInfo = {
        ...store.pageInfo,
        ...response.data.page
      }
    }
    store.loading = false;
  }

  render() {
    const { tableConfig } = this.props;
    return (
      <div>
        <Table rowKey={tableConfig.rowKey} pagination={tableConfig.showPage ? store.pageInfo : false} columns={tableConfig.columns} dataSource={store.dataSource.slice()} loading={store.loading}></Table>
      </div>
    )
  }
}