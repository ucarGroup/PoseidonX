import React from 'react';
import classnames from 'classnames'
import ReactDOM from 'react-dom';
import { qs, util, config } from "libs";
import { observer, inject } from "mobx-react";

export default class welcome extends React.Component {

    render() {
        const { location } = this.props;
        return (
            <div className="welcome">
              <h2>欢迎登录实时数据应用平台</h2>
            </div>
        );
    }
};