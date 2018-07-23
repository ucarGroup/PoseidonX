import React from 'react';
import ReactDOM from 'react-dom';
import PropTypes from "prop-types";
import Redbox from 'redbox-react'
import { observer, inject, Provider } from "mobx-react";
import { browserHistory, hashHistory, Router } from 'react-router';
import zhCN from 'antd/lib/locale-provider/zh_CN';
import { LocaleProvider } from 'antd';
import routes from "./routes";
import appStore from "./stores/glob";
import { util, auth } from "libs";


const render = Component => {
  ReactDOM.render(
    <Provider appStore={ appStore }>
        <LocaleProvider locale={zhCN}>
            <Component />
        </LocaleProvider>
    </Provider>,
    document.getElementById('root')
  )
}

class App extends React.Component {
  render() {
    return (
      <Router history={ hashHistory } routes={ routes }/>
    );
  }
};


render(App)

if (module.hot) {
  module.hot.accept(App, () => {
    const NextApp = App.default;
    render(NextApp);
  });
}