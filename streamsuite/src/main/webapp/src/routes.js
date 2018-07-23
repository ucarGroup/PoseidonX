import App from "./App";
import auth from "libs/auth";

const checkAuth = (nextState, replace) => {
    if (!auth.isLogin()) {
        replace("/login");
    }
}


export default [{
    path: '/',
    component: App,
    onEnter: checkAuth,
    indexRoute: {
        getComponent(location, cb) {
            require.ensure([], (require) => {
                cb(null, require('./views/index').default)
            }, 'index')
        }
    },
    childRoutes: [{
        path: 'index',
        getComponent(location, cb) {
            require.ensure([], require => {
                cb(null, require('./views/index').default)
            })
        }
    },
        /*<<< 用户管理 菜单部分 */
        {
            path: 'user/list',
            getComponent(location, cb) {
                require.ensure([], require => {
                    cb(null, require('./views/user/userList').default)
                })
            }
        },
        {
            path: 'usergroup/list',
            getComponent(location, cb) {
                require.ensure([], require => {
                    cb(null, require('./views/user/userGroupList').default)
                })
            }
        },

        /*  用户管理 菜单部分  >>>*/

        /*<<< 配置管理 菜单部分 */
        {
            path: 'config/list',
            getComponent(location, cb) {
                require.ensure([], require => {
                    cb(null, require('./views/config/configList').default)
                })
            }
        },
        {
            path: 'config/hadoop/list',
            getComponent(location, cb) {
                require.ensure([], require => {
                    cb(null, require('./views/config/hadoopConfigList').default)
                })
            }
        },
        {
            path: '/config/engineVersion/list',
            getComponent(location, cb) {
                require.ensure([], require => {
                    cb(null, require('./views/config/engineVersionList').default)
                })
            }
        },

        /*  配置管理 菜单部分  >>>*/


        /*<<< 任务管理 菜单部分 */
        {
            path: '/task/archive/list',
            getComponent(location, cb) {
                require.ensure([], require => {
                    cb(null, require('./views/task/archiveList').default)
                })
            }
        },
        {
            path: '/task/task/list',
            getComponent(location, cb) {
                require.ensure([], require => {
                    cb(null, require('./views/task/taskList').default)
                })
            }
        },

        /*  任务管理 菜单部分  >>>*/

        /*<<< StreamCQL 菜单部分 */
        {
            path: '/streamcql/list',
            getComponent(location, cb) {
                require.ensure([], require => {
                    cb(null, require('./views/streamcql/cqlList').default)
                })
            }
        },

        /*  StreamCQL 菜单部分  >>>*/

        /*<<< 历史记录管理 菜单部分 */
        {
            path: '/user/userLoginHistoryList',
            getComponent(location, cb) {
                require.ensure([], require => {
                    cb(null, require('./views/user/userLoginHistoryList').default)
                })
            }
        },

        /*  历史记录管理 菜单部分  >>>*/

        /*<<< 监控管理 菜单部分 */
        {
            path: '/moniter/jstormTaskMoniter',
            getComponent(location, cb) {
                require.ensure([], require => {
                    cb(null, require('./views/moniter/jstormTaskMoniter').default)
            })
            }
        },

        /*  监控管理 菜单部分  >>>*/
        {
            path: '/moniter/flinkTaskMoniter',
            getComponent(location, cb) {
                require.ensure([], require => {
                    cb(null, require('./views/moniter/flinkTaskMoniter').default)
            })
            }
        }
    ]
}, {
    path: '/login',
    getComponent(location, cb) {
        require.ensure([], require => {
            cb(null, require('./views/login').default)
        })
    }
}];