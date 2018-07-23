import {config, qs, util} from "libs";

export default {

    isLogin(glob) {
        let cookieKey = 'stream_user';
        if (glob && glob.cookieKey) {
            cookieKey = glob.cookieKey;
        }
        let userInfo = util.getCookie(cookieKey);

        return userInfo;
    },

    getUserName(cookieKey) {
        let userName = util.getCookie(cookieKey || 'stream_user');
        if (userName) {
            return userName;
        } else {
            return '请登录';
        }
    },

    getUserRole(cookieKey) {
        let userRole = util.getCookie(cookieKey || 'stream_user_role');
        if (userRole) {
            return userRole;
        } else {
            //默认返回普通用户角色
            return "1";
        }
    },

    isSuperManager(cookieKey) {
        let userRole = this.getUserRole();
        //超级管理员是0
        if (userRole == "0") {
            return true;
        } else {
            return false;
        }
    },


    toLogin() {
        let router = window.router;
        let redirectUrl = encodeURIComponent(top.location.href);
        router.replace({
            pathname: "login",
            query: {
                redirectUrl: redirectUrl
            }
        });
    },
    toLogOut() {

        let router = window.router;
        let redirectUrl = encodeURIComponent(top.location.href);

        qs.form("/streamsuite/user/clearcookie", null).then((data) => {
            this.setState({

            });
        });
        router.replace({
            pathname: "login",
            query: {
                redirectUrl: redirectUrl
            }
        });
    }


}