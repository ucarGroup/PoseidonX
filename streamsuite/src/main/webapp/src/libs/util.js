var inBrowser = typeof window !== 'undefined';
var ua = inBrowser && navigator.userAgent.toLowerCase();
var util = {
    page: {
        getWidth: function () {
            var doc = document,
                body = doc.body,
                html = doc.documentElement,
                client = doc.compatMode == 'BackCompat' ? body : doc.documentElement;
            return Math.max(html.scrollWidth, body.scrollWidth, client.clientWidth);
        },
        getHeight: function () {
            var doc = document,
                body = doc.body,
                html = doc.documentElement,
                client = doc.compatMode == 'BackCompat' ? body : doc.documentElement;
            return Math.max(html.scrollHeight, body.scrollHeight, client.clientHeight);
        },
        getViewWidth: function () {
            var doc = document,
                client = doc.compatMode == 'BackCompat' ? doc.body : doc.documentElement;
            return client.clientWidth;
        },
        getViewHeight: function (doc) {
            var doc = document || doc,
                client = doc.compatMode == 'BackCompat' ? doc.body : doc.documentElement;
            return client.clientHeight;
        }
    },
    isEmptyObject(e) {
        var t;
        for (t in e) {
            return !1;
        }
        return !0
    },
    objectToFormData(params) {
        let _formData = new FormData();
        for (let n in params) {
            _formData.append(n, params[n]);
        }
        return _formData;
    },
    getCookie: function (name) {
        var cookieValue = null;
        if (document.cookie && document.cookie != '') {
            var cookies = document.cookie.split(';');
            for (var i = 0; i < cookies.length; i++) {
                var cookie = cookies[i].trim();
                if (cookie.substring(0, name.length + 1) == (name + '=')) {
                    cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                    break;
                }
            }
        }
        return cookieValue;
    },


    cookie: function (name, value, options) {
        if (typeof value != 'undefined') {
            options = options || {};
            if (value === null) {
                value = '';
                // options.expires = -1;
            }
            var expires = '';
            if (options.expires && (typeof options.expires == 'number' || options.expires.toUTCString)) {
                var date;
                if (typeof options.expires == 'number') {
                    date = new Date();
                    date.setTime(date.getTime() + (options.expires * 24 * 60 * 60 * 1000));
                } else {
                    date = options.expires;
                }
                expires = '; expires=' + date.toUTCString();
            }
            var path = options.path ? '; path=' + options.path : '';
            var domain = options.domain ? '; domain=' + options.domain : '';
            var secure = options.secure ? '; secure' : '';
            // document.cookie = [name, '=', value, expires, path, domain, secure].join('');
        } else {
            var cookieValue = null;
            if (document.cookie && document.cookie != '') {
                var cookies = document.cookie.split(';');
                for (var i = 0; i < cookies.length; i++) {
                    var cookie = cookies[i].trim();
                    if (cookie.substring(0, name.length + 1) == (name + '=')) {
                        cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                        break;
                    }
                }
            }
            return cookieValue;
        }
    },
    loadScript: function (doc, src) {
        var doc = doc || document;
        return new Promise(function (resolve, reject) {
            if (doc.querySelector('script[src="' + src + '"]')) {
                resolve();
                return;
            }

            const el = doc.createElement('script');

            el.type = 'text/javascript';
            el.src = src;

            el.addEventListener('load', resolve);
            el.addEventListener('error', reject);
            el.addEventListener('abort', reject);

            doc.head.appendChild(el);
        })
    },
    forEach(arr, iterator, thisObject) {
        var returnValue;
        if ('function' == typeof iterator) {
            for (let i = 0, len = arr.length; i < len; i++) {
                let item = arr[i];
                returnValue = iterator.call(thisObject || arr, item, i);
                if (returnValue === false) {
                    break;
                }
            }
        }
        return arr;
    },
    styleOnload: function (node, callback) {
        var me = this;
        // for IE6-9 and Opera
        if (node.attachEvent) {
            node.attachEvent('onload', callback);
        }
        // polling for Firefox, Chrome, Safari
        else {
            setTimeout(function () {
                me.poll(node, callback);
            }, 0); // for cache
        }
    },
    poll: function (node, callback) {
        var me = this;
        if (callback.isCalled) {
            return;
        }
        var isLoaded = false;
        if (/webkit/i.test(navigator.userAgent)) { //webkit
            if (node['sheet']) {
                isLoaded = true;
            }
        }
        // for Firefox
        else if (node['sheet']) {
            try {
                if (node['sheet'].cssRules) {
                    isLoaded = true;
                }
            } catch (ex) {
                // NS_ERROR_DOM_SECURITY_ERR
                if (ex.code === 1000) {
                    isLoaded = true;
                }
            }
        }
        if (isLoaded) {
            // give time to render.
            setTimeout(function () {
                callback();
            }, 1);
        } else {
            setTimeout(function () {
                me.poll(node, callback);
            }, 1);
        }
    },
    link: function (doc, href, fn) {
        var that = this,
            doc = doc || document,
            link = doc.createElement('link');
        var head = doc.getElementsByTagName('head')[0];
        var app = href.replace(/\.|\//g, '');
        var id = link.id = 'DSPCSSRULE' + (+new Date()),
            timeout = 0;
        link.rel = 'stylesheet';
        link.href = href + '?v=' + new Date().getTime();
        link.media = 'all';

        if (!doc.getElementById(id)) {
            head.appendChild(link);
        }

        if (typeof fn !== 'function') return;

        this.styleOnload(link, function () {
            fn();
        })
    },
    /**
     * [url 地址栏相关操作方法]
     * @type {Object}
     */
    url: {
        /**
         * [getParam 获取地址栏参数]
         * @param  {[type]} o [description]
         * @return {[type]}   [description]
         */
        getParam: function (o) {
            var reg = new RegExp("(^|\\?|&|#)" + o + "=([^&#]*)(&|\x24|#)", ""),
                url = location.href,
                match = url.match(reg);
            if (match) {
                return decodeURIComponent(match[2]);
            }
            return "";
        },
        urlToObject: function () {
            var url = location.href,
                reg = new RegExp("^.*\\?([a-zA-z0-9&=_%\\u2E80-\\u9FFF]*).*$"),
                match = url.match(reg),
                params = {};
            if (match && match.length > 1) {
                var urlParams = match[1];
                if (urlParams && urlParams.length > 0) {
                    urlParams = urlParams.split("&");
                    for (var i = 0, len = urlParams.length; i < len; i++) {
                        var query = urlParams[i].split("=");
                        if (query && query.length > 1) {
                            try {
                                params[query[0]] = decodeURIComponent(decodeURIComponent(query[1]));
                            } catch (e) {
                            }
                        }
                    }
                }
            }
            if (!params.invitecode) {

            }
            return params;
        }
    },
    math: {
        add: function (arg1, arg2) {
            var r1,
                r2,
                m;
            try {
                r1 = arg1.toString().split(".")[1].length
            } catch (e) {
                r1 = 0
            }
            try {
                r2 = arg2.toString().split(".")[1].length
            } catch (e) {
                r2 = 0
            }
            m = Math.pow(10, Math.max(r1, r2));
            return (arg1 * m + arg2 * m) / m;
        },
        sub: function (arg1, arg2) {
            var r1,
                r2,
                m,
                n;
            try {
                r1 = arg1.toString().split(".")[1].length
            } catch (e) {
                r1 = 0
            }
            try {
                r2 = arg2.toString().split(".")[1].length
            } catch (e) {
                r2 = 0
            }
            m = Math.pow(10, Math.max(r1, r2));
            n = (r1 >= r2) ? r1 : r2;
            return ((arg1 * m - arg2 * m) / m).toFixed(n);
        },
        mult: function (arg1, arg2) {
            var m = 0,
                s1 = arg1.toString(),
                s2 = arg2.toString();
            try {
                m += s1.split(".")[1].length
            } catch (e) {
            }
            try {
                m += s2.split(".")[1].length
            } catch (e) {
            }
            return Number(s1.replace(".", "")) * Number(s2.replace(".", "")) / Math.pow(10, m);
        },
        div: function (arg1, arg2) {
            var t1 = 0,
                t2 = 0,
                r1,
                r2;
            try {
                t1 = arg1.toString().split(".")[1].length
            } catch (e) {
            }
            try {
                t2 = arg2.toString().split(".")[1].length
            } catch (e) {
            }
            r1 = Number(arg1.toString().replace(".", ""));
            r2 = Number(arg2.toString().replace(".", ""));
            return (r1 / r2) * Math.pow(10, t2 - t1);
        }
    },
    /**
     * [date 日期相关操作]
     * @type {Object}
     */
    date: {
        /**
         * [pad 日期补0]
         * @param  {[type]} source [description]
         * @param  {[type]} length [description]
         * @return {[type]}        [description]
         */
        pad: function (source, length) {
            var pre = "",
                negative = (source < 0),
                string = String(Math.abs(source));
            length = length || 2;
            if (string.length < length) {
                pre = (new Array(length - string.length + 1)).join('0');
            }

            return (negative ? "-" : "") + pre + string;
        },
        toDate(str) {
            if (str) {
                return str.toString().replace(/(\d{4})(\d{2})/, "$1/$2/");
            }
        },
        getDifferDay: function (time, count) {
            var dd;
            if (time) {
                dd = new Date(time.replace(/-/g, "/"));
            } else {
                dd = new Date();
            }
            var targetday_milliseconds = dd.getTime() + 1000 * 60 * 60 * 24 * count;
            dd.setTime(targetday_milliseconds);
            var y = dd.getFullYear();
            var m = dd.getMonth() + 1;
            var d = dd.getDate();
            if (m < 10)
                m = "0" + m;
            if (d < 10)
                d = "0" + d;
            var res = y + "/" + m + "/" + d;
            return res;
        },
        /**
         * [format 日期格式化]
         * @param  {[type]} source  [description]
         * @param  {[type]} pattern [description]
         * @return {[type]}         [description]
         */
        format: function (source, pattern) {
            if ('string' != typeof pattern) {
                return source.toString();
            }

            function replacer(patternPart, result) {
                pattern = pattern.replace(patternPart, result);
            }

            var pad = util.date.pad,
                year = source.getFullYear(),
                month = source.getMonth() + 1,
                date2 = source.getDate(),
                hours = source.getHours(),
                minutes = source.getMinutes(),
                seconds = source.getSeconds();

            replacer(/yyyy/g, pad(year, 4));
            replacer(/yy/g, pad(parseInt(year.toString().slice(2), 10), 2));
            replacer(/MM/g, pad(month, 2));
            replacer(/M/g, month);
            replacer(/dd/g, pad(date2, 2));
            replacer(/d/g, date2);

            replacer(/HH/g, pad(hours, 2));
            replacer(/H/g, hours);
            replacer(/hh/g, pad(hours % 12, 2));
            replacer(/h/g, hours % 12);
            replacer(/mm/g, pad(minutes, 2));
            replacer(/m/g, minutes);
            replacer(/ss/g, pad(seconds, 2));
            replacer(/s/g, seconds);

            return pattern;
        }
    },
    /**
     * 获取一段随机字符串(可指定长度)
     */
    getRandom: function (len) {
        return Math.random().toString(36).substr(2, len || 15);
    },
    /**
     * 生成时间戳
     */
    timesTamp: function () {
        return parseInt(new Date().getTime() / 1e3) + '';
    },
    /**
     * 64位编码程序
     */
    enBase64: function (a) {
        for (var e, f, g, b = "", c = 0, d = a.length, h = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"; d > c;) {
            if (e = 255 & a.charCodeAt(c++), c == d) {
                b += h.charAt(e >> 2), b += h.charAt((3 & e) << 4), b += "==";
                break;
            }
            if (f = a.charCodeAt(c++), c == d) {
                b += h.charAt(e >> 2), b += h.charAt((3 & e) << 4 | (240 & f) >> 4), b += h.charAt((15 & f) << 2),
                    b += "=";
                break;
            }
            g = a.charCodeAt(c++), b += h.charAt(e >> 2), b += h.charAt((3 & e) << 4 | (240 & f) >> 4),
                b += h.charAt((15 & f) << 2 | (192 & g) >> 6), b += h.charAt(63 & g);
        }
        return b;
    },
    /**
     * 64位解码程序
     */
    deBase64: function (a) {
        for (var b, c, d, e, f = 0, g = a.length, h = "", i = [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1]; g > f;) {
            do b = i[255 & a.charCodeAt(f++)]; while (g > f && -1 == b);
            if (-1 == b) break;
            do c = i[255 & a.charCodeAt(f++)]; while (g > f && -1 == c);
            if (-1 == c) break;
            h += String.fromCharCode(b << 2 | (48 & c) >> 4);
            do {
                if (d = 255 & a.charCodeAt(f++), 61 == d) return h;
                d = i[d];
            } while (g > f && -1 == d);
            if (-1 == d) break;
            h += String.fromCharCode((15 & c) << 4 | (60 & d) >> 2);
            do {
                if (e = 255 & a.charCodeAt(f++), 61 == e) return h;
                e = i[e];
            } while (g > f && -1 == e);
            if (-1 == e) break;
            h += String.fromCharCode((3 & d) << 6 | e);
        }
        return h;
    },
    enJson(json) {
        var str = this.enString(JSON.stringify(json), "a");
        return str;
    },
    deJson(str) {
        var str = this.deString(str, "a");
        if (str) {
            return JSON.parse(str);
        }
        return str;
    },
    /**
     * 加密字符串
     * @str 待加密字符串
     * @key 密钥  可选，若无密钥传入，则会生成一枚随机密钥，并在结果中传出
     */
    enString: function (str, key) {
        // 如果没有key, 取一个随机数做为密钥
        key = key || this.getRandom(),
            // 将经过url编码的字符串进行base64位编码
            str = this.enBase64(encodeURIComponent(str));
        // 加密操作
        var keyLen = key.length,
            strLen = str.length,
            Str = "",
            i = 0;
        for (; i < strLen; i += 1) {
            Str += String.fromCharCode(str.charCodeAt(i) ^ key.charCodeAt(i % keyLen));
        }
        // 如果有key, 传出base64编码后的加密字符串, 如果没有key, 传出加密字符串与key
        return key ? this.enBase64(Str) : {
            str: this.enBase64(Str),
            key: key
        };
    },
    /**
     * 解密字符串
     * @str 待解密字符串
     * @key 密钥
     */
    deString: function (str, key) {
        str = this.deBase64(str);
        var keyLen = key.length,
            strLen = str.length,
            Str = "",
            i = 0;
        for (; i < strLen; i += 1) {
            Str += String.fromCharCode(str.charCodeAt(i) ^ key.charCodeAt(i % keyLen));
        }
        return decodeURIComponent(this.deBase64(Str));
    },
    hasStorage: function () {
        var testKey = 'isEnable',
            storage = inBrowser && window.localStorage,
            isEnable = false;
        if (inBrowser) {
            try {
                storage.setItem(testKey, '1');
                storage.removeItem(testKey);
                isEnable = true;
            } catch (error) {
                console.info(error);
            }
            return isEnable;
        }

    }(),
    /**
     * set local storage
     * @key string 必须
     * @val string || object 必须
     * @expires timesTamp 可选，如未传入则默认为永久有效(如浏览器不支持storage则录入cookie中，三年有效)
     */
    setStorage: function (key, val, expires) {
        if (!this.hasStorage) return;
        if (typeof val !== 'string')
            val = JSON.stringify(val);
        // 加密字符串值
        val = this.enString(val, key);
        // 如果有有效时长，写入有效期
        if (expires) {
            var date = new Date();
            date.setTime(date.getTime() + (expires * 1e3));
            expires = ';expires:' + date.toGMTString()
        } else {
            expires = '';
        }
        if (inBrowser) {
            localStorage.setItem(key, 'en:/str;' + val + expires);
        }
    },
    getStorage: function (key) {
        if (!this.hasStorage) return;
        if (inBrowser) {
            var val = localStorage.getItem(key),
                data;
            if (!val) {
                return;
            }
            if (val.indexOf(';expires:') > 0) {
                data = val.split(';expires:');
                val = data[0];
                // 有效期判断
                if (new Date().getTime() >= new Date(data[1]).getTime()) return this.removeStorage(key), null;
            }
            // 判断是否加密，如未加密则清除本条内容
            if (val.indexOf('en:/str;') === 0)
                val = this.deString(val.slice(8), key);
            else return this.removeStorage(key), null;
            try {
                return JSON.parse(val);
            } catch (err) {
                return (val);
            }
        }
    },
    removeStorage: function (key) {
        return this.hasStorage && inBrowser ? localStorage.removeItem(key) : '';
    },
    /**
     * set local storage
     * @key string 必须
     * @val string || object 必须
     * @expires timesTamp 可选，如未传入则默认为永久有效(如浏览器不支持storage则录入cookie中，退出浏览器后失效)
     */
    setSession: function (key, val) {
        if (!this.hasStorage) return;
        if (typeof val !== 'string') {
            val = JSON.stringify(val);
        }
        val = this.enString(val, key);
        inBrowser && sessionStorage.setItem(key, 'en:/str;' + val);
    },
    getSession: function (key) {
        if (!this.hasStorage) return;
        var val = inBrowser && sessionStorage.getItem(key);
        if (!val) {
            return;
        }
        // 判断是否加密，如未加密则清除本条内容
        if (val.indexOf('en:/str;') === 0)
            val = this.deString(val.slice(8), key);
        else return this.removeSession(key), null;
        try {
            return JSON.parse(val);
        } catch (err) {
            return val;
        }
    },
    removeSession: function (key) {
        return !this.hasStorage ? '' : sessionStorage.removeItem(key);
    },
    /**
     * [string 字符串相关]
     * @type {Object}
     */
    string: {
        /**
         * [format 格式化字符串]
         * @param  {[type]} source [description]
         * @param  {[type]} opts   [description]
         * @return {[type]}        [description]
         */
        format: function (source, opts) {
            source = String(source);
            var data = Array.prototype.slice.call(arguments, 1),
                toString = Object.prototype.toString;
            if (data.length) {
                data = data.length == 1 ?
                    (opts !== null && (/\[object Array\]|\[object Object\]/.test(toString.call(opts))) ? opts : data) : data;
                return source.replace(/#\{(.+?)\}/g, function (match, key) {
                    var replacer = data[key];
                    if ('[object Function]' == toString.call(replacer)) {
                        replacer = replacer(key);
                    }
                    return ('undefined' == typeof replacer ? '' : replacer);
                });
            }
            return source;
        },
        decode: function (str) {
            return str.replace(/&/g, "&amp;");
        },
        stripTags: function (source) {
            return String(source || '').replace(/<[^>]+>/g, '');
        },
        toCamelCase: function (source) {
            if (source.indexOf('-') < 0 && source.indexOf('_') < 0) {
                return source;
            }
            return source.replace(/[-_][^-_]/g, function (match) {
                return match.charAt(1).toUpperCase();
            });
        },
        /**
         * [getByteLength 获取字符串字节长度]
         * @param  {[type]} str [description]
         * @return {[type]}     [description]
         */
        getByteLength: function (str) {
            var len = 0;
            for (var i = 0; i < str.length; i++) {
                if (str.charCodeAt(i) >= 0x4e00 && str.charCodeAt(i) <= 0x9fa5) {
                    len += 2;
                } else {
                    len++;
                }
            }
            return len;
        }
    }
}
export default util