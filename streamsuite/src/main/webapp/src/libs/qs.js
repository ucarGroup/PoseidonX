import axios from "axios";
const qs1 = require('qs');
import { util } from "libs";
export default {
  success(url, response, resolve, reject) {
    return new Promise(() => {
      if (response.status == 200) {
        let data = response.data;
        if (data.status == 5) {

        } else {
          resolve(response.data)
        }
      } else {
        reject && reject(response);
      }
    })
  },
  get(url, params = {}, options = {}) {
    let me = this;
    return new Promise((resolve, reject) => {
      var me = this;
      axios.get(url, {
        params: params
      }).then((response) => {
        return me.success(url, response, resolve, reject);
      });
    })
  },
  form(url, params) {
    let me = this;
    return new Promise((resolve, reject) => {
      var me = this;
      axios.post(url, params, {
        headers: { 'Content-Type': 'multipart/form-data' },
        transformRequest: [function(data) {
          let formData = util.objectToFormData(data)
          return formData;
        }],
      }).then((response) => {
        return me.success(url, response, resolve, reject);
      });
    })
  },
  post(url, params = {}, options = {}) {
    return new Promise((resolve, reject) => {
      var me = this;
      axios.post(url, qs1.stringify(params), {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      }).then((response) => {
        return me.success(url, response, resolve, reject)
      });
    });
  }
}