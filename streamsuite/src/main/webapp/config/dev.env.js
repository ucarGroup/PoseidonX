var merge = require('webpack-merge')
var prodEnv = require('./prod.env')

module.exports = merge(prodEnv, {
  NODE_ENV: '"dev"'
/*  NODE_ENV: '"development"'*/
})
