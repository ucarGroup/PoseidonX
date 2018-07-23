var webpack = require('webpack');
var WebpackDevServer = require('webpack-dev-server');
var webpackConfig = require('./webpack.dev.conf');
var compiler = webpack(webpackConfig)
var config = require('../config')
var path = require('path')
var port = process.env.PORT || config.dev.port
new WebpackDevServer(compiler, {
    publicPath: webpackConfig.output.publicPath,
    hot: true,
    noInfo: false,
    compress: true,
    proxy: config.dev.proxyTable,
    historyApiFallback: true,
    clientLogLevel: "info",
    filename: "app.js",
    disableHostCheck: true,
    stats: { colors: true },
    watchOptions: {
        ignored: /node_modules/,
    }
}).listen(port, '127.0.0.1', function(err, result) {
    if (err) {
        console.log(err);
    }
    console.log('Listening at localhost:' + config.dev.port);
});
