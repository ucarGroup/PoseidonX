var path = require('path')
var utils = require('./utils')
var config = require('../config')
var isProduction = (process.env.NODE_ENV == "production" || process.env.NODE_ENV == "test");
var webpack = require('webpack');
var autoprefixer = require('autoprefixer');
var ExtractTextPlugin = require('extract-text-webpack-plugin')
process.traceDeprecation = true;

function resolve(dir) {
    return path.join(__dirname, '..', dir)
}

var app = [
    './src/index'
];

if (!isProduction) {
    app.unshift(
        'react-hot-loader/patch',
        'webpack-dev-server/client?http://127.0.0.1:' + config.dev.port,
        'webpack/hot/only-dev-server'
    )
}

module.exports = {
    entry: {
        app: app,
        vendor: [
            'react',
            'react-dom',
            'react-router',
            'mobx',
            'mobx-react',
            'mobx-react-router'
        ]
    },
    output: {
        path: config.build.assetsRoot,
        filename: '[name].js',
        chunkFilename: '[name].[chunkhash:5].chunk.js',
        publicPath: process.env.NODE_ENV !== 'dev' ? config.build.assetsPublicPath : config.dev.assetsPublicPath
    },
    resolve: {
        extensions: ['.js', '.jsx', '.json'],
        alias: {
            'components': path.resolve(__dirname, '../src/components'),
            'libs': path.resolve(__dirname, '../src/libs'),
            'styles': path.resolve(__dirname, '../src/styles')
        }
    },
    module: {
        rules: [{
                test: /\.jsx?$/,
                include: [resolve('src')],
                use: [{
                    loader: 'babel-loader',
                    options: {
                        plugins: [
                            ['import', [{
                                libraryName: 'antd',
                                style: false
                            }]], // import less
                        ]
                    }
                }],
            },
            {
                test: /\.(png|jpe?g|gif)(\?.*)?$/,
                use: 'url-loader?limit=8192&name=' + utils.assetsPath('images/[name].[hash:7].[ext]')
            }
        ]
    },
    plugins: [
        new webpack.HotModuleReplacementPlugin(),
        new webpack.NamedModulesPlugin(),
        new webpack.NoEmitOnErrorsPlugin()
    ],
    devtool: 'source-map'
}