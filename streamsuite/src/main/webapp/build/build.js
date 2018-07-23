var ora = require('ora')
var rm = require('rimraf')
var path = require('path')
var chalk = require('chalk')
var shell = require('shelljs')
var webpack = require('webpack')
var config = require('../config')
var webpackConfig = require('./webpack.prod.conf')
var env = process.env.NODE_ENV

var spinner = ora('building for ' + env + '...')
spinner.start()

rm(config.build.assetsRoot, err => {
   if (err) throw err
  webpack(webpackConfig, function(err, stats) {
     if (err) throw err
    process.stdout.write(stats.toString({
      colors: true,
      modules: false,
      children: false,
      chunks: false,
      chunkModules: false
    }) + '\n\n')
    console.log(chalk.cyan('  Build complete.\n'))
    console.log(chalk.yellow(
      '  Tip: built files are meant to be served over an HTTP server.\n' +
      '  Opening index.html over file:// won\'t work.\n'
    ))
    // 由于html页面用到了img path,所以拷贝src/images到dist/static/images
   // shell.cp('', 'favicon.ico', config.build.assetsRoot)
      shell.rm('-rf', ['./static/', './index.html'])

      shell.cp('-R', 'dist/streamsuite/*', './')
      shell.cp('-R', 'dist/index.html', './')

      //  shell.cp('-R', './mock', config.build.assetsRoot)
  })
})