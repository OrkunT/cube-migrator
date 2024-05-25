// webpack.config.js

const path = require('path');

module.exports = {
  entry: './index.js',
  output: {
    filename: 'main.js',
    path: path.resolve(__dirname, 'dist'),
  },
  resolve: {
    fallback: {
      "util": require.resolve("util/"),
      "buffer": require.resolve("buffer/"),
      "stream": require.resolve("stream-browserify"),
      "crypto": require.resolve("crypto-browserify"),
      "aws4": require.resolve("aws4"),
      "mongodb-client-encryption": require.resolve("mongodb-client-encryption"),
      "http": require.resolve("http"),
      "url": require.resolve("url"),
      "querystring": require.resolve("querystring"),
      "os": require.resolve("os"),
      "zlib": require.resolve("zlib"),
      "fs": require.resolve("fs/"),
      "path": require.resolve("path/"),
      "dns": require.resolve("dns/"),
      "assert": require.resolve("assert/"),
      "module": require.resolve("module/")
    },
};
