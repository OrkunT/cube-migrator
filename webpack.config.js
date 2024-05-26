const path = require('path');

module.exports = {
  entry: './index.js',
  output: {
    filename: 'main.js',
    path: path.resolve(__dirname, 'dist'),
  },
  resolve: {
    fallback: {
      "fs": false,
      "constants": require.resolve("constants-browserify"),
      "http": require.resolve("stream-http"),
      "net": false,
      "dgram": false,
      "tls": false,
      "zlib": require.resolve("browserify-zlib"),
      "util": require.resolve("util/"),
      "buffer": require.resolve("buffer/"),
      "stream": require.resolve("stream-browserify"),
      "crypto": require.resolve("crypto-browserify"),
      "aws4": require.resolve("aws4"),
      "mongodb-client-encryption": require.resolve("mongodb-client-encryption"),
      "url": require.resolve("url"),
      "querystring": require.resolve("querystring"),
      "os": require.resolve("os-browserify"),
      "path": require.resolve("path-browserify"),
      "dns": false,
      "assert": require.resolve("assert/"),
      "module": false
    },
  }
};
