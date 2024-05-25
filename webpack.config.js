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
      "crypto": require.resolve("crypto-browserify")
    },
};
