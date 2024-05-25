// webpack.config.js

const path = require('path');

module.exports = {
  entry: './mongodb/index.js',
  output: {
    filename: 'main.js',
    path: path.resolve(__dirname, 'dist'),
  },
};
