const path = require('path');
const {getDefaultConfig, mergeConfig} = require('@react-native/metro-config');
const {FileStore} = require('metro-cache');

module.exports = mergeConfig(getDefaultConfig(__dirname), {
  projectRoot: __dirname,
  watchFolders: [],
  maxWorkers: 1,
  cacheStores: [new FileStore({root: path.join(__dirname, '.metro-cache')})],
  resolver: {
    disableHierarchicalLookup: true,
    nodeModulesPaths: [path.join(__dirname, 'node_modules')],
  },
});
