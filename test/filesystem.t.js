require('proof')(2, async okay => {
    const Magazine = require('magazine')
    const FileSystem = require('../filesystem')

    const storage = new FileSystem.HandleCache(new Magazine, 'fsync')

    okay(storage.strategy, 'fsync', 'set sync strategy')
    okay(storage.subordinate(), 'create subordinate')
})
