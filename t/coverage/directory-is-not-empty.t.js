#!/usr/bin/env node

require('./proof')(1, function (async, assert) {
    var strata = new Strata({ directory: __dirname })
    async([function () {
        strata.create(async())
    }, function (_, error) {
        assert(/database .* is not empty\./.test(error.message), 'directory not empty')
    }])
})
