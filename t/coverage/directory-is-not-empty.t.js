#!/usr/bin/env node

require('./proof')(1, prove)

function prove (async, assert) {
    var strata = createStrata({ directory: __dirname })
    async([function () {
        strata.create(async())
    }, function (error) {
        assert(/database .* is not empty\./.test(error.message), 'directory not empty')
    }])
}
