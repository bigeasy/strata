#!/usr/bin/env node

require('./proof')(1, function (step, assert) {
    var strata = new Strata({ directory: __dirname })
    step([function () {
        strata.create(step())
    }, function (_, error) {
        assert(/database .* is not empty\./.test(error.message), 'directory not empty')
    }])
})
