require('./proof')(1, prove)

function prove (async, okay) {
    var strata = createStrata({ directory: __dirname })
    async([function () {
        strata.create(async())
    }, function (error) {
        okay(/database .* is not empty\./.test(error.message), 'directory not empty')
    }])
}
