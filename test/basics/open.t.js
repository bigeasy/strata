require('./proof')(2, prove)

function prove (async, okay) {
    var strata
    async(function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.create(async())
    }, function () {
        strata.close(async())
    }, function () {
        strata = createStrata({ directory: tmp, leafSize: 3, branchSize: 3 })
        strata.open(async())
    }, function () {
        okay(strata.sheaf.magazine.heft, 0, 'json size')
        okay(strata.sheaf.nextAddress, 2, 'next address')
    })
}
