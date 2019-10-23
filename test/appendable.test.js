require('proof')(1, (okay) => {
    const appendable = require('../appendable')
    const sorted = ([ '0.1', '2.1', '3.3', '0.2', '0.0' ]).sort(appendable)
    okay(sorted, [
        '0.0', '0.1', '0.2', '2.1', '3.3'
    ], 'sorted')
})
