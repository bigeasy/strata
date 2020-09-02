require('proof')(1, okay => {
    const Strata = require('..')
    const cursor = Strata.nullCursor()
    okay(cursor.indexOf(cursor.page.ghosts, 'a'), {
        index: null, found: false
    }, 'null')
    cursor.release()
})
