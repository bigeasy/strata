describe('appendable', () => {
    const assert = require('assert')
    const appendable = require('../appendable')
    it('can sort', () => {
        const sorted = ([ '0.1', '2.1', '3.3', '0.2', '0.0' ]).sort(appendable)
        assert.deepStrictEqual(sorted, [
            '0.0', '0.1', '0.2', '2.1', '3.3'
        ], 'sorted')
    })
})
