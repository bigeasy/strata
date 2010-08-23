var vows = require('vows'),
    assert = require('assert'); 

vows.describe('Serializer').addBatch({
    'Serializer provides ': {
        topic: require('__internal/serializer'),
        'the integer32 method': function (s) {
            assert.ok(s.integer32);
        },
        'the integer64 method': function (s) {
            assert.ok(s.integer32);
        }
    }
}).export(module);

/* vim: set ts=2 sw=2 et tw=0: */
