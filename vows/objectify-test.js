var vows = require('vows'),
    assert = require('assert'); 

vows.describe('Objectify').addBatch({
    'Objectify provides': {
        topic: require('__internal/objectify'),
        'the objectify object': function (o) {
            assert.ok(o.objectify);
        }
    },
    'Objectify will': {
        topic: require('__internal/objectify'),
        'create new objects': function (o) {
            var bar = o.objectify()();
            assert.ok(typeof bar == 'object');
        },
        'create objects with methods': function (o) {
            function foo() { return 1; }
            var bar = o.objectify(foo)();
            assert.ok(typeof bar.foo == 'function');
            assert.equal(bar.foo(), 1);
        },
        'create properties from values': function (o) {
            var scope = { a: 1, b: 2 };
            var bar = o.objectify()(scope)();
            assert.equal(bar.a, 1);
            assert.equal(bar.b, 2);
        },
        'create accessors from values': function (o) {
            var scope = { a: 1, b: 2 };
            var bar = o.objectify()(o.accessors, scope)();
            assert.equal(bar.a, 1);
            assert.equal(bar.b, 2);
            bar.a = 2;
            assert.equal(bar.a, 2);
        }
    }
}).export(module);

/* vim: set ts=2 sw=2 et tw=0: */
