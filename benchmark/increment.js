const Benchmark = require('benchmark')

const suite = new Benchmark.Suite('async')

function one (select, value) {
    for (let i = 0xffffffff; i < 0xffffffff + 1024 * 64; i++) {
    }
}

function two (select, value) {
    for (let i = 0xffffffffn; i < 0xffffffffn + 1024n * 64n; i++) {
    }
}

for (let i = 1; i <= 4; i++)  {
    suite.add({
        name: 'number ' + i,
        fn: one
    })
    suite.add({
        name: 'BigInt ' + i,
        fn: two
    })
}

suite.on('cycle', function(event) {
    console.log(String(event.target));
})

suite.on('complete', function() {
    console.log('Fastest is ' + this.filter('fastest').map('name'));
})

suite.run()
