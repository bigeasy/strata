const Benchmark = require('benchmark')

const suite = new Benchmark.Suite('async')

const promises = {}

for (let i = 0; i < 4096; i++) {
    promises[i] = {
        resolved: true,
        promise: new Promise(resolve => resolve())
    }
}

async function awaited () {
    for (let i = 0; i < 4096 * 4; i++) {
        for (const key in promises) {
            await promises[key].promise
        }
    }
}

async function skipped () {
    for (let i = 0; i < 4096 * 4; i++) {
        for (const key in promises) {
            if (!promises[key].resolved) {
                await promises[key].promise
            }
        }
    }
}

async function main () {
    for (let i = 0; i < 4; i++) {
        {
            const start = Date.now()
            await awaited()
            console.log('awaited', (Date.now() - start) / 1000)
        }
        {
            const start = Date.now()
            await skipped()
            console.log('skipped', (Date.now() - start) / 1000)
        }
    }
}

main()
