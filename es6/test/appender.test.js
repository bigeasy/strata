describe('appender', () => {
    const assert = require('assert')
    const stream = require('stream')
    const events = require('events')
    const Appender = require('../appender')
    it('can be constructed', () => {
        const appender = new Appender(new stream.PassThrough)
        assert(appender != null, 'constructed')
    })
    it('can write to a stream', async () => {
        const through = new stream.PassThrough({})
        const appender = new Appender(through)
        await appender.append([ Buffer.from('a') ])
        await appender.end()
        assert.equal(through.read().toString(), 'a', 'written')
    })
    it('can propagate errors', async () => {
        const through = new stream.PassThrough({})
        const appender = new Appender(through)
        const messages = []
        through.emit('error', new Error('error'))
        try {
            await appender.append([ Buffer.from('a') ])
        } catch (e) {
            messages.push(e.message)
        }
        assert.deepStrictEqual(messages, [ 'error' ], 'error')
    })
    it('can wait for drain', async () => {
        const through = new stream.PassThrough({ highWaterMark: 1 })
        const appender = new Appender(through)
        const data = []
        through.on('data', (chunk) => data.push(chunk))
        await appender.append([ Buffer.from('a'), Buffer.from('b'), Buffer.from('c') ])
        await appender.end()
        assert.equal('abc', Buffer.concat(data).toString(), 'written')
    })
    it('can handle errors waiting for events', async () => {
        const through = new stream.PassThrough({ highWaterMark: 1 })
        class Erroneous extends events.EventEmitter {
            constructor () {
                super()
            }
            write () {
                setImmediate(() => this.emit('error', new Error('error')))
                return false
            }
        }
        const messages = []
        const appender = new Appender(new Erroneous)
        try {
            await appender.append([ Buffer.from('a') ])
        } catch (error) {
            messages.push(error.message)
        }
        assert.deepStrictEqual(messages, [ 'error' ], 'error')
    })
})
