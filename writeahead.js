'use strict'

const assert = require('assert')

const fs = require('fs').promises
const path = require('path')
const Strata = { Error: require('./error') }
const Storage = require('./storage')
const Player = require('transcript/player')
const Recorder = require('transcript/recorder')

const wal = {
    async *iterator (writeahead, qualifier, key) {
        const player = new Player(() => '0')
        let apply
        const entries = []
        for await (const block of writeahead.get([ qualifier, key ])) {
           for (const entry of player.split(block)) {
                const header = JSON.parse(String(entry.parts.shift()))
                if (header.method == 'apply') {
                    apply = header.key == key
                } else if (apply) {
                    entries.push({ header, parts: entry.parts, sizes: entry.sizes })
                }
            }
            if (entries.length != 0) {
                yield entries
                entries.length = 0
            }
        }
        if (entries.length != 0) {
            yield entries
        }
    },
    async last (writeahead, qualifier, key, last = null) {
        const player = new Player(() => '0')
        let apply
        for await (const block of writeahead.get([ qualifier, key ])) {
            for (const entry of player.split(block)) {
                const header = JSON.parse(String(entry.parts.shift()))
                if (header.method == 'apply') {
                    apply = header.key == key
                } else if (apply) {
                    last = header
                }
            }
        }
        return last
    }
}

class WriteAheadOnly {
    static wal = wal

    static async open (options) {
        options = Storage.options(options)
        const writer = new WriteAheadOnly.Writer(options)
        if (options.create) {
            await writer.create(options.create)
        } else {
            await writer.open()
        }
        return writer
    }

    static Serializer = class {
        constructor (writeahead, qualifier) {
            this._writeahead = writeahead
            this._qualifier = qualifier
            this._records = {}
        }
        push (key, ...records) {
            if (this._records[key] == null) {
                this._records[key] = [{
                    header: { method: 'apply', key: key }
                }]
            }
            this._records[key].push.apply(this._records[key], records)
        }
        get body () {
            const body = []
            for (const key in this._records) {
                body.push.apply(body, this._records[key])
            }
            return body
        }
        get keys () {
            return Object.keys(this._records).map(key => [ this._qualifier, key ])
        }
        serialize () {
            return {
                keys: this.keys,
                buffer: this._writeahead._recordify.apply(this._writeahead, this.body)
            }
        }

    }

    static Reader = class {
        called = 0

        constructor (options) {
            options = Storage.options(options)
            this.writeahead = options.writeahead
            this.key = options.key
            this.serializer = options.serializer
            this.extractor = options.extractor
            this.checksum = options.checksum
        }

        page (id) {
            return this.log(id, null)
        }

        async log (id, stop) {
            this.called++
            const leaf = +id.split('.')[1] % 2 == 1
            const page = leaf ? {
                id: id,
                items: [],
                right: null,
                key: null,
                stop: 0,
                leaf: true
            } : {
                id: id,
                items: [],
                stop: 0,
                leaf: false
            }
            let apply = false
            const player = new Player(this.checksum)
            WAL: for await (const entries of wal.iterator(this.writeahead, this.key, id)) {
                for (const { header, parts, sizes } of entries) {
                    switch (header.method) {
                    case 'stop': {
                            assert(! isNaN(header.stop))
                            page.stop = header.stop
                            if (page.stop == stop) {
                                break WAL
                            }
                            page.stop++
                            assert(! isNaN(header.stop))
                        }
                        break
                    case 'clear': {
                            page.items = []
                        }
                        break
                    case 'load': {
                            const { page: load } = await this.log(header.id, header.stop)
                            page.items = load.items
                            page.stop = load.stop + 1
                            if (leaf) {
                                page.key = load.key
                                page.right = load.right
                            }
                        }
                        break
                    case 'split': {
                            page.items = page.items.slice(header.index, header.length)
                            if (!leaf) {
                                page.items[0].key = null
                            }
                        }
                        break
                    case 'merge': {
                            const { page: load } = await this.log(header.id)
                            if (leaf) {
                                page.right = load.right
                            } else {
                                load.items[0].key = this.serializer.key.deserialize(parts)
                            }
                            page.items.push.apply(page.items, load.items)
                        }
                        break
                    case 'key': {
                            page.key = this.serializer.key.deserialize(parts)
                        }
                        break
                    case 'right': {
                            page.right = this.serializer.key.deserialize(parts)
                        }
                        break
                    case 'insert': {
                            const heft = sizes.reduce((sum, size) => sum + size, 0)
                            if (leaf) {
                                const deserialized = this.serializer.parts.deserialize(parts)
                                page.items.splice(header.index, 0, {
                                    key: this.extractor(deserialized),
                                    parts: deserialized,
                                    heft: heft
                                })
                            } else {
                                const key = parts.length != 0
                                    ? this.serializer.parts.deserialize(parts)
                                    : null
                                page.items.splice(header.index, 0, {
                                    id: header.id,
                                    key: key,
                                    heft: heft
                                })
                            }
                        }
                        break
                    case 'delete': {
                            page.items.splice(header.index, 1)
                        }
                        break
                    }
                }
            }
            const heft = page.items.reduce((sum, item) => sum + item.heft, 0)
            return { page, heft }
        }
    }

    static Writer = class {
        constructor (options) {
            this._writeahead = options.writeahead
            this._key = options.key
            this._id = 0
            this._pageId = 0
            this.instance = 0
            this.extractor = options.extractor
            this.serializer = options.serializer
            this._recorder = Recorder.create(() => '0')
            this.reader = new WriteAheadOnly.Reader(options)
        }

        recordify (header, parts = []) {
            return this._recorder([[ Buffer.from(JSON.stringify(header)) ].concat(parts)])
        }

        nextId (leaf) {
            let id
            do {
                id = this._pageId++
            } while (leaf ? id % 2 == 0 : id % 2 == 1)
            return String(this.instance) + '.' +  String(id)
        }

        _recordify (...records) {
            const buffers = []
            for (const record of records) {
                buffers.push(this._recorder([[ Buffer.from(JSON.stringify(record.header)) ].concat(record.parts || [])]))
            }
            return Buffer.concat(buffers)
        }

        async create (create) {
            await this._writeahead.write([{
                keys: [[ this._key, '0.0' ], [ this._key, 'instance' ], create ],
                buffer: Buffer.concat([{
                    header: {
                        method: 'apply',
                        key: 'instance'
                    }
                }, {
                    header: {
                        method: 'instance',
                        instance: 0
                    }
                }, {
                    header: {
                        method: 'apply',
                        key: '0.0'
                    }
                }, {
                    header: {
                        method: 'insert',
                        index: 0,
                        id: '0.1'
                    }
                }, {
                    header: {
                        method: 'apply',
                        key: create[1]
                    }
                }, {
                    header: {
                        method: 'locate',
                        value: this._key
                    }
                }].map(entry => this._recordify(entry)))
            }]).promise
            this._pageId = 2
            this.instance = 0
        }

        async open () {
            this.instance = (await wal.last(this._writeahead, this._key, 'instance')).instance
            await this._writeahead.write([{
                keys: [[ this._key, 'instance' ]],
                buffer: Buffer.concat([{
                    header: {
                        method: 'apply',
                        key: 'instance'
                    }
                }, {
                    header: {
                        method: 'instance',
                        instance: ++this.instance
                    }
                }].map(entry => this._recordify(entry)))
            }]).promise
        }

        read (id) {
            return this.reader.page(id)
        }

        async append (page, writes) {
            this._writeahead.write([{ keys: [ [ this.key, page.id ] ], buffer: Buffer.concat(writes) }])
        }

        writeLeaf (page, writes) {
            writes.unshift(this._recordify({ header: { method: 'apply', key: page.id } }))
            this._writeahead.write([{ keys: [[ this._key, page.id ]], buffer: Buffer.concat(writes) }])
        }

        //
        _startBalance (serializer, messages) {
            const id = [ 'balance', this.instance, this._id++ ].join('.')

            serializer.push('balance', {
                header: {
                    method: 'balance',
                    key: id
                }
            })

            serializer.push(id, {
                header: {
                    method: 'messages',
                    messages: messages
                }
            })

            this._write = serializer
        }

        // Even more approixmate than usual because we're not accounting for
        // keys that were set to `null` or set to a value from `null`.

        //
        _setHeft (...branches) {
            for (const branch of branches) {
                branch.cartridge.heft = branch.page.items.reduce((sum, item) => sum + item.heft, 0)
            }
        }

        writeDrainRoot ({ left, right, root }) {
            this._setHeft(left, right, root)
            const stop = root.page.stop++
            this._write.push(root.page.id, {
                header: {
                    method: 'stop',
                    stop: stop
                }
            }, {
                header: {
                    method: 'clear'
                }
            }, {
                header: {
                    method: 'insert',
                    index: 0,
                    id: root.page.items[0].id
                }
            }, {
                header: {
                    method: 'insert',
                    index: 1,
                    id: root.page.items[1].id
                },
                parts: this.serializer.key.serialize(root.page.items[1].key)
            })
            this._write.push(left.page.id, {
                header: {
                    method: 'load',
                    id: root.page.id,
                    stop: stop
                }
            }, {
                header: {
                    method: 'split',
                    index: 0,
                    length: left.page.items.length
                }
            })
            this._write.push(right.page.id, {
                header: {
                    method: 'load',
                    id: root.page.id,
                    stop: stop
                }
            }, {
                header: {
                    method: 'split',
                    index: left.page.items.length,
                    length: left.page.items.length + right.page.items.length
                }
            })
        }

        writeSplitBranch ({ promotion, left, right, parent }) {
            this._setHeft(left, right, parent)
            const stop = left.page.stop++
            this._write.push(left.page.id, {
                header: {
                    method: 'stop',
                    stop: stop
                }
            }, {
                header: {
                    method: 'split',
                    index: 0,
                    length: left.page.items.length
                }
            })
            this._write.push(right.page.id, {
                header: {
                    method: 'load',
                    id: left.page.id,
                    stop: stop
                }
            }, {
                header: {
                    method: 'split',
                    index: left.page.items.length,
                    length: left.page.items.length + right.page.items.length
                }
            })
            this._write.push(parent.page.id, {
                header: {
                    method: 'insert',
                    index: parent.index + 1,
                    id: right.page.id
                },
                parts: this.serializer.key.serialize(promotion)
            })

        }

        async writeSplitLeaf({ left, right, parent, writes, messages }) {
            await this.writeLeaf(left.page, writes)

            const partition = left.page.items.length
            const length = left.page.items.length + right.page.items.length

            const body = []

            const serializer = new WriteAheadOnly.Serializer(this, this._key)

            serializer.push(left.page.id, {
                header: {
                    method: 'stop',
                    stop: left.page.stop++
                }
            }, {
                header: {
                    method: 'split',
                    index: 0,
                    length: partition
                }
            }, {
                header: {
                    method: 'right'
                },
                parts: this.serializer.key.serialize(right.page.key)
            })

            serializer.push(right.page.id, {
                header: {
                    method: 'load',
                    id: left.page.id,
                    stop: left.page.stop - 1
                }
            }, {
                header: {
                    method: 'split',
                    index: partition,
                    length: length
                }
            }, {
                header: {
                    method: 'key'
                },
                parts: this.serializer.key.serialize(right.page.key)
            })

            serializer.push(parent.page.id, {
                header: {
                    method: 'insert',
                    index: parent.index + 1,
                    id: right.page.id
                },
                parts: this.serializer.key.serialize(right.page.items[0].key)
            })

            this._startBalance(serializer, messages)
        }

        writeFillRoot({ root, child }) {
            this._setHeft(root)
            this._write.push(root.page.id, {
                header: {
                    method: 'clear'
                }
            }, {
                header: {
                    method: 'load',
                    id: child.page.id,
                    stop: child.page.stop
                }
            })
        }

        writeMerge ({ key, serializer, left, right, surgery, pivot }) {
            this._setHeft(left, pivot, surgery.splice)
            serializer.push(left.page.id, {
                header: {
                    method: 'merge',
                    id: right.page.id
                },
                parts: left.page.leaf ? [] : this.serializer.key.serialize(key)
            })

            serializer.push(surgery.splice.page.id, {
                header: {
                    method: 'delete',
                    index: surgery.splice.index
                }
            })

            if (surgery.splice.index == 0) {
                serializer.push(surgery.splice.page.id, {
                    header: {
                        method: 'key',
                        index: 0
                    }
                })
            }

            if (surgery.replacement != null) {
                serializer.push(pivot.page.id, {
                    header: {
                        method: 'key',
                        index: pivot.index
                    },
                    parts: this.serializer.key.serialize(surgery.replacement)
                })
            }
        }

        writeMergeBranch ({ key, left, right, surgery, pivot }) {
            this.writeMerge({ key, serializer: this._write, left, right, surgery, pivot })
        }

        async writeMergeLeaf ({ left, right, surgery, pivot, writes, messages }) {
            await this.writeLeaf(left.page, writes.left)
            await this.writeLeaf(right.page, writes.right)

            const serializer = new WriteAheadOnly.Serializer(this, this._key)

            this.writeMerge({ serializer, left, right, surgery, pivot })

            this._startBalance(serializer, messages)
        }

        async balance (sheaf) {
            const cartridges = []
            for (;;) {
                cartridges.splice(0).forEach(cartridge => cartridge.release())
                if (this._write != null) {
                    const write = this._write.serialize()
                    this._write = null
                    this._writeahead.write([ write ])
                }
                const balance = await wal.last(this._writeahead, this._key, 'balance')
                let messages = []
                if (balance != null) {
                    messages = (await wal.last(this._writeahead, this._key, balance.key, { messages })).messages
                }
                if (messages.length == 0) {
                    break
                }
                this._write = new WriteAheadOnly.Serializer(this, this._key)
                this._write.push(balance.key, {
                    header: {
                        method: 'messages', messages: messages
                    }
                })
                const message = messages.shift()
                Strata.Error.assert(message.method == 'balance', 'JOURNAL_CORRUPTED')
                switch (message.method) {
                case 'balance':
                    await sheaf.balance(message.key, message.level, messages, cartridges)
                    break
                }
            }
        }
    }
}

module.exports = WriteAheadOnly
