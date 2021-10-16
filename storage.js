const { coalesce } = require('extant')

class Storage {
    static options (options) {
        if (options.checksum == null) {
            options.checksum = (() => '0')
        }
        if (options.extractor == null) {
            options.extractor = parts => [ parts[0] ]
        }
        options.serializer = function () {
            const serializer = coalesce(options.serializer, 'json')
            switch (serializer) {
            case 'json':
                return {
                    parts: {
                        serialize: function (parts) {
                            return parts.map(part => Buffer.from(JSON.stringify(part)))
                        },
                        deserialize: function (parts) {
                            return parts.map(part => JSON.parse(part.toString()))
                        }
                    },
                    key: {
                        serialize: function (key) {
                            if (key == null) {
                                throw new Error
                            }
                            return [ Buffer.from(JSON.stringify(key)) ]
                        },
                        deserialize: function (parts) {
                            return JSON.parse(parts[0].toString())
                        }
                    }
                }
            case 'buffer':
                return {
                    parts: {
                        serialize: function (parts) { return parts },
                        deserialize: function (parts) { return parts }
                    },
                    key: {
                        serialize: function (part) { return [ part ] },
                        deserialize: function (parts) { return parts[0] }
                    }
                }
            default:
                return serializer
            }
        } ()
        return options
    }
}

module.exports = Storage
