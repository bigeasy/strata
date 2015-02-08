var fs = require('fs')

function Scribe (filename, flags) {
    this.operations = [ {} ]
    this.filename = filename
    this.flags = flags
}

Scribe.prototype.open = function () {
    fs.open(this.filename, this.flags, this._done.bind(this, '_opened'))
}

Scribe.prototype._done = function (method, error, result) {
    if (error) {
        while (this.operations.length) {
            var callback = this.operations.shift().callback
            if (callback) {
                callback(error)
                return
            }
        }
        this.error = error
    } else {
        this[method](result)
        this.operations.shift()
        if (this.operations.length) {
            this[this.operations[0].method](this.operations[0])
        }
    }
}

Scribe.prototype._opened = function (fd) {
    this.fd = fd
}

Scribe.prototype.write = function (buffer, offset, length, position, callback) {
    this._push({
        method: '_write',
        buffer: buffer,
        offset: offset,
        length: length,
        position: position,
        callback: callback
    })
}

Scribe.prototype._push = function (operation) {
    if (this.error) {
        var callback = operation.callback
        if (callback) {
            var error = this.error
            delete this.error
            callback(error)
        }
    } else {
        this.operations.push(operation)
        if (this.operations.length == 1) {
            this[this.operations[0].method](this.operations[0])
        }
    }
}

Scribe.prototype.close = function (callback) {
    this._push({ method: '_close', callback: callback })
}

Scribe.prototype._write = function (write) {
    fs.write(this.fd, write.buffer, write.offset, write.length,
        write.position, this._done.bind(this, '_written'))
}

Scribe.prototype._written = function (written, buffer) {
    var callback = this.operations[0].callback
    if (callback) {
        callback(null, written, buffer)
    }
}

Scribe.prototype._close = function (write) {
    fs.close(this.fd, write.callback)
}

module.exports = Scribe
