function Queue () {
    this.buffers = []
    this.buffer = new Buffer(4098)
    this.offset = 0
    this.length = 0
}

Queue.prototype.slice = function (size) {
    if (size > this.buffer.length - this.offset) {
        var buffer = this.buffer.slice(0, this.offset)
        this.length += buffer.length
        this.buffers.push(buffer)
        this.buffer = new Buffer(Math.max(size, 4098))
        this.offset = 0
    }
    var slice = this.buffer.slice(this.offset, this.offset + size)
    this.offset += size
    return slice
}

Queue.prototype.clear = function () {
    this.buffers.length = 0
    this.length = 0
}

Queue.prototype.finish = function (size) {
    if (this.offset != 0) {
        var buffer = this.buffer.slice(0, this.offset)
        this.buffers.push(buffer)
        this.length += buffer.length
    }
    this.buffer = null
}

module.exports = Queue
