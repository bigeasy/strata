class Latch {
    constructor () {
        this.promise = new Promise(resolve => this._resolve = resolve)
    }

    unlatch () {
        this._resolve.call()
    }
}

module.exports = Latch
