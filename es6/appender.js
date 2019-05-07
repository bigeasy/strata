class Appender {
    constructor (stream) {
        this._output = stream
        this._errors = []
        this._output.on('error', error => this._errors.push(error))
    }

    _checkErrors () {
        if (this._errors.length != 0) {
            throw this._errors.shift()
        }
    }

    async _waitFor (event) {
        await new Promise(resolve => {
            this._output.on('error', resolve.bind())
            this._output.on(event, resolve.bind())
        })
        if (this._errors.length != 0) {
            throw this._errors.shift()
        }
    }

    async append (buffers) {
        this._checkErrors()
        for (let buffer of buffers) {
            if (!this._output.write(buffer)) {
                this._checkErrors()
                await this._waitFor('drain')
            }
        }
    }

    async end () {
        this._checkErrors()
        const finish = this._waitFor('finish')
        this._output.end()
        await finish
    }
}

module.exports = Appender
