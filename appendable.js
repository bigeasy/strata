const ascension = require('ascension')
const whittle = require('whittle')

const appendable = whittle(ascension([ Number, Number ]), function (file) {
    return file.split('.')
})

module.exports = appendable
