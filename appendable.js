const ascension = require('ascension')
const whittle = require('whittle')

module.exports = whittle(ascension([ Number, Number ]), file => file.split('.'))
