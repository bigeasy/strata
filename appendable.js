const ascension = require('ascension')

const appendable = ascension([ Number, Number ], function (file) {
    return file.split('.')
})

module.exports = appendable
