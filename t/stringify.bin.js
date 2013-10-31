#!/usr/bin/env node

require('./proof').stringify(process.argv[2], function (error, result) {
    if (error) throw error
    console.log(result)
})
