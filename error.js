const Interrupt = require('interrupt')

module.exports = Interrupt.create('Strata.Error', {
    BRANCH_BAD_HASH: 'branch load failed hash validation',
    VACUUM_FILE_IO: 'vacuum file manipulation error',
    CREATE_NOT_DIRECTORY: 'strata database location is not a directory',
    CREATE_NOT_EMPTY: 'cannot create strata database in a directroy that is not empty',
    INVALID_ARGUMENT: {},
    IO_ERROR: 'a file system error occured',
    OPTION_REQUIRED: {
        code: 'INVALID_ARGUMENT',
        message: 'the %(_option)s option is required'
    }
})
