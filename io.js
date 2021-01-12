const Operation = require('operation')

// https://stackoverflow.com/a/41976600
// https://stackoverflow.com/questions/694188/when-does-the-write-system-call-write-all-of-the-requested-buffer-versus-just
// https://stackoverflow.com/questions/31737793/posix-partial-write?rq=1
// https://stackoverflow.com/questions/694188/when-does-the-write-system-call-write-all-of-the-requested-buffer-versus-just#comment70956787_694239
exports.appendv = async function (filename, buffers, sync) {
    const open = await Operation.open(filename, sync.flag)
    try {
        const written = await Operation.writev(open, buffers)
        await sync.sync(open)
        return written
    } finally {
        await Operation.close(open)
    }
}
