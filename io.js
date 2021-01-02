const fs = require('fs').promises
const Strata = { Error: require('./error') }

// https://stackoverflow.com/a/41976600
// https://stackoverflow.com/questions/694188/when-does-the-write-system-call-write-all-of-the-requested-buffer-versus-just
// https://stackoverflow.com/questions/31737793/posix-partial-write?rq=1
// https://stackoverflow.com/questions/694188/when-does-the-write-system-call-write-all-of-the-requested-buffer-versus-just#comment70956787_694239

exports.append = async function (handle, flush, generator, sandwich = null) {
    let buffered = 0, size = 0
    const buffers = []
    if (sandwich != null) {
        const buffer = sandwich('header')
        buffered += buffer.length
        size += buffer.length
        buffers.push(buffer)
    }
    let index = 0
    for (;;) {
        const buffer = generator(index++)
        if (buffer == null) {
            break
        }
        buffers.push(buffer)
        buffered += buffer.length
        size += buffer.length
        if (buffered >= flush) {
            await Strata.Error.resolve(handle.writev(buffers), 'IO_ERROR')
            buffers.length = 0
            buffered = 0
        }
    }
    if (sandwich != null) {
        const buffer = sandwich('footer')
        buffered += buffer.length
        size += buffer.length
        buffers.push(buffer)
    }
    if (buffers.length) {
        await Strata.Error.resolve(handle.writev(buffers), 'IO_ERROR')
    }
    return size
}

exports.play = async function (player, filename, buffer, consumer) {
    let size = 0, index = 0
    const gathered = []
    const handle = await Strata.Error.resolve(fs.open(filename), 'IO_ERROR')
    try {
        for (;;) {
            const { bytesRead } = await Strata.Error.resolve(handle.read(buffer, 0, buffer.length), 'IO_ERROR')
            if (bytesRead == 0) {
                return { gathered, length: index, size }
            }
            size += bytesRead
            const slice = bytesRead < buffer.length ? buffer.slice(0, bytesRead) : buffer
            for (const entry of player.split(slice)) {
                const result = consumer(entry, index++)
                if (result != null) {
                    gathered.push(result)
                }
            }
        }
    } finally {
        await Strata.Error.resolve(handle.close(), 'IO_ERROR')
    }
}

exports.player = async function* (player, filename, buffer) {
    const handle = await Strata.Error.resolve(fs.open(filename), 'IO_ERROR')
    try {
        for (;;) {
            const { bytesRead } = await Strata.Error.resolve(handle.read(buffer, 0, buffer.length), 'IO_ERROR')
            if (bytesRead == 0) {
                break
            }
            const slice = bytesRead < buffer.length ? buffer.slice(0, bytesRead) : buffer
            const entries = player.split(slice)
            yield { entries, read: bytesRead }
        }
    } finally {
        await Strata.Error.resolve(handle.close(), 'IO_ERROR')
    }
}
