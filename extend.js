module.exports = function extend (to, from) {
    for (var key in from) to[key] = from[key]
    return to
}
