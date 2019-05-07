module.exports = function (ee, event, errored) {
    var argc = arguments.length
    return new Promise((resolve, reject) => {
        function resolver () {
            unlisten()
            var vargs = []
            vargs.push.apply(vargs, arguments)
            resolve(vargs)
        }
        function rejector (error) {
            unlisten()
            if (argc == 3) {
                resolve(errored)
            } else {
                reject(error)
            }
        }
        function unlisten () {
            ee.removeListener(event, resolver)
            ee.removeListener('error', rejector)
        }
        ee.on(event, resolver)
        ee.on('error', rejector)
    })
}
