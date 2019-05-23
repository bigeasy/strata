module.exports = function (f) {
    return new Promise((resolve, reject) => {
        f(function (error) {
            if (error == null) {
                var vargs = []
                vargs.push.apply(vargs, arguments)
                vargs.splice(0, 1)
                resolve(vargs)
            } else {
                reject(error)
            }
        })
    })
}
