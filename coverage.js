module.exports = {
    'coverage': function () {
        var tests = [ './vows/objectify-test', './vows/serializer-test' ];
        tests.forEach(function (element) {
            var o = require(element);
            for (var i in o) { o[i].run(); }
        });
    }
};
