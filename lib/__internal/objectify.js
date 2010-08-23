var switches = { accessors: {} };

function accessor (scope, key) {
  return {
    get: function () { return scope[key]; },
    set: function (v) { scope[key] = v; }
  }
}

function objectify() {
  var object = [];
  for (var i  in arguments) {
    object[/^function\s([\w\d]+)/.exec(arguments[i].toString())[1]] = arguments[i];
  }
  var properties = {};
  return definitions = function (define) {
    if (arguments.length == 0) {
      for (var key in properties) {
        Object.defineProperty(object, key, properties[key]);
      }
      return object;
    } else if (arguments[0] === switches.accessors) {
      for (var key in arguments[1]) {
        properties[key] = accessor(arguments[1], key);
      }
    } else {
      for (var key in arguments[0]) {
        properties[key] = { value: arguments[0][key] };
      }
    }
    return definitions;
  }
}

module.exports = objectify(objectify)(switches)();

/* vim: set ts=2 sw=2 et tw=0: */
