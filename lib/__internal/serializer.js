var o = require('./objectify');
var shift8  = Math.pow(2, 8),
    shift16 = Math.pow(2, 16),
    shift24 = Math.pow(2, 24),
    shift32 = Math.pow(2, 32);

function integer32 (buffer, offset) {
  if (arguments.length == 3) {
    var value = arguments[2];
    buffer[offset]     = (value / shift24 & 0xFF);
    buffer[offset + 1] = (value / shift16 & 0xFF);
    buffer[offset + 2] = (value / shift8 & 0xFF);
    buffer[offset + 3] = (value & 0xFF);
    return value;
  }
  return (buffer[offset]     * shift24) +
         (buffer[offset + 1] * shift16) +
         (buffer[offset + 2] * shift8)  +
          buffer[offset + 3];
}
function  integer64 (buffer, offset) {
  if (arguments.length == 3) {
    var value = arguments[2];
    serializer.integer32(buffer, offset, Math.floor(value / shift32));
    serializer.integer32(buffer, offset + 4, Math.floor(value));
    return value;
  }
  return serializer.integer32(buffer, offset) +
         serializer.integer32(buffer, offset + 4);
}

module.exports = o.objectify(integer32, integer64)();

/* vim: set ts=2 sw=2 et tw=0: */
