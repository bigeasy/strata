function require() {
  fd = 3, fds = {};
  return {
    open: function (filename, mode, callback) {
      fds[fd] = filename;
      callback(null, fd++);
    }
  }
}

__filename = "visualization.html";
