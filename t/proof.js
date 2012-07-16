var fs = require("fs")
  , path = require("path")
  , crypto = require("crypto")
  , Strata = require("../lib/enclosed")
  ;

function check (callback, forward) {
  return function (error, result) {
    if (error) callback(error);
    else forward(result);
  }
}

function objectify (directory, callback) {
  var files, segments = {}, count = 0;

  fs.readdir(directory, check(callback, list));

  function list ($1) {
    (files = $1).forEach(function (file) {
      if (!/^\./.test(file)) readFile(file);
      else read();
    });
  }

  function readFile (file) {
    segments[file] = [];

    fs.readFile(path.resolve(directory, file), "utf8", check(callback, lines));

    function lines (lines) {
      lines = lines.split(/\n/);
      lines.pop(); 
      lines.forEach(function (json) {
        json = json.replace(/[\da-f]+$/, "");
        segments[file].push(JSON.parse(json));
      });
      read();
    }
  }

  function read () {
    if (++count == files.length) callback(null, segments);
  }
}

function stringify (directory, callback) {
  objectify(directory, check(callback, segments));

  function segments (segments) {
    console.log(JSON.stringify(segments, null, 2));
    callback();
  }
}

function load (segments, callback) {
  fs.readFile(segments, "utf8", check(callback, parse));

  function parse (json) { callback(null, JSON.parse(json)) }
}

function gather (async, strata) {
  var records = [];
  async(function () {
    records = []
    strata.iterator("a", async());
  }, function (cursor) {
    return true;
  }, function page (more, cursor) {
    if (more) return 0;
    cursor.unlock();
    async(null, records);
  }, function item (i, cursor, page) {
    if (i < cursor.length) {
      cursor.get(i, async()); 
    } else {
      cursor.next(async(page));
    }
  }, function (record, i, item) {
    records.push(record);
    async(item)(null, i + 1);
  });
}

function serialize (segments, directory, callback) {
  if (typeof segments == "string") load(segments, check(callback, write));
  else write (segments);

  function write (segments) {
    var files = Object.keys(segments), count = 0;

    files.forEach(function (segment) {
      var records = [];
      segments[segment].forEach(function (line) {
        var record = [ JSON.stringify(line) ];
        record.push(crypto.createHash("sha1").update(record[0]).digest("hex"));
        records.push(record.join(" "));
      });
      records = records.join("\n") + "\n";
      fs.writeFile(path.resolve(directory, segment), records, "utf8", check(callback, written));
    });

    function written () { if (++count == files.length) callback(null) }
  }
}

function deltree (directory, callback) {
  var files, count = 0;

  readdir();

  function readdir () {
    fs.readdir(directory, extant);
  }

  function extant (error, $1) {
    if (error) {
      if (error.code != "ENOENT") callback(error);
      else callback();
    } else {
      list($1);
    }
  }

  function list ($1) {
    (files = $1).forEach(function (file) {
      stat(path.resolve(directory, file)); 
    });
    deleted();
  }

  function stat (file) {
    var stat;

    fs.stat(file, check(callback, inspect));

    function inspect ($1) {
      if ((stat = $1).isDirectory()) deltree(file, check(callback, unlink));
      else unlink();
    }

    function unlink () {
      if (stat.isDirectory()) fs.rmdir(file, check(callback, deleted));
      else fs.unlink(file, check(callback, deleted));
    }
  }

  function deleted () {
    if (++count > files.length) fs.rmdir(directory, callback);
  }
}

module.exports = function (dirname) {
  var tmp = dirname + "/tmp";
  return require("proof")(function cleanup (async) {
    deltree(tmp, async());
  }, function (async) {
    async(function () {
      fs.mkdir(tmp, 0755, async());
    }, function () {
      return { Strata: Strata
             , tmp: tmp
             , load: load
             , stringify: stringify
             , serialize: serialize
             , gather: gather
             , objectify: objectify
             };
    });
  });
};
