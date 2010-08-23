var o = require('./objectify');

/**
    A branch or leaf tier. Creation is internal to the branch or leaf storage
    modules.

    storage => the storage manager for the tier.
    position => the file position of the tier.
    childrenAreLeaves => the type of child tiers for branch tiers.
 */
function tier(storage, position, childrenAreLeaves) {
  var size = 0,                                         // tier item count.
      buffer = new Buffer(storage.pageSize),            // tier buffer.
      scope = { childrenAreLeaves: childrenAreLeaves }; // tier properties.

  // Closure scope for methods specific to branch tiers.

  function branch() {
    /**
      tier =>
        Adds a branch value and and an associated tier to a branch tier.

        index => the index of the new branch.
        value => the pivot value of the new branch.
        tier => the referenced tier of the branch.
        callback => the callback to invoke when the branch has been added.
     */
    function add(index, value, tier, callback) {
      var offset = self.offset(index);
      options.io.write(self.buffer, offset, value, function () {
        serializer.integer64(buffer, offset + options.io.size, position);
        callback();
      });
    }

    return o.objectify(add)(o.accessors, scope)();
  }

  // Closure scope for methods specific to branch tiers.

  function leaf() {
    function add (position, record, callback) {
      var offset = self.offset(index);
      options.io.write(buffer, offset, record, callback);
    }

    return objectify(add)();
  }

  /**
    tier =>
      The the offset of the record at the given index.

      index => the index.
   */
  function offset (index) {
    return 8 + (index * storage.recordSize);
  }

  // Create a leaf or branch prototype and extend with common methods.

  return Object.create(storage.leaf ? leaf() : branch(), objectify(offset)());
}

// Closure scope for methods for the storage object.

function create(leaf) {
  var self, end = 0;
  var recordSize = leaf ? options.leafSize : options.innerSize + 8;
  var pageSize = 8 + recordSize * options.io.size;
  function allocate (childrenAreLeaves) {
    var position = end;
    end += pageSize;
    return tier(self, position, childrenAreLeaves);
  }
  return self = objectify(allocate)({ recordSize: recordSize, pageSize: pageSize })();
}

module.exports.instance = function () { return { leaf: create(true), branch: create(false) } };

/* vim: set ts=2 sw=2 et tw=0: */
