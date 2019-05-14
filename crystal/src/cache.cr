# A LRU cache used to store b-tree pages. The cache will hold pages in memory
# in a `Hash` of cache entries that also form a doubly-linked list. When you
# access an entry using `Cache#hold`.
class Cache(T)
  class Entry(T)
    # Previous entry according to the least-recently used linked-list.
    protected property previous : Entry(T)?

    # Next entry according to the least-recently used linked-list.
    protected property next : Entry(T)?

    # Count of referneces to this cache entry.
    protected property references : Int32

    # Cache hash key.
    protected property stringified : String

    # The user-assigned cached value.
    property value : T?

    protected def initialize(cache : Cache(T), stringified : String, value : T?)
      @cache = cache
      @stringified = stringified
      @value = value
      @when = Time.now
      @value = value
      @references = 1
      @heft = 0
    end

    # Coalesce our previous reference to non-`nil`. Our doubly-linked list is
    # circular, so previous will never be `nil`, but the compiler requires that
    # we define our member as `Entry(T)?` because we can't set `@previous` in
    # our constructor. We could, but we get a chicken/egg problem with the head
    # node, so we set our `@next` and `@previous` after initialization. This
    # accessor keeps us from having to say nonsens like:
    #
    # ```crystal
    # entry.try &.next.try &.previous = self
    # ```
    protected def previous : Entry(T)
      @previous.not_nil!
    end

    # Coalesce our next reference to non-`nil`. See `previous`.
    protected def next : Entry(T)
      @next.not_nil!
    end

    protected def link(entry : Entry(T))
      self.next = entry.next
      self.previous = entry
      self.previous.next = self
      self.next.previous = self
    end

    protected def unlink() : Void
      self.next.previous = self.previous
      self.previous.next = self.next
    end

    # Assign an arbitrary measure of the weight of the entry. Rather than using
    # `sizeof` or `instance_sizeof` to determine an entry's size, we use the
    # size of the record as it is serialized to disk to represent the entry size
    # and we call it our entry's "heft" so we're reminded that it is a relative
    # size not an accurate one.
    #
    # Setting the heft of an entry adjusts the heft of the cache. We use
    # `Cache#purge(heft)` to attempt to reduce the total heft of the cache to a
    # desired heft.
    def heft=(heft : Int32)
      @cache.heft -= @heft
      @heft = heft
      @cache.heft += @heft
    end

    # Get the assigned heft. See `heft=`.
    def heft
      @heft
    end

    # Release the reference to the cache entry. Releasing the refernece reduces
    # the reference count by one. You should only call it once for each entry
    # and you should always call it when you are done with the entry. When the
    # reference count is zero, the entry is elegiable for removal during a
    # `Cache#purge`.
    def release() : Void
      @references -= 1
    end

    # Remove the entry from the cache. Caller's must be the only oustanding
    # reference to the entry, otherwise an exception is raised.
    def remove() : Void
      raise Exception.new("outstanding references") if @references != 1
      @cache.remove(self)
    end
  end

  getter heft : Int32
  protected setter heft : Int32

  getter entries : Int32

  @head : Entry(T)?

  def initialize()
    @map = Hash(String, Entry(T)).new
    @entries = 0
    @heft = 0
    head = Entry(T).new(self, "", nil)
    head.next = head
    head.previous = head
    @head = head
  end

  private def head : Entry(T)
    @head.not_nil!
  end

  def hold (key, initializer) : Entry(T)
    if ! @map.has_key?(key)
      @map[key] = entry = Entry.new(self, key, initializer)
      @entries += 1
      entry.link(head)
      return entry
    end

    entry = @map[key]

    entry.unlink()
    entry.link(head)

    entry.references += 1

    entry
  end

  protected def remove (entry : Entry(T)) : Void
    @heft -= entry.heft
    entry.unlink()
    @map.delete(entry.stringified)
    @entries -= 1
  end

  def purge(heft : Int32) : Void
    iterator = self.head.previous
    while (self.heft > heft && iterator != self.head)
      if (iterator.references == 0)
        remove(iterator)
      end
      iterator = iterator.previous
    end
  end
end
