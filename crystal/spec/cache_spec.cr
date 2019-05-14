require "spec"

require "../src/cache"

describe Cache do
  describe "construct" do
    it "can be constructed" do
      cache = Cache(Int32).new
      cache.heft.should eq 0
    end
    it "can cache an object" do
      cache = Cache(Int32).new
      entry = cache.hold("1", 1)
      entry.value.should eq 1
      cache.entries.should eq 1
      entry.release()
    end
    it "can set heft" do
        cache = Cache(Int32).new
        cache.heft.should eq 0
        entry = cache.hold("1", 1)
        entry.value.should eq 1
        entry.heft.should eq 0
        entry.heft = 1
        entry.heft.should eq 1
        cache.heft.should eq 1
        entry.release()
    end
    it "can get a cached object" do
        cache = Cache(Int32).new
        cache.heft.should eq 0
        first = cache.hold("1", 1)
        first.value.should eq 1
        first.release()
        second = cache.hold("1", 2)
        second.value.should eq 1
        second.release()
    end
    it "can remove a cached object" do
        cache = Cache(Int32).new
        first = cache.hold("1", 1)
        first.value.should eq 1
        first.heft = 1
        cache.heft.should eq 1
        first.heft.should eq 1
        first.release()
        cache.heft.should eq 1
        second = cache.hold("1", 2)
        second.value.should eq 1
        second.remove()
        cache.heft.should eq 0
        cache.entries.should eq 0
        third = cache.hold("1", 2)
        third.value.should eq 2
        third.remove()
    end
    it "can purge objects" do
        cache = Cache(Int32).new
        first = cache.hold("1", 1)
        first.heft = 1
        second = cache.hold("2", 1)
        second.heft = 1
        second.release()
        third = cache.hold("3", 1)
        third.heft = 1
        third.release()
        cache.heft.should eq 3
        cache.purge(2)
        cache.heft.should eq 2
    end
  end
end
