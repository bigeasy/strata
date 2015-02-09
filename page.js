function Page (sheaf, address, modulus) {
    if (address == null) {
        while (sheaf.nextAddress % 2 !== modulus) sheaf.nextAddress++
        address = sheaf.nextAddress++
    }
    this.address = address
    this.entries = 0
    this.rotation = 0
    this.items = []
    this.queue = sheaf.sequester.createQueue()
    this.cartridge = null
    if (modulus === 1) {
        this.right = { address: 0, key: null }
        this.ghosts = 0
    }
}

Page.prototype.splice = function (offset, length, insert) {
    var items = this.items, cartridge = this.cartridge, heft, removals

    if (length) {
        removals = items.splice(offset, length)
        heft = removals.reduce(function (heft, item) { return heft + item.heft }, 0)
        cartridge.adjustHeft(-heft)
    } else {
        removals = []
    }

    if (insert != null) {
        if (! Array.isArray(insert)) insert = [ insert ]
        heft = insert.reduce(function (heft, item) { return heft + item.heft }, 0)
        cartridge.adjustHeft(heft)
        items.splice.apply(items, [ offset, 0 ].concat(insert))
    }
    return removals
}

module.exports = Page
