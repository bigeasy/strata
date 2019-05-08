var sequester = require('sequester')

function Page (sheaf, address, modulus) {
    if (address == null) {
        while (sheaf.nextAddress % 2 !== modulus) sheaf.nextAddress++
        address = sheaf.nextAddress++
    }
    this.address = address
    this.entries = 0
    this.rotation = 0
    this.items = []
    this.queue = sequester.createQueue()
    this.cartridge = null
    if (modulus === 1) {
        this.right = { address: null, key: null }
        this.ghosts = 0
    }
}

Page.prototype.splice = function (offset, length, insert) {
    var items = this.items, cartridge = this.cartridge, heft = 0, removals

    if (length) {
        removals = items.splice(offset, length)
        for (var i = 0, I = removals.length; i < I; i++) {
            heft -= removals[i].heft
        }
    } else {
        removals = []
    }

    if (insert != null) {
        if (! Array.isArray(insert)) insert = [ insert ]
        for (var i = 0, I = insert.length; i < I; i++) {
            heft += insert[i].heft
        }
        items.splice.apply(items, [ offset, 0 ].concat(insert))
    }

    cartridge.adjustHeft(heft)

    return removals
}

module.exports = Page
