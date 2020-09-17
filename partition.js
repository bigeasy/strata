function seek (comparator, items, direction, index, stop) {
    while (index != stop) {
        const compare = comparator(items[index].key, items[index + direction].key)
        if (compare != 0) {
            return index
        }
        index += direction
    }
    return null
}

module.exports = function (comparator, items) {
    const mid = Math.floor(items.length / 2)
    const backward = seek(comparator, items, -1, mid, 0)
    const forward = seek(comparator, items, 1, mid, items.length - 1)
    if (backward == null) {
        if (forward == null) {
            return null
        }
        return forward + 1
    } else if (forward == null || mid - backward < forward - mid + 1) {
        return backward
    } else {
        return forward + 1
    }
}
