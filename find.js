module.exports = function (comparator, page, key, low) {
    let mid, high = page.items.length - 1

    while (low <= high) {
        mid = low + ((high - low) >>> 1)
        const compare = comparator(key, page.items[mid].key)
        if (compare < 0) high = mid - 1
        else if (compare > 0) low = mid + 1
        else return mid
    }

    return ~low
}
