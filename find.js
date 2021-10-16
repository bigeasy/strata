module.exports = function (comparator, array, key, low, length) {
    let mid, high = array.length - 1

    while (low <= high) {
        mid = low + ((high - low) >>> 1)
        const compare = comparator(key, array[mid].key.slice(0, length))
        if (compare < 0) high = mid - 1
        else if (compare > 0) low = mid + 1
        else return mid
    }

    return ~low
}
