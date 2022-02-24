from heapq import merge


def mymerge(l, r):
    last = None
    for a in merge(l, r):
        if a != last:
            if last is not None:
                yield last
            last = a
        else:
            if last is not None:
                yield a + last
                last = None
    if last is not None:
        yield last


print(list(mymerge([1, 3, 5, 7, 11], [1, 3, 4, 5, 8, 11, 18])))
