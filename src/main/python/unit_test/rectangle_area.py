def rectangle_area(l, w):
    if l <= 0 or w <= 0:
        raise("l or w can't be 0")
    if type(w) not in [int, float]:
        raise TypeError('pleae enter correct int or float value!')
    return l * w
