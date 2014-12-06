"""
Copyright (c) 2014 Alex Kramer <kramer.alex.kramer@gmail.com>

See the LICENSE.txt file at the top-level directory of this distribution.
"""

import re


def eq(_val, val):
    """
    Equality test
    """
    return _val == val


def ge(_val, val):
    """
    Greater than or Equal test
    """
    return _val >= val


def gt(_val, val):
    """
    Greater than test
    """
    return _val > val


def le(_val, val):
    """
    Less than or Equal test
    """
    return _val <= val


def lt(_val, val):
    """
    Less than test
    """
    return _val < val


def ne(_val, val):
    """
    Not equal test
    """
    return _val != val


def rx(_val, val):
    """
    Case-insensitive regex matching
    """
    return re.match(val, _val, re.I) is not None
