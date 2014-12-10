"""
Copyright (c) 2014 Alex Kramer <kramer.alex.kramer@gmail.com>

See the LICENSE.txt file at the top-level directory of this distribution.
"""

import uuid
import pickle
import collections

import db_queries


class SimpleDB(object):
    """
    Base document-based database class

    A document is a simply a collection of key-value pairs.

    Fully indexable (searchable) documents are composed entirely of immutable
    keys and values. More complex documents (such as nested dictionaries,
    dictionaries containing lists, etc.) can be stored as long as the mutable
    elements are excluded from indexing (not searchable). All documents are
    assumed to be fully pickle-able.
    """
    def __init__(self):
        """
        Class variables:

        DB.el_db ::: map of ID : document
        DB.prop_db ::: map of property : value : ID
        """
        self.el_db = {}
        self.prop_db = {}
        self.fname = None

        self.eq = db_queries.eq
        self.ne = db_queries.ne
        self.ge = db_queries.ge
        self.gt = db_queries.gt
        self.le = db_queries.le
        self.lt = db_queries.lt
        self.rx = db_queries.rx

    def size(self):
        """
        Size of the database (total number of documents)
        """
        return len(self.el_db.keys())

    def access(self, id_iter):
        """
        Access documents using their internal ID

        id_iter ::: iterable container of string IDs

        returns ::: list of documents parallel to `id_iter`
        """
        res = []

        for id_ in id_iter:
            el = self.el_db.get(id_).copy()
            if el:
                res.append(el)

        return res

    def add(self, el, exclude=None, id_=None):
        """
        Add a document to the database

        el ::: hashable dictionary
        exclude ::: list of excluded (non-indexed / non-searchable) properties
        id_ ::: specify internal ID (or generate new ID, default)

        returns ::: internal string ID of the added element

        This method raises a TypeError if the document cannot be added, which
        means that at least one indexed / searchable property has an unhashable
        key or value.
        """

        if exclude is None:
            exclude = []

        valid = True
        try:
            for prop, val in el.items():
                valid = isinstance(prop, collections.Hashable) and \
                    isinstance(val, collections.Hashable)

            if not valid:
                raise TypeError("Indexed document properties are not" +
                                "hashable")
        except AttributeError:
            raise AttributeError("Improper document format.")

        if id_ is None:
            id_ = str(uuid.uuid4())

        self.el_db[id_] = el

        for prop, val in el.items():
            ex = (prop in exclude)

            self.set_prop(id_, prop, val, exclude=ex)

        return id_

    def query(self, prop, test, val, access=False):
        """
        Find all documents `el` such that `test(el[prop], val) == True`

        prop ::: name of property
        val ::: value to test against
        test ::: comparison function
        access ::: Boolean flag, determines whether to return IDs or documents

        returns ::: set() of result IDs or list of documents

        Custom test / comparison functions can be provided with the following
        format:
        * Accepts two arguments `_val` and `val` where `val` is the value to
          test against, and `_val` is a document's value for the specified
          property. The function call is `func(_val, val)`
        * Returns True if the comparison succeeds (a search result was found)
          or False if the comparison fails (a result was not found).

        Note that since full documents are not hashable types, query calls with
        `access = True` can not be chained together using logical operations of
        sets (union, intersection, etc.). For chained queries, `DB.access()`
        can be called with the final (combined) result to obtain located
        documents.
        """
        matches = set()

        if prop in self.prop_db:
            prop_db = self.prop_db[prop]

            for _val in prop_db.keys():
                if test(_val, val):
                    matches.update(prop_db[_val])

        res = matches
        if access:
            res = self.access(matches)
        return res

    def query_prop(self, prop, access=False):
        """
        Find all documents with a given property.

        prop ::: name of property
        access ::: Boolean flag, determines whether to return IDs or documents

        returns ::: set() of result IDs or list of documents

        See the note in `DB.query()` regarding chained queries.
        """
        matches = set()

        if prop in self.prop_db:
            matches.update(self.prop_db[prop].values())

        res = matches
        if access:
            res = self.access(matches)
        return res

    def remove(self, id_):
        """
        Remove a document from the database.

        id_ ::: id of the element to remove

        This method raises a LookupError if the ID `id_` is not in the
        database.
        """
        if id_ in self.el_db:
            for prop in self.el_db[id_].items():

                self.del_prop(id_, prop)

            del self.el_db[id_]
        else:
            raise LookupError("Document ID not found.")

    def replace(self, id_, el, exclude=None):
        """
        Replace a document in the database.

        id_ ::: id of the element to replace
        el ::: replacement document
        """
        self.remove(id_)
        self.add(el, exclude=exclude, id_=id_)

    def set_prop(self, id_, prop, val, exclude=False):
        """
        Create or modify a property of a document

        id_ ::: ID of document to change
        prop ::: name of property
        val ::: value of property
        exclude ::: exclude value from prop_db
        """

        if id_ not in self.el_db:
            raise LookupError("Document ID not found.")

        valid = isinstance(prop, collections.Hashable) and \
            isinstance(val, collections.Hashable)
        if not valid:
            raise TypeError("Document property is not hashable.")

        self.el_db[id_][prop] = val

        if not exclude:
            if prop not in self.prop_db:
                self.prop_db[prop] = {}

            if val not in self.prop_db[prop]:
                self.prop_db[prop][val] = set()

            self.prop_db[prop][val].add(id_)

    def remove_prop(self, id_, prop):
        """
        Remove a property of a document

        id_ ::: document id
        prop ::: property to delete
        """
        if id_ not in self.el_db:
            raise LookupError("Document ID not found.")

        if prop not in self.el_db[id_]:
            raise KeyError("Document property not present.")

        del self.el_db[id_][prop]

        if id_ in self.query_prop(prop):
            val = self.el_db[id_][prop]

            prop_dict = self.prop_db[prop]
            prop_dict[val].discard(id_)
            if len(prop_dict[val]) is 0:
                del prop_dict[val]

            if len(prop_dict) is 0:
                del self.prop_db[prop]

    def reset(self):
        """
        Reset the database, clearing all stored documents and search indexes.
        """
        self.el_db = {}
        self.prop_db = {}

    def save(self, *args):
        """
        Saves the database to disk via pickling.

        fname ::: location of output file
        """

        fname = args.get(1)
        if fname is None:
            if self.fname:
                fname = self.fname
            else:
                raise ValueError("No save file specified.")

        out = {}
        out["el_db"] = self.el_db
        out["prop_db"] = self.prop_db
        try:
            pickle.dump(out, open(fname, "wb"))
        except:
            raise IOError("Database save failed.")

    def load(self, fname):
        """
        Loads the database from a pickled file on disk

        fname ::: location of pickled file

        If DB was loaded from file, fname defaults to that file
        """
        try:
            _in = pickle.load(open(fname, "rb"))
            self.el_db = _in["el_db"]
            self.prop_db = _in["prop_db"]
            self.fname = fname
        except IOError:
            raise IOError("Database open failed.")
        except KeyError:
            raise KeyError("Incorrect database format.")
