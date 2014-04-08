import uuid, pickle, collections

class DB(object):
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

    def __len__(self):
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

        for _id in id_iter:
            el = self.el_db.get(_id).copy()
            if el:
                res.append(el)

        return res

    def add(self, el, exclude=None):
        """
        Add a document to the database

        exclude ::: list of excluded (non-indexed / non-searchable) properties

        returns ::: internal string ID of the added element

        This method raises a TypeError if the document cannot be added, which
        means that at least one indexed / searchable property has an unhashable
        key or value.
        """

        if exclude is None:
            exclude = []

        # Check that we can index non-excluded items; raise a TypeError if not
        valid = True
        for prop, val in el.items():
            if prop in exclude:
                continue

            valid = isinstance(prop, collections.Hashable) and \
                isinstance(val, collections.Hashable)
            
            if not valid:
                raise TypeError("Indexed properties of %s are not hashable."
                                %(repr(el)))

        _id = str(uuid.uuid4())
        self.el_db[_id] = el
        
        for prop, val in el.items():

            if prop in exclude:
                continue

            if prop not in self.prop_db:
                self.prop_db[prop] = {}

            prop_db = self.prop_db[prop]

            if val in prop_db:
                prop_db[val].add(_id)
            else:
                prop_db[val] = set([_id])

        return _id

    def query(self, prop, test, val, access = False):
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
        * Returns True if the comparison succeeds (a search result was found) or
          False if the comparison fails (a result was not found).

        Note that since full documents are not hashable types, query calls with
        `access = True` can not be chained together using logical operations of
        sets (union, intersection, etc.). For chained queries, `DB.access()` can
        be called with the final (combined) result to obtain located documents.
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

    def remove(self, _id):
        """
        Remove a document from the database.

        _id ::: id of the element to remove

        This method raises a LookupError if the ID `_id` is not in the database.
        """
        if _id in self.el_db:
            for prop, val in self.el_db[_id].items():

                prop_db = self.prop_db[prop]
                
                prop_db[val].remove(_id)
                if len(prop_db[val]) == 0:
                    del prop_db[val]
                if len(prop_db) == 0:
                    del slef.prop_db[prop]

            del self.el_db[_id]
        else:
            raise LookupError("ID %s not found."%(_id))

    def reset(self):
        """
        Reset the database, clearing all stored documents and search indexes.
        """
        self.el_db = {}
        self.prop_db = {}

    def save(self, fname):
        """
        Saves the database to disk via pickling.

        fname ::: location of output file
        """
        out = {}
        out["el_db"] = self.el_db
        out["prop_db"] = self.prop_db
        try:
            pickle.dump(out, open(fname, "wb"))
        except:
            raise IOError("Database save failed")

    def open(self, fname):
        """
        Loads the database from a pickled file on disk 

        fname ::: location of pickled file
        """
        try:
            _in = pickle.load(open(fname, "rb"))
            self.el_db = _in["el_db"]
            self.prop_db = _in["prop_db"]
        except:
            raise IOError("Database open failed")

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
