Copyright (c) 2015 Alex Kramer <kramer.alex.kramer@gmail.com>

See the LICENSE.txt file at the top-level directory of this distribution.

Description
-----------
SimpleDB is a minimal document-oriented database. It's intended for smaller
applications that would benefit from persistant data storage and / or
structured in-memory data management, but don't need an enterprise-level
solution.

Usage
-----
[TODO]

Usage Example
-------------
>>> import simple_db as sdb
>>> db = sdb.SimpleDB()
>>> db.add({"name": "Magnus Carlson", "elo": 2863, "country": "NOR"})
>>> db.add({"name": "Fabiano Caruana", "elo": 2839, "country": "ITA"})
>>> db.add({"name": "Veselin Topalov", "elo": 2800, "country": "BGR"})
>>> q1 = db.query("elo", db.gt, 2800)
>>> q2 = db.query("country", db.ne, "NOR")
>>> db.access(q1 & q2)
[{'country': 'ITA', 'name': 'Fabiano Caruana', 'elo': 2839}]

Documentation
-------------
[TODO]

Installation
------------
[TODO]

Bug reports / Planned features
------------------------------
[TODO]

Author(s)
---------
Alex Kramer <kramer.alex.kramer@gmail.com>
