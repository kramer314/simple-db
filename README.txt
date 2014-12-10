Copyright (c) 2014 Alex Kramer <kramer.alex.kramer@gmail.com>

See the LICENSE.txt file at the top-level directory of this distribution.

Description
-----------
SimpleDB is a minimal document-oriented database, designed for small- and
medium-scale applications that would benefit from persistent data storage
and / or structured in-memory data management, but do not require a highly
optimized or scalable library.

Downloading
-----------
[Eventually, on GitHub / PyPi]

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
Full documentation is available in the ./doc/ directory of this distribution.

Installation
------------
[TODO]

Bug reports / Planned features
------------------------------
[Eventually on Github or in TODO.txt file]

Changelog
---------
[See CHANGELOG.TXT]

Author(s)
---------
Alex Kramer <kramer.alex.kramer@gmail.com>
