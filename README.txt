Introduction
============

This package implements a simple SQLAlchemy blueprint for
`collective.transmogrifier`_. 

If you are not familiar with transmogrifier please read its documentation
first to get a basic understanding of how you can use this package.

This package implements the ''transmogrify.sqlalchemy'' blueprint which
executes a SQL statement, generally a query, and feeds the return values
from that query into the transmogrifier pipeline.

Configuration
=============

A transmogrify.sqlalchemy blueprint only takes two parameters:

dsn
   Connection information for the SQL database. The exact format is documented
   in the SQLAlchemy documentation for `create_engine() arguments`_.
  
query
   The SQL query that will be executed. 

Example
=======

This will feed all data from the menu table in a local postgres
database into the pipeline::

    [sqlite]
    blueprint=transmogrify.sqlalchemy
    dsn=postgres://scott:tiger@localhost:5432/mydatabase
    query=SELECT * FROM menu

.. _create_engine() arguments: http://www.sqlalchemy.org/docs/04/dbengine.html#dbengine_establishing
.. _collective.transmogrifier: https://pypi.python.org/pypi/collective.transmogrifier


