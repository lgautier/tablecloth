# ![GitHub Logo](logo.png) Tablecloth

![Travis build status](https://travis-ci.com/lgautier/tablecloth.svg?token=xf7sgesS9RQ1p1iLDXo9&branch=master)
![Github workflow status](https://github.com/rpy2/rpy2/workflows/Python%20package/badge.svg)

---

Tablecloth is proposing to help with empirical and often interactive work with remote data tables in Python.
This can also be described as a Python package to help data scientists working with data accessible through
some dialect of SQL.

## Why do this ?

While there exists a selection of ORM and SQL code generators for Python (e.g., SQLAlchemy[1], PonyORM[2]m or Peewee[3]),
this project explores the use of SQL with Python from a rather different angle. Instead of starting from Python and
have a system that generates SQL from Python structures, expressions and library calls, we are starting
from SQL and are using Python to manage dependencies and eventual bookkeeping related to query evaluation.
Something like an hybrid of SQL pre-processor or a build engine.

## References and relevant links

1. https://www.sqlalchemy.org/
2. https://ponyorm.org/
3. http://docs.peewee-orm.com/en/latest/
4. https://github.com/hashedin/jinjasql
5. https://teradata.github.io/jupyterextensions/
6. https://aws.amazon.com/athena/
7. https://cloud.google.com/bigquery/
