"""
Persistence of query results.

The persistence of subqueries can be specified, for example using
context managers, and these specification are used when rendering
the SQL for a particular query.

```
with alias('query2', querygraph) as qg:
    sql = qg.render('query3')
```

```
with view('query2', querygraph) as qg:
    # query2 as defined in the querygraph is first created as a view,
    # and rendering SQL for query3 will therefore assume
    # the existence of that view.
    sql = qg.render('query3')
```

```
with ctas('query2', querygraph) as qg:
    # query2 as defined in the querygraph is first created as an Athena
    # CTAS (https://docs.aws.amazon.com/athena/latest/ug/ctas.html). As
    # with a view, rendering SQL for query3 will therefore assume the
    # existence of that table. 
    sql = qg.render('query3')
```

Multiple persistence definitions can also be achieved:

```
with querycontext(querygraph,
                  query1=alias,
                  query2=view) as qg:
    sql = qg.render('query3')
```

"""

from abc import ABC
import contextlib
import uuid


class TableFromQuery(contextlib.AbstractContextManager):

    def __init__(self, name, querygraph):
        self.name = name
        self.queryquerygraph = querygraph
        self._realname = None

    @property
    def realname(self):
        if self._realname is None:
            self._realname = '%s_%s' % (self.name, uuid.uuid().hex)
        return self._realname


class alias(TableFromQuery):

    def __enter__(self):
        # The resulting context manager will essentially be a querygraph
        # that will render any query using the subquery of name "self.name"
        # using WITH statement for it.
        pass

    def __exit__(self, *exc):
        # A WITH statement is ephemeral (local to the query)
        # by definition. There is no cleanup required.
        pass


class view(TableFromQuery):

    def __enter__(self):
        # This should result in using querygraph to create the render
        # of the (sub)query under the name in self.name as a view of
        # name self.realname.
        # The resulting context manager will essentially be a querygraph
        # that will render any query using the subquery of name "self.name"
        # using that view.
        pass

    def __exit__(self, *exc):
        # The view created is deleted
        pass
    

class ctas(TableFromQuery):

    def __enter__(self):
        # This should result in using querygraph to create the render
        # of the (sub)query under the name in self.name as a file of
        # name self.realname.
        # The resulting context manager will essentially be a querygraph
        # that will render any query using the subquery of name "self.name"
        # using that file.
        pass

    def __exit__(self, *exc):
        # The file is deleted
        pass


@contextlib.contextmanager
def querycontext(querygraph, *args, **kwargs):
    if len(args) == 0:
        raise TypeError('Query graph argument missing.')
    elif len(args) == 1:
        contextspecs = dict()
    elif len(args) == 2:
        # TODO: check that args[1] can be made into a dict.
        contextspecs = dict(args[1])
    else:
        raise ValueError('Only two unnamed argument are allowed at most: '
                         'the query graph and a mapping of query names and '
                         'table-from-query context manager constructors.')
    contextspecs.update(kwargs)

    with contextlib.ExitStack() as stack:
        qg = querygraph
        for name, contextcls in contextspecs:
            qg = stack.enter_context(contextcls(name, qg))
        yield qg 
