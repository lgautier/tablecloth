"""Defines the QuerySpace and related classes.

Sample usage:

    import tablecloth as tc 

    qs = tc.QuerySpace()
    qs['query1'] = tc.QueryTemplate('SELECT * FROM {my_table}')
    qs['query2'] = tc.QueryTemplate('SELECT * FROM {query1}')
    qs['query3'] = tc.QueryTemplate('SELECT * FROM {query2}')

    sql = qs.make(
        'query3', {'my_table': tc.TableName('source_table')}
    )
    
    # or
    
    sql = qs.make(
        'query3', my_table=tc.TableName('source_table')
    )
"""

import abc
import string
import uuid


class NameNotFoundError(Exception):
    """Exception when a requested TableName is not in the query space."""
    pass


class CyclicDependencyError(Exception):
    """Exception when a new QueryNode introduces a circular dependency."""
    pass


class AbstractQueryNode(metaclass=abc.ABCMeta):
    """A single node in a QuerySpace.

    A concrete child class is responsible for implementing how 
    to return dependency keys (strings identifying notes) as well as
    for interpolating values for these dependencies.
    """

    @property
    @abc.abstractmethod
    def dependencies(self):
        pass

    @abc.abstractmethod
    def render(self, **kwargs):
        pass


class TableName(AbstractQueryNode):
    """A table name.

    The name represents the name of the table as known by an SQL interpreter
    when the SQL is evaluated.
    
    This class is meant to indicate to a QuerySpace instance that a key
    (node in the subquery dependcy graph) does not have any parent in the DAG.
    """

    def __init__(self, name):
        self.sql = name

    @property
    def dependencies(self):
        return tuple()

    def render(self, **kwargs):
        # TODO: should a non-empty kwargs raise an exception ?
        return self.sql


class QueryTemplate(AbstractQueryNode):
    """An SQL query template.

    The string should be an SQL query template that would evaluate
    successfully (that is be a valid SQL query) when
    all placeholders in the template are substituted with values.
    
    This class is meant to indicate to a QuerySpace instance that a key
    (node in the subquery dependcy graph) is a query, with (potentially)
    parent nodes in the DAG.
    """

    _sql_formatter = string.Formatter()

    def __init__(self, template):
        self.sql = template

    @property
    def dependencies(self):
        return tuple(x[1] for x in self._sql_formatter.parse(self.sql))

    def render(self, **kwargs):
        # TODO: shouldn't the rendered string be wrapped in parenthesis ?
        return self._sql_formatter.format(self.sql, **kwargs)

                    
class QuerySpace(object):
    """A "namespace" of SQL queries."""

    def __init__(self, d={}):
        self._query_nodes = dict(d)

    def __getitem__(self, name):
        return self._query_nodes[name]
    
    def __setitem__(self, name, value):
        if name in value.dependencies:
            raise CyclicDependencyError(
                '{} would depend on itself'.format(name)
            )
        cycles = []
        for depname in value.dependencies:
            if self.has_dependency(depname, name):
                cycles.append(depname)
        if cycles:
            raise CyclicDependencyError(
                '{} has a cyclic dependency via ({})'.format(
                    name, ', '.join(cycles))
            )
        self._query_nodes[name] = new_node

    def __iter__(self):
        return self.keys()

    def dependencies(self, key):
        res = set()
        queue = set(key.dependencies)
        while queue:
            k = queue.pop()
            res.add(k)
            for x in self[k].dependencies:
                queue.add(x)
        return res
    
    def keys(self):
        return self._query_nodes.keys()

    def has_dependency(self, dependency_name, name):
        """Determine if dependency_name is a transitive dependency of name."""
        parents = set([name])
        while parents:
            cur_name = parents.pop()
            if cur_name == dependency_name:
                return True
            if cur_name in self._query_nodes:
                for next_name in self[cur_name].dependencies:
                    parents.add(next_name)
        return False

    def make(self, name, *args, **kwargs):
        assert name in self._query_nodes
        renders = dict()
        for name in self.iter_topological(keys=self.dependencies(name)):
             renders[name] = self[name].render(**renders)
        return renders[name]

    def iter_topological(self, keys=None):
        """Iterate over keys in a topological order.

        Iterating in a topological order means that each for each
        key returned we are garanteed that all eventual dependencies
        were returned in an earlier iteration.

        Args:
        - keys: an optional subset of keys.
        Returns:
        An iterator over keys.
        """
        if keys is None:
            keys = set(self.keys())
        # TODO: Naive implementation
        while keys:
            for k in keys:
                if not self[k].dependencies:
                    keys.remove(k)
                    yield k
