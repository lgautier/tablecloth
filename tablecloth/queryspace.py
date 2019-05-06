"""Defines the QuerySpace and related classes.

Sample usage:

    import tablecloth as tc 

    qs = tc.QuerySpace()
    qs['query1'] = tc.QueryTemplate('SELECT * FROM {qs.my_table}')
    qs['query2'] = tc.QueryTemplate('SELECT * FROM {qs.query1}')
    qs['query3'] = tc.QueryTemplate('SELECT * FROM {qs.query2}')

    sql = qs.make(
        'query3', {'my_table': tc.TableName('source_table')}
    )
    
    # or
    
    sql = qs.make(
        'query3', my_table=tc.TableName('source_table')
    )
"""

import abc
import collections
import copy
import os
import string
import uuid


class NameNotFoundError(Exception):
    """Exception when a requested TableName is not in the query space."""
    pass


class CyclicDependencyError(Exception):
    """Exception when a new QueryNode introduces a circular dependency."""
    pass


class AbstractQueryElement(metaclass=abc.ABCMeta):
    """An element in a QuerySpace.

    Concrete child classes are responsible for implementing how 
    to return dependency keys (strings identifying other
    `AbstractQueryElements` in a `QuerySpace`) as well as
    for interpolating values for these dependencies keys when given.
    """

    @property
    @abc.abstractmethod
    def dependencies(self):
        """Sequence of dependency names for the element."""
        pass

    @property
    @abc.abstractmethod
    def isinline(self):
        """Should the element be rendered 'inline'.

        If false, the element will be rendered in a WITH statement,
        to the extent possible."""
        pass

    @abc.abstractmethod
    def render(self, **kwargs):
        """Render the query element when not inline."""
        pass

    @abc.abstractmethod
    def render_nested(self, **kwargs):
        """Render the query element when inline."""
        pass


class Placeholder(AbstractQueryElement):
    """A placeholder.

    Placeholders are typically added to a query space an AbstractQueryElement
    with dependencies not yet defined in the query space are added.

    A Placeholder does not have dependencies by definition, as it represent
    a query element not yet known to the QuerySpace.
    """

    def __init__(self):
        pass

    @property
    def dependencies(self):
        return tuple()

    @property
    def isinline(self):
        return None

    def render(self, **kwargs):
        raise Exception('Placeholders cannot be rendered.')

    def render_nested(self, **kwargs):
        raise Exception('Placeholders cannot be rendered.')


class TableName(AbstractQueryElement):
    """A table name.

    The name represents the name of the table as known by an SQL interpreter
    when the SQL is evaluated.
    
    This class is meant to indicate to a QuerySpace instance that a key
    (node in the subquery dependcy graph) does not have any parent in the DAG.
    """

    def __init__(self, name):
        self.sql = name

    @property
    def nest(self):
        return False

    @property
    def dependencies(self):
        return tuple()

    @property
    def isinline(self):
        return True

    def render(self, **kwargs):
        # TODO: should a non-empty kwargs raise an exception ?
        return self.sql

    render_nested = render


class QueryTemplateFormatter(string.Formatter):

    def get_value(self, key, args, kwds):
        if isinstance(key, str):
            return kwds.get(key, '{{{0}}}'.format(key))
        else:
            return string.Formatter.get_value(key, args, kwds)


class QueryTemplate(AbstractQueryElement):
    """An SQL query template.

    The string should be an SQL query template that would evaluate
    successfully (that is be a valid SQL query) when
    all placeholders in the template are substituted with values.
    
    This class is meant to indicate to a QuerySpace instance that a key
    (node in the subquery dependcy graph) is a query, with (potentially)
    parent nodes in the DAG.
    """

    _sql_formatter = QueryTemplateFormatter()

    def __init__(self, template, inline=False):
        self.sql = template
        self._inline = inline

    @property
    def dependencies(self):
        return tuple(x[1][3:]
                     for x in self._sql_formatter.parse(self.sql)
                     if x[1] is not None and x[1].startswith('qs.'))

    @property
    def isinline(self):
        return self._inline

    def render(self, **kwargs):
        namedargs = tuple(kwargs.items())
        qs = collections.namedtuple(
            'NamedArgs',
            tuple(x[0] for x in namedargs)
        )(
            *tuple(x[1] for x in namedargs)
        )
        return self._sql_formatter.format(self.sql, qs=qs)

    def render_nested(self, **kwargs):
        return '(%s)' % self.render(**kwargs)


# TODO: use an existing graph library or just an overkill ?
class DAG(object):

    def __init__(self):
        self._nodes = set()
        self._forward = collections.defaultdict(set)
        self._reverse = collections.defaultdict(set)

    def __contains__(self, key):
        return key in self._nodes

    def __len__(self):
        return len(self._nodes)

    def __repr__(self):
        return os.linesep.join(
            (super().__repr__(),
             '%i nodes' % len(self._nodes),
             '%i edges' % self.n_edges)
        )

    @property
    def n_edges(self):
        return sum(len(x) for x in self._forward.values())
    
    def remove_edge(self, key_a, key_b):
        self._reverse[key_b].remove(key_a)
        self._forward[key_a].remove(key_b)
        
    def add_edge(self, key_a, key_b):
        self._nodes.add(key_a)
        self._nodes.add(key_b)
        self._forward[key_a].add(key_b)
        self._reverse[key_b].add(key_a)

    def edges_to(self, key):
        return tuple(self._reverse[key])
    
    def edges_from(self, key):
        return tuple(self._forward[key])


class QuerySpace(object):
    """A "namespace" of SQL queries."""

    def __init__(self, d={}):
        self._query_nodes = dict()
        self._dag = DAG()
        for k, v in d.items():
            self[k] = v

    def __add__(self, other):
        assert isinstance(other, QuerySpace)
        res = type(self)()
        for k, v in self.items():
            res[k] = v
        for k, v in other.items():
            res[k] = v
        return res
        
    def __getitem__(self, name):
        return self._query_nodes[name]

    def __setitem__(self, name, value):
        assert isinstance(value, AbstractQueryElement)
        if name in self and not isinstance(self[name], Placeholder):
            raise NotImplementedError(
                'Replacing other elements than placeholders is not yet implemented.'
            )
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
        self._query_nodes[name] = value
        for d in value.dependencies:
            if d not in self._query_nodes:
                self._query_nodes[d] = Placeholder()
            self._dag.add_edge(d, name)

    def __iter__(self):
        return self.keys()

    def __contains__(self, key):
        return key in self._query_nodes

    def dependencies(self, key):
        res = set()
        queue = set(self[key].dependencies)
        while queue:
            k = queue.pop()
            res.add(k)
            for x in self[k].dependencies:
                queue.add(x)
        return res
    
    def keys(self):
        return self._query_nodes.keys()

    def items(self):
        return self._query_nodes.items()

    def has_dependency(self, name, dependency_name):
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
        """
        Make a query.

        Args:
        - name:  name (key) of the query to make
        - d [optional]: a dict of names and associated query elements
        - **kwargs: names and associated query elements
        Returns:
        An SQL query.
        """

        assert name in self._query_nodes
        new_nodes = {}
        if len(args) == 1:
            new_nodes.update(args[0])
        elif len(args) > 1:
            raise ValueError('At most two unnamed parameters can be specified.')
        new_nodes.update(kwargs)

        notplaceholders = []
        for k in new_nodes.keys():
            if not isinstance(self[k], Placeholder):
                notplaceholders.append(k)
        if notplaceholders:
            raise ValueError('Cannot assign values outside of placeholders (%s). '
                             'Consider building a derivative graph.' %
                             ', '.join(notplaceholders))

        renders = dict()
        render_defs = []
        n_notinline = 0
        for cur_name in self.iter_topological(keys=self.dependencies(name)):
            if cur_name in new_nodes:
                assert isinstance(self[cur_name], Placeholder)
                cur_node = new_nodes[cur_name]
            else:
                cur_node = self[cur_name]
            if cur_node.isinline:
                renders[cur_name] = cur_node.render_nested(**renders)
            else:
                n_notinline += 1
                renders[cur_name] = cur_name
                if isinstance(cur_node, Placeholder):
                    raise NameNotFoundError(cur_name)
                render_defs.append(
                    (cur_name,
                     cur_node.render(**renders)))
        if len(render_defs) == 0:
            return renders[name]
        elif (render_defs == 1 or n_notinline <= 1):
            return render_defs[0][1]
        else:
            return os.linesep.join(
                ('WITH',
                 ', '.join('%s AS (%s)' % (k, v) for k, v in render_defs[:-1]),
                 render_defs[-1][1])
            )

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
        # Kahn's algorithm
        dag = copy.deepcopy(self._dag)
        sorted_keys = list()
        without_dep = set()

        for k in keys:
            deps = self[k].dependencies
            if not deps:
                without_dep.add(k)
        while len(without_dep) > 0:
            n = without_dep.pop()
            sorted_keys.append(n)
            for m in dag.edges_from(n):
                dag.remove_edge(n, m)
                if not dag.edges_to(m):
                    without_dep.add(m)
        if dag.n_edges:
            raise CyclicDependencyError()
        else:
            return tuple(sorted_keys)
