"""Defines the QuerySpace and related classes.

Sample usage:

    space = QuerySpace()
    space['query1'] = 'SELECT * FROM {{my_table}}'
    space['query2'] = 'SELECT * FROM {{query1}}'
    space['query3'] = 'SELECT * FROM {{query2}}'

    query = space.compile(
        'query3', {'my_table': 'source_table'})
"""

import abc
import collections
import os
import re
import string
from . import graph


class NameNotFoundError(Exception):
    """Exception when a requested QueryNode is not in the query space."""
    pass


class UnspecifiedPlaceholder(Exception):
    """Exception when building an SQL query cannot be completed because of remaining placeholder."""
    pass


class QueryElement(abc.ABC):
    """This abstract class acts as an interface for query elements."""

    @property
    @abc.abstractmethod
    def dependencies(self):
        """Tuple of direct dependency names for the element."""
        pass

    @property
    @abc.abstractmethod
    def issubquery(self):
        """Is the query element a itself an SQL query.

        This is used to decide whether to place the element in a WITH statement."""
        pass

    @abc.abstractmethod
    def render(self, **kwargs):
        """"Render the SQL for that element."""
        pass


class Placeholder(QueryElement):
    """A placeholder.

    Placeholders are typically added to a query space an AbstractQueryElement
    with dependencies not yet defined in the query space are added.
    A Placeholder does not have dependencies by definition, as it represent
    a query element not yet known to the QuerySpace.
    """

    issubquery = None

    def __init__(self):
        pass

    @property
    def dependencies(self):
        # TODO: May be this should raise an exception instead?
        return None

    def render(self, **kwargs):
        raise Exception('Placeholders cannot be rendered.')


class TableName(QueryElement):
    """A table name (in a Queryspace).

    The name represents the name of the table as known by an SQL interpreter
    when the SQL is evaluated.
    
    This class is meant to indicate to a QuerySpace instance that a key
    (node in the subquery dependcy graph) is simply a table name.
    """

    issubquery = False

    def __init__(self, name):
        self.name = name

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, value):
        # TODO: check that value is a syntactically valid table name.
        #    This will facilitate the parametrization of query of the table
        #    name while preventing SQL injection.
        self.__name = value

    @property
    def dependencies(self):
        # A table always return a empty sequence.
        return tuple()

    def render(self, **kwargs):
        return self.name


class QueryTemplateFormatter(string.Formatter):

    def get_value(self, key, args, kwds):
        if isinstance(key, str):
            return kwds.get(key, '{{{0}}}'.format(key))
        else:
            return string.Formatter.get_value(key, args, kwds)


class TableName(QueryElement):
    """A table name (in a Queryspace).

    The name represents the name of the table as known by an SQL interpreter
    when the SQL is evaluated.
    
    This class is meant to indicate to a QuerySpace instance that a key
    (node in the subquery dependcy graph) is simply a table name.
    """

    issubquery = False

    def __init__(self, name):
        self.name = name

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, value):
        # TODO: check that value is a syntactically valid table name.
        #    This will facilitate the parametrization of query of the table
        #    name while preventing SQL injection.
        self.__name = value

    @property
    def dependencies(self):
        # A table always return a empty sequence.
        return tuple()

    def compile(self, querybuilder):
        return self.name


class QueryTemplate(QueryElement):
    """A query template (in a Queryspace).

    The node is responsible for identifying its dependencies and
    substituting inline names (either source tables or query nodes)
    and template values for placeholders when the compile() function
    is called.

    The node gets inline names from a QueryBuilder and relies on the
    QueryBuilder to choose between source tables or other query nodes
    for its dependencies and to track the necessary with statements.
    """

    issubquery = True
    _sql_formatter = QueryTemplateFormatter()

    def __init__(self, template):
        self._template = template
        self._dependencies = self._extract_dependencies()

    def _extract_dependencies(self):
         return tuple(
             x[1][3:]
             for x in self._sql_formatter.parse(self._template)
             if x[1] is not None and x[1].startswith('qs.'))

    @property
    def dependencies(self):
        return self._dependencies

    def render(self, **kwargs):
        namedargs = tuple(kwargs.items())
        qs = collections.namedtuple(
            'NamedArgs',
            tuple(x[0] for x in namedargs)
        )(
            *tuple(x[1] for x in namedargs)
        )
        
        return self._sql_formatter.format(self._template, qs=qs)


class QuerySpace(object):
    """A "namespace" of SQL queries.

    A namespace of SQL queries can be thought of as a convenience wrapper
    over a dependency graph of names/keys/labels and the QueryElement objects
    associated with those names/keys/labels.

    For example:
    >>> qs = QuerySpace()
    >>> sql = 'SELECT * FROM {vehicles} WHERE wheels=4'
    >>> qs['cars'] = QueryTemplate(sql)


    The role of a QuerySpace is to keep track of dependencies between all
    QueryElement objects it knows about using the name/key/label they are
    registered under, and the name of dependencies reported by the
    QueryElement itself. Dependencies the QuerySpace does not know about yet
    will be defined as Placeholder instances in the dependency graph.
    """

    def __init__(self, d={}):
        self._query_nodes = {}
        self._dag = graph.DAG()
        for k, v in d.items():
            self[k] = v

    def __getitem__(self, key):
        if key not in self._query_nodes:
            raise NameNotFoundError(key)
        return self._query_nodes[key]

    def _setitem_replace(self, key, value):
        if not isinstance(self[key], Placeholder):
            # Allowing the replacement of non-placeholder elements is not
            # always going to be desirable. An example of use-case would be to have
            # "parametric" namespaces in which only placeholders can be replaced.
            raise ValueError(
                'Only Placeholder elements can be replaced.'
            )
        self._query_nodes[key] = value

    def _setitem_missing_dependency(self, key):
        """What happens when a QueryElement lists an unknown dependency.

        This method allows an easy customization of the behavior when an added
        QueryElement lists a dependency not yet known to the QuerySpace."""
        self._query_nodes[key] = Placeholder()

    def __setitem__(self, name, value):
        if not isinstance(value, QueryElement):
            raise TypeError('Value must be a QueryElement.')
        
        if value.dependencies:
            if name in value.dependencies:
                raise graph.CyclicDependencyError(
                    '{} would depend on itself'.format(name)
                )

        if name in self:
            self._setitem_replace(name, value)
        else:
            self._query_nodes[name] = value

        if value.dependencies:
            cycles = []
            for d in value.dependencies:
                if d not in self._query_nodes:
                    self._setitem_missing_dependency(d)
                try:
                    self._dag.add_edge(d, name)
                except graph.CyclicDependencyError:
                    cycles.append(d)
            if cycles:
                raise graph.CyclicDependencyError(
                    '{} has a cyclic dependency via ({})'.format(
                        name, ', '.join(cycles))
                )


    def __iter__(self):
        return self.keys()

    def __contains__(self, key):
        return key in self._query_nodes

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

    def _make_check(self, key):
        """Make any relevant check for a given key when building an SQL query.

        This method allows an easy customization of the behavior, for example
        if the partial rendering of queries, that if the rendering of SQL queries
        that are templates, is wanted."""
        if isinstance(self[key], Placeholder):
            raise UnspecifiedPlaceholder(key)

    def make(self, name, *args, **kwargs):
        """
        Make a query.

        >>> qs = QuerySpace()
        >>> sql = 'SELECT * FROM {vehicles} WHERE wheels=4'
        >>> qs['cars'] = QueryTemplate(sql)
        >>> sql = qs.make('cars')  # UnspecifiedPlaceholder
        >>> qs['vehicles'] = TableName('vehicles_2019')
        >>> sql = qs.make('cars')

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

        qspace = type(self)(d = {name: self[name]})
        queue = set(d for d in qspace[name].dependencies if not isinstance(d, Placeholder))
        while queue:
            k = queue.pop()
            n = new_nodes.get(k, self[k])
            if k not in qspace or isinstance(qspace[k], Placeholder):
                qspace[k] = n
            for d in qspace._dag.nodes_to(k):
                queue.add(d)
        renders = dict()
        render_defs = []
        n_notinline = 0
        for cur_key in qspace._dag.keys_topological():
            qspace._make_check(cur_key)
            cur_node= qspace[cur_key]
            if cur_node.issubquery:
                # QueryTemplates that are subqueries will be rendered
                # using a WITH statement.
                n_notinline += 1
                renders[cur_key] = cur_key
                render_defs.append(
                    (cur_key,
                     cur_node.render(**renders)))
            else:
                renders[cur_key] = cur_node.render(**renders)
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

class ModifiableQuerySpace(QuerySpace):

    def _setitem_replace(self, key, value):
        if self._query_nodes[key].dependencies:
            for d in self._query_nodes[key].dependencies:
                self._dag.remove_edge(d, key)
        self._query_nodes[key] = value
