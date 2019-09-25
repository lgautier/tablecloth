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
import re

# Finds refrence names wrapped in {{*}} in a query.
DEPENDENCY_REGEX = re.compile('{{(.*?)}}')


class NameNotFoundError(Exception):
    """Exception when a requested QueryNode is not in the query space."""
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
        """Is the query element a itself an SQL query."""
        pass

    @abc.abstractmethod
    def compile(self, querybuilder):
        """"Compile" the SQL query for that element."""
        pass


class CyclicDependencyError(Exception):
    """Exception when a new QueryElement introduces a circular dependency."""
    pass


class QueryBuilder(object):
    """Tracks the dependencies and with statements to build a single query.

    The lifetime of this object is from when QuerySpace.compile() is called
    to when the query is returned. It tracks the with statements for this
    single query and passes each QueryElement the key/value pairs it needs.

    When a QueryElement calls get_inline_name(), the QueryBuilder determines
    if this reference should come from the TableSource or from another
    QueryElement, makes sure there's a with statement as necessary, then
    returns the inline name.

    In cases where the reference is to another QueryElement, the QueryBuilder
    gets the with statement by calling compile() on this query node, passing
    itself as the QueryBuilder so that it can capture any with statements
    that this second QueryElement needs.
    """

    def __init__(self, table_map, space, template_parameters):
        self._table_map = table_map
        self._space = space
        self._template_parameters = template_parameters or {}
        self._with_list = []
        # Maps inline name to reference name.
        self._visited_nodes = {}

    @property
    def with_list(self):
        return self._with_list

    @property
    def template_parameters(self):
        return self._template_parameters

    def get_inline_name(self, reference_name, from_query):
        if reference_name in self._visited_nodes:
            return self._visited_nodes[reference_name]
        elif reference_name in self._table_map:
            inline = self._table_map[reference_name].compile(self)
            self._visited_nodes[reference_name] = inline
            return inline
        elif reference_name in self._space.available_nodes:
            self._visited_nodes[reference_name] = reference_name
            node = self._space.query_node(reference_name)
            with_text = node.compile(self)
            self._with_list.append((reference_name, with_text))
            return reference_name
        else:
            raise NameNotFoundError(
                'No such query or table {}, referenced in query {}.'.format(
                    reference_name, from_query))


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

    def __init__(self, name, query_text):
        self._name = name
        assert query_text.lstrip().upper().startswith('SELECT')
        self._query_text = query_text
        dependency_list_dups = re.findall(DEPENDENCY_REGEX, query_text)
        # Remove duplicates will preserving a canonical order.
        dependency_list = []
        for d in dependency_list_dups:
            if d not in dependency_list:
                dependency_list.append(d)
        self._dependencies = tuple(dependency_list)

    @property
    def dependencies(self):
        return self._dependencies

    def compile(self, query_builder):
        substitutions = {}
        for reference_name in self.dependencies:
            inline_name = query_builder.get_inline_name(
                reference_name, self._name)
            substitutions[reference_name] = inline_name
        return (self._query_text
                    .format(**query_builder.template_parameters)
                    .format(**substitutions))


class QuerySpace(object):
    """A compute space of subqueries."""

    def __init__(self):
        self._query_nodes = {}

    def __setitem__(self, reference_name, query_element):
        assert isinstance(query_element, QueryElement)
        for d in query_element.dependencies:
            if self.find_in_dependencies(d, reference_name):
                raise CyclicDependencyError(
                    '{} has a cyclic dependency via {}'.format(
                        reference_name, d))
        self._query_nodes[reference_name] = query_element

    @property
    def available_nodes(self):
        return set(self._query_nodes.keys())

    def query_node(self, reference_name):
        if reference_name not in self._query_nodes:
            raise NameNotFoundError(reference_name)
        return self._query_nodes[reference_name]

    def find_in_dependencies(self, node_name, reference_name):
        """Determine if reference_name is a transitive dependency."""
        queue = [node_name]
        while queue:
            next_node = queue.pop(0)
            if next_node == reference_name:
                return True
            if next_node in self.available_nodes:
                queue += self.query_node(next_node).dependencies
        return False

    def compile(self, target_name, table_map, template_parameters=None):
        for k, v in table_map.items():
            if not isinstance(v, TableName):
                raise TypeError(
                    '{k} is not an instance of type TableName.'.format(k=k)
                )

        query_builder = QueryBuilder(table_map, self, template_parameters)
        main_query = self.query_node(target_name).compile(query_builder)

        query = ''
        if query_builder.with_list:
            query = 'WITH {}\n\n'.format(',\n'.join(
                '{} AS (\n{})'.format(name, text)
                for name, text in query_builder.with_list))
        return query + main_query
