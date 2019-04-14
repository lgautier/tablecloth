"""Defines the QueryGraph and related classes.

Sample usage:

    graph = QueryGraph()
    graph['query1'] = 'SELECT * FROM {{my_table}}'
    graph['query2'] = 'SELECT * FROM {{query1}}'
    graph['query3'] = 'SELECT * FROM {{query2}}'

    query = graph.compile(
        'query3', {'my_table': 'source_table'})
"""

import re

# Finds refrence names wrapped in {{*}} in a query.
DEPENDENCY_REGEX = re.compile('{{(.*?)}}')


class NameNotFoundError(Exception):
    """Exception when a requested QueryNode is not in the query graph."""
    pass


class CyclicDependencyError(Exception):
    """Exception when a new QueryNode introduces a circular dependency."""
    pass


class TableSource(object):
    """A collection of source tables which may require WITH statements.

    For each source table, the class defines how the query should
    refer to the table (inline_name) and an optional query to
    include in a with statement (with_text).

    The caller has its own name for each table (reference_name) when
    calling the lookup functions.

    This default implementation takas a dictionary that maps reference_name
    to inline_name, and does not allow with statements. Subclasses should
    override the with_text() function to include with statements.
    """

    def __init__(self, table_dict=None):
        self._table_dict = table_dict or {}

    @property
    def available_tables(self):
        return set(self._table_dict.keys())

    def inline_name(self, reference_name):
        return self._table_dict[reference_name]

    def with_text(self, reference_name):
        return None


class QueryBuilder(object):
    """Tracks the dependencies and with statements to build a single query.

    The lifetime of this object is from when QueryGraph.compile() is called
    to when the query is returned. It tracks the with statements for this
    single query and passes each QueryNode the key/value pairs it needs.

    When a QueryNode calls get_inline_name(), the QueryBuilder determines
    if this reference should come from the TableSource or from another
    QueryNode, makes sure there's a with statement as necessary, then returns
    the inline name.

    In cases where the reference is to another QueryNode, the QueryBuilder
    gets the with statement by calling compile() on this query node, passing
    itself as the QueryBuilder so that it can capture any with statements
    that this second QueryNode needs.
    """

    def __init__(self, table_map, graph, template_parameters):
        self._table_map = table_map
        self._graph = graph
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
            inline = self._table_map[reference_name]
            self._visited_nodes[reference_name] = inline
            return inline
        elif reference_name in self._graph.available_nodes:
            self._visited_nodes[reference_name] = reference_name
            node = self._graph.query_node(reference_name)
            with_text = node.compile(self)
            self._with_list.append((reference_name, with_text))
            return reference_name
        else:
            raise NameNotFoundError(
                'No such query or table {}, referenced in query {}.'.format(
                    reference_name, from_query))


class QueryNode(object):
    """A single node in a QueryGraph.

    The node is responsible for identifying its dependencies and
    substituting inline names (either source tables or query nodes)
    and template values for placeholders when the compile() function
    is called.

    The node gets inline names from a QueryBuilder and relies on the
    QueryBuilder to choose between source tables or other query nodes
    for its dependencies and to track the necessary with statements.
    """

    def __init__(self, name, query_text):
        self._name = name
        self._query_text = query_text
        dependency_list_dups = re.findall(DEPENDENCY_REGEX, query_text)
        # Remove duplicates will preserving a canonical order.
        self._dependency_list = []
        for d in dependency_list_dups:
            if d not in self._dependency_list:
                self._dependency_list.append(d)

    @property
    def dependency_list(self):
        return self._dependency_list

    def compile(self, query_builder):
        substitutions = {}
        for reference_name in self.dependency_list:
            inline_name = query_builder.get_inline_name(
                reference_name, self._name)
            substitutions[reference_name] = inline_name
        return (self._query_text
                    .format(**query_builder.template_parameters)
                    .format(**substitutions))


class QueryGraph(object):
    """A compute graph of subqueries."""

    def __init__(self):
        self._query_nodes = {}

    def __setitem__(self, reference_name, query_text):
        new_node = QueryNode(reference_name, query_text)
        for d in new_node.dependency_list:
            if self.find_in_dependencies(d, reference_name):
                raise CyclicDependencyError(
                    '{} has a cyclic dependency via {}'.format(
                        reference_name, d))
        self._query_nodes[reference_name] = new_node

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
                queue += self.query_node(next_node).dependency_list
        return False

    def compile(self, target_name, table_map, template_parameters=None):
        query_builder = QueryBuilder(table_map, self, template_parameters)
        main_query = self.query_node(target_name).compile(query_builder)

        query = ''
        if query_builder.with_list:
            query = 'WITH {}\n\n'.format(',\n'.join(
                '{} AS (\n{})'.format(name, text)
                for name, text in query_builder.with_list))
        return query + main_query
