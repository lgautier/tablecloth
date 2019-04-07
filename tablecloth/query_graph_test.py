"""Unit tests for query_graph.py"""

from query_graph import QueryGraph, TableSource
from query_graph import NameNotFoundError, CyclicDependencyError
import pytest


class CustomTableSource(TableSource):
    def with_text(self, table_name):
        return 'query for ' + table_name


def homogenize(text):
    return ' '.join(text.split())


def test_homogenize():
    assert homogenize('  foo \t bar   baz   ') == 'foo bar baz'


def test_table_source():
    source = TableSource({'my_table': 'table1', 'your_table': 'table2'})
    assert source.available_tables == set(['my_table', 'your_table'])
    assert source.inline_name('my_table') == 'table1'
    assert source.with_text('my_table') is None


def test_compile_single_node():
    graph = QueryGraph()

    graph.add_node('my_query', '''
SELECT *
FROM {{my_table}}
''')

    query = graph.compile(
        'my_query', TableSource({'my_table': 'source_table'}))

    assert homogenize(query) == homogenize('''
SELECT *
FROM source_table
''')


def test_single_node_custom_source():
    graph = QueryGraph()

    graph.add_node('my_query', '''
SELECT *
FROM {{my_table}}
''')

    query = graph.compile(
        'my_query', CustomTableSource({'my_table': 'source_table'}))

    assert homogenize(query) == homogenize('''
WITH source_table AS (
  query for my_table)

SELECT *
FROM source_table
''')


def test_name_not_found():
    graph = QueryGraph()

    graph.add_node('my_query', '''
SELECT *
FROM {{my_table}}
''')

    with pytest.raises(NameNotFoundError):
        assert graph.compile('my_query', TableSource())


def test_multiple_nodes():
    graph = QueryGraph()
    graph.add_node('query2', 'SELECT * FROM {{query1}}')
    graph.add_node('query1', 'SELECT * FROM {{my_table}}')
    graph.add_node('query3', 'SELECT * FROM {{query2}}')

    query = graph.compile(
        'query3', TableSource({'my_table': 'source_table'}))

    assert homogenize(query) == homogenize('''
WITH
  query1 AS ( SELECT * FROM source_table),
  query2 AS ( SELECT * FROM query1)
SELECT * FROM query2''')

    query = graph.compile(
        'query3', TableSource({'query2': 'source_table'}))

    assert homogenize(query) == homogenize('''
SELECT * FROM source_table''')


def test_template_parameters():
    graph = QueryGraph()
    graph.add_node('query1', 'SELECT {v1} FROM {{my_table}}')
    graph.add_node('query2', 'SELECT {v2} FROM {{query1}}')

    query = graph.compile(
        'query2', TableSource({'my_table': 'source_table'}),
        {'v1': 'variable_1', 'v2': 'variable_2'})

    assert homogenize(query) == homogenize('''
WITH
  query1 AS ( SELECT variable_1 FROM source_table)
SELECT variable_2 FROM query1''')


def test_reuse_previously_seen_nodes():
    graph = QueryGraph()
    graph.add_node('query2', 'SELECT * FROM {{query1}}')
    graph.add_node('query1', 'SELECT * FROM {{my_table}}')
    graph.add_node('query3', 'SELECT * FROM {{query1}}')
    graph.add_node('query4', 'SELECT * FROM {{query2}} JOIN {{query3}}')

    query = graph.compile(
        'query4', TableSource({'my_table': 'source_table'}))

    assert homogenize(query) == homogenize('''
WITH
  query1 AS ( SELECT * FROM source_table),
  query2 AS ( SELECT * FROM query1),
  query3 AS ( SELECT * FROM query1)
SELECT * FROM query2 JOIN query3''')


def test_detect_graph_cycles():
    graph = QueryGraph()
    graph.add_node('query1', 'SELECT * FROM {{query2}}')
    graph.add_node('query2', 'SELECT * FROM {{query3}}')

    with pytest.raises(CyclicDependencyError):
        graph.add_node('query3', 'SELECT * FROM {{query1}}')

# TODO: Include source name in KeyNotFoundErrors
