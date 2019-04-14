"""Unit tests for query_graph.py"""

from query_graph import QueryGraph
from query_graph import NameNotFoundError, CyclicDependencyError
import pytest


def homogenize(text):
    return ' '.join(text.split())


def test_homogenize():
    assert homogenize('  foo \t bar   baz   ') == 'foo bar baz'


def test_compile_single_node():
    graph = QueryGraph()

    graph['my_query'] = '''
SELECT *
FROM {{my_table}}
'''

    query = graph.compile(
        'my_query', {'my_table': 'source_table'})

    assert homogenize(query) == homogenize('''
SELECT *
FROM source_table
''')


def test_name_not_found():
    graph = QueryGraph()

    graph['my_query'] = '''
SELECT *
FROM {{my_table}}
'''

    with pytest.raises(NameNotFoundError):
        assert graph.compile('my_query', {})


def test_multiple_nodes():
    graph = QueryGraph()
    graph['query2'] = 'SELECT * FROM {{query1}}'
    graph['query1'] = 'SELECT * FROM {{my_table}}'
    graph['query3'] = 'SELECT * FROM {{query2}}'

    query = graph.compile(
        'query3', {'my_table': 'source_table'})

    assert homogenize(query) == homogenize('''
WITH
  query1 AS ( SELECT * FROM source_table),
  query2 AS ( SELECT * FROM query1)
SELECT * FROM query2''')

    query = graph.compile(
        'query3', {'query2': 'source_table'})

    assert homogenize(query) == homogenize('''
SELECT * FROM source_table''')


def test_template_parameters():
    graph = QueryGraph()
    graph['query1'] = 'SELECT {v1} FROM {{my_table}}'
    graph['query2'] = 'SELECT {v2} FROM {{query1}}'

    query = graph.compile(
        'query2', {'my_table': 'source_table'},
        {'v1': 'variable_1', 'v2': 'variable_2'})

    assert homogenize(query) == homogenize('''
WITH
  query1 AS ( SELECT variable_1 FROM source_table)
SELECT variable_2 FROM query1''')


def test_reuse_previously_seen_nodes():
    graph = QueryGraph()
    graph['query2'] = 'SELECT * FROM {{query1}}'
    graph['query1'] = 'SELECT * FROM {{my_table}}'
    graph['query3'] = 'SELECT * FROM {{query1}}'
    graph['query4'] = 'SELECT * FROM {{query2}} JOIN {{query3}}'

    query = graph.compile(
        'query4', {'my_table': 'source_table'})

    assert homogenize(query) == homogenize('''
WITH
  query1 AS ( SELECT * FROM source_table),
  query2 AS ( SELECT * FROM query1),
  query3 AS ( SELECT * FROM query1)
SELECT * FROM query2 JOIN query3''')


def test_detect_graph_cycles():
    graph = QueryGraph()
    graph['query1'] = 'SELECT * FROM {{query2}}'
    graph['query2'] = 'SELECT * FROM {{query3}}'

    with pytest.raises(CyclicDependencyError):
        graph['query3'] = 'SELECT * FROM {{query1}}'
