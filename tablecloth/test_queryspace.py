"""Unit tests for queryspace.py"""

import tablecloth.queryspace as qs
import pytest


def homogenize(text):
    return ' '.join(text.split())


def test_homogenize():
    assert homogenize('  foo \t bar   baz   ') == 'foo bar baz'


def test_queryspace_iter_topological():
    space = qs.QuerySpace()
    space['query1'] = qs.QueryTemplate('SELECT * FROM {table1}')
    space['query2'] = qs.QueryTemplate('SELECT * FROM {query1}')
    space['query3'] = qs.QueryTemplate('SELECT * FROM {query2}')
    expected_nodes = ('table1', 'query1', 'query2', 'query3')
    assert expected_nodes == tuple(space.iter_topological())
    

def test_compile_single_node():
    space = qs.QuerySpace()

    space['my_query'] = qs.QueryTemplate("""
    SELECT *
    FROM {my_table}
    """)

    query = space.make(
        'my_query', {'my_table': qs.TableName('source_table')})

    assert homogenize(query) == homogenize("""
    SELECT *
    FROM source_table
    """)


def test_name_not_found():
    space = qs.QuerySpace()

    space['my_query'] = qs.QueryTemplate("""
    SELECT *
    FROM {my_table})
    """)

    with pytest.raises(qs.NameNotFoundError):
        assert space.make('my_query', {})


def test_multiple_nodes():
    space = qs.QuerySpace()
    space['query2'] = qs.QueryTemplate('SELECT * FROM {query1}')
    space['query1'] = qs.QueryTemplate('SELECT * FROM {my_table}')
    space['query3'] = qs.QueryTemplate('SELECT * FROM {query2}')

    query = space.make(
        'query3', {'my_table': qs.TableName('source_table')})

    assert homogenize(query) == homogenize("""
    WITH
      query1 AS ( SELECT * FROM source_table),
      query2 AS ( SELECT * FROM query1)
    SELECT * FROM query2""")

    query = space.make(
        'query3', {'query2': qs.TableName('source_table')})

    assert homogenize(query) == homogenize("""
    SELECT * FROM source_table""")


def test_template_parameters():
    space = qs.QuerySpace()
    space['query1'] = qs.QueryTemplate('SELECT {v1} FROM {my_table}')
    space['query2'] = qs.QueryTemplate('SELECT {v2} FROM {query1}')

    query = space.make(
        'query2', {'my_table': 'source_table'},
        {'v1': 'variable_1', 'v2': 'variable_2'})

    assert homogenize(query) == homogenize('''
WITH
  query1 AS ( SELECT variable_1 FROM source_table)
SELECT variable_2 FROM query1''')


def test_reuse_previously_seen_nodes():
    space = qs.QuerySpace()
    space['query2'] = qs.QueryTemplate('SELECT * FROM {query1}')
    space['query1'] = qs.QueryTemplate('SELECT * FROM {my_table}')
    space['query3'] = qs.QueryTemplate('SELECT * FROM {query1}')
    space['query4'] = qs.QueryTemplate('SELECT * FROM {query2} JOIN {query3}')

    query = space.make(
        'query4', {'my_table': qs.TableName('source_table')}
    )

    assert homogenize(query) == homogenize("""
    WITH
      query1 AS ( SELECT * FROM source_table ),
      query2 AS ( SELECT * FROM query1 ),
      query3 AS ( SELECT * FROM query1 )
    SELECT * FROM query2 JOIN query3""")


def test_detect_space_cycles():
    space = qs.QuerySpace()
    space['query1'] = qs.QueryTemplate('SELECT * FROM {query2}')
    space['query2'] = qs.QueryTemplate('SELECT * FROM {query3}')

    with pytest.raises(qs.CyclicDependencyError):
        space['query3'] = qs.QueryTemplate('SELECT * FROM {query1}')
