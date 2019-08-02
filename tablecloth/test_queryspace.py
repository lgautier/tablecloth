"""Unit tests for queryspace.py"""

from tablecloth import graph
from tablecloth import queryspace
import pytest


def homogenize(text):
    return ' '.join(text.split())


def test_homogenize():
    assert homogenize('  foo \t bar   baz   ') == 'foo bar baz'


def test_make_single_node():
    space = queryspace.QuerySpace()

    space['my_query'] = queryspace.QueryTemplate(
        """
        SELECT *
        FROM {qs.my_table}""")

    # Fail if remaining placeholder.
    with pytest.raises(queryspace.UnspecifiedPlaceholder):
        query = space.make('my_query')

    # Fail if not a tablename object.
    with pytest.raises(TypeError):
        query = space.make(
            'my_query', my_table='source_table'
        )

    query = space.make(
        'my_query',
        my_table=queryspace.TableName('source_table'))

    assert homogenize(query) == homogenize('''
SELECT *
FROM source_table
''')


def test_multiple_nodes():
    space = queryspace.QuerySpace()
    space['query2'] = queryspace.QueryTemplate(
        'SELECT * FROM {qs.query1}')
    space['query1'] = queryspace.QueryTemplate(
        'SELECT * FROM {qs.my_table}')
    space['query3'] = queryspace.QueryTemplate(
        'SELECT * FROM {qs.query2}')

    query = space.make(
        'query3', my_table=queryspace.TableName('source_table'))

    assert homogenize(query) == homogenize('''
WITH
  query1 AS (SELECT * FROM source_table),
  query2 AS (SELECT * FROM query1)
SELECT * FROM query2''')

    query = space.make(
        'query3',
        query2=queryspace.TableName('source_table'))

    assert homogenize(query) == homogenize('''
    SELECT * FROM source_table''')


def test_template_parameters():
    space = queryspace.QuerySpace()
    space['query1'] = queryspace.QueryTemplate(
        'SELECT {v1} FROM {qs.my_table}')
    space['query2'] = queryspace.QueryTemplate(
        'SELECT {v2} FROM {qs.query1}')

    query = (
        space.make(
            'query2',
            my_table=queryspace.TableName('source_table'))
        .format(v1='variable_1',
                v2='variable_2')
    )

    assert homogenize(query) == homogenize('''
    WITH
      query1 AS (SELECT variable_1 FROM source_table)
    SELECT variable_2 FROM query1''')


def test_reuse_previously_seen_nodes():
    space = queryspace.QuerySpace()
    space['query2'] = queryspace.QueryTemplate(
        'SELECT * FROM {qs.query1}')
    space['query1'] = queryspace.QueryTemplate(
        'SELECT * FROM {qs.my_table}')
    space['query3'] = queryspace.QueryTemplate(
        'SELECT * FROM {qs.query1}')
    space['query4'] = queryspace.QueryTemplate(
        'SELECT * FROM {qs.query2} JOIN {qs.query3}')

    query = space.make(
        'query4', my_table=queryspace.TableName('source_table'))

    hquery = homogenize(query)
    assert (hquery == homogenize('''
    WITH
      query1 AS (SELECT * FROM source_table),
      query2 AS (SELECT * FROM query1),
      query3 AS (SELECT * FROM query1)
    SELECT * FROM query2 JOIN query3''')
            or
            hquery == homogenize('''
    WITH
      query1 AS (SELECT * FROM source_table),
      query3 AS (SELECT * FROM query1),
      query2 AS (SELECT * FROM query1)
    SELECT * FROM query2 JOIN query3'''))


def test_detect_space_cycles():
    space = queryspace.QuerySpace()
    space['query1'] = queryspace.QueryTemplate(
        'SELECT * FROM {qs.query2}')
    space['query2'] = queryspace.QueryTemplate(
        'SELECT * FROM {qs.query3}')

    with pytest.raises(graph.CyclicDependencyError):
        space['query3'] = queryspace.QueryTemplate(
            'SELECT * FROM {qs.query1}')
