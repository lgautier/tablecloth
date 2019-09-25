"""Unit tests for queryspace.py"""

from tablecloth import queryspace
import pytest


def homogenize(text):
    return ' '.join(text.split())


def test_homogenize():
    assert homogenize('  foo \t bar   baz   ') == 'foo bar baz'


def test_compile_single_node():
    space = queryspace.QuerySpace()

    space['my_query'] = queryspace.QueryTemplate(
        'my_query',
        """
        SELECT *
        FROM {{my_table}}""")

    # Fail if not a tablename object.
    with pytest.raises(TypeError):
        query = space.compile(
            'my_query', {'my_table': 'source_table'})

    query = space.compile(
        'my_query',
        {'my_table': queryspace.TableName('source_table')})

    assert homogenize(query) == homogenize('''
SELECT *
FROM source_table
''')


def test_name_not_found():
    space = queryspace.QuerySpace()

    space['my_query'] = queryspace.QueryTemplate(
        'my_query',
        """
        SELECT *
        FROM {{my_table}}
        """)

    with pytest.raises(queryspace.NameNotFoundError):
        assert space.compile('my_query', {})


def test_multiple_nodes():
    space = queryspace.QuerySpace()
    space['query2'] = queryspace.QueryTemplate(
        'query2', 'SELECT * FROM {{query1}}')
    space['query1'] = queryspace.QueryTemplate(
        'query1', 'SELECT * FROM {{my_table}}')
    space['query3'] = queryspace.QueryTemplate(
        'query3', 'SELECT * FROM {{query2}}')

    query = space.compile(
        'query3', {'my_table': queryspace.TableName('source_table')})

    assert homogenize(query) == homogenize('''
WITH
  query1 AS ( SELECT * FROM source_table),
  query2 AS ( SELECT * FROM query1)
SELECT * FROM query2''')

    query = space.compile(
        'query3', {'query2': queryspace.TableName('source_table')})

    assert homogenize(query) == homogenize('''
SELECT * FROM source_table''')


def test_template_parameters():
    space = queryspace.QuerySpace()
    space['query1'] = queryspace.QueryTemplate(
        'query1', 'SELECT {v1} FROM {{my_table}}')
    space['query2'] = queryspace.QueryTemplate(
        'query2', 'SELECT {v2} FROM {{query1}}')

    query = space.compile(
        'query2', {'my_table': queryspace.TableName('source_table')},
        {'v1': 'variable_1', 'v2': 'variable_2'})

    assert homogenize(query) == homogenize('''
WITH
  query1 AS ( SELECT variable_1 FROM source_table)
SELECT variable_2 FROM query1''')


def test_reuse_previously_seen_nodes():
    space = queryspace.QuerySpace()
    space['query2'] = queryspace.QueryTemplate(
        'query2', 'SELECT * FROM {{query1}}')
    space['query1'] = queryspace.QueryTemplate(
        'query1', 'SELECT * FROM {{my_table}}')
    space['query3'] = queryspace.QueryTemplate(
        'query3', 'SELECT * FROM {{query1}}')
    space['query4'] = queryspace.QueryTemplate(
        'query4', 'SELECT * FROM {{query2}} JOIN {{query3}}')

    query = space.compile(
        'query4', {'my_table': queryspace.TableName('source_table')})

    assert homogenize(query) == homogenize('''
WITH
  query1 AS ( SELECT * FROM source_table),
  query2 AS ( SELECT * FROM query1),
  query3 AS ( SELECT * FROM query1)
SELECT * FROM query2 JOIN query3''')


def test_detect_space_cycles():
    space = queryspace.QuerySpace()
    space['query1'] = queryspace.QueryTemplate(
        'query1', 'SELECT * FROM {{query2}}')
    space['query2'] = queryspace.QueryTemplate(
        'query2', 'SELECT * FROM {{query3}}')

    with pytest.raises(queryspace.CyclicDependencyError):
        space['query3'] = queryspace.QueryTemplate(
            'query3', 'SELECT * FROM {{query1}}')
