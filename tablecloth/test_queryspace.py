"""Unit tests for queryspace.py"""

from queryspace import QuerySpace, QueryTemplate
from queryspace import NameNotFoundError, CyclicDependencyError
from queryspace import NameCollisionError
import pytest


def homogenize(text):
    return ' '.join(text.split())


def test_homogenize():
    assert homogenize('  foo \t bar   baz   ') == 'foo bar baz'


def test_compile_single_node():
    space = QuerySpace()

    space['my_query'] = QueryTemplate('''
SELECT *
FROM {my_table}
''')

    query = space.make(
        'my_query', my_table='source_table')

    assert homogenize(query) == homogenize('''
SELECT *
FROM source_table
''')


def test_inline_modify_node():
    space = QuerySpace()

    space['my_query'] = QueryTemplate('''
SELECT foo FROM {my_table}''')

    space['my_query'] = QueryTemplate('''
SELECT bar FROM {my_table}''')

    query = space.make(
        'my_query', my_table='source_table')

    assert homogenize(query) == homogenize('''
SELECT bar
FROM source_table
''')


def test_strict_name_collision_error():
    space = QuerySpace(strict=True)

    space['my_query'] = QueryTemplate('''
SELECT foo FROM {my_table}''')

    with pytest.raises(NameCollisionError):
        space['my_query'] = QueryTemplate('''
SELECT bar FROM {my_table}''')


def test_name_not_found():
    space = QuerySpace()

    space['my_query'] = QueryTemplate('''
SELECT *
FROM {my_table}
''')

    with pytest.raises(NameNotFoundError):
        assert space.make('my_query')


def test_leave_name_if_not_strict():
    space = QuerySpace()

    source_query = '''
SELECT *
FROM {my_table}
'''

    space['my_query'] = QueryTemplate(source_query)

    query = space.make('my_query', partial=True)
    assert query == source_query


def test_multiple_nodes():
    space = QuerySpace()
    # Note that these are defined out of order.
    space['query2'] = QueryTemplate('SELECT * FROM {query1}')
    space['query1'] = QueryTemplate('SELECT * FROM {my_table}')
    space['query3'] = QueryTemplate('SELECT * FROM {query2}')

    query = space.make(
        'query3', my_table='source_table')

    assert homogenize(query) == homogenize('''
WITH
  query1 AS ( SELECT * FROM source_table),
  query2 AS ( SELECT * FROM query1)
SELECT * FROM query2''')

    query = space.make(
        'query3', query2='source_table')

    assert homogenize(query) == homogenize('''
SELECT * FROM source_table''')


def test_template_parameters():
    space = QuerySpace()
    space['query1'] = QueryTemplate('SELECT {v1} FROM {my_table}')
    space['query2'] = QueryTemplate('SELECT {v2} FROM {query1}')

    query = space.make(
        'query2', my_table='source_table',
        v1='variable_1', v2='variable_2')

    assert homogenize(query) == homogenize('''
WITH
  query1 AS ( SELECT variable_1 FROM source_table)
SELECT variable_2 FROM query1''')


def test_reuse_previously_seen_nodes():
    space = QuerySpace()
    space['query2'] = QueryTemplate('SELECT * FROM {query1}')
    space['query1'] = QueryTemplate('SELECT * FROM {my_table}')
    space['query3'] = QueryTemplate('SELECT * FROM {query1}')
    space['query4'] = QueryTemplate('SELECT * FROM {query2} JOIN {query3}')

    query = space.make(
        'query4', my_table='source_table')

    assert homogenize(query) == homogenize('''
WITH
  query1 AS ( SELECT * FROM source_table),
  query2 AS ( SELECT * FROM query1),
  query3 AS ( SELECT * FROM query1)
SELECT * FROM query2 JOIN query3''')


def test_detect_space_cycles():
    space = QuerySpace()
    space['query1'] = QueryTemplate('SELECT * FROM {query2}')
    space['query2'] = QueryTemplate('SELECT * FROM {query3}')

    with pytest.raises(CyclicDependencyError):
        space['query3'] = QueryTemplate('SELECT * FROM {query1}')
