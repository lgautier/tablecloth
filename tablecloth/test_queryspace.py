"""Unit tests for queryspace.py"""

import tablecloth.queryspace as qs
import pytest


def homogenize(text):
    return ' '.join(text.split())


def test_homogenize():
    assert homogenize('  foo \t bar   baz   ') == 'foo bar baz'


class TestQueryTemplate:

    def test_dependencies_none(self):
        qt = qs.QueryTemplate('SELECT * FROM foo')
        assert len(qt.dependencies) == 0

        qt = qs.QueryTemplate('SELECT * FROM foo WHERE id={id}')
        assert len(qt.dependencies) == 0

    def test_dependencies_some(self):
        qt = qs.QueryTemplate('SELECT * FROM {qs.foo} INNER JOIN {qs.bar} USING (id)')
        assert len(qt.dependencies) == 2

        qt = qs.QueryTemplate('SELECT * FROM {qs.foo} WHERE id={id}')
        assert len(qt.dependencies) == 1


class TestQuerySpace:

    def test_queryspace_iter_topological(self):
        space = qs.QuerySpace()
        space['query1'] = qs.QueryTemplate('SELECT * FROM {qs.table1}')
        space['query2'] = qs.QueryTemplate('SELECT * FROM {qs.query1}')
        space['query3'] = qs.QueryTemplate('SELECT * FROM {qs.query2}')
        expected_nodes = ('table1', 'query1', 'query2', 'query3')
        assert expected_nodes == tuple(space.iter_topological())
    

    def test_compile_single_node(self):
        space = qs.QuerySpace()

        space['my_query'] = qs.QueryTemplate("""
        SELECT *
        FROM {qs.my_table}
        """)

        query = space.make(
            'my_query', {'my_table': qs.TableName('source_table')})

        assert homogenize(query) == homogenize("""
        SELECT *
        FROM source_table
        """)

    def test_name_not_found(self):
        space = qs.QuerySpace()

        space['my_query'] = qs.QueryTemplate("""
        SELECT *
        FROM {qs.my_table})
        """)

        with pytest.raises(qs.NameNotFoundError):
            assert space.make('my_query', {})

    def test_multiple_nodes(self):
        space = qs.QuerySpace()
        space['query2'] = qs.QueryTemplate('SELECT * FROM {qs.query1}')
        space['query1'] = qs.QueryTemplate('SELECT * FROM {qs.my_table}')
        space['query3'] = qs.QueryTemplate('SELECT * FROM {qs.query2}')

        query = space.make(
            'query3', {'my_table': qs.TableName('source_table')})

        assert homogenize(query) == homogenize("""
        WITH
          query1 AS (SELECT * FROM source_table),
          query2 AS (SELECT * FROM query1)
        SELECT * FROM query2""")

    def test_notplaceholder(self):
        space = qs.QuerySpace()
        space['query2'] = qs.QueryTemplate('SELECT * FROM {qs.query1}')
        space['query1'] = qs.QueryTemplate('SELECT * FROM {qs.my_table}')
        space['query3'] = qs.QueryTemplate('SELECT * FROM {qs.query2}')

        with pytest.raises(ValueError):
            query = space.make(
                'query3', {'query2': qs.TableName('source_table')})

    def test_template_parameters(self):
        space = qs.QuerySpace()
        space['query1'] = qs.QueryTemplate('SELECT {v1} FROM {qs.my_table}')
        space['query2'] = qs.QueryTemplate('SELECT {v2} FROM {qs.query1}')

        query = (space.make(
            'query2', {'my_table': qs.TableName('source_table')})
                 .format(v1='variable_1', v2='variable_2'))

        assert homogenize(query) == homogenize('''
    WITH
      query1 AS (SELECT variable_1 FROM source_table)
    SELECT variable_2 FROM query1''')


    def test_reuse_previously_seen_nodes(self):
        space = qs.QuerySpace()
        space['query2'] = qs.QueryTemplate('SELECT * FROM {qs.query1}')
        space['query1'] = qs.QueryTemplate('SELECT * FROM {qs.my_table}')
        space['query3'] = qs.QueryTemplate('SELECT * FROM {qs.query1}')
        space['query4'] = qs.QueryTemplate('SELECT * FROM {qs.query2} JOIN {qs.query3}')

        query = space.make(
            'query4', {'my_table': qs.TableName('source_table')}
        )

        assert (homogenize(query) == homogenize("""
        WITH
          query1 AS (SELECT * FROM source_table),
          query3 AS (SELECT * FROM query1),
          query2 AS (SELECT * FROM query1)
        SELECT * FROM query2 JOIN query3""") or
                homogenize(query) == homogenize("""
        WITH
          query1 AS (SELECT * FROM source_table),
          query2 AS (SELECT * FROM query1),
          query3 AS (SELECT * FROM query1)
        SELECT * FROM query2 JOIN query3"""))

    def test_has_dependency(self):
        space = qs.QuerySpace()
        # query1 <--- query2
        space['query1'] = qs.QueryTemplate('SELECT * FROM {qs.query2}')
        # query2 <--- query3
        space['query2'] = qs.QueryTemplate('SELECT * FROM {qs.query3}')
        
        assert space.has_dependency('query1', 'query2')
        assert not space.has_dependency('query2', 'query1')
        assert space.has_dependency('query2', 'query3')
        assert not space.has_dependency('query3', 'query2')
        assert space.has_dependency('query1', 'query3')
        assert not space.has_dependency('query3', 'query1')

    def test_detect_space_cycles(self):
        space = qs.QuerySpace()
        space['query1'] = qs.QueryTemplate('SELECT * FROM {qs.query2}')
        space['query2'] = qs.QueryTemplate('SELECT * FROM {qs.query3}')

        with pytest.raises(qs.CyclicDependencyError):
            space['query3'] = qs.QueryTemplate('SELECT * FROM {qs.query1}')
