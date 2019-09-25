from tablecloth import graph
import pytest


def test_add_edge():
    g = graph.DAG()
    assert g.n_edges == 0
    g.add_edge('a', 'b')
    g.add_edge('a', 'c')
    assert g.n_edges == 2
    assert g.nodes_to('c') == ('a',)
    assert tuple(sorted(g.nodes_from('a'))) == ('b', 'c')


def test_remove_edge():
    g = graph.DAG()
    g.add_edge('a', 'b')
    g.add_edge('a', 'c')
    g.add_edge('b', 'c')
    assert g.n_edges == 3
    assert tuple(sorted(g.nodes_to('c'))) == ('a', 'b')
    g.remove_edge('a', 'c')
    assert g.n_edges == 2
    assert tuple(sorted(g.nodes_to('c'))) == ('b', )


def test_keys_topological():
    g = graph.DAG()
    g.add_edge('b', 'c')
    g.add_edge('a', 'b')
    g.add_edge('a', 'c')
    assert tuple(sorted(g.keys_topological())) == ('a', 'b', 'c')
