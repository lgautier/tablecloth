import collections
import copy
import os


class CyclicDependencyError(Exception):
    """Exception when a new QueryElement introduces a circular dependency."""
    pass


# TODO: use an existing graph library or just an overkill ?
class DAG(object):

    def __init__(self):
        self._nodes = set()
        self._forward = collections.defaultdict(set)
        self._reverse = collections.defaultdict(set)

    def __contains__(self, key):
        return key in self._nodes

    def __len__(self):
        return len(self._nodes)

    def __repr__(self):
        return os.linesep.join(
            (super().__repr__(),
             '%i nodes' % len(self._nodes),
             '%i edges' % self.n_edges)
        )

    @property
    def n_edges(self):
        """Return the number of edges."""
        return sum(len(x) for x in self._forward.values())

    def remove_edge(self, key_a, key_b):
        """Remove the edge between two nodes."""
        self._reverse[key_b].remove(key_a)
        self._forward[key_a].remove(key_b)

    def has_dependency(self, key, dependency_key):
        """Determine if dependency_key is a transitive dependency of key."""
        parents = set([key])
        while parents:
            cur_key = parents.pop()
            if cur_key == dependency_key:
                return True
            if cur_key in self._nodes:
                for next_key in self.nodes_from(cur_key):
                    parents.add(next_key)
        return False

    def add_edge(self, key_a, key_b):
        if self.has_dependency(key_b, key_a):
            raise CyclicDependencyError(
                '{} - {} introduces a cyclic dependency'.format(
                    key_a, key_b
                )
            )
        self._nodes.add(key_a)
        self._nodes.add(key_b)
        self._forward[key_a].add(key_b)
        self._reverse[key_b].add(key_a)

    def nodes_to(self, key):
        return tuple(self._reverse[key])

    def nodes_from(self, key):
        return tuple(self._forward[key])

    def subgraph(self, keys):
        """Create a subgraph with the specified keys."""
        res = type(self)()
        for k in keys:
            if k not in self._nodes:
                raise ValueError('No node {}'.format(k))
            res._nodes.add(k)
            for k_b in self.nodes_from(k):
                if k_a in keys:
                    res.add_edge(k, k_b)
            for k_a in self.nodes_to(k):
                if k_a in keys:
                    res.add_edge(k_a, k)

    def keys_topological(self):
        """Iterate over keys in a topological order.

        Iterating in a topological order means that each for each
        key returned we are garanteed that all eventual dependencies
        were returned in an earlier iteration.
        Returns:
        An iterator over keys.
        """
        keys = set(self._nodes)
        # Kahn's algorithm
        dag = copy.deepcopy(self)
        without_dep = set()

        for k in keys:
            deps = self.nodes_to(k)
            if not deps:
                without_dep.add(k)
        while len(without_dep) > 0:
            n = without_dep.pop()
            yield n
            for m in dag.nodes_from(n):
                dag.remove_edge(n, m)
                if not dag.nodes_to(m):
                    without_dep.add(m)
        if dag.n_edges:
            raise CyclicDependencyError()

