import collections
import copy
import os
import typing


class CyclicDependencyError(Exception):
    """Circular dependency found in DAG."""
    pass


T = typing.TypeVar('T', bound='DAG')


# TODO: use an existing graph library or just an overkill ?
class DAG(object):
    """Directed Acyclic Graph."""

    def __init__(self):
        self._nodes = set()
        self._forward = collections.defaultdict(set)
        self._reverse = collections.defaultdict(set)

    def __contains__(self, key: str) -> bool:
        """
        Args:
        - key: a "node ID"
        Returns:
        A bool.
        """
        return key in self._nodes

    def __len__(self) -> int:
        """Number of vertices (nodes) in the graph."""
        return len(self._nodes)

    def __repr__(self) -> str:
        return os.linesep.join(
            (super().__repr__(),
             '%i nodes' % len(self._nodes),
             '%i edges' % self.n_edges)
        )

    @property
    def n_edges(self) -> int:
        """Return the number of edges."""
        return sum(len(x) for x in self._forward.values())

    @property
    def n_vertices(self) -> int:
        """Return the number of vertices."""
        return len(self)

    def remove_edge(self, key_a, key_b) -> None:
        """Remove the edge between two nodes."""
        self._reverse[key_b].remove(key_a)
        self._forward[key_a].remove(key_b)

    def dependencies(self, key) -> set:
        """Return all direct and indirect dependencies.

        Args:
        - key: the node ID for which dependencies should be returned.
        Returns:
        - A set with node IDs."""
        res = set()
        queue = set(self.nodes_to(key))
        while queue:
            k = queue.pop()
            res.add(k)
            for x in self.nodes_to(k):
                queue.add(x)
        return res

    def has_dependency(self, key, dependency_key) -> bool:
        """Determine if dependency_key is a transitive dependency of key.

        Args:
        - key: node ID
        - dependency_key: node ID
        Returns:
        A bool.
        """
        parents = set([key])
        while parents:
            cur_key = parents.pop()
            if cur_key == dependency_key:
                return True
            if cur_key in self._nodes:
                for next_key in self.nodes_from(cur_key):
                    parents.add(next_key)
        return False

    def add_edge(self, key_a, key_b) -> None:
        """Add an edge between two vertices.
        Args:
        - key_a: a node ID
        - key_b: a node ID
        """
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

    def nodes_to(self, key) -> tuple:
        return tuple(self._reverse[key])

    def nodes_from(self, key) -> tuple:
        return tuple(self._forward[key])

    def keys_topological(self) -> typing.Generator[typing.Any, None, None]:
        """Iterate over keys in a topological order.

        Iterating in a topological order means that each for each
        key returned we are garanteed that all eventual dependencies
        were returned in an earlier iteration.

        Returns:
        A generator over keys.

        Raises:
        - CyclicDependencyError
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
