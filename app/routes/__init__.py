from .cluster import register_routes  # re-export for convenience
from .node import register_node_routes

__all__ = ["register_routes", "register_node_routes"]
