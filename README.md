# resource-pooling

A library for pooling resources like connections, threads, or similar

This package is derived from the module Lwt_pool from the lwt package, which
implements resource pooling. With Resource_pool this package provides a
modified version with additional features. Also there is a module called
`Server_pool` that manages resource clusters, specifically a cluster of servers
each with its own connection pool.
