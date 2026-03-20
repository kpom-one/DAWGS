# DAWGS (fork)

Database Abstraction Wrapper for Graph Schemas

![A Corgi Treat](logo_small.png)

> **This is a personal fork of [SpecterOps/DAWGS](https://github.com/SpecterOps/BloodHound-DAWGS).**
> It contains experimental features that are not part of the upstream project and should be
> treated accordingly — they are works in progress, lightly tested, and subject to change.
>
> **What's different:**
>
> - **Sonic** (`drivers/sonic/`) — An in-memory graph driver. Useful for testing and lightweight
>   workloads where you don't want a database process. No changes to existing code.
> - **Temporal queries** (`drivers/pg/temporal.go`) — `AsOfReadTransaction` lets you query the
>   PG graph as it existed at a past timestamp. Requires schema changes (`created_at` columns
>   and deletion log tables). See `drivers/pg/caveats.md` for known limitations.
> - **OpenGraph loader** (`opengraph/`) — A small utility for loading/generating graph data from
>   a portable JSON format.

## Purpose

DAWGS is a collection of tools and query language helpers to enable running property graphs on vanilla PostgreSQL
without the need for additional plugins.

At the core of the library is an abstraction layer that allows users to swap out existing database backends (currently
Neo4j and PostgreSQL) or build their own with no change to query implementation. The query interface is built around
openCypher with translation implementations for backends that do not natively support the query language.

## Development Setup

For users making changes to `dawgs` and its packages, the [go mod replace](https://go.dev/ref/mod#go-mod-file-replace)
directive can be utilized. This allows changes made in the checked out `dawgs` repo to be immediately visible to
consuming projects.

**Example**

```
replace github.com/specterops/dawgs => /home/zinic/work/dawgs
```

### Building and Testing

The [Makefile](Makefile) drives build and test automation. The default `make` target should suffice for normal
development processes.

```bash
make
```
