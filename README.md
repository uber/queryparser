# queryparser

Parsing and analysis of Vertica, Hive, and Presto SQL.


## Gentle introduction

Queryparser supports parsing for three sql-dialects (Vertica, Hive, and
Presto).

Each dialect implements its own tokenization and parsing logic. There is a
single abstract syntax tree (AST) for representing queries of any dialect. This
AST is defined in Database/Sql/Type.hs and Database/Sql/Type/Query.hs.

The parsing logic produces an AST with table and column identifiers that are
"raw" or optionally qualified. Frequently, it is desirable to convert the AST
over raw names to an AST over resolved names, where identifiers are fully
qualified. This transformation is called "name resolution" or simply
"resolution". It requires as input the full list of columns in every table and
the full list of tables in every schema, otherwise known as "catalog
information".

An example of resolution:

 * `SELECT                  column_c FROM          table_t` is a query with raw names
 * `SELECT schema_s.table_t.column_c FROM schema_s.table_t` is the same query with resolved names

Various utility functions ("analyses") have been defined for ASTs over resolved
names, such as:

 * what tables appear in the query?
 * what columns appear in the query, per clause?
 * what is the table/column lineage of a query?
 * what sets of columns does a query compare for equality?

The analyses are the main value-add of this library.


## Demo

Enough talk, let's show what it can do!

    $ stack ghci
    ...
    ...> :set prompt "> "
    > import Demo
    > -- for the purposes of our demo, we have two tables: foo with columns a,b,c and bar with columns x,y,z
    > demoAllAnalyses "SELECT * FROM foo" -- note that the SELECT * expands to a,b,c
    Tables accessed:
        public.foo
    Columns accessed by clause:
        public.foo.a	SELECT
        public.foo.b	SELECT
        public.foo.c	SELECT
    Joins:
        no joins
    Table lineage:
        no tables modified
    > demoAllAnalyses "SELECT * FROM bar" -- and here the SELECT * expands to x,y,z
    Tables accessed:
        public.bar
    Columns accessed by clause:
        public.bar.x	SELECT
        public.bar.y	SELECT
        public.bar.z	SELECT
    Joins:
        no joins
    Table lineage:
        no tables modified
    > demoAllAnalyses "SELECT x, count(1) FROM foo JOIN bar ON foo.a = bar.y WHERE z IS NOT NULL GROUP BY 1 ORDER BY 2 DESC, b"
    Tables accessed:
        public.bar
        public.foo
    Columns accessed by clause:
        public.bar.x	GROUPBY
        public.bar.x	SELECT
        public.bar.y	JOIN
        public.bar.z	WHERE
        public.foo.a	JOIN
        public.foo.b	ORDER
    Joins:
        public.bar.y <-> public.foo.a
    Table lineage:
        no tables modified
    > -- let's play with some queries that modify table-data!
    > demoTableLineage "INSERT INTO foo SELECT * FROM bar"
    public.foo after the query depends on public.bar, public.foo before the query
    > demoTableLineage "TRUNCATE TABLE foo"
    public.foo no longer has data
    > demoTableLineage "ALTER TABLE bar, foo RENAME TO baz, bar"
    public.bar after the query depends on public.foo before the query
    public.baz after the query depends on public.bar before the query
    public.foo no longer has data
    > -- let's explore a few subtler behaviors of the "joins" analysis (admittedly, something of a misnomer)
    > demoJoins "SELECT * FROM foo JOIN bar ON a=x AND b+c = y+z"
    public.bar.x <-> public.foo.a
    public.bar.y <-> public.foo.b
    public.bar.y <-> public.foo.c
    public.bar.z <-> public.foo.b
    public.bar.z <-> public.foo.c
    > demoJoins "SELECT a FROM foo UNION SELECT x FROM bar"
    public.bar.x <-> public.foo.a

Spin up your own ghci and paste in your own queries!


## Requirements

To build, you need:

- stack
- docker (recommended)

### OS X

You can install stack on OS X with brew:

    brew install ghc haskell-stack

To install docker, use the default installer found on https://docs.docker.com/mac/

To allow stack to see the docker daemon, add `eval "$(docker-machine
env default)"` to your bashrc or equivalent. (This is following https://docs.docker.com/machine/reference/env/)

### Linux & Other

Follow the directions at https://github.com/commercialhaskell/stack#how-to-install


## Installation

Once you've got what you need, check out the repo and change to the directory.

Stack will download other dependencies for you:

    stack setup


Now you can build the package with:

    stack build


Or run the tests with:

    stack test


Or run the benchmarks with:

    stack bench


Or pull things up in ghci with:

    stack ghci

## Contributing

If you'd like to contribute to the repo, use the above installation instructions to get started.

When you're ready, make sure that the code compiles with the `Development` flag, i.e.:

    stack build --flag queryparser:development

## Use

Mostly it boils down to this function:

    parse :: Text -> Either ParseError SqlQuery

To parse some sql from the repl,

    parse "SELECT 1;"

## Areas of future interest

There is substantial room for future work in Queryparser. For more details, see
[Areas of future interest](FUTURE.md).
