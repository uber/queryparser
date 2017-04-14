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


## Use

Mostly it boils down to this function:

    parse :: Text -> Either ParseError SqlQuery

To parse some sql from the repl,

    parse "SELECT 1;"
