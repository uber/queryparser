-- Copyright (c) 2017 Uber Technologies, Inc.
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included in
-- all copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
-- THE SOFTWARE.

module Database.Sql.Presto.Parser.Test where

import Test.HUnit
import Test.HUnit.Ticket

import Database.Sql.Type
import Database.Sql.Position
import Database.Sql.Presto.Parser
import Database.Sql.Presto.Type

import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL

parsesSuccessfully :: Text -> Assertion
parsesSuccessfully sql = case parse sql of
    (Right _) -> return ()
    (Left e) -> assertFailure $ unlines
        [ "failed to parse:"
        , show sql
        , show e
        ]

parsesUnsuccessfully :: Text -> Assertion
parsesUnsuccessfully sql = case parse sql of
    (Left _) -> return ()
    (Right _) -> assertFailure $ unlines
        [ "parsed broken sql:"
        , show sql
        ]

testParser :: Test
testParser = test
    [ "Parse some arbitrary examples" ~: map (TestCase . parsesSuccessfully)
        [ "SELECT 1;"
        , "SELECT 1"
        , ";"

        -- test expr parsing
        , "SELECT                         column_;"
        , "SELECT                  table_.column_;"
        , "SELECT          schema_.table_.column_;"
        , "SELECT catalog_.schema_.table_.column_;"
        , "SELECT catalog_.schema_.table_.column_.field;"
        , "SELECT                         *;"
        , "SELECT                  table_.*;"
        , "SELECT          schema_.table_.*;"
        , "SELECT catalog_.schema_.table_.*;"
        , "SELECT column_ AS alias;"
        , "SELECT column_ AS \"alias\";"
        , "SELECT column_ alias;"
        , "SELECT column_ \"alias\";"

        , "SELECT 1 AND 1 OR NOT 1;"
        , "SELECT NOT NOT NOT 1;"
        , "SELECT 1 || 2;"
        , "SELECT 1 + 5 - 4 * 2 / 54 % 2;"
        , "SELECT +1, -2;"
        , "SELECT 0, 0.1, .01, 1.;"
        , "SELECT foo AT TIME ZONE 'UTC';"
        , "SELECT 1 IS DISTINCT FROM 2;"
        , "SELECT 1 IS NOT DISTINCT FROM 2;"
        , "SELECT 1 IS NULL;"
        , "SELECT 1 IS NOT NULL;"
        , "SELECT foo LIKE 'bar' ESCAPE '!';"
        , "SELECT foo NOT LIKE 'bar' ESCAPE '!';"
        , "SELECT 1 IN (SELECT 2);"
        , "SELECT 1 NOT IN (SELECT 2);"
        , "SELECT 1 IN (1 + 3, 2);"
        , "SELECT 1 NOT IN (1 + 3, 2);"
        , "SELECT foo BETWEEN 1 AND 5;"
        , "SELECT foo NOT BETWEEN 1 AND 5;"
        , "SELECT 1 = ANY (SELECT 2);"
        , "SELECT 1 = SOME (SELECT 2);"
        , "SELECT 1 = ALL (SELECT 2);"
        , "SELECT 1 >= ALL (SELECT 2);"
        , "SELECT 1 <> ALL (SELECT 2);"
        , "SELECT 1 = 1;"
        , "SELECT 1 > 1;"
        , "SELECT 1 >= 1;"
        , "SELECT 1 < 1;"
        , "SELECT 1 <= 1;"
        , "SELECT 1 != 1;"
        , "SELECT 1 <> 1;"

        , "SELECT extract(YEAR from '');"
        , "SELECT normalize('asdf');"
        , "SELECT normalize('asdf', nfd);"
        , "SELECT normalize('asdf', nfkc);"
        , "SELECT normalize('asdf', nfkd);"
        , "SELECT substring('asdf' FROM 1);"
        , "SELECT substring('asdf' FROM 1 FOR 2);"
        , "SELECT LOCALTIMESTAMP;"
        , "SELECT LOCALTIMESTAMP(1);"
        , "SELECT LOCALTIME;"
        , "SELECT LOCALTIME(1);"
        , "SELECT CURRENT_TIMESTAMP;"
        , "SELECT CURRENT_TIMESTAMP(1);"
        , "SELECT CURRENT_TIME;"
        , "SELECT CURRENT_TIME(1);"
        , "SELECT CURRENT_DATE;"
        , "SELECT ARRAY[1+1, LOCALTIME];"
        , "SELECT ARRAY[];"
        , "SELECT CAST(NULL AS BIGINT);"
        , "SELECT TRY_CAST(NULL AS BIGINT);"
        , "SELECT TRY_CAST(NULL AS BIGINT ARRAY);"
        , "SELECT TRY_CAST(NULL AS ARRAY<BIGINT>);"
        , "SELECT TRY_CAST(NULL AS ARRAY<ARRAY<TINYINT>>);"
        , "SELECT TRY_CAST(NULL AS MAP<ARRAY<BIGINT>, ARRAY<TINYINT>>);"
        , "SELECT TRY_CAST(NULL AS ROW(foo TINYINT, bar ARRAY<BIGINT>));"
        , "SELECT TRY_CAST(NULL AS VARCHAR(20));"
        , "SELECT TRY_CAST(NULL AS VARCHAR(20) ARRAY ARRAY ARRAY);"
        , "SELECT TRY_CAST(NULL AS DOUBLE);"
        , "SELECT TRY_CAST(NULL AS DOUBLE PRECISION);"
        , "SELECT TRY_CAST(NULL AS TIME);"
        , "SELECT TRY_CAST(NULL AS TIME WITH TIME ZONE);"
        , "SELECT TRY_CAST(NULL AS TIMESTAMP);"
        , "SELECT TRY_CAST(NULL AS TIMESTAMP WITH TIME ZONE);"
        , "SELECT CASE WHEN foo = 7 THEN 7 END;"
        , "SELECT CASE WHEN foo = 7 THEN 7 WHEN bar = 9 THEN 9 ELSE 0 END;"
        , "SELECT CASE 1 WHEN 2 THEN 2 WHEN 3 THEN 3 END;"
        , "SELECT CASE 1 WHEN 2 THEN 2 WHEN 3 THEN 3 ELSE 1 END;"
        , "SELECT EXISTS (SELECT 1);"
        , "SELECT (SELECT 1);"
        , "SELECT count(*);"
        , "SELECT count(*) FILTER (WHERE true);"
        , "SELECT count(*) OVER (RANGE CURRENT ROW);"
        , "SELECT count(*) FILTER (WHERE true) OVER (RANGE CURRENT ROW);"
        , "SELECT count();"
        , "SELECT count(1, 2);"
        , "SELECT count(distinct 1, 2);"
        , "SELECT count(all 1, 2);"
        , "SELECT count(1 + 1) FILTER (WHERE true);"
        , "SELECT count(1 + 1) OVER (RANGE CURRENT ROW);"
        , "SELECT count(1 + 1) FILTER (WHERE true) OVER (RANGE CURRENT ROW);"
        , "SELECT count(1 + 1) FILTER (WHERE true) OVER ();"
        , TL.unwords
          [ "SELECT count(1) OVER ("
          , "  PARTITION BY 1, 2, foo * 10"
          , "  ORDER BY 1 ASC NULLS FIRST, 2 DESC NULLS LAST, 3 ASC, 4 DESC, 5 NULLS FIRST, 6"
          , "  RANGE CURRENT ROW"
          , ");"
          ]
        , "SELECT count() OVER (RANGE UNBOUNDED PRECEDING);"
        , "SELECT count() OVER (RANGE UNBOUNDED FOLLOWING);"
        , "SELECT count() OVER (RANGE CURRENT ROW);"
        , "SELECT count() OVER (RANGE 1 PRECEDING);"
        , "SELECT count() OVER (RANGE 1 FOLLOWING);"
        , "SELECT count() OVER (ROWS UNBOUNDED PRECEDING);"
        , "SELECT count() OVER (RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING);"
        , "SELECT count() OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING);"
        , "SELECT schema_.count(1);"
        , "SELECT catalog_.schema_.count(1);"
        , "SELECT (1);" -- this is a parenthesized expr bc only one arg
        , "SELECT (1, 2, 3 + 3);" -- this is an implicit row constructor bc multiple args
        , "SELECT ROW(1, 2, 3 + 3);"
        , "SELECT ROW(3 + 3);"
        , "SELECT ROW();" -- this ISN'T the row constructor special function; it parses as a regular function
        , "SELECT foo row;" -- here, row is an alias
        , "SELECT POSITION('sd' IN 'asdf');"
        , "SELECT ?;"
        , "SELECT X'00 01';"
        , "SELECT BIGINT '1';"
        , "SELECT VARCHAR '1';"
        , "SELECT JONIREGEXP '1';"
        , "SELECT DOUBLE PRECISION '1';"
        , "SELECT DOUBLE '1';"
        , "SELECT INTERVAL '3' DAY;"
        , "SELECT INTERVAL + '3' DAY;"
        , "SELECT INTERVAL - '3' DAY;"
        , "SELECT INTERVAL '3' DAY TO SECOND;"
        , "SELECT 1;"
        , "SELECT 'asdf';"
        , "SELECT true;"
        , "SELECT false;"
        , "SELECT NULL;"
        , "SELECT table_.column_.field_ FROM table_;"
        , "SELECT foo[1+1];"
        , "SELECT ARRAY[1, 2, 3][1+1];"

        -- test FROM parsing
        , "SELECT * FROM foo               ;"
        , "SELECT * FROM foo    x          ;"
        , "SELECT * FROM foo    x (a, b, c);"
        , "SELECT * FROM foo AS x (a, b, c);"
        , "SELECT * FROM foo                TABLESAMPLE BERNOULLI (10);"
        , "SELECT * FROM foo    x           TABLESAMPLE BERNOULLI (10);"
        , "SELECT * FROM foo    x (a, b, c) TABLESAMPLE BERNOULLI (10);"
        , "SELECT * FROM foo AS x (a, b, c) TABLESAMPLE BERNOULLI (10);"
        , "SELECT * FROM foo AS x (a, b, c) TABLESAMPLE SYSTEM (10);"
        , "SELECT * FROM foo AS x (a, b, c) TABLESAMPLE POISSONIZED (10);"
        , "SELECT * FROM catalog_.schema_.table_;"
        , "SELECT * FROM          schema_.table_;"
        , "SELECT * FROM                  table_;"
        , "SELECT * FROM (SELECT 1);"
        , "SELECT * FROM (SELECT 1) x;"
        , "SELECT * FROM (SELECT 1) AS x (a, b, c) TABLESAMPLE SYSTEM (10);"
        , "SELECT * FROM UNNEST(col1_)                                                                 ;"
        , "SELECT * FROM UNNEST(col1_)                              x                                  ;"
        , "SELECT * FROM UNNEST(col1_)                           AS x                                  ;"
        , "SELECT * FROM UNNEST(col1_)                           AS x (a, b, c)                        ;"
        , "SELECT * FROM UNNEST(col1_)                           AS x (a, b, c) TABLESAMPLE SYSTEM (10);"
        , "SELECT * FROM UNNEST(col1_)           WITH ORDINALITY AS x (a, b, c) TABLESAMPLE SYSTEM (10);"
        , "SELECT * FROM UNNEST(col1_, ARRAY[1]) WITH ORDINALITY AS x (a, b, c) TABLESAMPLE SYSTEM (10);"
        , "SELECT * FROM (foo);"
        , "SELECT * FROM (foo x (a, b, c));"
        , "SELECT * FROM ((SELECT 1));"
        , "SELECT * FROM ((SELECT 1) x);"
        , "SELECT * FROM ((SELECT 1) x) y (a);"
        , "SELECT * FROM (UNNEST(column_));"

        , "SELECT * FROM foo CROSS JOIN bar;"
        , "SELECT * FROM foo CROSS JOIN bar x TABLESAMPLE SYSTEM (10);"
        , "SELECT * FROM foo JOIN bar USING (a, b, c);"
        , "SELECT * FROM foo             JOIN bar ON true;"
        , "SELECT * FROM foo INNER       JOIN bar ON true;"
        , "SELECT * FROM foo LEFT        JOIN bar ON true;"
        , "SELECT * FROM foo LEFT  OUTER JOIN bar ON true;"
        , "SELECT * FROM foo RIGHT       JOIN bar ON true;"
        , "SELECT * FROM foo RIGHT OUTER JOIN bar ON true;"
        , "SELECT * FROM foo FULL        JOIN bar ON true;"
        , "SELECT * FROM foo FULL  OUTER JOIN bar ON true;"
        , "SELECT * FROM foo NATURAL            JOIN bar;"
        , "SELECT * FROM foo NATURAL INNER      JOIN bar;"
        , "SELECT * FROM foo NATURAL LEFT       JOIN bar;"
        , "SELECT * FROM foo NATURAL RIGHT      JOIN bar;"
        , "SELECT * FROM foo NATURAL FULL OUTER JOIN bar;"

        , "SELECT * FROM foo    TABLESAMPLE                                ;"
        , "SELECT * FROM foo                        TABLESAMPLE SYSTEM (10);"
        , "SELECT * FROM foo    TABLESAMPLE         TABLESAMPLE SYSTEM (10);"
        , "SELECT * FROM foo AS TABLESAMPLE         TABLESAMPLE SYSTEM (10);"
        , "SELECT * FROM foo    TABLESAMPLE (a,b,c) TABLESAMPLE SYSTEM (10);"
        , "SELECT * FROM foo AS TABLESAMPLE (a,b,c) TABLESAMPLE SYSTEM (10);"

        -- test WHERE parsing
        , "SELECT * FROM foo WHERE true;"
        , "SELECT * FROM foo WHERE     x > 1;"
        , "SELECT * FROM foo WHERE foo.x > 1;"

        -- test GROUP BY parsing
        , "SELECT a, b, SUM(c) FROM foo GROUP BY a, b;"
        , "SELECT a, b, SUM(c) FROM foo GROUP BY ALL a, b;"
        , "SELECT a, b, SUM(c) FROM foo GROUP BY DISTINCT a, b;"

        , "SELECT a, b, SUM(c) FROM foo GROUP BY (foo.a + 1, foo.b - 1);"
        , "SELECT a, b, SUM(c) FROM foo GROUP BY true;"
        , "SELECT a, b, SUM(c) FROM foo GROUP BY ROLLUP (foo.a, foo.b);"
        , "SELECT a, b, SUM(c) FROM foo GROUP BY CUBE (foo.a, foo.b);"
        , "SELECT a, b, SUM(c) FROM foo GROUP BY GROUPING SETS ((foo.a), (foo.a, foo.b), foo.b);"
        , TL.unwords
          [ "SELECT a, b, SUM(c) FROM foo"
          , "GROUP BY (foo.a), (foo.a, foo.b)"
          , "       , 1+1"
          , "       , ROLLUP (foo.a, foo.b)"
          , "       , CUBE (foo.a, foo.b)"
          , "       , GROUPING SETS ((foo.a), (foo.a, foo.b), foo.b);"
          ]

        -- test HAVING parsing
        , "SELECT x, COUNT(*) FROM foo GROUP BY x HAVING COUNT(*) > 1;"

        -- test DISTINCT parsing
        , "SELECT 1;"
        , "SELECT DISTINCT 1;"
        , "SELECT ALL 1;"
        , "SELECT DISTINCT 1, 2;"
        , "SELECT ALL 1, 2;"

        -- test WITH parsing
        , "WITH x AS (SELECT 1) SELECT * FROM x;"
        , "WITH x (alias1, alias2) AS (SELECT 1, 2) SELECT * FROM x;"

        -- test ORDER parsing
        , "SELECT a FROM foo ORDER BY b;"
        , "WITH x AS (SELECT 1 AS a FROM dual)  SELECT * FROM X             UNION  SELECT * FROM X ORDER BY a ;"
        , "WITH x AS (SELECT 1 AS a FROM dual) (SELECT * FROM X ORDER BY a) UNION (SELECT * FROM X ORDER BY a);"
        , "WITH x AS (SELECT 1 AS a FROM dual) (SELECT * FROM X ORDER BY a) UNION (SELECT * FROM X) ORDER BY a;"
        , "WITH x AS (SELECT 1 AS a FROM dual) (SELECT * FROM X ORDER BY a) UNION  SELECT * FROM X ORDER BY a ;"
        , "WITH x AS (SELECT 1 AS a FROM dual) ((SELECT * FROM X ORDER BY a) UNION SELECT * FROM X) ORDER BY a;"
        , "WITH x AS (SELECT 1 AS a) ((SELECT * FROM X) ORDER BY a) ORDER BY a;"

        -- test UNION, INTERSECT, EXCEPT, etc
        , "SELECT 1 UNION          SELECT 1;"
        , "SELECT 1 UNION ALL      SELECT 1;"
        , "SELECT 1 UNION DISTINCT SELECT 1;"
        , "SELECT 1 INTERSECT          SELECT 1;"
        , "SELECT 1 INTERSECT DISTINCT SELECT 1;"
        , "SELECT 1 EXCEPT          SELECT 1;"
        , "SELECT 1 EXCEPT DISTINCT SELECT 1;"

        -- test LIMIT
        , "SELECT 1 LIMIT 1;"
        , "SELECT 1 LIMIT ALL;"
        , "(SELECT 1 LIMIT 1) UNION SELECT 1 LIMIT ALL;"

        -- test some unhandled examples
        , "SHOW FUNCTIONS;"
        , "EXPLAIN SELECT 1;"
        , "CALL system.runtime.kill_query('20161116_172749_20819_78uwn');"
        , "DESC foo;"
        , "DESCRIBE foo;"

        -- test INSERT
        , "INSERT INTO foo.bar VALUES ('null', timestamp '2016-12-12 16:12:09.207'), (1, false);"
        , "INSERT INTO orders SELECT * FROM new_orders;"
        , "INSERT INTO nation (nationkey, name, regionkey, comment) VALUES (26, 'POLAND', 3, 'no comment');"

        -- test DROP VIEW
        , "DROP VIEW foo;"
        , "DROP VIEW IF EXISTS foo;"

        -- test parenthesized relation
        , "SELECT * FROM (foo CROSS JOIN bar);"
        , "SELECT a,b FROM (foo CROSS JOIN bar);"
        , "SELECT a,b FROM (foo CROSS JOIN bar) a;"
        , "SELECT a,b FROM (foo CROSS JOIN bar) a (a,b);"

        -- test set statement
        , "SET ROLE NONE;"
        , "SET SESSION optimize_hash_generation = true;"
        , "SET SESSION data.optimize_locality_enabled = false;"
        , "SET TIME ZONE LOCAL;"
        , "SET TIME ZONE '-08:00';"
        , "SET TIME ZONE INTERVAL '10' HOUR;"
        , "SET TIME ZONE INTERVAL -'08:00' HOUR TO MINUTE;"
        , "SET TIME ZONE 'America/Los_Angeles';"
        , "SET TIME ZONE concat_ws('/', 'America', 'Los_Angeles');"

        -- Named windows can substitute for window exprs in OVER clauses
        , "SELECT RANK() OVER x FROM potato WINDOW x AS (PARTITION BY a ORDER BY b ASC);"
        -- They can also be inherited, as long as orderby is not double-defined
        , TL.unlines
            [ "SELECT RANK() OVER (x ORDER BY b ASC),"
            , "DENSE_RANK() OVER (x ORDER BY b DESC)"
            , "FROM potato WINDOW x AS (PARTITION BY a);"
            ]
        , TL.unlines
            [ "SELECT RANK() OVER (x ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"
            , "FROM potato WINDOW x AS (PARTITION BY a ORDER BY b ASC);"
            ]
        -- Unlike vertica, hive can have anything in its named window clause
        -- Which is vomitous, since frame clauses mean nothing without order
        , "SELECT RANK() OVER (x) FROM potato WINDOW x AS (PARTITION BY a);"
        , "SELECT RANK() OVER (x) FROM potato WINDOW x AS (ORDER BY a);"
        , "SELECT RANK() OVER (x) FROM potato WINDOW x AS (ROWS UNBOUNDED PRECEDING);"
        , "SELECT RANK() OVER (x) FROM potato WINDOW x AS ();"
        , "SELECT RANK() OVER (x) FROM potato WINDOW x AS (PARTITION BY a ORDER BY a ROWS UNBOUNDED PRECEDING);"
        -- Named windows can also inherit from each other,
        -- similar to WITH clauses
        , TL.unlines
            [ "SELECT RANK() OVER (w1 ORDER BY sal DESC),"
            , "RANK() OVER w2"
            , "FROM EMP"
            , "WINDOW w1 AS (PARTITION BY deptno), w2 AS (w1 ORDER BY sal);"
            ]
        -- In hive, they can inherit in all their glory.
        -- Ambiguous definitions? Frames without orders? It's got it.
        , TL.unlines
            [ "SELECT RANK() OVER (w2) FROM EMP"
            , "WINDOW w1 AS (PARTITION BY deptno),"
            , "w2 AS (w1 PARTITION BY alt ORDER BY sal);"
            ]
        , TL.unlines
            [ "SELECT RANK() OVER (w2) FROM EMP"
            , "WINDOW w1 AS (PARTITION BY deptno),"
            , "w2 AS (w1 ORDER BY sal ROWS UNBOUNDED PRECEDING);"
            ]
        , TL.unlines
            [ "SELECT RANK() OVER (w2 PARTITION BY foo) FROM EMP"
            , "WINDOW w1 AS (PARTITION BY deptno),"
            , "w2 AS (w1 PARTITION BY sal ROWS UNBOUNDED PRECEDING);"
            ]
        , TL.unlines
            [ "SELECT RANK() OVER (w2 PARTITION BY a ORDER BY b ROWS UNBOUNDED PRECEDING)"
            , "FROM EMP"
            , "WINDOW w1 AS (PARTITION BY c ORDER BY d ROWS UNBOUNDED PRECEDING),"
            , "w2 AS (w1 PARTITION BY e ORDER BY f ROWS UNBOUNDED PRECEDING);"
            ]
        -- These should parse Successfully, but fail to resolve:
        -- Named window uses cannot include already defined components
        , TL.unlines
            [ "SELECT RANK() OVER (x ORDER BY c) FROM potato"
            , "WINDOW x AS (PARTITION BY a ORDER BY b);"
            ]
        -- Named windows must have unique names
        , TL.unlines
            [ "SELECT RANK() OVER x FROM potato"
            , "WINDOW x as (PARTITION BY a), x AS (PARTITION BY b);"
            ]
        , TL.unlines
            [ "SELECT RANK() OVER (x) FROM potato"
            , "WINDOW x AS (ORDER BY b);"
            ]

        -- test lambda
        , "SELECT numbers, transform(numbers, (n) -> n * n);"
        , "SELECT transform(prices, n -> TRY_CAST(n AS VARCHAR) || '$');"
        , "SELECT * FROM foo WHERE any_match(numbers, n ->  COALESCE(n, 0) > 100);"

        -- test create table as
        , "CREATE TABLE t (order_date, total_price) AS SELECT orderdate, totalprice"
        , "CREATE TABLE t AS (SELECT orderdate, totalprice)"
        , "CREATE TABLE t COMMENT 'Summary of orders by date' WITH (format = 'ORC') AS SELECT orderdate, totalprice"
        , "CREATE TABLE IF NOT EXISTS t AS SELECT orderdate, totalprice"
        , "CREATE TABLE t AS SELECT orderdate, totalprice WITH NO DATA"
        , "CREATE TABLE t AS SELECT orderdate, totalprice WITH DATA"
        , "CREATE TABLE IF NOT EXISTS t (a, b) COMMENT 'Summary of orders by date' WITH (format = 'ORC') AS SELECT orderdate, totalprice WITH NO DATA"
       ]

    , "Exclude some broken examples" ~: map  (TestCase . parsesUnsuccessfully)
      [ "foo bar;"
      , "SELECT metacatalog_.catalog_.schema_.table_.*;"
      , "SELECT column_ 'alias'" --alias must be tokword not tokstring
      , "SELECT current_date();" --no parens allowed
      , "SELECT time with time zone '1';" -- of the three multi word types
      -- (time with time zone, timestamp with time zone, double precision),
      -- only double precision can be a typed constant constructor.
      , "SELECT timestamp with time zone '1';"
      , "SELECT * FROM foo JOIN bar;" -- joins require a condition
      , "SELECT * FROM foo TABLESAMPLE SYSTEM;"          -- tablesamples require exactly one expr
      , "SELECT * FROM foo TABLESAMPLE SYSTEM ();"       -- tablesamples require exactly one expr
      , "SELECT * FROM foo TABLESAMPLE SYSTEM (10, 11);" -- tablesamples require exactly one expr
      , "SELECT * FROM foo (a, b, c);" -- column aliases can't standalone; may only follow a table alias
      , "SELECT a, b, SUM(c) FROM foo GROUP BY ();"
      , "SELECT a, b, SUM(c) FROM foo GROUP BY ROLLUP (1+1);" -- no exprs, just column refs
      , "SELECT a, b, SUM(c) FROM foo GROUP BY ROLLUP ();" -- may not be empty
      , "SELECT a, b, SUM(c) FROM foo GROUP BY CUBE (1+1);" -- as above
      , "SELECT a, b, SUM(c) FROM foo GROUP BY CUBE ();" -- as above
      , "SELECT a, b, SUM(c) FROM foo GROUP BY GROUPING SETS (1+1);" -- as above
      , "SELECT a, b, SUM(c) FROM foo GROUP BY GROUPING SETS ();" -- as above
      , "SELECT a, b, SUM(c) FROM foo GROUP BY GROUPING SETS (());" -- as above
      , "SELECT 1, DISTINCT 2;" -- distinct/all may only precede the first selection
      , "SELECT 1, ALL 2;" -- distinct/all may only precede the first selection
      , "WITH x AS (SELECT 1 AS a FROM dual)  SELECT * FROM X ORDER BY a  UNION  SELECT * FROM X ORDER BY a ;"
      , "WITH x AS (SELECT 1 AS a FROM dual)  SELECT * FROM X ORDER BY a  UNION  SELECT * FROM X            ;"
      , "SELECT 1 EXCEPT    ALL SELECT 1;"
      , "SELECT 1 INTERSECT ALL SELECT 1;"
      , "SELECT n -> n;"
      , "SELECT (n) -> n;"
      ]

    , ticket "T655130"
      [ parsesSuccessfully "SELECT * FROM foo TABLESAMPLE SYSTEM(1+1);"
      , parsesSuccessfully "SELECT * FROM foo TABLESAMPLE SYSTEM(SELECT bar FROM baz LIMIT 1);"
      ]

    , "Parse exactly" ~:

      [ parse "SELECT 1" ~?= Right
          -- dummy col aliases start from 1
            ( PrestoStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect (Range (Position 1 0 0) (Position 1 8 8))
                        Select
                            { selectInfo = Range (Position 1 0 0) (Position 1 8 8)
                            , selectCols = SelectColumns
                                { selectColumnsInfo = Range (Position 1 7 7) (Position 1 8 8)
                                , selectColumnsList = [
                                      SelectExpr
                                      ( Range (Position 1 7 7) (Position 1 8 8) )
                                      [ ColumnAlias
                                         (Range (Position 1 7 7) (Position 1 8 8))
                                         "_col0" (ColumnAliasId 1)
                                      ]
                                      ( ConstantExpr
                                          (Range (Position 1 7 7) (Position 1 8 8))
                                          (NumericConstant (Range (Position 1 7 7) (Position 1 8 8)) "1")
                                      )
                                      ]
                                }
                            , selectFrom = Nothing
                            , selectWhere = Nothing
                            , selectTimeseries = Nothing
                            , selectGroup = Nothing
                            , selectHaving = Nothing
                            , selectNamedWindow = Nothing
                            , selectDistinct = notDistinct
                            }
                    )
                )
            )

      , parse "SELECT (1)" ~?= Right
          -- parens around a single expr are just for show
            ( PrestoStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect (Range (Position 1 0 0) (Position 1 9 9))
                        Select
                            { selectInfo = Range (Position 1 0 0) (Position 1 9 9)
                            , selectCols = SelectColumns
                                { selectColumnsInfo = Range (Position 1 8 8) (Position 1 9 9)
                                , selectColumnsList = [
                                      SelectExpr
                                      ( Range (Position 1 8 8) (Position 1 9 9) )
                                      [ ColumnAlias
                                         (Range (Position 1 8 8) (Position 1 9 9))
                                         "_col0" (ColumnAliasId 1)
                                      ]
                                      ( ConstantExpr
                                          (Range (Position 1 8 8) (Position 1 9 9))
                                          (NumericConstant (Range (Position 1 8 8) (Position 1 9 9)) "1")
                                      )
                                      ]
                                }
                            , selectFrom = Nothing
                            , selectWhere = Nothing
                            , selectTimeseries = Nothing
                            , selectGroup = Nothing
                            , selectHaving = Nothing
                            , selectNamedWindow = Nothing
                            , selectDistinct = notDistinct
                            }
                    )
                )
            )

      , parse "SELECT (1, 2)" ~?= Right
        -- parens around multiple exprs are an implicit row constructor
            ( PrestoStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect (Range (Position 1 0 0) (Position 1 13 13))
                        Select
                            { selectInfo = Range (Position 1 0 0) (Position 1 13 13)
                            , selectCols = SelectColumns
                                { selectColumnsInfo = Range (Position 1 7 7) (Position 1 13 13)
                                , selectColumnsList = [
                                      SelectExpr
                                      ( Range (Position 1 7 7) (Position 1 13 13) )
                                      [ ColumnAlias
                                         ( Range (Position 1 7 7) (Position 1 13 13) )
                                         "_col0" (ColumnAliasId 1)
                                      ]
                                      ( FunctionExpr
                                        ( Range (Position 1 7 7) (Position 1 13 13) )
                                        ( QFunctionName
                                           ( Range (Position 1 7 7) (Position 1 8 8) )
                                           Nothing
                                           "implicit row"
                                        )
                                        notDistinct
                                        [ ConstantExpr
                                           ( Range (Position 1 8 8) (Position 1 9 9) )
                                           ( NumericConstant (Range (Position 1 8 8) (Position 1 9 9)) "1" )
                                        , ConstantExpr
                                           ( Range (Position 1 11 11) (Position 1 12 12) )
                                           ( NumericConstant (Range (Position 1 11 11) (Position 1 12 12)) "2" )
                                        ]
                                        []
                                        Nothing
                                        Nothing
                                      )
                                      ]
                                }
                            , selectFrom = Nothing
                            , selectWhere = Nothing
                            , selectTimeseries = Nothing
                            , selectGroup = Nothing
                            , selectHaving = Nothing
                            , selectNamedWindow = Nothing
                            , selectDistinct = notDistinct
                            }
                    )
                )
            )

      , parse "SELECT          schema_.table_.col.field FROM schema_.table_" ~?= Right
        -- field access test #1
        ( PrestoStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect (Range (Position 1 0 0) (Position 1 60 60))
                        Select
                            { selectInfo = Range (Position 1 0 0) (Position 1 60 60)
                            , selectCols = SelectColumns
                                { selectColumnsInfo = Range (Position 1 16 16) (Position 1 40 40)
                                , selectColumnsList = [
                                      SelectExpr
                                      ( Range (Position 1 16 16) (Position 1 40 40) )
                                      [ ColumnAlias
                                         ( Range (Position 1 16 16) (Position 1 40 40) )
                                         "_col0" (ColumnAliasId 1)
                                      ]
                                      ( FieldAccessExpr
                                        ( Range (Position 1 16 16) (Position 1 40 40) )
                                        ( ColumnExpr (Range (Position 1 16 16) (Position 1 34 34))
                                          ( QColumnName
                                            (Range (Position 1 31 31) (Position 1 34 34))
                                            ( Just ( QTableName
                                                     (Range (Position 1 24 24) (Position 1 30 30))
                                                     ( Just (mkNormalSchema "schema_" (Range (Position 1 16 16) (Position 1 23 23))))
                                                     "table_"))
                                            "col"))
                                        ( StructFieldName (Range (Position 1 35 35) (Position 1 40 40)) "field"))
                                      ]
                                }
                            , selectFrom = Just
                              ( SelectFrom (Range (Position 1 41 41) (Position 1 60 60))
                                [ TablishTable
                                  ( Range (Position 1 46 46) (Position 1 60 60) )
                                  TablishAliasesNone
                                  ( QTableName (Range (Position 1 54 54) (Position 1 60 60))
                                    (Just (mkNormalSchema "schema_" (Range (Position 1 46 46) (Position 1 53 53))))
                                    "table_")
                                ]
                              )
                            , selectWhere = Nothing
                            , selectTimeseries = Nothing
                            , selectGroup = Nothing
                            , selectHaving = Nothing
                            , selectNamedWindow = Nothing
                            , selectDistinct = notDistinct
                            }
                    )
                )
            )

        , parse "SELECT                  table_.col.field FROM schema_.table_" ~?= Right
        -- field access test #2
        ( PrestoStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect (Range (Position 1 0 0) (Position 1 60 60))
                        Select
                            { selectInfo = Range (Position 1 0 0) (Position 1 60 60)
                            , selectCols = SelectColumns
                                { selectColumnsInfo = Range (Position 1 24 24) (Position 1 40 40)
                                , selectColumnsList = [
                                      SelectExpr
                                      ( Range (Position 1 24 24) (Position 1 40 40) )
                                      [ ColumnAlias
                                         ( Range (Position 1 24 24) (Position 1 40 40) )
                                         "_col0" (ColumnAliasId 1)
                                      ]
                                      ( FieldAccessExpr
                                        ( Range (Position 1 24 24) (Position 1 40 40) )
                                        ( ColumnExpr (Range (Position 1 24 24) (Position 1 34 34))
                                          ( QColumnName
                                            (Range (Position 1 31 31) (Position 1 34 34))
                                            ( Just ( QTableName
                                                     (Range (Position 1 24 24) (Position 1 30 30))
                                                     Nothing
                                                     "table_"))
                                            "col"))
                                        ( StructFieldName (Range (Position 1 35 35) (Position 1 40 40)) "field"))
                                      ]
                                }
                            , selectFrom = Just
                              ( SelectFrom (Range (Position 1 41 41) (Position 1 60 60))
                                [ TablishTable
                                  ( Range (Position 1 46 46) (Position 1 60 60) )
                                  TablishAliasesNone
                                  ( QTableName
                                    (Range (Position 1 54 54) (Position 1 60 60))
                                    (Just (mkNormalSchema "schema_" (Range (Position 1 46 46) (Position 1 53 53))))
                                    "table_")
                                ]
                              )
                            , selectWhere = Nothing
                            , selectTimeseries = Nothing
                            , selectGroup = Nothing
                            , selectHaving = Nothing
                            , selectNamedWindow = Nothing
                            , selectDistinct = notDistinct
                            }
                    )
                )
            )

        , parse "SELECT                         col.field FROM schema_.table_" ~?= Right
        -- field access test #3
        ( PrestoStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect (Range (Position 1 0 0) (Position 1 60 60))
                        Select
                            { selectInfo = Range (Position 1 0 0) (Position 1 60 60)
                            , selectCols = SelectColumns
                                { selectColumnsInfo = Range (Position 1 31 31) (Position 1 40 40)
                                , selectColumnsList = [
                                      SelectExpr
                                      ( Range (Position 1 31 31) (Position 1 40 40) )
                                      [ ColumnAlias
                                         ( Range (Position 1 31 31) (Position 1 40 40) )
                                         "_col0" (ColumnAliasId 1)
                                      ]
                                      ( FieldAccessExpr
                                        ( Range (Position 1 31 31) (Position 1 40 40) )
                                        ( ColumnExpr (Range (Position 1 31 31) (Position 1 34 34))
                                          ( QColumnName
                                            (Range (Position 1 31 31) (Position 1 34 34))
                                            Nothing
                                            "col"))
                                        ( StructFieldName (Range (Position 1 35 35) (Position 1 40 40)) "field"))
                                      ]
                                }
                            , selectFrom = Just
                              ( SelectFrom (Range (Position 1 41 41) (Position 1 60 60))
                                [ TablishTable
                                  ( Range (Position 1 46 46) (Position 1 60 60) )
                                  TablishAliasesNone
                                  ( QTableName
                                    (Range (Position 1 54 54) (Position 1 60 60))
                                    (Just (mkNormalSchema "schema_" (Range (Position 1 46 46) (Position 1 53 53))))
                                    "table_")
                                ]
                              )
                            , selectWhere = Nothing
                            , selectTimeseries = Nothing
                            , selectGroup = Nothing
                            , selectHaving = Nothing
                            , selectNamedWindow = Nothing
                            , selectDistinct = notDistinct
                            }
                    )
                )
            )

        , parse "SELECT catalog_.schema_.table_.col.field FROM schema_.table_" ~?= Right
        -- field access test #4
        ( PrestoStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect (Range (Position 1 0 0) (Position 1 60 60))
                        Select
                            { selectInfo = Range (Position 1 0 0) (Position 1 60 60)
                            , selectCols = SelectColumns
                                { selectColumnsInfo = Range (Position 1 7 7) (Position 1 40 40)
                                , selectColumnsList = [
                                      SelectExpr
                                      ( Range (Position 1 7 7) (Position 1 40 40))
                                      [ ColumnAlias
                                        ( Range (Position 1 7 7) (Position 1 40 40))
                                        "_col0" (ColumnAliasId 1)
                                      ]
                                      ( FieldAccessExpr
                                        ( Range (Position 1 7 7) (Position 1 40 40) )
                                        ( FieldAccessExpr
                                          ( Range (Position 1 7 7) (Position 1 34 34) )
                                          ( FieldAccessExpr
                                            ( Range (Position 1 7 7) (Position 1 30 30) )
                                            ( FieldAccessExpr
                                              ( Range (Position 1 7 7) (Position 1 23 23) )
                                              ( ColumnExpr
                                                ( Range (Position 1 7 7) (Position 1 15 15) )
                                                ( QColumnName (Range (Position 1 7 7) (Position 1 15 15)) Nothing "catalog_")
                                              )
                                              ( StructFieldName (Range (Position 1 16 16) (Position 1 23 23)) "schema_" )
                                            )
                                            ( StructFieldName (Range (Position 1 24 24) (Position 1 30 30)) "table_")
                                          )
                                          ( StructFieldName (Range (Position 1 31 31) (Position 1 34 34)) "col" )
                                        )
                                        ( StructFieldName (Range (Position 1 35 35) (Position 1 40 40)) "field" )
                                      )
                                      ]
                                }
                            , selectFrom = Just
                              ( SelectFrom (Range (Position 1 41 41) (Position 1 60 60))
                                [ TablishTable
                                  ( Range (Position 1 46 46) (Position 1 60 60) )
                                  TablishAliasesNone
                                  ( QTableName
                                    (Range (Position 1 54 54) (Position 1 60 60))
                                    (Just (mkNormalSchema "schema_" (Range (Position 1 46 46) (Position 1 53 53))))
                                    "table_")
                                ]
                              )
                            , selectWhere = Nothing
                            , selectTimeseries = Nothing
                            , selectGroup = Nothing
                            , selectHaving = Nothing
                            , selectNamedWindow = Nothing
                            , selectDistinct = notDistinct
                            }
                    )
                )
            )

        , parse "SELECT * FROM table_ WHERE table_.x;" ~?= Right
          -- table aliases are in scope in WHERE clauses
          ( PrestoStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect (Range (Position 1 0 0) (Position 1 35 35))
                        Select
                            { selectInfo = Range (Position 1 0 0) (Position 1 35 35)
                            , selectCols = SelectColumns
                                { selectColumnsInfo = Range (Position 1 7 7) (Position 1 8 8)
                                , selectColumnsList =
                                    [ SelectStar
                                      (Range (Position 1 7 7) (Position 1 8 8))
                                      Nothing
                                      Unused
                                    ]
                                }
                            , selectFrom = Just
                              ( SelectFrom (Range (Position 1 9 9) (Position 1 20 20))
                                [ TablishTable
                                  ( Range (Position 1 14 14) (Position 1 20 20) )
                                  TablishAliasesNone
                                  ( QTableName
                                    (Range (Position 1 14 14) (Position 1 20 20))
                                    Nothing
                                    "table_")
                                ]
                              )
                            , selectWhere = Just
                              ( SelectWhere (Range (Position 1 21 21) (Position 1 35 35))
                                ( ColumnExpr
                                  ( Range (Position 1 27 27) (Position 1 35 35) )
                                  ( QColumnName
                                    ( Range (Position 1 34 34) (Position 1 35 35) )
                                    ( Just ( QTableName
                                             ( Range (Position 1 27 27) (Position 1 33 33) )
                                             Nothing
                                             "table_"
                                           )
                                    )
                                    "x"
                                  )
                                )
                              )
                            , selectTimeseries = Nothing
                            , selectGroup = Nothing
                            , selectHaving = Nothing
                            , selectNamedWindow = Nothing
                            , selectDistinct = notDistinct
                            }
                    )
                )
            )

        , parse "SELECT 1 FROM table_ GROUP BY table_.column_;" ~?= Right
        -- qualified column names in GROUP BY, not struct access
        ( PrestoStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect (Range (Position 1 0 0) (Position 1 44 44))
                        Select
                            { selectInfo = Range (Position 1 0 0) (Position 1 44 44)
                            , selectCols = SelectColumns
                                { selectColumnsInfo = Range (Position 1 7 7) (Position 1 8 8)
                                , selectColumnsList = [
                                      SelectExpr
                                      ( Range (Position 1 7 7) (Position 1 8 8))
                                      [ ColumnAlias
                                        ( Range (Position 1 7 7) (Position 1 8 8))
                                        "_col0" (ColumnAliasId 1)
                                      ]
                                      ( ConstantExpr
                                        ( Range (Position 1 7 7) (Position 1 8 8) )
                                        ( NumericConstant (Range (Position 1 7 7) (Position 1 8 8)) "1" )
                                      )
                                      ]
                                }
                            , selectFrom = Just
                              ( SelectFrom (Range (Position 1 9 9) (Position 1 20 20))
                                [ TablishTable
                                  ( Range (Position 1 14 14) (Position 1 20 20) )
                                  TablishAliasesNone
                                  ( QTableName
                                    (Range (Position 1 14 14) (Position 1 20 20))
                                    Nothing
                                    "table_")
                                ]
                              )
                            , selectWhere = Nothing
                            , selectTimeseries = Nothing
                            , selectGroup = Just
                              ( SelectGroup
                                ( Range (Position 1 21 21) (Position 1 44 44) )
                                [ GroupingElementExpr
                                  ( Range (Position 1 30 30) (Position 1 44 44) )
                                  ( PositionOrExprExpr
                                    ( ColumnExpr
                                      ( Range (Position 1 30 30) (Position 1 44 44) )
                                      ( QColumnName
                                        ( Range (Position 1 37 37) (Position 1 44 44) )
                                        ( Just (QTableName (Range (Position 1 30 30) (Position 1 36 36)) Nothing "table_") )
                                        "column_"
                                      )
                                    )
                                  )
                                ]
                              )
                            , selectHaving = Nothing
                            , selectNamedWindow = Nothing
                            , selectDistinct = notDistinct
                            }
                    )
                )
            )

        , parse "SELECT * FROM schema_.table_ ORDER BY table_.col.field" ~?= Right
        -- field access is handled correctly in ORDER
        ( PrestoStandardSqlStatement
            ( QueryStmt
                ( QueryOrder
                    ( Range (Position 1 0 0) (Position 1 54 54) )
                    [ Order
                        ( Range (Position 1 38 38) (Position 1 54 54) )
                        ( PositionOrExprExpr
                            ( FieldAccessExpr
                                ( Range (Position 1 38 38) (Position 1 54 54) )
                                ( ColumnExpr
                                    ( Range (Position 1 38 38) (Position 1 48 48) )
                                    ( QColumnName
                                        { columnNameInfo = Range (Position 1 45 45) (Position 1 48 48)
                                        , columnNameTable = Just
                                            ( QTableName { tableNameInfo = Range (Position 1 38 38) (Position 1 44 44)
                                                         , tableNameSchema = Nothing
                                                         , tableNameName = "table_"
                                                         }
                                            )
                                        , columnNameName = "col"
                                        }
                                    )
                                )
                                ( StructFieldName (Range (Position 1 49 49) (Position 1 54 54)) "field")
                            )
                        )
                        ( OrderAsc Nothing )
                        ( NullsAuto Nothing )
                    ]
                    ( QuerySelect
                        ( Range (Position 1 0 0) (Position 1 28 28) )
                        ( Select
                            { selectInfo = Range (Position 1 0 0) (Position 1 28 28)
                            , selectCols =
                                SelectColumns { selectColumnsInfo = Range (Position 1 7 7) (Position 1 8 8)
                                              , selectColumnsList = [SelectStar (Range (Position 1 7 7) (Position 1 8 8)) Nothing Unused]
                                              }
                            , selectFrom = Just
                                ( SelectFrom
                                    ( Range (Position 1 9 9) (Position 1 28 28) )
                                    [ TablishTable
                                        ( Range (Position 1 14 14) (Position 1 28 28) )
                                        TablishAliasesNone
                                        ( QTableName
                                            { tableNameInfo = Range (Position 1 22 22) (Position 1 28 28)
                                            , tableNameSchema = Just (mkNormalSchema "schema_" (Range (Position 1 14 14) (Position 1 21 21)))
                                            , tableNameName = "table_"
                                            }
                                        )
                                    ]
                                )
                            , selectWhere = Nothing
                            , selectTimeseries = Nothing
                            , selectGroup = Nothing
                            , selectHaving = Nothing
                            , selectNamedWindow = Nothing
                            , selectDistinct = notDistinct
                            }
                        )
                    )
                )
            )
        )

        , parse "SELECT a, rank() OVER (PARTITION BY a ORDER BY 1) FROM foo ORDER BY 1;" ~?= Right
          ( PrestoStandardSqlStatement
            ( QueryStmt
              ( QueryOrder
                ( Range (Position 1 0 0) (Position 1 69 69) )
                [ Order
                  ( Range (Position 1 68 68) (Position 1 69 69) )
                  ( PositionOrExprPosition (Range (Position 1 68 68) (Position 1 69 69)) 1 Unused )
                  ( OrderAsc Nothing )
                  ( NullsAuto Nothing )
                ]
                ( QuerySelect
                  ( Range (Position 1 0 0) (Position 1 58 58) )
                  ( Select
                    { selectInfo = Range (Position 1 0 0) (Position 1 58 58)
                    , selectCols = SelectColumns
                        { selectColumnsInfo = Range (Position 1 7 7) (Position 1 49 49)
                        , selectColumnsList =
                            [ SelectExpr
                              ( Range (Position 1 7 7) (Position 1 8 8) )
                              [ ColumnAlias (Range (Position 1 7 7) (Position 1 8 8)) "a" (ColumnAliasId 1) ]
                              ( ColumnExpr
                                ( Range (Position 1 7 7) (Position 1 8 8) )
                                ( QColumnName (Range (Position 1 7 7) (Position 1 8 8)) Nothing "a" )
                              )
                            , SelectExpr
                              ( Range (Position 1 10 10) (Position 1 49 49) )
                              [ ColumnAlias (Range (Position 1 10 10) (Position 1 49 49)) "_col1" (ColumnAliasId 2) ]
                              ( FunctionExpr
                                ( Range (Position 1 10 10) (Position 1 49 49) )
                                ( QFunctionName (Range (Position 1 10 10) (Position 1 14 14)) Nothing "rank" )
                                (Distinct False)
                                []
                                []
                                Nothing
                                ( Just
                                  ( OverWindowExpr
                                    ( Range (Position 1 17 17) (Position 1 49 49) )
                                    ( WindowExpr
                                      { windowExprInfo = Range (Position 1 17 17) (Position 1 49 49)
                                      , windowExprPartition = Just
                                          ( PartitionBy
                                            ( Range (Position 1 23 23) (Position 1 37 37) )
                                            [ ColumnExpr
                                              ( Range (Position 1 36 36) (Position 1 37 37) )
                                              ( QColumnName (Range (Position 1 36 36) (Position 1 37 37)) Nothing "a" )
                                            ]
                                          )
                                      , windowExprOrder =
                                        [ Order
                                          ( Range (Position 1 47 47) (Position 1 48 48) )
                                          ( PositionOrExprExpr
                                            ( ConstantExpr
                                              ( Range (Position 1 47 47) (Position 1 48 48) )
                                              ( NumericConstant
                                               ( Range (Position 1 47 47) (Position 1 48 48) )
                                               "1"
                                              )
                                            )
                                          )
                                          (OrderAsc Nothing)
                                          (NullsLast Nothing)
                                        ]
                                      , windowExprFrame = Nothing
                                      }))))]}
                    , selectFrom = Just
                        ( SelectFrom
                          ( Range (Position 1 50 50) (Position 1 58 58) )
                          [ TablishTable
                            ( Range (Position 1 55 55) (Position 1 58 58) )
                            TablishAliasesNone
                            ( QTableName (Range (Position 1 55 55) (Position 1 58 58)) Nothing "foo" )])
                    , selectWhere = Nothing
                    , selectTimeseries = Nothing
                    , selectGroup = Nothing
                    , selectHaving = Nothing
                    , selectNamedWindow = Nothing
                    , selectDistinct = Distinct False
                    })))))

      ]
    ]

tests :: Test
tests = test [ testParser ]
