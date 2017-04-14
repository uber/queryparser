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

module Database.Sql.Vertica.Parser.Test where

import Test.HUnit
import Test.HUnit.Ticket
import Database.Sql.Type
import Database.Sql.Position
import Database.Sql.Vertica.Parser
import Database.Sql.Vertica.Type

import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL

import qualified Data.List as L

import Control.Monad (void)

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
        , ";"
        , "SELECT foo FROM bar;"
        , "SELECT id AS foo FROM bar;"
        , "SELECT id foo FROM bar;"
        , "SELECT bar.id foo FROM bar;"
        , "SELECT 1 UNION SELECT 2;"
        , "SELECT * FROM foo INNER JOIN bar ON foo.id = bar.id;"
        , "SELECT * FROM foo JOIN bar ON foo.id = bar.id;"
        , "SELECT * FROM foo NATURAL JOIN bar;"
        , "WITH foo AS (SELECT 1) SELECT * FROM foo NATURAL JOIN bar;"
        , "SELECT f.uuid, /* comment */ count(1) FROM foo f GROUP BY 1;"
        , "SELECT f.x, count(distinct f.x) FROM foo f GROUP BY 1;"
        , "SELECT sum(x = 7) FROM foo f;"
        , "SELECT CASE WHEN x = 7 THEN 1 ELSE 0 END FROM foo f;"
        , "SELECT sum(CASE WHEN x = 7 THEN 1 ELSE 0 END) FROM foo f;"
        , "SELECT CASE 1 WHEN 2 THEN 2 WHEN NULLSEQUAL 3 THEN 5 END;"
        , "SELECT CASE 1 WHEN 2 THEN 2 WHEN NULLSEQUAL 3 THEN 5 ELSE 4 END;"
        , "SELECT 0, 0.1, .01, 1.;"
        , TL.unwords
            [ "SELECT CASE WHEN SUM(foo) = 0 then 0 ELSE"
            , "ROUND(SUM(CASE WHEN (bar = 'Yes')"
            , "THEN baz ELSE 0 END)/(SUM(qux) + .000000001), 2.0) END"
            , "AS \"Stuff. Y'know?\";"
            ]

        , "SELECT foo WHERE bar IS NULL;"
        , "SELECT foo WHERE bar IS NOT NULL;"
        , "SELECT foo WHERE bar IS TRUE;"
        , "SELECT foo WHERE bar IS UNKNOWN;"
        , TL.unlines
            [ "SELECT foo"
            , "FROM baz -- this is a comment"
            , "WHERE bar IS NOT NULL;"
            ]

        , "SELECT 1=1;"
        , "SELECT 1!=3, 1<>4;"
        , "SELECT 1<=>5;"
        , "SELECT 1>1, 1>=1, 1<1, 1<=1;"
        , "SELECT TRUE;"
        , "SELECT NULL;"
        , "SELECT COUNT(*);"
        , TL.unwords
            [ "SELECT site,date,num_hits,an_rank()"
            , "over (partition by site order by num_hits desc);"
            ]

        , TL.unwords
            [ "SELECT site,date,num_hits,an_rank()"
            , "over (partition by site, blah order by num_hits desc);"
            ]

        , "SELECT CAST(1 as int);"
        , "SELECT 1 || 2;"
        , "SELECT current_timestamp AT TIMEZONE 'America/Los_Angeles';"
        , "SELECT (DATE '2007-02-16', DATE '2007-12-21') OVERLAPS (DATE '2007-10-30', DATE '2008-10-30');"
        , "SELECT 1 IN (1, 2);"
        , "SELECT 1 NOT IN (1, 2);"
        , "SELECT LEFT('foo', 7);"
        , "SELECT ((1));"
        , "((SELECT 1));"
        , TL.unwords
            [ "SELECT name FROM \"user\""
            , "WHERE \"user\".id IN (SELECT user_id FROM whatever);"
            ]
        , "SELECT 1 WHERE EXISTS (SELECT 1);"
        , "SELECT 1 WHERE NOT EXISTS (SELECT 1);"
        , "SELECT 1 WHERE NOT NOT EXISTS (SELECT 1);"
        , "SELECT 1 WHERE NOT NOT NOT EXISTS (SELECT 1);"
        , "SELECT CURRENT_TIME;"
        , "SELECT SYSDATE;"
        , "SELECT SYSDATE();"
        , "SELECT (3 // 2) as integer_division;"
        , "SELECT 'foo' LIKE 'bar' ESCAPE '!';"
        , "SELECT 1 AS ESCAPE;"
        , "SELECT deptno, sal, empno, COUNT(*) OVER (PARTITION BY deptno ORDER BY sal ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);"
        , "INSERT INTO foo DEFAULT VALUES;"
        , "INSERT INTO foo VALUES (1,2,2+3);"
        , "INSERT INTO foo (a, b, c) SELECT * FROM baz WHERE qux > 3;"
        , "INSERT INTO foo (SELECT * FROM baz);"
        , TL.unlines
          [ "MERGE INTO foo.bar t1 USING foo.baz t2 ON t1.a = t2.a"
          , "WHEN MATCHED THEN UPDATE SET b = t2.b, c = t2.c"
          , "WHEN NOT MATCHED THEN INSERT (b, c) VALUES (t2.b, t2.c)"
          ]
        , TL.unlines
          [ "MERGE INTO foo.bar t1 USING foo.baz t2 ON t1.a = t2.a"
          , "WHEN MATCHED THEN UPDATE SET b = t2.b, c = t2.c"
          ]
        , TL.unlines
          [ "MERGE INTO foo.bar t1 USING foo.baz t2 ON t1.a = t2.a"
          , "WHEN NOT MATCHED THEN INSERT (b, c) VALUES (t2.b, t2.c)"
          ]
        , "SELECT 2 = ALL(ARRAY[1,2]);"
        , "CREATE TABLE blah AS SELECT 1;"
        , "CREATE TABLE blah AS (SELECT 1);"
        , "CREATE TABLE blah LIKE qux;"
        , "CREATE TABLE blah LIKE qux EXCLUDING PROJECTIONS;"
        , "CREATE TABLE blah (id INT);"
        , "CREATE LOCAL TEMPORARY TABLE foo (bar INT) ON COMMIT DELETE ROWS;"
        , "CREATE GLOBAL TEMPORARY TABLE foo (bar INT) ON COMMIT PRESERVE ROWS UNSEGMENTED ALL NODES;"
        , "CREATE LOCAL TEMP TABLE foo ON COMMIT PRESERVE ROWS AS (SELECT 1);"
        , "CREATE LOCAL TEMP TABLE foo LIKE qux NO PROJECTION;"
        , "CREATE TABLE foo (bar INT NULL NULL NULL NULL NULL NULL);"

        , "CREATE TABLE blah (id INT) INCLUDE SCHEMA PRIVILEGES;"
        , "CREATE TABLE blah (id INT) EXCLUDE SCHEMA PRIVILEGES;"
        , "CREATE LOCAL TEMP TABLE foo ON COMMIT PRESERVE ROWS INCLUDE SCHEMA PRIVILEGES AS (SELECT 1);"
        , "CREATE LOCAL TEMP TABLE foo ON COMMIT PRESERVE ROWS EXCLUDE SCHEMA PRIVILEGES AS (SELECT 1);"
        , "CREATE TABLE blah INCLUDE PRIVILEGES LIKE qux;"
        , "CREATE TABLE blah EXCLUDE PRIVILEGES LIKE qux;"

        , "SELECT * FROM foo AS a;"
        , "SELECT * FROM (foo JOIN bar ON baz);"
        , "SELECT 1 FROM ((foo JOIN bar ON blah) JOIN baz ON qux);"
        , "SELECT 1 FROM ((foo JOIN bar ON blah) JOIN baz ON qux) AS wat;"
        , "SELECT 1 AS (a, b);"
        , "SELECT * FROM ((SELECT 1)) as a;"
        , TL.unlines
            [ "with a as (select 1),"
            , "     b as (select 2),"
            , "     c as (select 3)"
            , "select * from ((a join b on true) as foo join c on true);"
            ]
        , "SELECT ISNULL(1, 2);"
        , "SELECT 1 NOTNULL;"
        , "SELECT 1 ISNULL;"
        , TL.unlines
            [ "CREATE TABLE foo ("
            , "  a timestamptz DEFAULT now()::timestamptz"
            , ", b long varbinary(1048960)"
            , ", c varchar(3)"
            , ", d long varchar(3)"
            , ", e long"
            , ");"
            ]
        , "CREATE TABLE blah (foo INT DEFAULT 7, bar DATE NOT NULL);"
        , TL.unlines
            [ "CREATE EXTERNAL TABLE foo.bar"
            , "(a INT, b VARCHAR(100))"
            , "AS COPY FROM 'webhdfs://host:port/path/*' ORC"
            , ";"
            ]
        , "SELECT COUNT(1 USING PARAMETERS wat=7);"
        , "SELECT (interval '1' month), interval '1 month', interval '2' day as bar;"
        , "SELECT slice_time, symbol, TS_LAST_VALUE(bid IGNORE NULLS) AS last_bid FROM TickStore;"
        , "CREATE LOCAL TEMPORARY TABLE times ON COMMIT PRESERVE ROWS AS /*+direct*/ ( SELECT slice_time as start_time, slice_time + '1 hour'::interval as end_time FROM (SELECT '2015-12-15 23:00:00'::TIMESTAMP AS d UNION SELECT '2015-12-18 03:09:55'::TIMESTAMP AS d) a TIMESERIES slice_time AS '1 hour' OVER (ORDER BY d::TIMESTAMP) ) ORDER BY start_time UNSEGMENTED ALL NODES;"
        , TL.unlines
          [ "CREATE TABLE times AS"
          , "SELECT slice_time as start_time"
          , "FROM (SELECT '2015-12-15 23:00:00'::TIMESTAMP AS d UNION SELECT '2015-12-18 03:09:55'::TIMESTAMP AS d) a"
          , "TIMESERIES slice_time AS '1 hour' OVER (ORDER BY d::TIMESTAMP)"
          , ";"
          ]
        , "SELECT +1, -2;"
        , "SELECT zone FROM foo;"
        , "SELECT foo as order FROM bar;"
        , "DELETE FROM foo;"
        , "DELETE FROM foo WHERE EXISTS (SELECT * FROM bar WHERE foo.a = bar.b);"
        , "DROP TABLE foo;"
        , "DROP TABLE IF EXISTS foo CASCADE;"
        , "BEGIN;"
        , "BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED READ WRITE;"
        , "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE;"
        , "BEGIN WORK ISOLATION LEVEL REPEATABLE READ READ ONLY;"
        , "BEGIN WORK ISOLATION LEVEL SERIALIZABLE READ ONLY;"
        , "START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY;"
        , "COMMIT;"
        , "COMMIT WORK;"
        , "COMMIT TRANSACTION;"
        , "END;"
        , "END WORK;"
        , "END TRANSACTION;"
        , "ROLLBACK;"
        , "ROLLBACK WORK;"
        , "ROLLBACK TRANSACTION;"
        , "ABORT;"
        , "ABORT WORK;"
        , "ABORT TRANSACTION;"
        , "EXPLAIN SELECT 1;"
        , "EXPLAIN DELETE FROM foo;"
        , "EXPLAIN INSERT INTO foo SELECT 1;"
        , "SELECT 1^2;"
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
        -- By the way, window frame clauses require a window order clause
        -- Named windows can also inherit from each other,
        -- similar to WITH clauses
        , TL.unlines
          [ "SELECT RANK() OVER(w1 ORDER BY sal DESC),"
          , "RANK() OVER w2"
          , "FROM EMP"
          , "WINDOW w1 AS (PARTITION BY deptno), w2 AS (w1 ORDER BY sal);"
          ]
        -- Parse Successes / Resolve Fails:
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
        , "SELECT 1" -- the `;` is optional
        , "SELECT 1\n  " -- the `;` is optional and trailing whitespace is ok
        , "WITH x AS (SELECT 1 AS a FROM dual)  SELECT * FROM X             UNION  SELECT * FROM X ORDER BY a ;"
        , "WITH x AS (SELECT 1 AS a FROM dual) (SELECT * FROM X ORDER BY a) UNION (SELECT * FROM X ORDER BY a);"
        , "WITH x AS (SELECT 1 AS a FROM dual) (SELECT * FROM X ORDER BY a) UNION (SELECT * FROM X) ORDER BY a;"
        , "WITH x AS (SELECT 1 AS a FROM dual) (SELECT * FROM X ORDER BY a) UNION  SELECT * FROM X ORDER BY a ;"
        , "WITH x AS (SELECT 1 AS a FROM dual) ((SELECT * FROM X ORDER BY a) UNION SELECT * FROM X) ORDER BY a;"

        , "CREATE TABLE foo (x TIMESTAMP WITHOUT TIME ZONE);"
        , "SELECT 1 AS OR;"
        , "SELECT 1 AS AND;"
        , "SELECT t.'a' FROM (SELECT 1 AS 'a') t;"
        ]

    , "Parse dialect-specific statements:" ~: map (TestCase . parsesSuccessfully)
        [ "CREATE PROJECTION this_projection (a, b, c) AS SELECT a, b, a*b FROM foo SEGMENTED BY HASH(a) ALL NODES KSAFE;"
        , "CREATE ACCESS POLICY ON foo FOR COLUMN bar CASE WHEN true THEN hmac(bar) ELSE bar END ENABLE;"
        , "CREATE ACCESS POLICY ON foo FOR COLUMN bar CASE WHEN true THEN hmac(bar) ELSE bar END DISABLE;"
        , "CREATE SCHEMA myschema;"
        , "CREATE SCHEMA IF NOT EXISTS myschema AUTHORIZATION foo;"
        , "ALTER TABLE foo RENAME TO bar;"
        , "ALTER TABLE foo, baz RENAME TO bar, quux;"
        , "ALTER TABLE foo SET SCHEMA other_schema;"
        , "ALTER TABLE foo SET SCHEMA other_schema RESTRICT;"
        , "ALTER TABLE foo SET SCHEMA other_schema CASCADE;"
        , "ALTER TABLE foo ADD PRIMARY KEY (bar);"
        , "ALTER TABLE foo ADD CONSTRAINT \"my_constraint\" PRIMARY KEY (bar, baz);"
        , "ALTER TABLE foo ADD FOREIGN KEY (a, b) REFERENCES bar;"
        , "ALTER TABLE foo ADD FOREIGN KEY (a, b) REFERENCES bar (c, d);"
        , "ALTER TABLE foo ADD CONSTRAINT foo_key_UK UNIQUE (foo_key);"
        , "ALTER PROJECTION foo RENAME TO bar;"
        , "CREATE RESOURCE POOL some_pool RUNTIMECAP '5 minutes';"
        , "DROP RESOURCE POOL some_pool;"
        , "ALTER RESOURCE POOL some_pool PRIORITY 5;"
        , "CONNECT TO VERTICA ExampleDB USER dbadmin PASSWORD 'Password123' ON 'VerticaHost01',5433;"
        , "CONNECT TO VERTICA ExampleDB USER dbadmin PASSWORD ******** ON 'VerticaHost01', 5433;"
        , "CONNECT TO VERTICA ExampleDB USER dbadmin PASSWORD********;"
        , "DISCONNECT ExampleDB;"
        , "SET TIME ZONE TO DEFAULT;"
        , "SET TIME ZONE TO 'PST8PDT';"
        , "SET TIME ZONE TO 'Europe/Rome';"
        , "SET TIME ZONE TO '-7';"
        , "SET TIME ZONE TO INTERVAL '-08:00 HOURS';"
        -- grants
        , "GRANT SELECT, REFERENCES ON TABLE foo TO bar WITH GRANT OPTION;", "GRANT SELECT ON ALL TABLES IN SCHEMA foo TO bar;"
        , "GRANT AUTHENTICATION v_gss to DBprogrammer;"
        , "GRANT CREATE ON DATABASE vmartdb TO Fred;"
        , "GRANT EXECUTE ON PROCEDURE tokenize(varchar) TO Bob, Jules, Operator;"
        , "GRANT USAGE ON RESOURCE POOL Joe_pool TO Joe;"
        , "GRANT appdata TO bob;"
        , "GRANT USAGE ON SCHEMA online_sales TO Joe;"
        , "GRANT ALL PRIVILEGES ON SEQUENCE my_seq TO Joe;"
        , "GRANT ALL ON LOCATION '/home/dbadmin/UserStorage/BobStore' TO Bob;"
        , "GRANT EXECUTE ON SOURCE ExampleSource() TO Alice;", "GRANT ALL ON TRANSFORM FUNCTION Pagerank(varchar) to dbadmin;"
        , "GRANT ALL PRIVILEGES ON ship TO Joe;"
        -- revokes
        , "REVOKE ALL ON TABLE foo FROM bar CASCADE;" , "REVOKE GRANT OPTION FOR ALL PRIVILEGES ON TABLE foo FROM bar CASCADE;"
        , "REVOKE AUTHENTICATION localpwd from Public;"
        , "REVOKE TEMPORARY ON DATABASE vmartdb FROM Fred;"
        , "REVOKE EXECUTE ON tokenize(varchar) FROM Bob;"
        , "REVOKE USAGE ON RESOURCE POOL Joe_pool FROM Joe;"
        , "REVOKE ADMIN OPTION FOR pseudosuperuser FROM dbadmin;"
        , "REVOKE USAGE ON SCHEMA online_sales FROM Joe;"
        , "REVOKE ALL PRIVILEGES ON SEQUENCE my_seq FROM Joe;"
        , "REVOKE ALL ON LOCATION '/home/dbadmin/UserStorage/BobStore' ON v_mcdb_node0007 FROM Bob;"
        , "REVOKE ALL ON TRANSFORM FUNCTION Pagerank (float) FROM Doug;"
        , "REVOKE ALL PRIVILEGES ON ship FROM Joe;"
        -- functions
        , "CREATE OR REPLACE FUNCTION Add2Ints AS LANGUAGE 'C++' NAME 'Add2IntsFactory' LIBRARY ScalarFunctions;"
        , TL.unwords
          [ "CREATE FUNCTION myzeroifnull(x INT) RETURN INT"
          , "AS BEGIN"
          , "RETURN (CASE WHEN (x IS NOT NULL) THEN x ELSE 0 END);"
          , "END;"
          ]
        , "CREATE OR REPLACE TRANSFORM FUNCTION transFunct AS LANGUAGE 'C++' NAME 'myFactory' LIBRARY myFunction;"
        , "CREATE OR REPLACE ANALYTIC FUNCTION an_rank AS LANGUAGE 'C++' NAME 'RankFactory' LIBRARY AnalyticFunctions;"
        , "CREATE OR REPLACE AGGREGATE FUNCTION ag_avg AS LANGUAGE 'C++' NAME 'AverageFactory' LIBRARY AggregateFunctions;"
        , "CREATE OR REPLACE FILTER Iconverter AS LANGUAGE 'C++' NAME 'IconverterFactory' LIBRARY IconverterLib;"
        , "CREATE OR REPLACE PARSER BasicIntegerParser AS LANGUAGE 'C++' NAME 'BasicIntegerParserFactory' LIBRARY BasicIntegerParserLib;"
        , "CREATE OR REPLACE SOURCE curl AS LANGUAGE 'C++' NAME 'CurlSourceFactory' LIBRARY curllib;"
        , "SHOW search_path;"
        -- EXPORT
        , "EXPORT TO STDOUT FROM foo (a, b);"  -- back half of COPY FROM VERTICA, not actually runnable but appears in logs
        , TL.unlines
          [ "COPY public.foo (\"a\", \"b\")"
          , "FROM VERTICA VerticaDB.public.bar (\"c\", \"d\")"
          , "DIRECT"
          , ";"
          ]
        , TL.unlines
          [ "COPY public.foo (\"a\", \"b\")"
          , "FROM VERTICA VerticaDB.public.bar (\"c\", \"d\")"
          , "AUTO"
          , "STREAM NAME 'foobar'"
          , "NO COMMIT"
          , ";"
          ]
        , "COPY public.foo (\"a\", \"b\") FROM VERTICA VerticaDB.public.bar (\"c\", \"d\") TRICKLE;"
        , "COPY public.foo (\"a\", \"b\") FROM VERTICA VerticaDB.public.bar (\"c\", \"d\") AUTO;"
        , "COPY public.foo (a, b AS func(arg1, arg2 || '.' || arg3)) SOURCE HDFS(url='');"
        , TL.unlines  -- same options, different order
          [ "COPY public.foo(a, t as '2016-10-09T17-13-14.594')"
          , "REJECTMAX 4096"
          , "SOURCE HDFS(url='http://host:port/webhdfs/v1/path/*')"
          , "ENCLOSED BY '\"'"
          , "DIRECT"
          , "SKIP 1"
          , "DELIMITER E'\037'"
          , "null as 'null'"
          , "RECORD TERMINATOR E'\036\012'"
          , ";"
          ]
        , TL.unlines
          [ "COPY public.foo (a, t as '2016-10-14T07-00-23.582')"
          , "SOURCE HDFS(url='http://host:port/webhdfs/v1/path/*')"
          , "DELIMITER E'\\037'"
          , "RECORD TERMINATOR E'\\036\\012'"
          , ";"
          ]
        , "SELECT * FROM foo INNER JOIN bar USING (a);"
        , "SELECT datediff(qq, 'a', 'b');"
        , "SELECT datediff('qq', 'a', 'b');"
        , "SELECT datediff((SELECT 'qq'), 'a', 'b');"
        , "SELECT cast('1 minutes' AS INTERVAL MINUTE);"
        , "SELECT date_trunc('week', foo.at) FROM foo;"
        , "SELECT foo::TIME FROM bar;"
        , "CREATE VIEW foo.bar AS SELECT * FROM foo.baz;"
        , "CREATE LOCAL TEMPORARY VIEW bar AS SELECT * FROM baz;"
        , TL.unlines
          [ "CREATE OR REPLACE VIEW foo.bar (a, b) "
          , "INCLUDE SCHEMA PRIVILEGES "
          , "AS SELECT * FROM foo.baz;"
          ]
        , TL.unlines
          [ "CREATE OR REPLACE LOCAL TEMP VIEW bar (a, b) "
          , "AS SELECT * FROM baz;"
          ]
        , "DROP VIEW foo.bar;"
        , "DROP VIEW IF EXISTS foo.bar;"
        , TL.unlines
          [ "CREATE TABLE IF NOT EXISTS foo.bar ("
          , "    a VARCHAR(128) NOT NULL"
          , "  , b TIMESTAMP(6) NOT NULL"
          , "  , c DOUBLE PRECISION"
          , "  , PRIMARY KEY(a, b)"
          , ")"
          ]
        ]

    , let options = [ "SOURCE HDFS(url='http://host:port/webhdfs/v1/path/*')"
                    , "null as 'null'"
                    , "DIRECT"
                    , "ENCLOSED BY '\"'"
                    ]
          perms = L.permutations options
          buildSql os = TL.unlines $ ["COPY public.foo(a, t as '2016-10-09T17-13-14.594')"] ++ os ++ [";"]
          sqls = map buildSql perms
       in "Test the consumeUnorderedOptions behavior" ~: map (TestCase . parsesSuccessfully) sqls

    , "Exclude some broken examples" ~: map (TestCase . parsesUnsuccessfully)
        [ "SELECT CURRENT_TIME();"
        -- TODO - this should not parse but currently does: , "SELECT 1 ESCAPE;"
        , "SELECT LOCALTIMESTAMP();"
        , "SELECT 'foo' ~~ 'bar' ESCAPE '!';"
        , "SELECT datediff(x, '1977-05-25', now());"
        , "SELECT datediff('a', 'b');"
        , "SELECT * FROM (foo);"
        , "SELECT 1 == 2;"
        , "CREATE TABLE foo (GROUPED (a, b));"
        , "CREATE TABLE foo LIKE qux NO PROJECTION;"
        , "SELECT * FROM (SELECT 1);"
        , "SELECT * FROM ((SELECT 1) as a);"
        , "SELECT * FROM (foo) as a;"
        , "CREATE TABLE foo (bar INT NULL NOT NULL);"
        , "SELECT 1 (a, b);"
        , "ALTER TABLE a, b RENAME TO c;"
        , "ALTER TABLE foo SET SCHEMA other_schema CASCADE RESTRICT;"
        , "GRANT;"
        , "REVOKE;"
        , "SHOW;"
        , "EXPLAIN TRUNCATE foo;"
        -- Named window must have partition, may have order, cannot have frame
        , TL.unlines
          [ "SELECT RANK() OVER (x) FROM potato"
          , "WINDOW x AS (ORDER BY b);"
          ]
        , TL.unlines
          [ "SELECT RANK() OVER x FROM potato"
          , "WINDOW x AS (PARTITION BY a"
          , "RANGE UNBOUNDED PRECEDING AND CURRENT ROW);"
          ]
        , "INSERT INTO foo VALUES (1,2), (3,4);"
        , "" -- the only way to write the empty statement is `;`
        , "\n" -- the only way to write the empty statement is `;`
        , TL.unlines
          [ "COPY public.foo(a, t as '2016-10-09T17-13-14.594')"
          , "SOURCE HDFS(url='http://host:port/webhdfs/v1/path/*')"
          , "null as 'null'"
          , "DIRECT"
          , "DIRECT" -- the options may not be repeated
          , "ENCLOSED BY '\"'"
          , "SKIP 1"
          , "DELIMITER E'\037'"
          , "RECORD TERMINATOR E'\036\012'"
          , "REJECTMAX 4096"
          , ";"
          ]
        , "SELECT 1 OR;"
        , "SELECT 1 AND;"
        , "CREATE LOCAL TEMPORARY VIEW foo.bar AS SELECT * FROM baz;" -- local temp views must have UQTableName
        , "CREATE LOCAL TEMPORARY VIEW bar INCLUDE SCHEMA PRIVILEGES AS SELECT * FROM baz;" -- local temp views
        -- may not have 'include/exclude schema privileges'
        , "WITH x AS (SELECT 1 AS a FROM dual)  SELECT * FROM X ORDER BY a  UNION  SELECT * FROM X ORDER BY a ;"
        , "WITH x AS (SELECT 1 AS a FROM dual)  SELECT * FROM X ORDER BY a  UNION  SELECT * FROM X            ;"
        , "MERGE INTO foo.bar t1 USING foo.baz t2 ON t1.a = t2.a"
        ]

    , ticket "T275138"
        [ parsesUnsuccessfully "SELECT ALL(ARRAY[1,2]) = 2;"
        , parsesUnsuccessfully "SELECT 'foo' LIKE ANY(ARRAY['foo']) ESCAPE 'o';"
        ]

    , ticket "T293415"
        [ parsesSuccessfully "CREATE TABLE foo (GROUPED (a, b)) AS SELECT 1, 2;"
        ]

    , ticket "T293527"
        [ parsesUnsuccessfully "CREATE LOCAL TEMP TABLE foo AS (SELECT 1) NO PROJECTION;"
        , parsesUnsuccessfully "CREATE LOCAL TEMP TABLE foo LIKE qux KSAFE 3 NO PROJECTION;"
        ]

    , ticket "T295049"
        [ assert $
            void (parse "SELECT slice_time, symbol, TS_LAST_VALUE(bid IGNORE NULLS) AS last_bid FROM TickStore;")
                /= void (parse "SELECT slice_time, symbol, TS_LAST_VALUE(bid) AS last_bid FROM TickStore;")
        ]

    , ticket "T397318"
        [ parsesUnsuccessfully "GRANT nonexistentOption ON TABLE foo TO bar;"
        , parsesUnsuccessfully "REVOKE nonexistentOption ON TABLE foo FROM bar;"
        , parsesUnsuccessfully "REVOKE SELECT ON TABLE foo FROM bar baz quux;"
        , parsesUnsuccessfully "REVOKE REVOKE REVOKE REVOKE;"
        ]

    , "Parse exactly" ~:

        [ parse ";" ~?= Right
            (VerticaStandardSqlStatement (EmptyStmt (Range (Position 1 0 0) (Position 1 1 1))))

        , parse "SELECT 1" ~?= Right
          -- semicolon is optional
            ( VerticaStandardSqlStatement
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
                                         "?column?" (ColumnAliasId 1)
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


        , parse "SELECT foo FROM bar INNER JOIN baz ON 'true';" ~?= Right
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect (Range (Position 1 0 0) (Position 1 44 44))
                        Select
                            { selectInfo = Range (Position 1 0 0) (Position 1 44 44)
                            , selectDistinct = notDistinct
                            , selectCols = SelectColumns
                                (Range (Position 1 7 7) (Position 1 10 10))
                                [ SelectExpr
                                    (Range (Position 1 7 7) (Position 1 10 10))
                                    [ColumnAlias (Range (Position 1 7 7) (Position 1 10 10)) "foo" (ColumnAliasId 1)]
                                    (ColumnExpr (Range (Position 1 7 7)
                                                       (Position 1 10 10))
                                            (QColumnName
                                                (Range (Position 1 7 7) (Position 1 10 10))
                                                Nothing
                                                "foo"))
                                ]

                            , selectFrom = Just $ SelectFrom (Range (Position 1 11 11)
                                                                    (Position 1 44 44))
                                [ TablishJoin (Range (Position 1 16 16)
                                                     (Position 1 44 44))

                                    (JoinInner (Range (Position 1 20 20)
                                                      (Position 1 30 30)))

                                    (JoinOn (ConstantExpr (Range (Position 1 38 38)
                                                                 (Position 1 44 44))
                                                          (StringConstant (Range (Position 1 38 38)
                                                                                 (Position 1 44 44))
                                                                          "true")))
                                    (TablishTable
                                        (Range (Position 1 16 16)
                                               (Position 1 19 19))
                                        TablishAliasesNone
                                        (QTableName
                                            (Range (Position 1 16 16) (Position 1 19 19))
                                            Nothing
                                            "bar"))

                                    (TablishTable
                                        (Range (Position 1 31 31)
                                               (Position 1 34 34))
                                        TablishAliasesNone
                                        (QTableName
                                            (Range (Position 1 31 31) (Position 1 34 34))
                                            Nothing
                                            "baz"))
                                ]
                            , selectWhere = Nothing
                            , selectTimeseries = Nothing
                            , selectGroup = Nothing
                            , selectHaving = Nothing
                            , selectNamedWindow = Nothing
                            }
                    )
                )
            )
        , parse "SELECT 1 LIMIT 1;" ~?= Right
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QueryLimit
                        ( Range (Position 1 0 0) (Position 1 16 16) )
                        ( Limit
                            ( Range (Position 1 9 9) (Position 1 16 16) )
                            "1"
                        )
                        ( QuerySelect
                            ( Range (Position 1 0 0) (Position 1 8 8) )
                            ( Select
                                { selectInfo = Range (Position 1 0 0) (Position 1 8 8)
                                , selectCols = SelectColumns
                                    { selectColumnsInfo = Range (Position 1 7 7) (Position 1 8 8)
                                    , selectColumnsList =
                                        [ SelectExpr
                                            ( Range (Position 1 7 7) (Position 1 8 8) )
                                            [ ColumnAlias
                                                ( Range (Position 1 7 7) (Position 1 8 8) )
                                                "?column?"
                                                ( ColumnAliasId 1 )
                                            ]
                                            ( ConstantExpr
                                                ( Range (Position 1 7 7) (Position 1 8 8) )
                                                ( NumericConstant
                                                    ( Range (Position 1 7 7) (Position 1 8 8) )
                                                    "1"
                                                )
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
                )
            )

        , parse "(SELECT 1 LIMIT 0) UNION (SELECT 2 LIMIT 1) LIMIT 1;" ~?= Right
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QueryLimit
                        ( Range (Position 1 19 19) (Position 1 51 51) )
                        ( Limit
                            ( Range (Position 1 44 44) (Position 1 51 51) )
                            "1"
                        )
                        ( QueryUnion
                            ( Range (Position 1 19 19) (Position 1 24 24) )
                            ( Distinct True )
                            Unused
                            ( QueryLimit
                                ( Range (Position 1 1 1) (Position 1 17 17) )
                                ( Limit
                                    ( Range (Position 1 10 10) (Position 1 17 17) )
                                    "0"
                                )
                                ( QuerySelect
                                    ( Range (Position 1 1 1) (Position 1 9 9) )
                                    ( Select
                                        { selectInfo = Range (Position 1 1 1) (Position 1 9 9)
                                        , selectCols = SelectColumns
                                            { selectColumnsInfo = Range (Position 1 8 8) (Position 1 9 9)
                                            , selectColumnsList =
                                                [ SelectExpr
                                                    ( Range (Position 1 8 8) (Position 1 9 9) )
                                                    [ ColumnAlias
                                                        ( Range (Position 1 8 8) (Position 1 9 9) )
                                                        "?column?"
                                                        ( ColumnAliasId 1 )
                                                    ]
                                                    ( ConstantExpr
                                                        ( Range (Position 1 8 8) (Position 1 9 9) )
                                                        ( NumericConstant
                                                            ( Range (Position 1 8 8) (Position 1 9 9) )
                                                            "1"
                                                        )
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
                            ( QueryLimit
                                ( Range (Position 1 26 26) (Position 1 42 42) )
                                ( Limit
                                    ( Range (Position 1 35 35) (Position 1 42 42) )
                                    "1"
                                )
                                ( QuerySelect
                                    ( Range (Position 1 26 26) (Position 1 34 34) )
                                    ( Select
                                        { selectInfo = Range (Position 1 26 26) (Position 1 34 34)
                                        , selectCols = SelectColumns
                                            { selectColumnsInfo = Range (Position 1 33 33) (Position 1 34 34)
                                            , selectColumnsList =
                                                [ SelectExpr
                                                    ( Range (Position 1 33 33) (Position 1 34 34) )
                                                    [ ColumnAlias
                                                        ( Range (Position 1 33 33) (Position 1 34 34) )
                                                        "?column?"
                                                        ( ColumnAliasId 2 )
                                                    ]
                                                    ( ConstantExpr
                                                        ( Range (Position 1 33 33) (Position 1 34 34) )
                                                        ( NumericConstant
                                                            ( Range (Position 1 33 33) (Position 1 34 34) )
                                                            "2"
                                                        )
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
                        )
                    )
                )
            )

        , parse "SELECT a, rank() OVER (PARTITION BY a ORDER BY 1) FROM foo ORDER BY 1;" ~?= Right
          ( VerticaStandardSqlStatement
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
                              [ ColumnAlias (Range (Position 1 10 10) (Position 1 49 49)) "rank" (ColumnAliasId 2) ]
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

        , parse "SELECT created_at AT TIMEZONE 'PST' > now();" ~?= Right
          ( VerticaStandardSqlStatement
            ( QueryStmt
              ( QuerySelect
                ( Range (Position 1 0 0) (Position 1 37 37) )
                ( Select
                  { selectInfo = Range (Position 1 0 0) (Position 1 37 37)
                  , selectCols = SelectColumns
                    { selectColumnsInfo = Range (Position 1 36 36) (Position 1 37 37)
                    , selectColumnsList =
                      [ SelectExpr
                        ( Range (Position 1 36 36) (Position 1 37 37) )
                        [ ColumnAlias
                          ( Range (Position 1 36 36) (Position 1 37 37) )
                          "?column?"
                          ( ColumnAliasId 1 )
                        ]
                        ( BinOpExpr
                          ( Range (Position 1 36 36) (Position 1 37 37))
                          ( Operator ">")
                          ( AtTimeZoneExpr
                            ( Range (Position 1 7 7) (Position 1 35 35) )
                            ( ColumnExpr
                              ( Range (Position 1 7 7) (Position 1 17 17) )
                              ( QColumnName
                                { columnNameInfo = Range (Position 1 7 7) (Position 1 17 17)
                                , columnNameTable = Nothing, columnNameName = "created_at"
                                }
                              )
                            )
                            ( ConstantExpr
                                ( Range (Position 1 30 30) (Position 1 35 35) )
                                ( StringConstant
                                  ( Range (Position 1 30 30) (Position 1 35 35) )
                                  "PST"
                                )
                            )
                          )
                          ( FunctionExpr
                            ( Range (Position 1 38 38) (Position 1 43 43) )
                            ( QFunctionName
                              { functionNameInfo = Range (Position 1 38 38) (Position 1 41 41)
                              , functionNameSchema = Nothing
                              , functionNameName = "now"
                              }
                            )
                            (Distinct False)
                            []
                            []
                            Nothing
                            Nothing
                          )
                        )
                      ]
                    }
                  , selectFrom = Nothing
                  , selectWhere = Nothing
                  , selectTimeseries = Nothing
                  , selectGroup = Nothing
                  , selectHaving = Nothing
                  , selectNamedWindow = Nothing
                  , selectDistinct = Distinct False
                  }
                )
              )
            )
          )
        ]
    ]

tests :: Test
tests = test [ testParser ]
