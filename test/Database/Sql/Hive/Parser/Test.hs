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

module Database.Sql.Hive.Parser.Test where

import Test.HUnit hiding (State)
import Test.HUnit.Ticket
import Database.Sql.Type
import Database.Sql.Info
import Database.Sql.Position
import Database.Sql.Hive.Parser
import Database.Sql.Hive.Type
import Database.Sql.Util.Test (makeSelect)

import Data.List.NonEmpty (NonEmpty(..))
import Data.Monoid
import Data.Int (Int64)
import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL
import           Control.Monad.State

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

-- "SELECT 1;"
parseExactHelperSelect1 :: Int64 -> Query RawNames Range
parseExactHelperSelect1 offset =
    let o7 = offset + 7
        o8 = offset + 8
    in
    (QuerySelect
     (Range (Position 1 offset offset) (Position 1 o8 o8))
     (Select
      { selectInfo =
        (Range (Position 1 offset offset) (Position 1 o8 o8))
      , selectCols =
        (SelectColumns
         (Range (Position 1 o7 o7) (Position 1 o8 o8))
         [SelectExpr
          (Range (Position 1 o7 o7) (Position 1 o8 o8))
          [ColumnAlias
           (Range (Position 1 o7 o7) (Position 1 o8 o8))
           "_c0"
           (ColumnAliasId 1)
          ]
          (ConstantExpr
           (Range (Position 1 o7 o7) (Position 1 o8 o8))
           (NumericConstant
            (Range (Position 1 o7 o7) (Position 1 o8 o8))
            "1"
           )
          )
         ]
        )
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

parseExactHelperStar :: State Position (SelectColumns RawNames Range)
parseExactHelperStar = do
    star <- do
        s <- get
        modify $ advance "*"
        e <- get
        pure $ SelectStar (Range s e) Nothing Unused
    pure (SelectColumns (getInfo star) [star])

parseExactHelperTablePotato :: State Position (Tablish RawNames Range)
parseExactHelperTablePotato = do
    table <- do
        s <- get
        modify $ advance "potato"
        e <- get
        pure $ QTableName (Range s e) Nothing "potato"
    pure $ TablishTable (getInfo table) TablishAliasesNone table

parseExactHelperFromTablePotato :: State Position (SelectFrom RawNames Range)
parseExactHelperFromTablePotato = do
    s <- get
    modify $ advance "FROM "
    table <- parseExactHelperTablePotato
    e <- get
    pure $ SelectFrom (Range s e) [table]

-- "SELECT * FROM potato;"
parseExactHelperSelectStar :: State Position (Query RawNames Range)
parseExactHelperSelectStar = do
    select <- do
        s <- get
        modify $ advance "SELECT "
        star <- parseExactHelperStar
        modify $ advance " "
        from <- parseExactHelperFromTablePotato
        e <- get
        let selectInfo = Range s e
            selectDistinct = notDistinct
            selectCols = star
            selectFrom = Just from
            selectWhere = Nothing
            selectTimeseries = Nothing
            selectGroup = Nothing
            selectHaving = Nothing
            selectNamedWindow = Nothing

        pure Select{..}
    pure (QuerySelect (getInfo select) select)


testInvertedFrom :: Test
testInvertedFrom = test
    [ "Test allowed FROM inversions" ~: map (TestCase . parsesSuccessfully)
        [ "FROM foo SELECT *;"
        , "FROM foo INSERT INTO bar SELECT *;"
        , "WITH foo AS (SELECT * FROM bar) FROM foo SELECT *;"
        , "WITH foo AS (FROM bar SELECT *) FROM foo SELECT *;"
        , "WITH foo AS (SELECT * FROM bar) FROM foo INSERT INTO baz SELECT *;"
        , "WITH foo AS (SELECT * FROM bar) INSERT INTO baz SELECT * FROM foo;"
        , "INSERT INTO foo (col1, col2) SELECT * FROM bar;"
        , "INSERT INTO foo (col1, col2) SELECT * FROM bar UNION SELECT * FROM baz;"

        , "FROM bar INSERT OVERWRITE DIRECTORY 'hdfs://thingy' SELECT *;"
        , "INSERT OVERWRITE DIRECTORY 'hdfs://thingy' SELECT * FROM bar;"
        , "INSERT OVERWRITE DIRECTORY 'hdfs://thingy' SELECT * FROM bar UNION SELECT * FROM baz;"

        , "FROM foo INSERT OVERWRITE bar SELECT *;"
        , "SELECT * FROM (FROM foo SELECT *) bar;"
        ]

    , "Test forbidden FROM inversions" ~: map (TestCase . parsesUnsuccessfully)
        [ "INSERT INTO foo FROM bar SELECT *;"
        , "FROM foo INSERT INTO bar FROM foo SELECT *;"
        , "FROM foo INSERT INTO bar SELECT * FROM foo;"
        , "FROM foo DELETE;"
        , "FROM foo DELETE WHERE a is not null;"
        , "INSERT INTO foo (SELECT * FROM bar);"
        , "INSERT INTO foo (FROM bar SELECT *);"
        , "FROM foo WITH foo AS (SELECT * FROM bar) SELECT *;"
        , "INSERT INTO foo (col1, col2) FROM bar SELECT *;"

        , "FROM bar INSERT INTO foo (col1, col2) SELECT * UNION ALL SELECT * FROM baz;"
        , "FROM bar INSERT OVERWRITE DIRECTORY 'hdfs://thingy' SELECT * UNION ALL SELECT * FROM baz;"

        , "CREATE TABLE foo AS FROM bar SELECT *;"
        , "SELECT 1 FROM foo WHERE EXISTS (FROM bar SELECT a WHERE foo.a=bar.a);"
        , "SELECT 1 FROM foo WHERE a IN (FROM bar SELECT a);"
        ]

    , "Test allowed FROM inversion for UNIONs in particular" ~: map (TestCase . parsesSuccessfully)
        [ "FROM foo SELECT 1 UNION ALL      FROM foo SELECT 2;"

        , "SELECT 1 FROM foo UNION     SELECT 2 FROM foo;"
        , "SELECT 1 FROM foo UNION ALL SELECT 2 FROM foo;"

        -- The following are permitted. Note: the FROM only extends to the
        -- first SELECT, not the second. So the first SELECT could return many
        -- rows but the second SELECT returns only one row.
        , "FROM foo SELECT 1 UNION     SELECT 2;"
        , "FROM foo SELECT 1 UNION ALL SELECT 2;"

        -- The following will parse, but will fail to resolve.
        , "FROM foo SELECT * UNION ALL SELECT *;"
        ]

    , ticket "T539933"
        [ -- why are these forbidden but UNION ALL allowed? I dunno, Hive is cray.
          parsesUnsuccessfully "FROM foo SELECT 1 UNION          FROM foo SELECT 2;"
        , parsesUnsuccessfully "FROM foo SELECT 1 UNION DISTINCT FROM foo SELECT 2;"
          -- why can't you mix/match? IDK.
        , parsesUnsuccessfully "FROM foo SELECT 1 UNION ALL SELECT 2 FROM foo;"
        , parsesUnsuccessfully "SELECT 1 FROM foo UNION ALL FROM foo SELECT 2;"
        ]
    ]

testParser_hiveSuite :: Test
testParser_hiveSuite = test
    [ "Parse against exhaustive HiveQL definition" ~: map (TestCase . parsesSuccessfully)
      [ "SELECT 1;"
      , "SELECT * FROM t1;"
      , "SELECT current_database();"
      , "USE database_name;"
      , "SELECT * FROM sales WHERE amount > 10 AND region = \"US\";"
      , "SELECT\n1;"
      , "SELECT  -- I'm a comment!\n1;"
      , "SELECT * FROM t1;"
      , "SELECT 1 as FROM foo;" -- here, `as` is the column alias.
      , "SELECT 1 FROM foo AS localtime;"
      , TL.unlines
        [ "CREATE TABLE IF NOT EXISTS foo.bar"
        , "PARTITIONED BY (baz string)"
        , "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'"
        , "STORED AS avro"
        , "TBLPROPERTIES ('avro.schema.url'='hdfs://thingy')"
        , ";"
        ]
      , "SELECT current_database();"
      , "SELECT * FROM sales WHERE amount > 10 AND region = \"US\";"
      , "SELECT my_col from `my_table`;"
      , "select count(*) from `campaigns`;"
      , "SELECT /*+ label */ my_col from my_table;"
      , TL.unlines
            [ "-- comment"
            , "SELECT"
            , "1 -- comment;"
            , ";"
            ]
      -- TODO add Load statements
      -- Test for overwrite keyword
      , "INSERT OVERWRITE TABLE potato SELECT * FROM tomato;"
      , "INSERT OVERWRITE TABLE foo.bar VALUES ('foo', 1, 2), ('bar', 3, 4);"
      , "SELECT * FROM potato WHERE datestr >= date_sub(current_timestamp(), 31);"
      , "ANALYZE TABLE potato COMPUTE STATISTICS;"
      , "ANALYZE TABLE potato COMPUTE STATISTICS FOR COLUMNS CACHE METADATA NOSCAN;"
      , "ANALYZE TABLE potato PARTITION (soup) COMPUTE STATISTICS;"
      , "SHOW COLUMNS FROM foo;"
      , "INSERT INTO etltmp.potato SELECT * FROM tomato;"
      , "INSERT INTO etltmp.potato (col1, col2) SELECT * FROM tomato;"
      , "INSERT OVERWRITE etltmp.potato SELECT * FROM tomato;"
      , "INSERT OVERWRITE DIRECTORY \"/tmp/foo/bar\" SELECT * FROM potato;"
      , "INSERT OVERWRITE LOCAL DIRECTORY \"/tmp/foo/bar\" SELECT * FROM potato;"
      , "LOAD DATA INPATH '/path'           INTO TABLE foo;"
      , "LOAD DATA INPATH '/path' OVERWRITE INTO TABLE foo;"
      , "LOAD DATA INPATH '/path'           INTO TABLE foo PARTITION (col='val');"
      , "LOAD DATA INPATH '/path' OVERWRITE INTO TABLE foo PARTITION (col='val');"
      , "LOAD DATA LOCAL INPATH '/path' OVERWRITE INTO TABLE foo PARTITION (col='val', col2='val2');"
      , "WITH foo AS (SELECT 1) INSERT OVERWRITE DIRECTORY \"/tmp/foo/bar\" SELECT * FROM foo;"
      , "WITH foo AS (SELECT 1) INSERT OVERWRITE LOCAL DIRECTORY \"/tmp/foo/bar\" SELECT * FROM foo;"
      , "WITH foo AS (SELECT * FROM potato) INSERT INTO TABLE bar SELECT * FROM foo;"
      , "WITH foo AS (SELECT * FROM potato) INSERT OVERWRITE TABLE bar SELECT * FROM foo;"
      , "WITH foo AS (SELECT * FROM potato) INSERT INTO bar SELECT * FROM foo;"
      , "WITH foo AS (SELECT * FROM potato) INSERT OVERWRITE bar SELECT * FROM foo;"
      , TL.unlines
        [ "INSERT OVERWRITE DIRECTORY '/some/path'"
        , "ROW FORMAT DELIMITED"
        , "FIELDS TERMINATED BY '\t'"
        , "SELECT * FROM foo;"
        ]
      , "CREATE TABLE potato STORED AS ORC AS SELECT * FROM tomato;"
      , "CREATE TABLE potato STORED AS SEQUENCEFILE AS SELECT * FROM tomato;"
      , "CREATE TABLE potato STORED AS TEXTFILE AS SELECT * FROM tomato;"
      , "CREATE TABLE potato STORED AS RCFILE AS SELECT * FROM tomato;"
      , "CREATE TABLE potato STORED AS PARQUET AS SELECT * FROM tomato;"
      , "CREATE TABLE potato STORED AS AVRO AS SELECT * FROM tomato;"
      , TL.unlines
        [ "CREATE TABLE potato STORED AS"
        , "INPUTFORMAT \"com.hadoop.mapred.DeprecatedLzoTextInputFormat\""
        , "OUTPUTFORMAT \"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\""
        , "AS SELECT * FROM tomato;"
        ]
      , "CREATE TABLE potato TBLPROPERTIES(\'EXTERNAL\'=\'FALSE\') AS SELECT 1"
      , "CREATE TABLE potato STORED AS PARQUET LOCATION 'foo' AS SELECT * FROM tomato;"
      , "ALTER TABLE foo SET LOCATION 'hdfs://thingy';"
      , "ALTER TABLE foo PARTITION (bar='2015-10-15') SET LOCATION 'hdfs://thingy';"
      , "ALTER TABLE foo ADD PARTITION (bar='2015-10-15');"
      , TL.unwords
          [ "ALTER TABLE foo ADD IF NOT EXISTS"
          , "PARTITION (bar='2015-10-15',baz='asdf') LOCATION 'hdfs://foo/bar1'"
          , "PARTITION (bar='2015-10-16',baz='hjkl') LOCATION 'hdfs://foo/bar2'"
          , ";"
          ]
      , "ALTER TABLE foo DROP PARTITION (bar='2015-10-15');"
      , TL.unwords
          [ "ALTER TABLE foo DROP IF EXISTS"
          , "PARTITION (bar='2015-10-15')"
          , "PARTITION (bar='2015-10-16')"
          , "IGNORE PROTECTION"
          , "PURGE"
          , ";"
          ]
      , "ALTER TABLE foo.bar RENAME TO baz.quux;"
      , "INSERT INTO TABLE foo PARTITION (potato=1) SELECT * FROM bar;"
      , "INSERT INTO TABLE foo PARTITION (potato) SELECT * FROM bar;"
      , "INSERT INTO TABLE foo PARTITION (potato=1, soup=2) SELECT * FROM bar;"
      , "INSERT INTO TABLE foo PARTITION (potato=1, soup) SELECT * FROM bar;"
      , "INSERT INTO TABLE foo PARTITION (potato, soup) SELECT * FROM bar;"
      , "SELECT s.t.c.x0.x1 FROM s.t;"
      , "SELECT t.c.x0.x1 FROM t;"
      , "SELECT c.x0.x1 FROM t;"
      , "SELECT t.c.x0 from potato t;"
      , "EXPLAIN SELECT 1;"
      , "EXPLAIN DELETE FROM foo;"
      , "EXPLAIN INSERT INTO foo SELECT 1;"
      , "SELECT * FROM foo INNER JOIN bar;"
      , "SELECT * FROM foo JOIN bar;"
      , "SELECT * FROM foo LEFT JOIN bar;"
      , "SELECT * FROM foo RIGHT JOIN bar;"
      , "SELECT * FROM foo FULL OUTER JOIN bar;"
      , "SELECT * FROM foo CROSS JOIN bar;"
      , "SELECT datediff('a', 'b');"
      , "SELECT datediff('a', current_timestamp());"
      , "SELECT * FROM potato ca WHERE USER.uuid IS NOT NULL;"
      , "SELECT 'asdf' RLIKE '^[0-9]*$';"
      , "SELECT 'asdf' REGEXP '^[0-9]*$';"
      , "SELECT 'asdf' ! RLIKE '^[0-9]*$';"
      , "SELECT * FROM potato CLUSTER BY a;"
      , "SELECT * FROM potato DISTRIBUTE BY a;"
      , "SELECT * FROM potato SORT BY a;"
      , "SELECT * FROM potato DISTRIBUTE BY a SORT BY b;"
      , "SELECT * FROM foo CROSS JOIN bar ON foo.a = bar.a;"
      , "SELECT * FROM foo LEFT SEMI JOIN bar;"
      , "SELECT * FROM foo LEFT SEMI JOIN bar ON (foo.id = bar.id);"
      , "SELECT * FROM foo LATERAL VIEW explode(a) exploded AS x;"
      , "SELECT bar.col FROM foo LATERAL VIEW explode(foo.array_col) bar;"
      , "SELECT bar.key, bar.val FROM foo LATERAL VIEW explode(foo.map_col) bar;"
      , "SELECT bar.col1, bar.col2 FROM foo LATERAL VIEW inline(foo.array_of_struct_col) bar;"
      , "SELECT bar.c0, bar.c1, bar.c2 FROM foo LATERAL VIEW json_tuple(foo.json_col, k1, k2, k3) bar;"
      , "SELECT bar.c0, bar.c1, bar.c2 FROM foo LATERAL VIEW parse_url_tuple(foo.url_col, 'HOST', 'PORT', 'PROTOCOL') bar;"
      , "SELECT bar.pos, bar.val FROM foo LATERAL VIEW posexplode(foo.array_col) bar;"
      , "SELECT bar.col0, bar.col1, bar.col2 FROM foo LATERAL VIEW stack(2, 1, 2, 3, 4, 5) bar;"
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
      , TL.unlines
        [ "FROM foo_stg f"
        , "INSERT OVERWRITE TABLE foo PARTITION(dt='2008-06-08', x)"
        , "SELECT f.x, f.y, f.z;"
        ]
      , "WITH full AS (SELECT 1) SELECT * FROM full;"
      , "SELECT * FROM full;"
      , "SELECT * FROM full f;"
      , "SELECT * FROM f full;"
      , "SELECT * FROM f AS full;"
      , "SELECT * FROM full FULL OUTER JOIN bar;"
      , "SELECT * FROM full f FULL OUTER JOIN bar;"
      , "SELECT * FROM f full FULL OUTER JOIN bar;"
      , "SELECT * FROM f AS full FULL OUTER JOIN bar;"
      , "SET hive.exec.parallel=true;"
      , "SET hive.exec.parallel = true;"
      , "SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;"
      , "SET mapreduce.job.queuename=foo-bar-baz;"
      , "SET x*x=1*5;"
      , "SET foo-bar-baz=foo-bar-baz;"
      , "SET mapreduce.job.queuename;"
      , "SET;"
      , "SET -v;"
      , TL.unlines
        [ "CREATE TABLE IF NOT EXISTS foo.bar (a int)"
        , "CLUSTERED BY (a) SORTED BY (a ASC) INTO 256 BUCKETS"
        , "STORED AS ORC;"
        ]
      , "SELECT * FROM foo WHERE ${hiveconf:bar};"
      , "CREATE TABLE potato (foo INT COMMENT 'I am a stegosaurus!');"
      , "CREATE TABLE potato (foo INT) COMMENT 'I''m not little! I drink milk!';"
      , "CREATE TABLE potato (foo STRUCT<bar: STRING COMMENT 'Boop!'>);"
      , "ALTER TABLE foo.bar SET TBLPROPERTIES(\"EXTERNAL\"=\"FALSE\");"
      , TL.unlines
        [ "CREATE VIEW IF NOT EXISTS foo.bar"
        , "AS SELECT some_col"
        , "FROM foo.baz;"
        ]
      , "ALTER TABLE foo.bar PARTITION(baz) ADD COLUMNS (quux1 int, quux2 int);"
      , "SELECT stack(4, 'a', 364, 'b', 42, 'c', 7, 'd', 1) AS (col1, col2);"
      , "CREATE FUNCTION foo.bar AS 'class' USING JAR 'hdfs:///path';"
      , "CREATE TEMPORARY FUNCTION foo AS 'class';"
      , "DROP FUNCTION foo;"
      , "DROP TEMPORARY FUNCTION foo;"
      ]

    , "Parse failure on malformed HiveQL statements" ~: map (TestCase . parsesUnsuccessfully)
      [ "SELECT;"
      , "USE;"
      , "HI;"
      , "SELECT /* comment */ 1;"  -- Block-style comments are not allowed
      , TL.unlines
          -- no UNIONs in EXISTS clauses
          [ "SELECT 1 FROM foo WHERE EXISTS ("
          , "    SELECT 1 FROM bar WHERE foo.a=bar.a"
          , "    UNION ALL"
          , "    SELECT 2 FROM bar where foo.b=bar.b"
          , ");"
          ]
      -- Dynamic partitions cannot precede any static partition
      , "INSERT INTO TABLE foo PARTITION (potato, soup=2) SELECT * FROM bar;"
      -- Degenerate partitions are not allowed
      , "INSERT INTO TABLE foo PARTITION () SELECT * FROM bar;"
      , "EXPLAIN TRUNCATE foo;"
      , "SELECT * FROM foo f NATURAL JOIN bar b;"
      , "SELECT * FROM foo INNER JOIN bar USING (a);"
      , "SELECT datediff(ww, 'a', 'b');"
      , "SELECT * FROM potato CLUSTER BY a DISTRIBUTE BY b;"
      , "SELECT * FROM potato CLUSTER BY a SORT BY b;"
      , "SELECT * FROM potato CLUSTER BY a DISTRIBUTE BY b SORT BY c;"
        -- SubqueryExprs are not permitted in Hive.
      , "SELECT (SELECT foo.x) FROM (SELECT 1 x) foo;"
      -- nor are OFFSET clauses
      , "SELECT * FROM foo ORDER BY a LIMIT 10 OFFSET 5;"
      -- nor are TIMESERIES clauses
      , TL.unlines
          [ "CREATE TABLE times AS"
          , "SELECT slice_time as start_time"
          , "FROM (SELECT '2015-12-15 23:00:00' d UNION SELECT '2015-12-18 03:09:55' d) a"
          , "TIMESERIES slice_time AS '1 hour' OVER (ORDER BY d)"
          , ";"
          ]
      -- Normally, named window must have partition, may have order,
      -- cannot have frame. Hive does whatever.
      -- However, hive cares about frames without order.
      , TL.unlines
        [ "SELECT RANK() OVER x FROM potato"
        , "WINDOW x AS (PARTITION BY a"
        , "RANGE UNBOUNDED PRECEDING AND CURRENT ROW);"
        ]
      , "SELECT * FROM foo AS;"
      , "SELECT * FROM foo AS full OUTER JOIN bar;"
      , "INSERT OVERWRITE TABLE foo.bar VALUES ('foo', 1, 2 + 3);" -- VALUES must be constants
      , "SELECT * FROM ${hiveconf:bar};" -- variable substitution only allowed in Exprs
      ]

    , "Parse exact HiveQL statements" ~:

        [ parse "USE default;" ~?= Right
                      ( HiveUseStmt
                        ( UseDefault
                          (Range (Position 1 0 0)
                                     (Position 1 11 11))))

        , parse "USE database_name;" ~?= Right
                ( HiveUseStmt
                  ( UseDatabase
                    (QSchemaName
                      (Range (Position 1 0 0)
                                 (Position 1 17 17))
                      None
                      "database_name"
                      NormalSchema)))

        , parse "SELECT 1;" ~?= Right
          ( HiveStandardSqlStatement
            (QueryStmt $ parseExactHelperSelect1 0 ))

        , parse "SELECT * FROM potato;" ~?= Right
          ( HiveStandardSqlStatement
            (QueryStmt $ evalState parseExactHelperSelectStar (Position 1 0 0)))

        , parse "INSERT INTO TABLE foo PARTITION (bar) SELECT 1;" ~?= Right
            ( HiveStandardSqlStatement
              ( InsertStmt
                ( Insert
                  { insertInfo =
                    (Range (Position 1 0 0) (Position 1 46 46))
                  , insertBehavior = InsertAppendPartition
                      (Range (Position 1 7 7) (Position 1 11 11))
                      ()
                  , insertTable = QTableName
                      (Range (Position 1 18 18) (Position 1 21 21))
                      Nothing
                      "foo"
                  , insertColumns = Nothing
                  , insertValues =
                    InsertSelectValues $ parseExactHelperSelect1 38
                  }
                )
              )
            )

        , parse "CREATE TABLE foo (bar String, baz String) TBLPROPERTIES ('bar'='1')" ~?= Right
            ( HiveStandardSqlStatement
              ( CreateTableStmt
                ( CreateTable
                  { createTableInfo =
                      (Range (Position 1 0 0) (Position 1 67 67))
                  , createTablePersistence = Persistent
                  , createTableExternality = Internal
                  , createTableIfNotExists = Nothing
                  , createTableName =
                      QTableName
                      { tableNameInfo =
                          (Range (Position 1 13 13) (Position 1 16 16))
                      , tableNameSchema = Nothing
                      , tableNameName = "foo"
                      }
                  , createTableDefinition = TableColumns
                      (Range (Position 1 17 17) (Position 1 41 41))
                      (
                        (
                          ColumnOrConstraintColumn
                          ( ColumnDefinition
                            { columnDefinitionInfo = (Range (Position 1 18 18) (Position 1 28 28))
                            , columnDefinitionName =
                              QColumnName
                              { columnNameInfo = (Range (Position 1 18 18) (Position 1 21 21))
                              , columnNameTable = None
                              , columnNameName = "bar"
                              }
                            , columnDefinitionType =
                              PrimitiveDataType (Range (Position 1 22 22) (Position 1 28 28)) "string" []
                            , columnDefinitionNull = Nothing
                            , columnDefinitionDefault = Nothing
                            , columnDefinitionExtra = Nothing
                            }
                          )
                        )
                        :|
                        [
                          (
                            ColumnOrConstraintColumn
                            ( ColumnDefinition
                              { columnDefinitionInfo = (Range (Position 1 30 30) (Position 1 40 40))
                              , columnDefinitionName =
                                QColumnName
                                { columnNameInfo = (Range (Position 1 30 30) (Position 1 33 33))
                                , columnNameTable = None
                                , columnNameName = "baz"
                                }
                              , columnDefinitionType =
                                PrimitiveDataType (Range (Position 1 34 34) (Position 1 40 40)) "string" []
                              , columnDefinitionNull = Nothing
                              , columnDefinitionDefault = Nothing
                              , columnDefinitionExtra = Nothing
                              }
                            )
                          )
                        ]
                      )
                  , createTableExtra =
                    Just HiveCreateTableExtra
                    { hiveCreateTableExtraInfo = Range (Position 1 17 17) (Position 1 67 67)
                    , hiveCreateTableExtraTableProperties =
                      Just (
                        HiveMetadataProperties
                        { hiveMetadataPropertiesInfo = Range (Position 1 42 42) (Position 1 67 67)
                        , hiveMetadataPropertiesProperties =
                          [
                            HiveMetadataProperty
                            { hiveMetadataPropertyInfo = Range (Position 1 57 57) (Position 1 66 66)
                            , hiveMetadataPropertyKey = "bar"
                            , hiveMetadataPropertyValue = "1"
                            }
                          ]
                        }
                      )
                    }
                  }
                )
              )
            )

        , parse "INSERT OVERWRITE TABLE foo PARTITION (bar) SELECT 1;" ~?= Right
            ( HiveStandardSqlStatement
              ( InsertStmt
                ( Insert
                  { insertInfo =
                    (Range (Position 1 0 0) (Position 1 51 51))
                  , insertBehavior = InsertOverwritePartition
                      (Range (Position 1 7 7) (Position 1 16 16))
                      ()
                  , insertTable = QTableName
                      (Range (Position 1 23 23) (Position 1 26 26))
                      Nothing
                      "foo"
                  , insertColumns = Nothing
                  , insertValues =
                    InsertSelectValues $ parseExactHelperSelect1 43
                  }
                )
              )
            )

        -- inverted FROM produces normal QuerySelects; the scope of an inverted
        -- FROM extends only up to the UNION.
        , parse "FROM foo SELECT 1 UNION SELECT 2;" ~?= Right
          ( HiveStandardSqlStatement
              ( QueryStmt
                  ( QueryUnion
                      ( Range (Position 1 18 18) (Position 1 23 23) )
                      ( Distinct True )
                      Unused
                      ( QuerySelect
                          ( Range (Position 1 0 0) (Position 1 17 17) )
                          ( makeSelect
                              ( Range (Position 1 0 0) (Position 1 17 17) )
                              ( SelectColumns
                                  { selectColumnsInfo = Range (Position 1 16 16) (Position 1 17 17)
                                  , selectColumnsList =
                                      [ SelectExpr
                                          ( Range (Position 1 16 16) (Position 1 17 17) )
                                          [ ColumnAlias (Range (Position 1 16 16) (Position 1 17 17)) "_c0" (ColumnAliasId 1) ]
                                          ( ConstantExpr (Range (Position 1 16 16) (Position 1 17 17))
                                              ( NumericConstant (Range (Position 1 16 16) (Position 1 17 17)) "1")
                                          )
                                      ]
                                  }
                              )
                          )
                          { selectFrom = Just
                              ( SelectFrom (Range (Position 1 0 0) (Position 1 8 8))
                                  [ TablishTable (Range (Position 1 5 5) (Position 1 8 8)) TablishAliasesNone
                                      (QTableName (Range (Position 1 5 5) (Position 1 8 8)) Nothing "foo")
                                  ]
                              )
                          }
                      )
                      ( QuerySelect
                          ( Range (Position 1 24 24) (Position 1 32 32) )
                          ( makeSelect
                              ( Range (Position 1 24 24) (Position 1 32 32) )
                              ( SelectColumns
                                  { selectColumnsInfo = Range (Position 1 31 31)(Position 1 32 32)
                                  , selectColumnsList =
                                      [ SelectExpr
                                          ( Range (Position 1 31 31) (Position 1 32 32) )
                                          [ ColumnAlias (Range (Position 1 31 31) (Position 1 32 32)) "_c0" (ColumnAliasId 2) ]
                                          ( ConstantExpr (Range (Position 1 31 31) (Position 1 32 32))
                                              ( NumericConstant (Range (Position 1 31 31) (Position 1 32 32)) "2" )
                                          )
                                      ]
                                  }
                              )
                          )
                      )
                  )
              )
          )

        -- treat schema qualifiers as column qualifiers
        , parse "SELECT s.t FROM t;" ~?= Right
          ( HiveStandardSqlStatement
            (QueryStmt
             $ (flip evalState) (Position 1 0 0) $ do
                    select <- do
                        s <- get
                        modify $ advance "SELECT "
                        selectCols <- do
                            posS <- get
                            modify $ advance "s"
                            pos1 <- get
                            modify $ advance "."
                            pos2 <- get
                            modify $ advance "t"
                            posE <- get

                            let exprR = Range posS posE
                                colR = Range posS pos1
                                fieldR = Range pos2 posE
                                colId = ColumnAliasId 1
                                colAlias = ColumnAlias exprR "_c0" colId
                                colNameExpr = ColumnExpr colR (QColumnName colR Nothing "s")
                                fieldName = StructFieldName fieldR "t"
                                columnExpr = FieldAccessExpr exprR colNameExpr fieldName
                                selection = SelectExpr exprR [colAlias] columnExpr
                            pure $ SelectColumns exprR [selection]
                        modify $ advance " "

                        selectFrom <- do
                            posS <- get
                            modify $ advance "FROM "
                            pos1 <- get
                            modify $ advance "t"
                            posE <- get
                            let
                                fromR = Range posS posE
                                tableR = Range pos1 posE
                                table = TablishTable tableR TablishAliasesNone (QTableName tableR Nothing "t")
                            pure $ Just $ SelectFrom fromR [table]

                        e <- get

                        let selectInfo = Range s e
                            selectDistinct = notDistinct
                            selectWhere = Nothing
                            selectTimeseries = Nothing
                            selectGroup = Nothing
                            selectHaving = Nothing
                            selectNamedWindow = Nothing
                        pure Select{..}
                    pure (QuerySelect (getInfo select) select)

            )
          )

        -- match table qualifiers to table names
        , parse "SELECT t.c FROM t;" ~?= Right
          ( HiveStandardSqlStatement
            (QueryStmt
             $ (flip evalState) (Position 1 0 0) $ do
                    select <- do
                        s <- get
                        modify $ advance "SELECT "
                        selectCols <- do
                            posS <- get
                            modify $ advance "t"
                            pos1 <- get
                            modify $ advance "."
                            pos2 <- get
                            modify $ advance "c"
                            posE <- get

                            let exprR = Range posS posE
                                tableR = Range posS pos1
                                columnR = Range pos2 posE

                                tableName = QTableName tableR Nothing "t"
                                colId = ColumnAliasId 1
                                colAlias = ColumnAlias exprR "c" colId
                                colNameExpr = ColumnExpr exprR (QColumnName columnR (Just tableName) "c")
                                selection = SelectExpr exprR [colAlias] colNameExpr
                            pure $ SelectColumns exprR [selection]
                        modify $ advance " "

                        selectFrom <- do
                            posS <- get
                            modify $ advance "FROM "
                            pos1 <- get
                            modify $ advance "t"
                            posE <- get
                            let
                                fromR = Range posS posE
                                tableR = Range pos1 posE
                                table = TablishTable tableR TablishAliasesNone (QTableName tableR Nothing "t")
                            pure $ Just $ SelectFrom fromR [table]

                        e <- get

                        let selectInfo = Range s e
                            selectDistinct = notDistinct
                            selectWhere = Nothing
                            selectTimeseries = Nothing
                            selectGroup = Nothing
                            selectHaving = Nothing
                            selectNamedWindow = Nothing
                        pure Select{..}
                    pure (QuerySelect (getInfo select) select)
            )
          )

        -- masochism at its best
        , parse "SELECT t.c[1].x[2] FROM t;" ~?= Right
          ( HiveStandardSqlStatement
            (QueryStmt
             $ (flip evalState) (Position 1 0 0) $ do
                    select <- do
                        s <- get
                        modify $ advance "SELECT "
                        selectCols <- do
                            posS <- get
                            modify $ advance "t"
                            pos1 <- get
                            modify $ advance "."
                            pos2 <- get
                            modify $ advance "c"
                            pos3 <- get
                            modify $ advance "["
                            pos4 <- get
                            modify $ advance "1"
                            pos5 <- get
                            modify $ advance "]"
                            pos6 <- get
                            modify $ advance "."
                            pos7 <- get
                            modify $ advance "x"
                            pos8 <- get
                            modify $ advance "["
                            pos9 <- get
                            modify $ advance "2"
                            pos10 <- get
                            modify $ advance "]"
                            posE <- get

                            let exprR = Range posS posE
                                tableR = Range posS pos1
                                colR = Range pos2 pos3
                                colExpR = Range posS pos3
                                arr0R = Range posS pos6
                                arrIdx0R = Range pos4 pos5
                                fieldNameR = Range pos7 pos8
                                structR = Range posS pos8
                                arr1R = exprR
                                arrIdx1R = Range pos9 pos10
                                tableName = QTableName tableR Nothing "t"
                                colId = ColumnAliasId 1
                                colAlias = ColumnAlias exprR "_c0" colId
                                colNameExpr = ColumnExpr colExpR (QColumnName colR (Just tableName) "c")
                                arr0Expr = ArrayAccessExpr arr0R colNameExpr (ConstantExpr arrIdx0R (NumericConstant arrIdx0R "1"))
                                fieldName = StructFieldName fieldNameR "x"
                                structExpr = FieldAccessExpr structR arr0Expr fieldName
                                arr1Expr = ArrayAccessExpr arr1R structExpr (ConstantExpr arrIdx1R (NumericConstant arrIdx1R "2"))
                                selection = SelectExpr exprR [colAlias] arr1Expr
                            pure $ SelectColumns exprR [selection]
                        modify $ advance " "

                        selectFrom <- do
                            posS <- get
                            modify $ advance "FROM "
                            pos1 <- get
                            modify $ advance "t"
                            posE <- get
                            let
                                fromR = Range posS posE
                                tableR = Range pos1 posE
                                table = TablishTable tableR TablishAliasesNone (QTableName tableR Nothing "t")
                            pure $ Just $ SelectFrom fromR [table]

                        e <- get

                        let selectInfo = Range s e
                            selectDistinct = notDistinct
                            selectWhere = Nothing
                            selectTimeseries = Nothing
                            selectGroup = Nothing
                            selectHaving = Nothing
                            selectNamedWindow = Nothing
                        pure Select{..}
                    pure (QuerySelect (getInfo select) select)
            )
          )

        -- Why would you ever try to array access a table?
        --
        -- If the table is a column. (ba-dum tsss)
        , parse "SELECT t[0].c FROM t;" ~?= Right
          ( HiveStandardSqlStatement
            (QueryStmt
             $ (flip evalState) (Position 1 0 0) $ do
                    select <- do
                        s <- get
                        modify $ advance "SELECT "
                        selectCols <- do
                            posS <- get
                            modify $ advance "t"
                            pos1 <- get
                            modify $ advance "["
                            pos2 <- get
                            modify $ advance "0"
                            pos3 <- get
                            modify $ advance "]"
                            pos4 <- get
                            modify $ advance "."
                            pos5 <- get
                            modify $ advance "c"
                            posE <- get

                            let exprR = Range posS posE
                                colR = Range posS pos1
                                arr0R = Range posS pos4
                                arrIdx0R = Range pos2 pos3
                                fieldR = Range pos5 posE

                                colId = ColumnAliasId 1
                                colAlias = ColumnAlias exprR "_c0" colId
                                colNameExpr = ColumnExpr colR (QColumnName colR Nothing "t")
                                arr0Expr = ArrayAccessExpr arr0R colNameExpr (ConstantExpr arrIdx0R (NumericConstant arrIdx0R "0"))
                                fieldName = StructFieldName fieldR "c"
                                structExpr = FieldAccessExpr exprR arr0Expr fieldName
                                selection = SelectExpr exprR [colAlias] structExpr
                            pure $ SelectColumns exprR [selection]
                        modify $ advance " "

                        selectFrom <- do
                            posS <- get
                            modify $ advance "FROM "
                            pos1 <- get
                            modify $ advance "t"
                            posE <- get
                            let
                                fromR = Range posS posE
                                tableR = Range pos1 posE
                                table = TablishTable tableR TablishAliasesNone (QTableName tableR Nothing "t")
                            pure $ Just $ SelectFrom fromR [table]

                        e <- get

                        let selectInfo = Range s e
                            selectDistinct = notDistinct
                            selectWhere = Nothing
                            selectTimeseries = Nothing
                            selectGroup = Nothing
                            selectHaving = Nothing
                            selectNamedWindow = Nothing
                        pure Select{..}
                    pure (QuerySelect (getInfo select) select)
            )
          )

        -- It's arrays all the way down
        , parse "SELECT c[0][1] FROM t;" ~?= Right
          ( HiveStandardSqlStatement
            (QueryStmt
             $ (flip evalState) (Position 1 0 0) $ do
                    select <- do
                        s <- get
                        modify $ advance "SELECT "
                        selectCols <- do
                            posS <- get
                            modify $ advance "c"
                            pos1 <- get
                            modify $ advance "["
                            pos2 <- get
                            modify $ advance "0"
                            pos3 <- get
                            modify $ advance "]"
                            pos4 <- get
                            modify $ advance "["
                            pos5 <- get
                            modify $ advance "1"
                            pos6 <- get
                            modify $ advance "]"
                            posE <- get

                            let exprR = Range posS posE
                                colR = Range posS pos1
                                arr0R = Range posS pos4
                                arrIdx0R = Range pos2 pos3
                                arr1R = Range posS posE
                                arrIdx1R = Range pos5 pos6

                                colId = ColumnAliasId 1
                                colAlias = ColumnAlias exprR "_c0" colId
                                colNameExpr = ColumnExpr colR (QColumnName colR Nothing "c")
                                arr0Expr = ArrayAccessExpr arr0R colNameExpr (ConstantExpr arrIdx0R (NumericConstant arrIdx0R "0"))
                                arr1Expr = ArrayAccessExpr arr1R arr0Expr (ConstantExpr arrIdx1R (NumericConstant arrIdx1R "1"))
                                selection = SelectExpr exprR [colAlias] arr1Expr
                            pure $ SelectColumns exprR [selection]
                        modify $ advance " "

                        selectFrom <- do
                            posS <- get
                            modify $ advance "FROM "
                            pos1 <- get
                            modify $ advance "t"
                            posE <- get
                            let
                                fromR = Range posS posE
                                tableR = Range pos1 posE
                                table = TablishTable tableR TablishAliasesNone (QTableName tableR Nothing "t")
                            pure $ Just $ SelectFrom fromR [table]

                        e <- get

                        let selectInfo = Range s e
                            selectDistinct = notDistinct
                            selectWhere = Nothing
                            selectTimeseries = Nothing
                            selectGroup = Nothing
                            selectHaving = Nothing
                            selectNamedWindow = Nothing
                        pure Select{..}
                    pure (QuerySelect (getInfo select) select)
            )
          )

        -- bar.baz is column.structfield, because `bar` isn't in scope.
        , parse "SELECT 1 FROM foo WHERE bar.baz;" ~?= Right
            ( HiveStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect (Range (Position 1 0 0) (Position 1 31 31))
                        ( Select
                            { selectInfo = Range (Position 1 0 0) (Position 1 31 31)
                            , selectCols =
                                SelectColumns
                                    { selectColumnsInfo = Range (Position 1 7 7) (Position 1 8 8)
                                    , selectColumnsList =
                                        [ SelectExpr
                                            ( Range (Position 1 7 7) (Position 1 8 8) )
                                            [ ColumnAlias (Range (Position 1 7 7) (Position 1 8 8)) "_c0" (ColumnAliasId 1)]
                                            ( ConstantExpr (Range (Position 1 7 7) (Position 1 8 8))
                                                ( NumericConstant (Range (Position 1 7 7) (Position 1 8 8)) "1" ) )
                                        ]
                                    }
                            , selectFrom = Just
                                ( SelectFrom
                                    ( Range (Position 1 9 9) (Position 1 17 17) )
                                    [ TablishTable (Range (Position 1 14 14) (Position 1 17 17)) TablishAliasesNone
                                        ( QTableName (Range (Position 1 14 14) (Position 1 17 17)) Nothing "foo" )
                                    ]
                                )
                            , selectWhere = Just
                                ( SelectWhere
                                    ( Range (Position 1 18 18) (Position 1 31 31) )
                                    ( FieldAccessExpr
                                        ( Range (Position 1 24 24) (Position 1 31 31) )
                                        ( ColumnExpr
                                            ( Range (Position 1 24 24) (Position 1 27 27) )
                                            ( QColumnName (Range (Position 1 24 24) (Position 1 27 27)) Nothing "bar" )
                                        )
                                        ( StructFieldName (Range (Position 1 28 28) (Position 1 31 31)) "baz" )
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
            )


        -- foo.baz is table.column, because `foo` IS in scope.
        , parse "SELECT 1 FROM foo WHERE foo.baz;" ~?= Right
            ( HiveStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect (Range (Position 1 0 0) (Position 1 31 31))
                        ( Select
                            { selectInfo = Range (Position 1 0 0) (Position 1 31 31)
                            , selectCols =
                                SelectColumns
                                    { selectColumnsInfo = Range (Position 1 7 7) (Position 1 8 8)
                                    , selectColumnsList =
                                        [ SelectExpr
                                            ( Range (Position 1 7 7) (Position 1 8 8) )
                                            [ ColumnAlias (Range (Position 1 7 7) (Position 1 8 8)) "_c0" (ColumnAliasId 1)]
                                            ( ConstantExpr (Range (Position 1 7 7) (Position 1 8 8))
                                                ( NumericConstant (Range (Position 1 7 7) (Position 1 8 8)) "1" ) )
                                        ]
                                    }
                            , selectFrom = Just
                                ( SelectFrom
                                    ( Range (Position 1 9 9) (Position 1 17 17) )
                                    [ TablishTable (Range (Position 1 14 14) (Position 1 17 17)) TablishAliasesNone
                                        ( QTableName (Range (Position 1 14 14) (Position 1 17 17)) Nothing "foo" )
                                    ]
                                )
                            , selectWhere = Just
                                ( SelectWhere
                                    ( Range (Position 1 18 18) (Position 1 31 31) )
                                    ( ColumnExpr
                                        ( Range (Position 1 24 24) (Position 1 31 31) )
                                        ( QColumnName (Range (Position 1 28 28) (Position 1 31 31))
                                            ( Just (QTableName (Range (Position 1 24 24) (Position 1 27 27)) Nothing "foo") )
                                            "baz"
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
            )

        , parse "SELECT * FROM foo LEFT SEMI JOIN bar;" ~?= Right
           ( HiveStandardSqlStatement
               (QueryStmt
                  (QuerySelect (Range (Position 1 0 0) (Position 1 36 36))
                     (Select
                        { selectInfo = (Range (Position 1 0 0) (Position 1 36 36))
                        , selectCols = SelectColumns
                            { selectColumnsInfo = (Range (Position 1 7 7) (Position 1 8 8))
                            , selectColumnsList =
                                [SelectStar (Range (Position 1 7 7) (Position 1 8 8)) Nothing Unused]
                            }
                        , selectFrom = Just
                            (SelectFrom (Range (Position 1 9 9) (Position 1 36 36))
                               [ TablishJoin
                                   (Range (Position 1 14 14) (Position 1 36 36))
                                   -- NB the lack of ON clause turned this LEFT SEMI JOIN to an INNER JOIN
                                   (JoinInner (Range (Position 1 18 18) (Position 1 32 32)))
                                   (JoinOn
                                      (ConstantExpr
                                         (Range (Position 1 18 18) (Position 1 36 36))
                                         (BooleanConstant (Range (Position 1 18 18) (Position 1 36 36)) True)
                                      )
                                   )
                                   (TablishTable
                                      (Range (Position 1 14 14) (Position 1 17 17)) TablishAliasesNone
                                      (QTableName (Range (Position 1 14 14) (Position 1 17 17)) Nothing "foo")
                                   )
                                   (TablishTable
                                      (Range (Position 1 33 33) (Position 1 36 36)) TablishAliasesNone
                                      (QTableName (Range (Position 1 33 33) (Position 1 36 36)) Nothing "bar")
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

        , parse "SELECT bar.* FROM foo LEFT SEMI JOIN bar INNER JOIN baz ON foo.a = bar.b AND foo.a = baz.a WHERE bar.b IS NOT NULL;" ~?= Right
          (HiveStandardSqlStatement
             (QueryStmt
                (QuerySelect (Range (Position 1 0 0) (Position 1 109 109))
                   (Select
                      { selectInfo = (Range (Position 1 0 0) (Position 1 109 109))
                      , selectCols = SelectColumns
                          { selectColumnsInfo = (Range (Position 1 7 7) (Position 1 12 12))
                          , selectColumnsList = [SelectStar (Range (Position 1 7 7) (Position 1 12 12)) (Just (QTableName (Range (Position 1 7 7) (Position 1 10 10)) Nothing "bar")) Unused]
                          }
                      , selectFrom = Just
                          (SelectFrom (Range (Position 1 13 13) (Position 1 76 76))
                             [ TablishJoin
                                 (Range (Position 1 18 18) (Position 1 76 76))
                                 (JoinInner (Range (Position 1 41 41) (Position 1 51 51)))
                                 (JoinOn
                                    (BinOpExpr
                                       (Range (Position 1 73 73) (Position 1 76 76))
                                       (Operator "AND")
                                       (BinOpExpr
                                          (Range (Position 1 65 65) (Position 1 66 66))
                                          (Operator "=")
                                          (ColumnExpr (Range (Position 1 59 59) (Position 1 64 64))
                                             (QColumnName
                                                (Range (Position 1 63 63) (Position 1 64 64))
                                                (Just
                                                  (QTableName (Range (Position 1 59 59) (Position 1 62 62)) Nothing "foo")
                                                )
                                                "a"
                                             )
                                          )
                                          (ColumnExpr (Range (Position 1 67 67) (Position 1 72 72))
                                             (QColumnName
                                                (Range (Position 1 71 71) (Position 1 72 72))
                                                (Just
                                                  (QTableName (Range (Position 1 67 67) (Position 1 70 70)) Nothing "bar")
                                                )
                                                "b"
                                             )
                                          )
                                       )
                                       (BinOpExpr
                                          (Range (Position 1 83 83) (Position 1 84 84))
                                          (Operator "=")
                                          (ColumnExpr (Range (Position 1 77 77) (Position 1 82 82))
                                             (QColumnName
                                                (Range (Position 1 81 81) (Position 1 82 82))
                                                (Just
                                                  (QTableName (Range (Position 1 77 77) (Position 1 80 80)) Nothing "foo")
                                                )
                                                "a"
                                             )
                                          )
                                          (ColumnExpr (Range (Position 1 85 85) (Position 1 90 90))
                                             (QColumnName
                                                (Range (Position 1 89 89) (Position 1 90 90))
                                                (Just
                                                  (QTableName (Range (Position 1 85 85) (Position 1 88 88)) Nothing "baz")
                                                )
                                                "a"
                                             )
                                          )
                                       )
                                    )
                                 )
                                 (TablishJoin
                                    (Range (Position 1 18 18) (Position 1 40 40))
                                    -- NB the lack of ON clause turned this LEFT SEMI JOIN to an INNER JOIN
                                    (JoinInner (Range (Position 1 22 22) (Position 1 36 36)))
                                    (JoinOn
                                       (ConstantExpr
                                          (Range (Position 1 22 22) (Position 1 40 40))
                                          (BooleanConstant (Range (Position 1 22 22) (Position 1 40 40)) True)
                                       )
                                    )
                                    (TablishTable
                                       (Range (Position 1 18 18) (Position 1 21 21)) TablishAliasesNone
                                       (QTableName (Range (Position 1 18 18) (Position 1 21 21)) Nothing "foo")
                                    )
                                    (TablishTable
                                       (Range (Position 1 37 37) (Position 1 40 40)) TablishAliasesNone
                                       (QTableName (Range (Position 1 37 37) (Position 1 40 40)) Nothing "bar")
                                    )
                                 )
                                 (TablishTable
                                    (Range (Position 1 52 52) (Position 1 55 55)) TablishAliasesNone
                                    (QTableName (Range (Position 1 52 52) (Position 1 55 55)) Nothing "baz")
                                 )
                             ]
                          )
                      , selectWhere = Just
                          (SelectWhere (Range (Position 1 91 91) (Position 1 109 109))
                             (UnOpExpr (Range (Position 1 106 106) (Position 1 109 109))
                                (Operator "NOT")
                                (UnOpExpr (Range (Position 1 110 110) (Position 1 114 114))
                                   (Operator "ISNULL")
                                   (ColumnExpr (Range (Position 1 97 97) (Position 1 102 102))
                                      (QColumnName
                                         (Range (Position 1 101 101) (Position 1 102 102))
                                         (Just
                                            (QTableName (Range (Position 1 97 97) (Position 1 100 100)) Nothing "bar")
                                         )
                                         "b"
                                      )
                                   ))))
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

        , parse "SELECT a, rank() OVER (PARTITION BY a ORDER BY 1) FROM foo ORDER BY 1;" ~?= Right
          ( HiveStandardSqlStatement
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
                              [ ColumnAlias (Range (Position 1 10 10) (Position 1 49 49)) "_c1" (ColumnAliasId 2) ]
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

testParser :: Test
testParser = test
    [ "Parse some arbitrary examples" ~: map (TestCase . parsesSuccessfully)
        [ "SELECT 1;"
          -- these all return the string "im matt"
        , "SELECT 'i''m matt';" -- nb this is different from Vertica.  In
          -- Vertica, to represent a single quote inside of a string, you
          -- escape it by adding a second single quote. So in Vertica this is a
          -- single TokString "i'm matt", but in Hive it's two TokStrings that
          -- get collapsed into one "im matt".
        , "SELECT 'i' 'm matt';"
        , TL.unlines
            [ "SELECT 'i'"
            , "'m matt'"
            , ";"
            ]
        , "SELECT \"i\" 'm matt';"
        , TL.unlines
            [ "SELECT 'i' --comment"
            , "'m matt'"
            , ";"
            ]
          -- this returns the string "i'm matt"
        , "SELECT 'i\\'m matt';"
        , ";"
        , "SELECT 1 as order;"
        , "SELECT foo FROM bar;"
        , "SELECT id AS foo FROM bar;"
        , "SELECT id foo FROM bar;"
        , "SELECT bar.id foo FROM bar;"
        , "SELECT 1 FROM foo UNION ALL      SELECT 2 FROM bar;"
        , "SELECT 1 FROM foo UNION DISTINCT SELECT 2 FROM bar;"
        , "SELECT 1 FROM foo UNION          SELECT 2 FROM bar;"
        , "SELECT * FROM foo INNER JOIN bar ON foo.id = bar.id;"
        , "SELECT * FROM foo JOIN bar ON foo.id = bar.id;"
        , "SELECT f.x, count(distinct f.y) FROM foo f GROUP BY 1;"
        , "SELECT sum(x = 7) FROM foo f;"
        , "SELECT CASE WHEN x = 7 THEN 1 ELSE 0 END FROM foo f;"
        , "SELECT sum(CASE WHEN x = 7 THEN 1 ELSE 0 END) FROM foo f;"
        , "SELECT 0, 0.1, 1.;"
        , "INSERT INTO TABLE foo SELECT x FROM bar;"
        , TL.unwords
            [ "SELECT CASE WHEN SUM(foo) = 0 then 0 ELSE"
            , "ROUND(SUM(CASE WHEN (bar = 'Yes')"
            , "THEN baz ELSE 0 END)/(SUM(qux) + 0.000000001), 2.0) END"
            , "AS `Stuff. Y'know?`;"
            ]

        , "SELECT foo WHERE bar IS NULL;"
        , "SELECT foo WHERE bar IS NOT NULL;"
        , "SELECT 1 IS ! NULL;"
        , TL.unlines
            [ "SELECT foo"
            , "FROM baz -- this is a comment"
            , "WHERE bar IS NOT NULL;"
            ]

        , "SELECT TRUE;"
        , "SELECT ! FALSE;"
        , "SELECT !FALSE;"
        , "SELECT NOT FALSE;"
        , "SELECT NOT (NOT FALSE);"
        , "SELECT 1=1, 1==2;"
        , "SELECT 1!=3, 1<>4;"
        , "SELECT 1<=>5;"
        , "SELECT 1>1, 1>=1, 1<1, 1<=1;"
        , "SELECT NULL;"
        , "SELECT 1 BETWEEN 0 and 2 BETWEEN 25 and 29;"
        , "SELECT 1 NOT BETWEEN 0 and 2;"
        , "SELECT 1 | 2;"
        , "SELECT 1 ^ 1;"
        , "SELECT 1 & 3;"
        , "SELECT ~1;"
        , "SELECT ~(~1);"
        , "SELECT 'bar' || 'foo'"
        , "SELECT concat('bar', 'foo')"
        , "SELECT array(1,2,3,4)[1+1];"
        , "SELECT foo[0][12].col1.col1[3].bar;"
        , "SELECT CASE WHEN 1=1 THEN struct(struct(1)) END.col1.col1;"
        , "SELECT CASE WHEN 1=1 THEN array(struct(struct(1)),struct(struct(2))) END[0].col1.col1;"
        , "SELECT COUNT(*);"
        , TL.unwords
            [ "SELECT site,date,num_hits,an_rank()"
            , "over (partition by site order by num_hits desc);"
            ]

        , TL.unwords
            [ "SELECT site,date,num_hits,an_rank()"
            , "over (partition by site, blah order by num_hits desc);"
            ]
        , TL.unlines
            [ "SELECT ranked.*"
            , "     , ROW_NUMBER() OVER (PARTITION BY (user_uuid, event_type)) AS rownum"
            , "FROM ranked;"
            ]
        , "SELECT CAST(1 as int);"
        , "SELECT 1 IN (1, 2);"
        , "SELECT 1 NOT IN (1, 2);"
        , "SELECT 1 ! IN (1, 2);"
        , "SELECT LEFT('foo', 7);"
        , "SELECT ((1));"
        , "SELECT 1 FROM foo WHERE EXISTS (SELECT 1 FROM bar);"
        , "SELECT 1 FROM foo WHERE NOT EXISTS (SELECT 1 FROM bar);"
        , "SELECT CURRENT_TIME;"
        , "SELECT SYSDATE;"
        , "SELECT SYSDATE();"
        , "SELECT 1 AS ESCAPE;"
        , "SELECT interval FROM t;"
        , "SELECT cluster FROM t;"
        , "SELECT deptno, sal, empno, COUNT(*) OVER (PARTITION BY deptno ORDER BY sal ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);"
        , "SELECT a, b, SUM(c) FROM foo GROUP BY a, b GROUPING SETS ((a, b), a, b, ());"
        , "SELECT a + b, SUM(c) FROM foo GROUP BY a + b GROUPING SETS (a + b);"
        , "SELECT a, b, SUM(c) FROM foo GROUP BY a, b WITH CUBE;"
        , "SELECT a, b, SUM(c) FROM foo GROUP BY a, b WITH ROLLUP;"
        , "SELECT foo AS localtime FROM potato;"
        , "CREATE TABLE blah AS SELECT 1;"
        , "CREATE TABLE blah AS WITH foo AS (SELECT 1) SELECT * FROM foo;"
        , "CREATE TABLE blah LIKE qux;"
        , "CREATE TEMPORARY EXTERNAL TABLE IF NOT EXISTS blah LIKE qux;"
        , "CREATE TABLE blah LIKE qux STORED AS parquet;"
        , "CREATE TABLE blah LIKE qux LOCATION 'foo';"
        , "CREATE TABLE blah (id INT);"
        , TL.unlines
            [ "CREATE TABLE foo"
            , "( col1 int"
            , ", col2 array<struct<name: string, variance: double, field1: double, field2: double>>"
            , ", col3 map<int, uniontype<string, double>>"
            , ", col4 string"
            , ");"
            ]
        , "CREATE TABLE IF NOT EXISTS foo.bar (`baz` string) STORED AS ORC;"
        , "CREATE TABLE blah (id INT) PARTITIONED BY (col_name INT);"
        , TL.unwords
            [ "CREATE TABLE blah (id INT)"
            , "ROW FORMAT SERDE \"org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe\""
            , "WITH SERDEPROPERTIES ('field.delim' = ',');"
            , ";"
            ]
        , TL.unwords
            [ "CREATE TABLE IF NOT EXISTS blah (id INT)"
            , "PARTITIONED BY (col_name INT)"
            , "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001'"
            , "STORED AS ORC"
            , "LOCATION 'hdfs://thingy'"
            , "TBLPROPERTIES (\"orc.compress\"=\"SNAPPY\", \"orc.compress.size\"=\"4096\")"
            , ";"
            ]
        , "CREATE EXTERNAL TABLE blah (id INT) LOCATION 'hdfs://blah';"
        , "CREATE EXTERNAL TABLE blah LOCATION 'hdfs://blah';"
        , "SELECT * FROM foo AS a;"
        , "SELECT 1 AS (a);"
        , "SELECT ISNULL(1);"
        , "SELECT 1 NOTNULL;" -- `notnull` is the column alias
        , "SELECT 1 ISNULL;" -- `isnull` is the column alias
        , "SELECT slice_time, symbol, TS_LAST_VALUE(bid IGNORE NULLS) AS last_bid FROM TickStore;"
        , "SELECT +1, -2;"
        , "SELECT 1 ^ 2;"
        , "SELECT * FROM (SELECT * FROM foo) all;"
        , "DELETE FROM foo;"
        , "DELETE FROM foo WHERE EXISTS (SELECT * FROM bar WHERE foo.a = bar.b);"
        , "DROP TABLE foo;"
        , "DROP TABLE IF EXISTS foo PURGE;"
        , "GRANT SELECT, CREATE ON TABLE foo TO bar WITH GRANT OPTION;"
        , "REVOKE SELECT, DROP ON TABLE foo FROM bar;"
        , "REVOKE GRANT OPTION FOR SELECT, DROP ON TABLE foo FROM bar;"
        , "REVOKE ALL PRIVILEGES FROM foo;"
        , "DESCRIBE FUNCTION EXTENDED isnull;"
        , "DESCRIBE FUNCTION EXTENDED positive;"
        , "DESCRIBE FUNCTION EXTENDED hex;"
        , "DESCRIBE FUNCTION EXTENDED foo.bar;"
        , "DESCRIBE FUNCTION EXTENDED log10;"
        , "DESC FUNCTION EXTENDED log10;"
        , "DESC DATABASE default;"
        , "DESC default.foo;"
        , "CREATE SCHEMA foo;"
        , "CREATE DATABASE foo;"
        , TL.unwords
            [ "CREATE DATABASE IF NOT EXISTS foo"
            , "COMMENT 'comments are singlequoted strings'"
            , "LOCATION 'hdfs://foo'"
            , "WITH DBPROPERTIES (\"k1\"=\"v1\", \"k2\"=\"v2\");"
            ]
        , "SELECT 1 UNION SELECT 2;"
        , "SELECT 1 FROM foo UNION SELECT 2 FROM bar;"
        , "SELECT 1"  -- `;` is optional
        , "SELECT 1\n  "  -- `;` is optional and trailing whitespace is ok
        , "SELECT * FROM source TABLESAMPLE(BUCKET 3 OUT OF 16) s;"
        , "SELECT * FROM source TABLESAMPLE(BUCKET 3 OUT OF 16 on id) s;"
        , "SELECT * FROM source TABLESAMPLE(BUCKET 3 OUT OF 32 ON rand()) s;"
        , "SELECT * FROM source TABLESAMPLE(0.1 PERCENT) s;"
        , "SELECT * FROM source TABLESAMPLE(100B) s;"
        , "SELECT * FROM source TABLESAMPLE(100K) s;"
        , "SELECT * FROM source TABLESAMPLE(100M) s;"
        , "SELECT * FROM source TABLESAMPLE(100G) s;"
        , "SELECT * FROM source TABLESAMPLE(10 ROWS);"
        , "ALTER TABLE foo.bar CHANGE col_a col_a STRING;"
        , "ALTER TABLE foo.bar ADD COLUMNS (col_a STRING);"
        , "ALTER TABLE foo.bar ADD COLUMNS (col_a STRING) CASCADE;"
        , "ALTER TABLE foo.bar ADD COLUMNS (col_a STRING) RESTRICT;"
        , "ALTER TABLE foo.bar PARTITION (date='2015-01-01') ADD COLUMNS (col_a STRING, col_b LONG);"
        , "RELOAD FUNCTION;"
        , "WITH x AS (SELECT 1 AS a FROM dual)  SELECT * FROM X             UNION  SELECT * FROM X ORDER BY a ;"
        ]

    , "Exclude some broken examples" ~: map (TestCase . parsesUnsuccessfully)
        [ "SELECT 'foo' ~~ 'bar' ESCAPE '!';"
        , "SELECT * FROM (foo);"
        , "SELECT 1 INTERSECT SELECT 1;"
        , "SELECT 1 EXCEPT SELECT 1;"
        , "SELECT 1 LIMIT null;"
        , "CREATE TABLE foo (GROUPED (a, b));"
        , "CREATE TABLE foo LIKE qux NO PROJECTION;"
        , "SELECT * FROM (SELECT 1);"
        , "SELECT * FROM ((SELECT 1) as a);"
        , "SELECT * FROM (foo) as a;"
        , "CREATE TABLE foo (bar INT NULL NOT NULL);"
        , "CREATE TABLE foo (bar map<array<int>, array<int>>);"
        , "SELECT 1 (a, b);"
        , "SELECT 1 AS ((a));"
        , "SELECT f.uuid, /* comment */ count(1) FROM foo f GROUP BY 1;"
        , "SELECT CASE 1 WHEN 2 THEN 2 WHEN NULLSEQUAL 3 THEN 5 END;"
        , "SELECT CASE 1 WHEN 2 THEN 2 WHEN NULLSEQUAL 3 THEN 5 ELSE 4 END;"
        , "SELECT .01;"
        , "SELECT 1.0::INTEGER;"
        , "SELECT 1 AS 'foo';"
        , "SELECT ~~1;"
        , "SELECT 1 IS NULL IS NULL;"
        , "SELECT 1 NOT IS NULL;"
        , "SELECT 1 ISNULL ISNULL;"
        , "SELECT current_timestamp() AT TIMEZONE 'America/Los_Angeles';"
        , "SELECT (DATE '2007-02-16', DATE '2007-12-21') OVERLAPS (DATE '2007-10-30', DATE '2008-10-30');"
        , "SELECT (1,2,3);"
        , "((SELECT 1));"
        , TL.unwords
            [ "SELECT name FROM \"user\""
            , "WHERE \"user\".id IN (SELECT user_id FROM whatever);"
            ]
        , "SELECT (3 // 2) as integer_division;"
        , "SELECT 'foo' LIKE 'bar' ESCAPE '!';"
        , "INSERT INTO foo DEFAULT VALUES;"
        , "INSERT INTO foo VALUES (1,2,2+3);"
        , "INSERT INTO foo (SELECT * FROM baz);"
        , "SELECT 2 = ALL(ARRAY[1,2]);"
        , "CREATE TABLE foo (a int) AS (SELECT 1);"
        , "CREATE TABLE blah AS (SELECT 1);"
        , "CREATE TABLE blah LIKE qux EXCLUDING PROJECTIONS;"
        , "CREATE LOCAL TEMP TABLE foo ON COMMIT PRESERVE ROWS AS (SELECT 1);"
        , "CREATE TABLE foo (bar INT NULL NULL NULL NULL NULL NULL);"
        , "CREATE TABLE foo (a int) AS SELECT 1 FROM bar;"
        , "CREATE TABLE foo AS (a int) SELECT 1 FROM bar;"
        , "SELECT * FROM (foo JOIN bar ON baz);"
        , "SELECT 1 FROM ((foo JOIN bar ON blah) JOIN baz ON qux);"
        , "SELECT 1 FROM ((foo JOIN bar ON blah) JOIN baz ON qux) AS wat;"
        , "SELECT * FROM ((SELECT 1)) as a;"
        , TL.unlines
            [ "with a as (select 1),"
            , "     b as (select 2),"
            , "     c as (select 3)"
            , "select * from ((a join b on true) as foo join c on true);"
            ]
        , "CREATE TABLE blah (foo INT DEFAULT 7, bar DATE NOT NULL);"
        , "SELECT COUNT(1 USING PARAMETERS wat=7);"
        , "SELECT (interval '1' month);"
        , "SELECT interval '1 month';"
        , "SELECT interval '2' day;"
        , "GRANT;"
        , "REVOKE;"
        , "SELECT 1 FROM foo WHERE NOT NOT EXISTS (SELECT 1 FROM bar);"
        , "SELECT 1 FROM foo WHERE NOT NOT NOT EXISTS (SELECT 1 FROM bar);"
        , "SELECT ! NOT true;"
        , "SELECT NOT ! true;"
        , "SELECT NOT NOT true;"
        , "SELECT ! ! true;"
        , "SELECT foo WHERE bar IS TRUE;"
        , "SELECT foo WHERE bar IS UNKNOWN;"
        , "" -- the only way to write the empty statement is `;`
        , "\n" -- the only way to write the empty statement is `;`
        , "WITH x AS (SELECT 1 AS a FROM dual) (SELECT * FROM X ORDER BY a) UNION (SELECT * FROM X ORDER BY a);" -- this is valid in Vertica/Presto
        , "WITH x AS (SELECT 1 AS a FROM dual) (SELECT * FROM X ORDER BY a) UNION (SELECT * FROM X) ORDER BY a;" -- this is valid in Vertica/Presto
        , "WITH x AS (SELECT 1 AS a FROM dual) (SELECT * FROM X ORDER BY a) UNION  SELECT * FROM X ORDER BY a ;" -- this is valid in Vertica/Presto
        , "WITH x AS (SELECT 1 AS a FROM dual) ((SELECT * FROM X ORDER BY a) UNION SELECT * FROM X) ORDER BY a;" -- this is valid in Vertica/Presto
        , "WITH x AS (SELECT 1 AS a FROM dual)  SELECT * FROM X ORDER BY a  UNION  SELECT * FROM X ORDER BY a ;"
        , "WITH x AS (SELECT 1 AS a FROM dual)  SELECT * FROM X ORDER BY a  UNION  SELECT * FROM X            ;"
        ]

    , ticket "T397318"
        [ parsesUnsuccessfully "GRANT nonexistentOption ON TABLE foo TO bar;"
        , parsesUnsuccessfully "REVOKE nonexistentOption ON TABLE foo FROM bar;"
        , parsesUnsuccessfully "REVOKE SELECT ON TABLE foo FROM TO bar;"
        , parsesUnsuccessfully "REVOKE REVOKE REVOKE REVOKE;"
        ]

    , ticket "T527516"
        [ parsesUnsuccessfully "CREATE EXTERNAL TABLE foo AS SELECT 1;"
        ]

    , "Parse exactly" ~:
        [ parse "SELECT foo FROM bar INNER JOIN baz ON 'true';" ~?= Right
            ( HiveStandardSqlStatement
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
                                    (ColumnExpr (Range (Position 1 7 7) (Position 1 10 10))
                                                (QColumnName (Range (Position 1 7 7) (Position 1 10 10)) Nothing "foo"))
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
                                                (Range (Position 1 16 16)
                                                       (Position 1 19 19))
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
            ( HiveStandardSqlStatement
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
                                                "_c0"
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
        , parseManyEithers "asdf; SELECT 1; -- comment\nSELECT 2; GARBAGE" ~?= Right
            [ Left ( Unparsed ( Range { start = Position { positionLine = 1, positionColumn = 0, positionOffset = 0 }, end = Position { positionLine = 1, positionColumn = 5, positionOffset = 5 } } ) )
            , let Right stmt = parseAll (TL.replicate 6 " " <> "SELECT 1;")
               in Right stmt
            , let Right stmt = parseAll (TL.replicate 26 " " <> "\n" <> "SELECT 2;")
               in Right stmt
            , Left ( Unparsed ( Range { start = Position { positionLine = 2, positionColumn = 10, positionOffset = 37 }, end = Position { positionLine = 2, positionColumn = 17, positionOffset = 44 } } ) )
            ]
        ]
    ]

tests :: Test
tests = test [ testParser
             , testParser_hiveSuite
             , testInvertedFrom
             ]
