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

{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Database.Sql.Util.Columns.Test where

import Test.HUnit
import Test.HUnit.Ticket

import qualified Database.Sql.Util.Test as Test

import qualified Data.Text.Lazy as TL
import           Data.Proxy (Proxy(..))

import           Data.Generics.Aliases (extT)
import           Data.Generics.Schemes (everywhere)
import qualified Data.HashMap.Strict as HMS
import qualified Data.Set as S
import           Data.Set (Set)

import           Database.Sql.Util.Columns
import           Database.Sql.Type

import           Control.Arrow (first)


instance Test.TestableAnalysis HasColumns a where
    type TestResult HasColumns a = Set ColumnAccess
    runTest _ _ = getColumns

testHive :: TL.Text -> Catalog -> (Set ColumnAccess -> Assertion) -> [Assertion]
testHive = Test.testResolvedHive (Proxy :: Proxy HasColumns)

testVertica :: TL.Text -> Catalog -> (Set ColumnAccess -> Assertion) -> [Assertion]
testVertica = Test.testResolvedVertica (Proxy :: Proxy HasColumns)

testPresto :: TL.Text -> Catalog -> (Set ColumnAccess -> Assertion) -> [Assertion]
testPresto = Test.testResolvedPresto (Proxy :: Proxy HasColumns)

testAll :: TL.Text -> Catalog -> (Set ColumnAccess -> Assertion) -> [Assertion]
testAll = Test.testResolvedAll (Proxy :: Proxy HasColumns)


testColumnAccesses :: Test
testColumnAccesses = test
    [ "Report column accesses for queries" ~: concat
        [ -- SELECT
          testAll "SELECT 1;" defaultTestCatalog
          ((@=?) $ S.empty)
        , testAll "SELECT a FROM foo;" defaultTestCatalog
          ((@=?) $ S.singleton
              (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT")
          )
        , testAll "SELECT * FROM bar;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
              ]
          )
        , testAll "SELECT * FROM foo CROSS JOIN bar;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
              ]
          )
        , testPresto "SELECT * FROM (bar);" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
              ]
          )
        , testPresto "SELECT * FROM (bar) t(x,y);" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
              ]
          )
        , testPresto "SELECT * FROM (SELECT * FROM bar) t(x,y);" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
              ]
          )
        , testPresto "SELECT * FROM (foo CROSS JOIN bar) t(x,y,z);" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
              ]
          )
        , testPresto "SELECT * FROM (bar CROSS JOIN UNNEST(ARRAY[1,2]) AS t1(x)) t;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
              ]
          )
        , testPresto "SELECT f(a -> a + b) FROM bar;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
              ]
          )
        , testPresto "SELECT f(a, a -> a + b) FROM bar;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
              ]
          )

        -- FROM
        , testAll "SELECT 1 FROM foo JOIN bar ON foo.a = bar.a;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "JOIN")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "a", "JOIN")
              ]
          )
        , testVertica "SELECT 1 FROM foo JOIN bar USING (a);" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "JOIN")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "a", "JOIN")
              ]
          )
        , testVertica "SELECT 1 FROM foo NATURAL JOIN bar;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "JOIN")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "a", "JOIN")
              ]
          )
        , testAll "SELECT 1 FROM (SELECT a FROM bar) subq;" defaultTestCatalog
          ((@=?) $ S.singleton
              (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
          )
        , testPresto "SELECT 1 FROM (foo JOIN bar ON foo.a = bar.a);" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "JOIN")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "a", "JOIN")
              ]
          )

        -- WHERE
        , testAll "SELECT 1 FROM foo WHERE a IS NOT NULL;" defaultTestCatalog
          ((@=?) $ S.singleton
              (FullyQualifiedColumnName "default_db" "public" "foo" "a", "WHERE")
          )

        -- GROUP BY
        , testAll "SELECT COUNT(*) FROM foo GROUP BY a;" defaultTestCatalog
          ((@=?) $ S.singleton
              -- 'COUNT(*)' is a special case: the '*' doesn't actually represent 'all columns'.
              (FullyQualifiedColumnName "default_db" "public" "foo" "a", "GROUPBY")
          )
        , testAll "SELECT sum(a) FROM foo GROUP BY a;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "GROUPBY")
              ]
          )

        -- HAVING
        , testAll "SELECT a, COUNT(*) FROM bar GROUP BY a HAVING COUNT(DISTINCT b) > 1;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "a", "GROUPBY")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "HAVING")
              ]
          )

        -- ORDER
        , testAll "SELECT a, b FROM bar ORDER BY a, b DESC;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "a", "ORDER")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "ORDER")
              ]
          )

        -- aliases in SELECT
        , testAll "SELECT a + b AS c FROM bar ORDER BY c;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "a", "ORDER")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "ORDER")
              ]
          )

        -- aliases in CTEs
        , testVertica "WITH cte (x) AS (SELECT a FROM foo UNION SELECT a FROM bar) SELECT 1 FROM cte WHERE x;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT") -- from the CTE definition
              , (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT") -- from the CTE definition
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "WHERE")  -- from the main SELECT
              , (FullyQualifiedColumnName "default_db" "public" "bar" "a", "WHERE")  -- from the main SELECT
              ]
          )
          , testVertica "WITH cte (x) AS (SELECT a FROM foo ORDER BY a LIMIT 1) SELECT 1 FROM cte WHERE x;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT") -- from the CTE definition
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "ORDER")  -- from the CTE definition
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "WHERE")  -- from the main SELECT
              ]
          )

        -- aliases in TablishAliases
        , testPresto "SELECT cAlias FROM foo AS tAlias (cAlias) ORDER BY cAlias;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "ORDER")
              ]
          )
        , testPresto "SELECT cAlias FROM (SELECT a FROM foo) AS tAlias (cAlias) ORDER BY cAlias;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "ORDER")
              ]
          )
        , testPresto "SELECT cAlias FROM (foo) AS tAlias (cAlias) ORDER BY cAlias;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "ORDER")
              ]
          )
        , let query = TL.unlines
                [ "SELECT arrayVal"
                , "FROM foo AS fooAlias (arrayCol)"
                , "CROSS JOIN UNNEST(arrayCol) AS latViewAlias (arrayVal);"
                ]
           in testPresto query defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "LATERALVIEW")
              ]
          )
        , let query = TL.unlines
                [ "SELECT mapVal"
                , "FROM foo AS fooAlias (mapCol)"
                , "CROSS JOIN UNNEST(mapCol) AS latViewAlias (mapKey, mapVal)"
                , "ORDER BY mapKey;"
                ]
           in testPresto query defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "ORDER")
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "LATERALVIEW")
              ]
          )
        -- aliases in multi-select queries
        , testAll
          "select aAlias from (select a as aAlias from bar union select b as aAlias from bar) t"
          defaultTestCatalog
          ((@=?) $ S.fromList
           [ (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
           , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
           ]
          )
        , testVertica
          "select aAlias from (select a as aAlias from bar intersect select b as aAlias from bar) t"
          defaultTestCatalog
          ((@=?) $ S.fromList
           [ (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
           , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
           ]
          )
          -- Hive doesn't support intersect!
        , testPresto
          "select aAlias from (select a as aAlias from bar intersect select b as aAlias from bar) t"
          defaultTestCatalog
          ((@=?) $ S.fromList
           [ (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
           , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
           ]
          )

        -- positional references
        , testAll "SELECT a FROM foo ORDER BY 1;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "ORDER")
              ]
          )
        , testAll "SELECT a FROM foo GROUP BY 1;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "GROUPBY")
              ]
          )

        -- misc
        , let query = TL.unlines
                [ "SELECT my_timeseries AS start_time"
                , "FROM bar"
                , "WHERE a IS NOT NULL"
                , "TIMESERIES my_timeseries AS '1 hour'"
                , "OVER (PARTITION BY a ORDER BY b::TIMESTAMP);"
                ]
           in testVertica query defaultTestCatalog
              ((@=?) $ S.fromList
                  [ (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
                  , (FullyQualifiedColumnName "default_db" "public" "bar" "a", "WHERE")
                  , (FullyQualifiedColumnName "default_db" "public" "bar" "a", "PARTITION")
                  , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "ORDER")
                  ]
              )
        , testHive "SELECT 1 FROM foo LATERAL VIEW explode(a) exploded AS x;" defaultTestCatalog
          ((@=?) $ S.singleton
              (FullyQualifiedColumnName "default_db" "public" "foo" "a", "LATERALVIEW")
          )
        , testVertica "SELECT RANK() OVER x FROM foo WINDOW x AS (PARTITION BY a ORDER BY a ASC);" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "PARTITION")
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "ORDER")
              ]
          )
        , testHive "SELECT RANK() OVER x FROM foo WINDOW x AS (PARTITION BY a ORDER BY a ASC);" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "PARTITION")
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "ORDER")
              ]
          )
        , testAll "DELETE FROM foo WHERE a > 10;" defaultTestCatalog
          ((@=?) $ S.singleton
              (FullyQualifiedColumnName "default_db" "public" "foo" "a", "WHERE")
          )

        -- tricky test case from Eli
        , let catalogMap = HMS.singleton defaultDatabase $ HMS.fromList
                             [ ( publicSchema
                               , HMS.fromList
                                 [ ( QTableName () None (TL.toLower "tabA")
                                   , persistentTable [ QColumnName () None "col1"
                                                     , QColumnName () None "col2"
                                                     , QColumnName () None "col3"
                                                     ]
                                   )
                                 , ( QTableName () None (TL.toLower "tabB")
                                   , persistentTable [ QColumnName () None "col1"
                                                     , QColumnName () None "col2"
                                                     ]
                                   )
                                 , ( QTableName () None (TL.toLower "tabC")
                                   , persistentTable [ QColumnName () None "col1"
                                                     , QColumnName () None "col4"
                                                     ]
                                   )
                                 ]
                               )
                             ]
              specialCatalog = makeCatalog catalogMap [publicSchema] defaultDatabase
              query = TL.unlines
                [ "SELECT x.col1a, c.col4"
                , "FROM ("
                , "    SELECT a.col1 AS col1a"
                , "         , b.col1 AS col1b"
                , "    FROM tabA a"
                , "    JOIN tabB b"
                , "    ON a.col2 = b.col2"
                , "    WHERE a.col3 = 1"
                , ") x"
                , "JOIN tabC c ON c.col1 = x.col1a"
                , "WHERE x.col1b = 2;"
                ]
           in testVertica query specialCatalog
              ((@=?) $ S.fromList $ map (first $ everywhere $ extT id TL.toLower) $
                  [ (FullyQualifiedColumnName "default_db" "public" "tabA" "col1", "SELECT")
                  , (FullyQualifiedColumnName "default_db" "public" "tabB" "col1", "SELECT")
                  , (FullyQualifiedColumnName "default_db" "public" "tabA" "col2", "JOIN")
                  , (FullyQualifiedColumnName "default_db" "public" "tabB" "col2", "JOIN")
                  , (FullyQualifiedColumnName "default_db" "public" "tabA" "col3", "WHERE")
                    --from outer query:
                  , (FullyQualifiedColumnName "default_db" "public" "tabA" "col1", "SELECT") -- including this for readability purposes, though it will get deduped out with the inner SELECT
                  , (FullyQualifiedColumnName "default_db" "public" "tabC" "col4", "SELECT")
                  , (FullyQualifiedColumnName "default_db" "public" "tabC" "col1", "JOIN")
                  , (FullyQualifiedColumnName "default_db" "public" "tabA" "col1", "JOIN")
                  , (FullyQualifiedColumnName "default_db" "public" "tabB" "col1", "WHERE")
                  ]
              )
        , testPresto "WITH cte AS (SELECT a FROM foo) SELECT cAlias FROM cte AS tAlias (cAlias) ORDER BY cAlias;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "ORDER")
              ]
          )
        , testPresto "WITH cte (x) AS (SELECT a FROM foo) SELECT cAlias FROM cte AS tAlias (cAlias) ORDER BY cAlias;" defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "foo" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "foo" "a", "ORDER")
              ]
          )
        ]

    , ticket "T681602" $ concat
        [ let query = TL.unlines
                [ "SELECT c1prime, c2prime, c3prime"
                , "FROM bar AS barAlias (c1, c2)"
                , "CROSS JOIN UNNEST(c1, c2) AS latViewAlias (c1prime, c2prime, c3prime);"
                -- this is the "general case" because UNNEST has two args
                ]
           in testPresto query defaultTestCatalog
          ((@=?) $ S.fromList
              [ (FullyQualifiedColumnName "default_db" "public" "bar" "a", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "SELECT")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "a", "LATERALVIEW")
              , (FullyQualifiedColumnName "default_db" "public" "bar" "b", "LATERALVIEW")
              ]
          )
        ]

    ]


defaultDatabase :: DatabaseName ()
defaultDatabase = DatabaseName () "default_db"

publicSchema :: UQSchemaName ()
publicSchema = mkNormalSchema "public" ()

defaultTestCatalog :: Catalog
defaultTestCatalog = makeCatalog
    ( HMS.singleton defaultDatabase $ HMS.fromList
        [ ( publicSchema
          , HMS.fromList
            [ ( QTableName () None "foo"
              , persistentTable [ QColumnName () None "a" ]
              )
            , ( QTableName () None "bar"
              , persistentTable
                  [ QColumnName () None "a"
                  , QColumnName () None "b"
                  ]
              )
            ]
          )
        ]
    )
    [ publicSchema ]
    defaultDatabase

tests :: Test
tests = test [ testColumnAccesses ]
