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

module Database.Sql.Util.Lineage.Table.Test where

import Test.HUnit
import Test.HUnit.Ticket
import qualified Database.Sql.Util.Test as Test
import Database.Sql.Util.Lineage.Table
import Database.Sql.Type as SQL

import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.HashMap.Strict as HMS

import qualified Data.Text.Lazy as TL
import           Data.Proxy (Proxy(..))


instance Test.TestableAnalysis HasTableLineage a where
    type TestResult HasTableLineage a = TableLineage
    runTest _ _ = getTableLineage

testHive :: TL.Text -> Catalog -> (TableLineage -> Assertion) -> [Assertion]
testHive = Test.testResolvedHive (Proxy :: Proxy HasTableLineage)

testVertica :: TL.Text -> Catalog -> (TableLineage -> Assertion) -> [Assertion]
testVertica = Test.testResolvedVertica (Proxy :: Proxy HasTableLineage)

testAll :: TL.Text -> Catalog -> (TableLineage -> Assertion) -> [Assertion]
testAll = Test.testResolvedAll (Proxy :: Proxy HasTableLineage)


testTableLineage :: Test
testTableLineage = test
    [ "Generate table lineages for parsed queries" ~: concat
        [ testAll "SELECT 1;" defaultTestCatalog (@=? M.empty)
        , testAll "SELECT 1 FROM foo JOIN bar ON foo.a = bar.b;" defaultTestCatalog (@=? M.empty)
        , testVertica "INSERT INTO foo VALUES (1, 2);"
              defaultTestCatalog
              (@=? M.singleton
                  ( FullyQualifiedTableName "default_db" "public" "foo" )
                  ( S.singleton ( FullyQualifiedTableName "default_db" "public" "foo" ) )
              )
        , testVertica "INSERT INTO foo (SELECT * FROM bar);"
              defaultTestCatalog
              (@=? M.singleton
                  ( FullyQualifiedTableName "default_db" "public" "foo" )
                  ( S.fromList [ FullyQualifiedTableName "default_db" "public" "foo"
                               , FullyQualifiedTableName "default_db" "public" "bar"
                               ]
                  )
              )
        , testHive "INSERT INTO TABLE foo SELECT * FROM bar;"
              defaultTestCatalog
              (@=? M.singleton
                  ( FullyQualifiedTableName "default_db" "public" "foo" )
                  ( S.fromList [ FullyQualifiedTableName "default_db" "public" "foo"
                               , FullyQualifiedTableName "default_db" "public" "bar"
                               ]
                  )
              )
        , testVertica "CREATE TABLE baz LIKE bar;"
              defaultTestCatalog
              (@=? M.singleton ( FullyQualifiedTableName "default_db" "public" "baz" ) S.empty )
        , testHive "CREATE TABLE baz LIKE bar;"
              defaultTestCatalog
              (@=? M.singleton ( FullyQualifiedTableName "default_db" "public" "baz" ) S.empty )
        -- Presto support for CREATE TABLE is not yet implemented
        , testVertica "CREATE TABLE baz (quux int);"
              defaultTestCatalog
              (@=? M.singleton ( FullyQualifiedTableName "default_db" "public" "baz" ) S.empty )
        , testHive "CREATE TABLE baz (quux int);"
              defaultTestCatalog
              (@=? M.singleton ( FullyQualifiedTableName "default_db" "public" "baz" ) S.empty )
        -- Presto support for CREATE TABLE is not yet implemented
        , testVertica "CREATE TABLE baz AS (SELECT * FROM bar);"
              defaultTestCatalog
              (@=? M.singleton
                  ( FullyQualifiedTableName "default_db" "public" "baz" )
                  ( S.singleton ( FullyQualifiedTableName "default_db" "public" "bar" ) )
              )
        , testHive "CREATE TABLE baz AS SELECT * FROM bar;"
              defaultTestCatalog
              (@=? M.singleton
                  ( FullyQualifiedTableName "default_db" "public" "baz" )
                  ( S.singleton ( FullyQualifiedTableName "default_db" "public" "bar" ) )
              )
        , testVertica "CREATE TABLE baz AS (SELECT * FROM bar CROSS JOIN foo);"
              defaultTestCatalog
              (@=? M.singleton
                  ( FullyQualifiedTableName "default_db" "public" "baz" )
                  ( S.fromList [ FullyQualifiedTableName "default_db" "public" "foo"
                               , FullyQualifiedTableName "default_db" "public" "bar"
                               ]
                  )
              )
        , testHive "CREATE TABLE baz AS SELECT * FROM bar CROSS JOIN foo;"
              defaultTestCatalog
              (@=? M.singleton
                  ( FullyQualifiedTableName "default_db" "public" "baz" )
                  ( S.fromList [ FullyQualifiedTableName "default_db" "public" "foo"
                               , FullyQualifiedTableName "default_db" "public" "bar"
                               ]
                  )
              )
        , testAll "DROP TABLE IF EXISTS foo;" defaultTestCatalog
              (@=? M.singleton (FullyQualifiedTableName "default_db" "public" "foo") S.empty)
        , testAll "DELETE FROM foo;"
              defaultTestCatalog
              (@=? M.singleton ( FullyQualifiedTableName "default_db" "public" "foo" ) S.empty )
        , testAll "DELETE FROM foo WHERE EXISTS (SELECT * FROM bar);"
              defaultTestCatalog
              (@=? M.singleton
                  ( FullyQualifiedTableName "default_db" "public" "foo" )
                  ( S.fromList [ FullyQualifiedTableName "default_db" "public" "foo"
                               , FullyQualifiedTableName "default_db" "public" "bar"
                               ]
                  )
              )
        , testAll "DELETE FROM foo WHERE EXISTS (SELECT * FROM bar WHERE foo.a = bar.a);"
              defaultTestCatalog
              (@=? M.singleton
                  ( FullyQualifiedTableName "default_db" "public" "foo" )
                  ( S.fromList [ FullyQualifiedTableName "default_db" "public" "foo"
                               , FullyQualifiedTableName "default_db" "public" "bar"
                               ]
                  )
              )
        , testVertica "ALTER TABLE foo RENAME TO bar;" defaultTestCatalog
              (@=? M.fromList
                  [ ( FullyQualifiedTableName "default_db" "public" "bar" , S.singleton $ FullyQualifiedTableName "default_db" "public" "foo")
                  , ( FullyQualifiedTableName "default_db" "public" "foo", S.empty)
                  ]
              )
        , testVertica "ALTER TABLE foo SET SCHEMA other_schema;" defaultTestCatalog
              (@=? M.fromList
                  [ ( FullyQualifiedTableName "default_db" "other_schema" "foo", S.singleton $ FullyQualifiedTableName "default_db" "public" "foo")
                  , ( FullyQualifiedTableName "default_db" "public" "foo", S.empty)
                  ]
              )
        , testVertica "ALTER TABLE foo ADD PRIMARY KEY (bar);" defaultTestCatalog (@=? M.empty)
        , testVertica "TRUNCATE TABLE foo;" defaultTestCatalog
              (@=? M.singleton (FullyQualifiedTableName "default_db" "public" "foo") S.empty)
        , testHive "TRUNCATE TABLE foo;" defaultTestCatalog
              (@=? M.singleton (FullyQualifiedTableName "default_db" "public" "foo") S.empty)
        -- Presto doesn't have TRUNCATE
        , testHive "ALTER TABLE foo SET LOCATION 'hdfs://some/random/path';" defaultTestCatalog (@=? M.empty)
        , testVertica "ALTER PROJECTION foo RENAME TO bar;" defaultTestCatalog (@=? M.empty)

        , testAll "GRANT SELECT ON foo TO bar;" defaultTestCatalog (@=? M.empty)
        , testVertica "ALTER TABLE foo, foo1, bar RENAME TO foo1, foo2, baz;" defaultTestCatalog
            (@=? M.fromList
                [ ( FullyQualifiedTableName "default_db" "public" "bar"
                  , S.empty
                  )
                , ( FullyQualifiedTableName "default_db" "public" "baz"
                  , S.singleton $ FullyQualifiedTableName "default_db" "public" "bar"
                  )
                , ( FullyQualifiedTableName "default_db" "public" "foo"
                  , S.empty
                  )
                , ( FullyQualifiedTableName "default_db" "public" "foo1"
                  , S.empty
                  )
                , ( FullyQualifiedTableName "default_db" "public" "foo2"
                  , S.singleton $ FullyQualifiedTableName "default_db" "public" "foo"
                  )
                ]
            )
        , testHive "INSERT OVERWRITE TABLE foo SELECT * FROM bar;" defaultTestCatalog
          (@=? M.fromList
                [ ( FullyQualifiedTableName "default_db" "public" "foo"
                  , S.singleton $ FullyQualifiedTableName "default_db" "public" "bar"
                  )
                ]
          )
        ]
    , ticket "T387328" $ concat
      [ testHive "INSERT OVERWRITE TABLE foo PARTITION(dt='2008-06-08') IF NOT EXISTS SELECT * FROM bar;" defaultTestCatalog
            (@=? M.singleton
              ( FullyQualifiedTableName "default_db" "public" "foo" )
              ( S.fromList [ FullyQualifiedTableName "default_db" "public" "foo"
                           , FullyQualifiedTableName "default_db" "public" "bar"
                           ]
              )
            )
      , testHive "INSERT OVERWRITE TABLE foo PARTITION(dt='2008-06-08') SELECT * FROM bar;" defaultTestCatalog
            (@=? M.singleton
              ( FullyQualifiedTableName "default_db" "public" "foo" )
              ( S.singleton (FullyQualifiedTableName "default_db" "public" "bar") )
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
tests = test [ testTableLineage ]
