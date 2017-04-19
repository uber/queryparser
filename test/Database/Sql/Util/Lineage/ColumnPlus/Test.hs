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

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Database.Sql.Util.Lineage.ColumnPlus.Test where

import Test.HUnit
-- import Test.HUnit.Ticket
import qualified Database.Sql.Util.Test as Test
import Database.Sql.Util.Catalog
import Database.Sql.Util.Lineage.ColumnPlus
import Database.Sql.Type as SQL
import Database.Sql.Position

import qualified Data.Map as M
import qualified Data.Set as S

import qualified Data.Text.Lazy as TL
import           Data.Proxy (Proxy(..))


instance Test.TestableAnalysis HasColumnLineage a where
    type TestResult HasColumnLineage a = ColumnLineagePlus
    runTest _ _ = snd . getColumnLineage

testHive :: TL.Text -> Catalog -> (ColumnLineagePlus -> Assertion) -> [Assertion]
testHive = Test.testResolvedHive (Proxy :: Proxy HasColumnLineage)

testVertica :: TL.Text -> Catalog -> (ColumnLineagePlus -> Assertion) -> [Assertion]
testVertica = Test.testResolvedVertica (Proxy :: Proxy HasColumnLineage)

testAll :: TL.Text -> Catalog -> (ColumnLineagePlus -> Assertion) -> [Assertion]
testAll = Test.testResolvedAll (Proxy :: Proxy HasColumnLineage)


testColumnLineage :: Test
testColumnLineage = test
    [ "Generate column lineages for parsed queries" ~: concat
        [ testAll "SELECT 1;" defaultTestCatalog (@?= M.empty)
        , testAll "SELECT 1 FROM foo JOIN bar ON foo.a = bar.b;" defaultTestCatalog (@?= M.empty)
        , testAll "SELECT 1 FROM foo GROUP BY 1;" defaultTestCatalog (@?= M.empty)
        , testVertica "INSERT INTO foo VALUES (1, 2);"
              defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "foo"
                    , singleTableSet Range{start = Position 1 12 12, end = Position 1 15 15}
                        $ FullyQualifiedTableName "default_db" "public" "foo"
                    )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                    , singleColumnSet Range{ start = Position 1 0 0, end = Position 1 29 29 }
                        $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                    )
                  ]
              )
        , testVertica "INSERT INTO foo (SELECT a FROM bar);"
              defaultTestCatalog
              (@?= M.fromList
                  [
                      ( Left $ FullyQualifiedTableName "default_db" "public" "foo"
                      , ColumnPlusSet M.empty $ M.fromList
                        [ ( FullyQualifiedTableName "default_db" "public" "bar"
                          , S.singleton Range{start = Position 1 31 31, end = Position 1 34 34}
                          )
                        , ( FullyQualifiedTableName "default_db" "public" "foo"
                          , S.singleton Range{start = Position 1 12 12, end = Position 1 15 15}
                          )
                        ]
                      )
                  ,
                      ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                      , ColumnPlusSet
                          ( M.fromList
                              [ ( FullyQualifiedColumnName "default_db" "public" "foo" "a"
                                , M.singleton (FieldChain M.empty) $ S.singleton Range{start = Position 1 0 0, end = Position 1 34 34}
                                )
                              , ( FullyQualifiedColumnName "default_db" "public" "bar" "a"
                                , M.singleton (FieldChain M.empty) $ S.singleton Range{start = Position 1 31 31, end = Position 1 34 34}
                                )
                              ]
                          )
                          M.empty
                      )
                  ]
              )
        , testHive "INSERT INTO TABLE foo SELECT a FROM bar;"
              defaultTestCatalog
              (@?= M.fromList
                  [
                      ( Left $ FullyQualifiedTableName "default_db" "public" "foo"
                      , ColumnPlusSet M.empty $ M.fromList
                        [ ( FullyQualifiedTableName "default_db" "public" "bar"
                          , S.singleton Range{start = Position 1 36 36, end = Position 1 39 39}
                          )
                        , ( FullyQualifiedTableName "default_db" "public" "foo"
                          , S.singleton Range{start = Position 1 18 18, end = Position 1 21 21}
                          )
                        ]
                      )
                  ,
                      ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                      , ColumnPlusSet
                          ( M.fromList
                              [ ( FullyQualifiedColumnName "default_db" "public" "foo" "a"
                                , M.singleton (FieldChain M.empty) $ S.singleton Range{start = Position 1 0 0, end = Position 1 39 39}
                                )
                              , ( FullyQualifiedColumnName "default_db" "public" "bar" "a"
                                , M.singleton (FieldChain M.empty) $ S.singleton Range{start = Position 1 36 36, end = Position 1 39 39}
                                )
                              ]
                          )
                          M.empty
                      )
                  ]
              )
        , testVertica "CREATE TABLE baz LIKE bar;"
              defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "baz", emptyColumnPlusSet )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "a", emptyColumnPlusSet )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "b", emptyColumnPlusSet )
                  ]
              )
        , testHive "CREATE TABLE baz LIKE bar;"
              defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "baz", emptyColumnPlusSet )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "a", emptyColumnPlusSet )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "b", emptyColumnPlusSet )
                  ]
              )
        -- Presto support for CREATE TABLE is not yet implemented
        , testVertica "CREATE TABLE baz (quux int);"
              defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "baz", emptyColumnPlusSet )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "quux", emptyColumnPlusSet )
                  ]
              )
        , testHive "CREATE TABLE baz (quux int);"
              defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "baz", emptyColumnPlusSet )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "quux", emptyColumnPlusSet )
                  ]
              )
        -- Presto support for CREATE TABLE is not yet implemented
        , testVertica "CREATE TABLE baz AS (SELECT * FROM bar);"
              defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "baz"
                    , singleTableSet Range{start = Position 1 35 35, end = Position 1 38 38}
                        $ FullyQualifiedTableName "default_db" "public" "bar"
                    )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "a"
                    , singleColumnSet Range{start = Position 1 35 35, end = Position 1 38 38}
                        $ FullyQualifiedColumnName "default_db" "public" "bar" "a"
                    )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "b"
                    , singleColumnSet Range{start = Position 1 35 35, end = Position 1 38 38}
                        $ FullyQualifiedColumnName "default_db" "public" "bar" "b"
                    )
                  ]
              )
        , testHive "CREATE TABLE baz AS SELECT * FROM bar;"
              defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "baz"
                    , singleTableSet Range{start = Position 1 34 34, end = Position 1 37 37}
                        $ FullyQualifiedTableName "default_db" "public" "bar"
                    )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "a"
                    , singleColumnSet Range{start = Position 1 34 34, end = Position 1 37 37}
                        $ FullyQualifiedColumnName "default_db" "public" "bar" "a"
                    )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "b"
                    , singleColumnSet Range{start = Position 1 34 34, end = Position 1 37 37}
                        $ FullyQualifiedColumnName "default_db" "public" "bar" "b"
                    )
                  ]
              )
        , testVertica "CREATE TABLE baz AS (SELECT foo.a, bar.b FROM bar CROSS JOIN foo);"
              defaultTestCatalog
              (@?= M.fromList
                  [ ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "a"
                    , singleColumnSet Range{start = Position 1 61 61, end = Position 1 64 64}  -- interesting that this matches the table, not the column
                        $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                    )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "b"
                    , singleColumnSet Range{start = Position 1 46 46, end = Position 1 49 49}
                        $ FullyQualifiedColumnName "default_db" "public" "bar" "b"
                    )
                  , ( Left $ FullyQualifiedTableName "default_db" "public" "baz"
                    , ColumnPlusSet M.empty $ M.fromList
                        [ ( FullyQualifiedTableName "default_db" "public" "foo"
                          , S.singleton Range{start = Position 1 61 61, end = Position 1 64 64}
                          )
                        , ( FullyQualifiedTableName "default_db" "public" "bar"
                          , S.singleton Range{start = Position 1 46 46, end = Position 1 49 49}
                          )
                        ]
                    )
                  ]
              )
        , testHive "CREATE TABLE baz AS SELECT foo.a, bar.b FROM bar CROSS JOIN foo;"
              defaultTestCatalog
              (@?= M.fromList
                  [ ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "a"
                    , singleColumnSet Range{start = Position 1 60 60, end = Position 1 63 63}
                        $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                    )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "b"
                    , singleColumnSet Range{start = Position 1 45 45, end = Position 1 48 48}
                        $ FullyQualifiedColumnName "default_db" "public" "bar" "b"
                    )
                  , ( Left $ FullyQualifiedTableName "default_db" "public" "baz"
                    , ColumnPlusSet M.empty $ M.fromList
                        [ ( FullyQualifiedTableName "default_db" "public" "foo"
                          , S.singleton Range{start = Position 1 60 60, end = Position 1 63 63}
                          )
                        , ( FullyQualifiedTableName "default_db" "public" "bar"
                          , S.singleton Range{start = Position 1 45 45, end = Position 1 48 48}
                          )
                        ]
                    )
                  ]
              )
        , testAll "DROP TABLE IF EXISTS foo;"
              defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "foo", emptyColumnPlusSet )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a", emptyColumnPlusSet )
                  ]
              )
        , testAll "DELETE FROM foo;"
              defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "foo", emptyColumnPlusSet )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a", emptyColumnPlusSet )
                  ]
              )
        , testAll "DELETE FROM foo WHERE EXISTS (SELECT * FROM bar);"
              defaultTestCatalog
              (@?= M.fromList
                  [ ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                    , ColumnPlusSet
                        ( M.fromList
                            [ ( FullyQualifiedColumnName "default_db" "public" "foo" "a"
                              , M.singleton (FieldChain M.empty)
                                  $ S.singleton Range{start = Position 1 12 12, end = Position 1 15 15}
                              )
                            ]
                        )
                        ( M.singleton (FullyQualifiedTableName "default_db" "public" "bar") 
                              $ S.singleton Range{start = Position 1 44 44, end = Position 1 47 47}
                        )
                    )
                  , ( Left $ FullyQualifiedTableName "default_db" "public" "foo"
                    , ColumnPlusSet M.empty
                        ( M.fromList
                            [ ( FullyQualifiedTableName "default_db" "public" "foo"
                              , S.singleton Range{start = Position 1 12 12, end = Position 1 15 15}
                              )
                            , ( FullyQualifiedTableName "default_db" "public" "bar"
                              , S.singleton Range{start = Position 1 44 44, end = Position 1 47 47}
                              )
                            ]
                        )
                    )
                  ]
              )
        , testAll "DELETE FROM foo WHERE EXISTS (SELECT * FROM bar WHERE foo.a = bar.a);"
              defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "foo"
                    , ColumnPlusSet
                        ( M.fromList
                            [ ( FullyQualifiedColumnName "default_db" "public" "bar" "a"
                              , M.singleton (FieldChain M.empty)
                                  $ S.singleton Range { start = Position 1 44 44, end = Position 1 47 47 } 
                              )
                            , ( FullyQualifiedColumnName "default_db" "public" "foo" "a"
                              , M.singleton (FieldChain M.empty)
                                  $ S.singleton Range { start = Position 1 12 12, end = Position 1 15 15 } 
                              )
                            ]
                        )
                        ( M.fromList
                            [ ( FullyQualifiedTableName "default_db" "public" "bar"
                              , S.singleton Range { start = Position 1 44 44, end = Position 1 47 47 } 
                              )
                            , ( FullyQualifiedTableName "default_db" "public" "foo"
                              , S.singleton Range { start = Position 1 12 12, end = Position 1 15 15 } 
                              )
                            ]
                        )
                    )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                    , ColumnPlusSet
                        ( M.fromList
                            [ ( FullyQualifiedColumnName "default_db" "public" "bar" "a"
                              , M.singleton (FieldChain M.empty)
                                  $ S.singleton Range { start = Position 1 44 44, end = Position 1 47 47 } 
                              )
                            , ( FullyQualifiedColumnName "default_db" "public" "foo" "a"
                              , M.singleton (FieldChain M.empty)
                                  $ S.singleton Range { start = Position 1 12 12, end = Position 1 15 15 } 
                              )
                            ]
                        )
                        ( M.singleton (FullyQualifiedTableName "default_db" "public" "bar")
                              $ S.singleton Range { start = Position 1 44 44, end = Position 1 47 47 } 
                        )
                    )
                  ]
              )
        , testVertica "ALTER TABLE foo RENAME TO bar;" defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "bar"
                    , singleTableSet Range{start = Position 1 12 12, end = Position 1 15 15}
                        $ FullyQualifiedTableName "default_db" "public" "foo"
                    )
                  , ( Left $ FullyQualifiedTableName "default_db" "public" "foo", emptyColumnPlusSet )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "bar" "a"
                    , singleColumnSet Range{start = Position 1 12 12, end = Position 1 15 15}
                        $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                    )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a", emptyColumnPlusSet )
                  ]
              )
        , testVertica "ALTER TABLE foo SET SCHEMA other_schema;" defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "other_schema" "foo"
                    , singleTableSet Range{start = Position 1 12 12, end = Position 1 15 15}
                        $ FullyQualifiedTableName "default_db" "public" "foo"
                    )
                  , ( Left $ FullyQualifiedTableName "default_db" "public" "foo", emptyColumnPlusSet )
                  , ( Right $ FullyQualifiedColumnName "default_db" "other_schema" "foo" "a"
                    , singleColumnSet Range{start = Position 1 12 12, end = Position 1 15 15}
                        $ FullyQualifiedColumnName "default_db" "public" "foo" "a" )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a", emptyColumnPlusSet )
                  ]
              )
        , testVertica "ALTER TABLE foo ADD PRIMARY KEY (bar);" defaultTestCatalog (@?= M.empty)
        , testVertica "TRUNCATE TABLE foo;" defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "foo", emptyColumnPlusSet )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a", emptyColumnPlusSet )
                  ]
              )
        , testHive "TRUNCATE TABLE foo;" defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "foo", emptyColumnPlusSet )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a", emptyColumnPlusSet )
                  ]
              )
        -- Presto doesn't have TRUNCATE
        , testHive "TRUNCATE TABLE foo PARTITION (datestr = '2016-04-01', a = '42');" defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "foo"
                    , singleTableSet Range{start = Position 1 15 15, end = Position 1 18 18}
                        $ FullyQualifiedTableName "default_db" "public" "foo"
                    )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                    , singleColumnSet Range{start = Position 1 15 15, end = Position 1 18 18}
                        $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                    )
                  ]
              )
        , testHive "ALTER TABLE foo SET LOCATION 'hdfs://some/random/path';" defaultTestCatalog (@?= M.empty)
        , testVertica "ALTER PROJECTION foo RENAME TO bar;" defaultTestCatalog (@?= M.empty)

        , testAll "GRANT SELECT ON foo TO bar;" defaultTestCatalog (@?= M.empty)
        , testVertica "ALTER TABLE foo, foo1, bar RENAME TO foo1, foo2, baz;" defaultTestCatalog
            (@?= M.fromList
                [ ( Left $ FullyQualifiedTableName "default_db" "public" "bar"
                  , emptyColumnPlusSet
                  )
                , ( Right $ FullyQualifiedColumnName "default_db" "public" "bar" "a"
                  , emptyColumnPlusSet
                  )
                , ( Right $ FullyQualifiedColumnName "default_db" "public" "bar" "b"
                  , emptyColumnPlusSet
                  )
                , ( Left $ FullyQualifiedTableName "default_db" "public" "baz"
                  , singleTableSet Range{start = Position 1 23 23, end = Position 1 26 26}
                      $ FullyQualifiedTableName "default_db" "public" "bar"
                  )
                , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "a"
                  , singleColumnSet Range{start = Position 1 23 23, end = Position 1 26 26}
                      $ FullyQualifiedColumnName "default_db" "public" "bar" "a"
                  )
                , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "b"
                  , singleColumnSet Range{start = Position 1 23 23, end = Position 1 26 26}
                      $ FullyQualifiedColumnName "default_db" "public" "bar" "b"
                  )
                , ( Left $ FullyQualifiedTableName "default_db" "public" "foo"
                  , emptyColumnPlusSet
                  )
                , ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                  , emptyColumnPlusSet
                  )
                , ( Right $ FullyQualifiedColumnName "default_db" "public" "foo1" "a"
                  , emptyColumnPlusSet
                  )
                , ( Left $ FullyQualifiedTableName "default_db" "public" "foo1"
                  , emptyColumnPlusSet
                  )
                , ( Left $ FullyQualifiedTableName "default_db" "public" "foo2"
                  , singleTableSet Range{start = Position 1 12 12, end = Position 1 15 15}
                      $ FullyQualifiedTableName "default_db" "public" "foo"
                  )
                , ( Right $ FullyQualifiedColumnName "default_db" "public" "foo2" "a"
                  , singleColumnSet Range{start = Position 1 12 12, end = Position 1 15 15}
                      $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                  )
                ]
            )
        , testHive "INSERT OVERWRITE TABLE foo SELECT a FROM bar;" defaultTestCatalog
          (@?= M.fromList
              [ ( Left $ FullyQualifiedTableName "default_db" "public" "foo"
                , singleTableSet Range{start = Position 1 41 41, end = Position 1 44 44}
                    $ FullyQualifiedTableName "default_db" "public" "bar"
                )
              , ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                , singleColumnSet Range{start = Position 1 41 41, end = Position 1 44 44}
                    $ FullyQualifiedColumnName "default_db" "public" "bar" "a"
                )
              ]
          )

        , testHive "INSERT OVERWRITE TABLE foo SELECT a.x.y FROM bar;" defaultTestCatalog
          (@?= M.fromList
              [ ( Left $ FullyQualifiedTableName "default_db" "public" "foo"
                , singleTableSet Range{start = Position 1 45 45, end = Position 1 48 48}
                    $ FullyQualifiedTableName "default_db" "public" "bar"
                )
              , ( Right $ FullyQualifiedColumnName "default_db" "public" "foo" "a"
                , mempty
                    { columnPlusColumns = M.singleton (FullyQualifiedColumnName "default_db" "public" "bar" "a")
                        $ M.singleton 
                            ( FieldChain $ M.singleton (StructFieldName () "x")
                                $ FieldChain $ M.singleton (StructFieldName () "y")
                                    $ FieldChain M.empty
                            )
                            ( S.singleton Range{start = Position 1 34 34, end = Position 1 37 37} )
                    }
                )
              ]
          )
        , testVertica "CREATE TABLE baz AS (SELECT a LIKE b as c FROM bar);"
              defaultTestCatalog
              (@?= M.fromList
                  [ ( Left $ FullyQualifiedTableName "default_db" "public" "baz"
                    , singleTableSet Range{start = Position 1 47 47, end = Position 1 50 50}
                        $ FullyQualifiedTableName "default_db" "public" "bar"
                    )
                  , ( Right $ FullyQualifiedColumnName "default_db" "public" "baz" "c"
                    , mempty
                        { columnPlusColumns = M.fromList
                            [ ( FullyQualifiedColumnName "default_db" "public" "bar" "a", M.singleton (FieldChain mempty)
                                    $ S.singleton Range{start = Position 1 47 47, end = Position 1 50 50}
                              )
                            , (FullyQualifiedColumnName "default_db" "public" "bar" "b", M.singleton (FieldChain mempty)
                                    $ S.singleton Range{start = Position 1 47 47, end = Position 1 50 50}
                              )
                            ]
                        }
                    )
                  ]
              )
        ]
    ]

type DefaultCatalogType =
    '[Database "default_db"
        '[ Schema "public"
            '[ Table "foo" '[Column "a" SqlType]
             , Table "bar"
                '[ Column "a" SqlType
                 , Column "b" SqlType
                 ]
             ]
         ]
    ]

defaultCatalogProxy :: Proxy DefaultCatalogType
defaultCatalogProxy = Proxy

defaultDatabase :: DatabaseName ()
defaultDatabase = DatabaseName () "default_db"

publicSchema :: UQSchemaName ()
publicSchema = mkNormalSchema "public" ()

defaultTestCatalog :: Catalog
defaultTestCatalog = makeCatalog (mkCatalog defaultCatalogProxy) [publicSchema] defaultDatabase

tests :: Test
tests = test [ testColumnLineage ]
