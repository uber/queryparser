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

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Database.Sql.Util.Joins.Test where

import Test.HUnit
import Test.HUnit.Ticket

import qualified Database.Sql.Util.Test as Test

import Database.Sql.Util.Joins
import Database.Sql.Type as SQL

import qualified Data.Set as S
import qualified Data.HashMap.Strict as HMS

import Data.Proxy (Proxy(..))
import Data.Text.Lazy (Text)


instance Test.TestableAnalysis HasJoins a where
    type TestResult HasJoins a = JoinsResult

    runTest _ _ = getJoins

testHive :: Text -> Catalog -> (JoinsResult -> Assertion) -> [Assertion]
testHive = Test.testResolvedHive (Proxy :: Proxy HasJoins)

testVertica :: Text -> Catalog -> (JoinsResult -> Assertion) -> [Assertion]
testVertica = Test.testResolvedVertica (Proxy :: Proxy HasJoins)

testAll :: Text -> Catalog -> (JoinsResult -> Assertion) -> [Assertion]
testAll = Test.testResolvedAll (Proxy :: Proxy HasJoins)

testJoins :: Test
testJoins = test
    [ "Generate joins for parsed queries" ~: concat
        [ testAll "SELECT 1;" defaultTestCatalog (@=? S.empty)
        , testAll "SELECT 1 FROM foo JOIN bar ON foo.a = bar.b;" defaultTestCatalog
            (@=? S.singleton
                ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                )
            )

        , testAll "SELECT 1 FROM foo, bar WHERE foo.a = bar.b;" defaultTestCatalog
            (@=? S.singleton
                ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                )
            )

        , testAll "SELECT 1 FROM (select * from foo) as f JOIN bar ON f.a = bar.b;" defaultTestCatalog
            (@=? S.singleton
                ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                )
            )

        , testAll "SELECT 1 FROM (select foo.* from foo) as f JOIN bar ON f.a = bar.b;" defaultTestCatalog
            (@=? S.singleton
                ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                )
            )

        , testVertica "SELECT foo.a as n FROM foo GROUP BY n;" defaultTestCatalog (@=? S.empty)
        , testAll "SELECT foo.a as n FROM foo ORDER BY n;" defaultTestCatalog (@=? S.empty)
        , testVertica "SELECT * from foo JOIN bar USING (a);" defaultTestCatalog
            (@=? S.singleton
                ( (FullyQualifiedColumnName "default_db" "public" "bar" "a", [])
                , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                )
            )
        , testVertica "SELECT * from foo NATURAL JOIN bar;" defaultTestCatalog
            (@=? S.singleton
                ( (FullyQualifiedColumnName "default_db" "public" "bar" "a", [])
                , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                )
            )

        , testAll "WITH blah AS (SELECT * FROM foo) SELECT * FROM bar, blah WHERE blah.a = bar.a;" defaultTestCatalog
            (@=? S.singleton
                ( (FullyQualifiedColumnName "default_db" "public" "bar" "a", [])
                , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                )
            )

        , testAll "SELECT 1 FROM foo AS x, bar AS y WHERE x.a = y.b;" defaultTestCatalog
            (@=? S.singleton
                ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                )
            )

        , testVertica "SELECT 1 FROM (SELECT * FROM foo) x JOIN (SELECT * FROM bar) y USING (a);" defaultTestCatalog
            (@=? S.singleton
                ( (FullyQualifiedColumnName "default_db" "public" "bar" "a", [])
                , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                )
            )

        , testAll "SELECT CURRENT_TIME = a FROM foo;" defaultTestCatalog (@=? S.empty)

        , testAll "SELECT * FROM (SELECT 1 AS a) j JOIN (select a FROM foo) k ON j.a = k.a;" defaultTestCatalog (@=? S.empty)

        , testVertica "SELECT t.count FROM (SELECT COUNT(1) :: integer) t;" defaultTestCatalog (@=? S.empty)

        , testAll "SELECT * FROM foo, bar WHERE foo.a = CASE WHEN bar.b > 7 THEN bar.a ELSE bar.a - 5 END;" defaultTestCatalog
            (@=? S.singleton
                ( (FullyQualifiedColumnName "default_db" "public" "bar" "a", [])
                , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                )
            )

        , testVertica "SELECT datediff(second, '1977-05-25', now());" defaultTestCatalog (@=? S.empty)
        , testHive "SELECT datediff('1977-05-25', now());" defaultTestCatalog (@=? S.empty)
        , testAll "DELETE FROM foo WHERE EXISTS (SELECT * FROM bar WHERE foo.a = bar.b);" defaultTestCatalog
            (@=? S.singleton
                ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                )
            )
        , testHive "TRUNCATE TABLE foo;" defaultTestCatalog (@=? S.empty)
        , testVertica "TRUNCATE TABLE foo;" defaultTestCatalog (@=? S.empty)
        -- presto doesn't have TRUNCATE
        , testVertica "ALTER TABLE foo ADD PRIMARY KEY (bar);" defaultTestCatalog (@=? S.empty)
        , testVertica "ALTER PROJECTION foo RENAME TO bar;" defaultTestCatalog (@=? S.empty)
        , testAll "GRANT SELECT ON foo TO bar;" defaultTestCatalog (@=? S.empty)
        , testHive "SELECT * FROM foo LEFT SEMI JOIN bar INNER JOIN bae ON (foo.a = bar.a) AND (foo.a = c);" defaultTestCatalog
            (@=? S.fromList
                [ ( (FullyQualifiedColumnName "default_db" "public" "bae" "c", [])
                  , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                  )
                , ( (FullyQualifiedColumnName "default_db" "public" "bar" "a", [])
                  , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                  )
                ]
            )
        , testAll "SELECT * FROM foo JOIN bar ON true WHERE foo.a > bar.b;" defaultTestCatalog
            (@=? S.fromList
                [ ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                  , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                  )
                ]
            )
        , testVertica "SELECT * FROM foo JOIN bar ON true WHERE foo.a LIKE concat('%', bar.b, '%');" defaultTestCatalog
            (@=? S.fromList
                [ ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                  , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                  )
                ]
            )
        , testAll "SELECT * FROM foo JOIN bar ON true JOIN bae ON true WHERE foo.a IN (bar.b * 2, bae.c + 1);" defaultTestCatalog
            (@=? S.fromList
                [ ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                  , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                  )
                , ( (FullyQualifiedColumnName "default_db" "public" "bae" "c", [])
                  , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                  )
                , ( (FullyQualifiedColumnName "default_db" "public" "bae" "c", [])
                  , (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                  )
                ]
            )
        , testVertica "SELECT * FROM foo WHERE foo.a IN (SELECT b from bar);" defaultTestCatalog
            (@=? S.fromList
                [ ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                  , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                  )
                ]
            )
        , testHive "SELECT * FROM foo JOIN bar ON foo.a.field = bar.b.field;" defaultTestCatalog
            (@=? S.fromList
                [ ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [StructFieldName () "field"])
                  , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [StructFieldName () "field"])
                  )
                ]
            )
        , testHive "SELECT * FROM foo JOIN bar ON foo.a = bar.b.field1.field2;" defaultTestCatalog
            (@=? S.fromList
                [ ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [StructFieldName () "field1", StructFieldName () "field2"])
                  , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                  )
                ]
            )
        , testAll "SELECT a FROM foo UNION SELECT b FROM bar;" defaultTestCatalog
            (@=? S.fromList
                [ ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                  , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                  )
                ]
            )
        , testVertica "SELECT a FROM foo INTERSECT SELECT b FROM bar;" defaultTestCatalog
            (@=? S.fromList
                [ ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                  , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                  )
                ]
            )
        , testVertica "SELECT a FROM foo EXCEPT SELECT b FROM bar;" defaultTestCatalog
            (@=? S.fromList
                [ ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                  , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                  )
                ]
            )

        -- This is why we don't add fields to aliases.  t.x involves foo.a, but foo.a.col1 is not a thing
        , testHive "WITH t AS (SELECT struct(a) x FROM foo) SELECT COUNT(1) FROM t JOIN bar ON t.x.col1 = bar.b;" defaultTestCatalog
            (@=? S.fromList
                [ ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                  , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                  )
                ]
            )
        ]

    , ticket "T293571" $ concat
        [ testVertica "WITH qux as (select 1 as a) SELECT * FROM (bar JOIN qux USING (a)) AS baz JOIN foo ON baz.b = foo.a;" defaultTestCatalog
            (@=? S.fromList
                [ ( (FullyQualifiedColumnName "default_db" "public" "bar" "b", [])
                  , (FullyQualifiedColumnName "default_db" "public" "foo" "a", [])
                  )
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
            , ( QTableName () None "bae"
              , persistentTable [ QColumnName () None "c" ]
              )
            ]
          )
        ]
    )
    [ publicSchema ]
    defaultDatabase

tests :: Test
tests = test [ testJoins ]
