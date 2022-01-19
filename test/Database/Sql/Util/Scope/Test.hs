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

{-# LANGUAGE TemplateHaskell #-}

module Database.Sql.Util.Scope.Test where

import           Test.HUnit
import           Test.HUnit.Ticket
import qualified Test.Tasty.Providers as Tasty
import qualified Test.Tasty.Runners as Tasty
import           Test.Tasty.Golden
import           Control.Exception (catch, AssertionFailed (..))
import           Control.Arrow (second)

import Database.Sql.Util.Scope
import Database.Sql.Type as SQL

import Database.Sql.Vertica.Type as VSQL
import Database.Sql.Hive.Type as HiveQL
import Database.Sql.Presto.Type as PrestoQL
import Database.Sql.Position (Range)

import qualified Database.Sql.Vertica.Parser as VP
import qualified Database.Sql.Hive.Parser as HP
import qualified Database.Sql.Presto.Parser as PP

import Database.Sql.Util.Test (defaultTestPath, makeSelect, Resolvable, resolve)

import qualified Data.HashMap.Strict as HMS
import qualified Data.Text.Lazy.Encoding as TL
import qualified Data.Text.Lazy as TL
import           Data.Text.Lazy (Text)
import           Data.Either (lefts)

import System.FilePath

import Language.Haskell.Exts (ParseResult(..), SrcLoc(..), parseExp, prettyPrint)

warnings :: [Either (ResolutionError Integer) (ResolutionSuccess Integer)] -> [ResolutionError Integer]
warnings = lefts

fmt :: Show a => a -> Text
fmt expr = TL.pack $ (++ "\n") $ case parseExp (show expr) of
  ParseOk ast -> prettyPrint ast
  ParseFailed SrcLoc{..} errMsg -> srcFilename ++ ":" ++ show srcLine ++ ":" ++ show srcColumn ++ ": " ++ errMsg

tastyToHUnit :: Tasty.TestTree -> Assertion
tastyToHUnit = (`Tasty.foldTestTree` mempty) Tasty.trivialFold{ Tasty.foldSingle = \ opts _ t -> assert . Tasty.resultSuccessful =<< Tasty.run opts t (const $ pure ()) }

testVertica :: FilePath -> Catalog -> VerticaStatement RawNames Integer -> Assertion
testVertica name catalog stmt =
    tastyToHUnit $ goldenVsString name ("goldens" </> "scope" </> name) (pure $ TL.encodeUtf8 $ fmt $ second warnings $ runResolverWarn (VSQL.resolveVerticaStatement stmt) VSQL.dialectProxy catalog)

testHive :: FilePath -> Catalog -> HiveStatement RawNames Integer -> Assertion
testHive name catalog stmt =
    tastyToHUnit $ goldenVsString name ("goldens" </> "scope" </> name) (pure $ TL.encodeUtf8 $ fmt $ second warnings $ runResolverWarn (HiveQL.resolveHiveStatement stmt) HiveQL.dialectProxy catalog)


class Resolvable q => ResolvesSuccessfully q where
    resolvesSuccessfully :: Text -> q -> Catalog -> Assertion

instance ResolvesSuccessfully (VerticaStatement RawNames Range) where
    resolvesSuccessfully sql q catalog =
        let resolved = resolve catalog q
         in assertEqual "" resolved resolved
              `catch` \ (AssertionFailed str) -> assertFailure $ unlines [str, show sql]

parsesAndResolvesSuccessfullyVertica :: Catalog -> Text -> Assertion
parsesAndResolvesSuccessfullyVertica catalog sql = case VP.parse sql of
    (Left e) -> assertFailure $ unlines
        [ "failed to parse:"
        , show sql
        , show e
        ]
    (Right ast) -> resolvesSuccessfully sql ast catalog

instance ResolvesSuccessfully (HiveStatement RawNames Range) where
    resolvesSuccessfully sql q catalog =
        let resolved = resolve catalog q
         in assertEqual "" resolved resolved
              `catch` \ (AssertionFailed str) -> assertFailure $ unlines [str, show sql]

parsesAndResolvesSuccessfullyHive :: Catalog -> Text -> Assertion
parsesAndResolvesSuccessfullyHive catalog sql = case HP.parse sql of
    (Left e) -> assertFailure $ unlines
        [ "failed to parse:"
        , show sql
        , show e
        ]
    (Right ast) -> resolvesSuccessfully sql ast catalog

instance ResolvesSuccessfully (PrestoStatement RawNames Range) where
    resolvesSuccessfully sql q catalog =
        let resolved = resolve catalog q
         in assertEqual "" resolved resolved
              `catch` \ (AssertionFailed str) -> assertFailure $ unlines [str, show sql]

parsesAndResolvesSuccessfullyPresto :: Catalog -> Text -> Assertion
parsesAndResolvesSuccessfullyPresto catalog sql = case PP.parse sql of
    (Left e) -> assertFailure $ unlines
        [ "failed to parse:"
        , show sql
        , show e
        ]
    (Right ast) -> resolvesSuccessfully sql ast catalog

defaultDatabase :: a -> DatabaseName a
defaultDatabase info = DatabaseName info "default_db"

inDefaultDatabase :: Applicative f => a -> Text -> QSchemaName f a
inDefaultDatabase info name = QSchemaName info (pure $ defaultDatabase info) name NormalSchema

testNoResolveErrors :: Test
testNoResolveErrors =
    let defaultSchema = mkNormalSchema "default" ()
        currentDatabase = defaultDatabase ()
        catalog = HMS.singleton currentDatabase $ HMS.fromList
            [ ( defaultSchema
              , HMS.fromList
                  [ ( QTableName () None "foo"
                    , persistentTable
                        [ QColumnName () None "col" ]
                    )
                  , ( QTableName () None "bar"
                    , persistentTable
                        [ QColumnName () None "b" ]
                    )
                  ]
              )
            ]
        path = [defaultSchema]

     in test
        [ "test for regressions on the ambiguous-columns bug (D687922)" ~:
          map (TestCase . parsesAndResolvesSuccessfullyVertica (makeCatalog catalog path currentDatabase))
            [ "SELECT col FROM foo GROUP BY col;"
            ]

        , "test that struct-field access resolves in various clauses" ~:
          map (TestCase . parsesAndResolvesSuccessfullyHive (makeCatalog catalog path currentDatabase))
            [ "SELECT col.field FROM foo;"  -- SELECT
            , "SELECT * FROM foo WHERE EXISTS (SELECT * FROM bar WHERE col.field = bar.b);" -- SELECT, correlated subquery (bonus points!)
            , "SELECT * FROM foo WHERE col.field > 0;"  --WHERE
            , "SELECT * FROM foo CLUSTER BY col.field;" -- CLUSTER (we discard
              -- the clustering info, so there's nothing to resolve!)

            , "DELETE FROM foo WHERE col.field IS NOT NULL;" -- DELETE
            ]

        , ticket "T541187" $
          map (parsesAndResolvesSuccessfullyHive (makeCatalog catalog path currentDatabase))
            [ "SELECT * FROM foo LATERAL VIEW explode(col.field) foo;"  -- FROM
            , "SELECT col.field, count(*) FROM foo GROUP BY col.field;" -- GROUP
            , TL.unlines -- HAVING
                [ "SELECT col.field, count(*) FROM foo"
                , "GROUP BY col.field HAVING count(DISTINCT col.field) > 1;"
                ]
            , TL.unlines -- NAMED WINDOW
                [ "SELECT RANK() OVER w FROM foo"
                , "WINDOW w AS (PARTITION BY col.field ORDER BY col.field);"
                ]
            , "SELECT * FROM foo ORDER BY col.field;" --ORDER
            ]

        , "test that TablishAliasesTC introduce the column aliases correctly" ~:
          map (TestCase . parsesAndResolvesSuccessfullyPresto (makeCatalog catalog path currentDatabase))
            [ "SELECT cAlias FROM foo AS tAlias (cAlias) WHERE cAlias > 10;"
            , "SELECT        cAlias FROM foo AS tAlias (cAlias) ORDER BY        cAlias;"
            , "SELECT        cAlias FROM foo AS tAlias (cAlias) ORDER BY tAlias.cAlias;"
            , "SELECT tAlias.cAlias FROM foo AS tAlias (cAlias) ORDER BY tAlias.cAlias;"
            , "SELECT        cAlias FROM (SELECT col FROM foo) AS tAlias (cAlias) ORDER BY        cAlias;"
            , "SELECT        cAlias FROM (SELECT col FROM foo) AS tAlias (cAlias) ORDER BY tAlias.cAlias;"
            , "SELECT tAlias.cAlias FROM (SELECT col FROM foo) AS tAlias (cAlias) ORDER BY tAlias.cAlias;"
            , TL.unlines
                [ "SELECT n"
                , "FROM bar AS x (numbers)"
                , "CROSS JOIN UNNEST(numbers) AS t (n);"
                ]
            ]
        , "test named window" ~:
          map (TestCase . parsesAndResolvesSuccessfullyPresto (makeCatalog catalog path currentDatabase))
            [ "SELECT sum(col) OVER x FROM foo WINDOW x AS (partition by col);"
            , "SELECT sum(col) OVER (x) FROM foo WINDOW x AS (partition by col);"
            , "SELECT sum(col) OVER (x ORDER BY col) FROM foo WINDOW x AS (partition by col);"
            ]
        ]



testResolutionOnASTs :: Test
testResolutionOnASTs = test
    [ "test resolution on some simple queries" ~:
        [ "SELECT a FROM foo;" ~: testVertica "SIMPLE" defaultTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "a" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 7 Nothing "a" )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 8
                                [ TablishTable 9 TablishAliasesNone
                                    ( QTableName 10 Nothing "foo" )
                                ]
                            )
                        }
                    )
                )
            )
        , "SELECT x y FROM (select 1 x) foo GROUP BY y;" ~: testVertica "GROUP" defaultTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "y" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 7 Nothing "x" )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 8
                                [ TablishSubQuery 9
                                    ( TablishAliasesT ( TableAlias 10 "foo" ( TableAliasId 3 ) ) )
                                    ( QuerySelect 11
                                        ( makeSelect 12
                                            ( SelectColumns 13
                                                [ SelectExpr 14
                                                    [ ColumnAlias 15 "x"
                                                        ( ColumnAliasId 2 )
                                                    ]
                                                    ( ConstantExpr 16
                                                        ( NumericConstant 17 "1" )
                                                    )
                                                ]
                                            )
                                        )
                                    )
                                ]
                            )
                        , selectGroup = Just
                            ( SelectGroup 18
                                [ GroupingElementExpr 19
                                    ( PositionOrExprExpr ( ColumnExpr 19 ( QColumnName 20 Nothing "y" ) ) )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT * FROM foo JOIN bar USING (a);" ~: ticket' "T481972"
            [ assertFailure "no golden output - test hasn't passed yet" ]
            [ testVertica "USING.broken" defaultTestCatalog
                ( VerticaStandardSqlStatement
                    ( QueryStmt
                        ( QuerySelect 1
                            ( makeSelect 2
                                ( SelectColumns
                                    { selectColumnsInfo = 3
                                    , selectColumnsList = [ SelectStar 4 Nothing Unused ]
                                    }
                                )
                            )
                            { selectFrom = Just
                                ( SelectFrom 5
                                    [ TablishJoin 6
                                        ( JoinInner 7 )
                                        ( JoinUsing 8
                                            [ QColumnName 9 None "a" ]
                                        )
                                        ( TablishTable 10 TablishAliasesNone
                                            ( QTableName 11 Nothing "foo" )
                                        )
                                        ( TablishTable 12 TablishAliasesNone
                                            ( QTableName 13 Nothing "bar" )
                                        )
                                    ]
                                )
                            }
                        )
                    )
                )
            ]

        , "SELECT * FROM foo NATURAL JOIN bar;" ~: ticket' "T481972"
            [ assertFailure "no golden output - test hasn't passed yet" ]
            [ testVertica "NATURAL.broken" defaultTestCatalog
                ( VerticaStandardSqlStatement
                    ( QueryStmt
                        ( QuerySelect 1
                            ( makeSelect 2
                                ( SelectColumns
                                    { selectColumnsInfo = 3
                                    , selectColumnsList = [ SelectStar 4 Nothing Unused ]
                                    }
                                )
                            )
                            { selectFrom = Just
                                ( SelectFrom 5
                                    [ TablishJoin 7
                                        ( JoinInner 8 )
                                        ( JoinNatural 6 Unused )
                                        ( TablishTable 9 TablishAliasesNone
                                            ( QTableName 10 Nothing "foo" )
                                        )
                                        ( TablishTable 11 TablishAliasesNone
                                            ( QTableName 12 Nothing "bar" )
                                        )
                                    ]
                                )
                            }
                        )
                    )
                )
            ]
        ]

    , "test errors and warnings" ~:
        [ "SELECT b FROM foo;" ~: ticket' "T406873"
            [ assertFailure "no golden output - test hasn't passed yet" ]
            [ testVertica "MISSINGCOL.broken" defaultTestCatalog
                ( VerticaStandardSqlStatement
                    ( QueryStmt
                        ( QuerySelect 1
                            ( makeSelect 2
                                ( SelectColumns 3
                                    [ SelectExpr 4
                                        [ ColumnAlias 5 "b" ( ColumnAliasId 1 ) ]
                                        ( ColumnExpr 6
                                            ( QColumnName 7 Nothing "b" )
                                        )
                                    ]
                                )
                            )
                            { selectFrom = Just
                                ( SelectFrom 8
                                    [ TablishTable 9 TablishAliasesNone
                                        ( QTableName 10 Nothing "foo" )
                                    ]
                                )
                            }
                        )
                    )
                )
            ]
        , "SELECT b FROM (SELECT 1 a) foo;" ~: testVertica "SUBQUERY" defaultTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "b" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 7 Nothing "b" )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 8
                                [ TablishSubQuery 9
                                    ( TablishAliasesT ( TableAlias 10 "foo" ( TableAliasId 3 ) ) )
                                    ( QuerySelect 11
                                        ( makeSelect 12
                                            ( SelectColumns 13
                                                [ SelectExpr 14
                                                    [ ColumnAlias 15 "a" ( ColumnAliasId 2 ) ]
                                                    ( ConstantExpr 16
                                                        ( NumericConstant 17 "1" )
                                                    )
                                                ]
                                            )
                                        )
                                    )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT foo.b FROM foo;" ~: testVertica "QUALMISSING" defaultTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "b" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8
                                            ( Just
                                                ( QTableName 7 Nothing "foo" )
                                            )
                                            "b"
                                        )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 9
                                [ TablishTable 10 TablishAliasesNone
                                    ( QTableName 11 Nothing "foo" )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT qux.x FROM public.qux;" ~: testVertica "MISSINGTABLE" defaultTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "x" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8
                                            ( Just
                                                ( QTableName 7 Nothing "qux" )
                                            )
                                            "x"
                                        )
                                    )
                                ]
                            )
                        )
                        {  selectFrom = Just
                            ( SelectFrom 9
                                [ TablishTable 10 TablishAliasesNone
                                    ( QTableName 12
                                        (Just (mkNormalSchema "public" 11)) "qux"
                                    )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT foo.x FROM bar;" ~: testVertica "UNINTRODUCEDTABLE" defaultTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "x" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8
                                            ( Just
                                                ( QTableName 7 Nothing "foo" )
                                            )
                                            "x"
                                        )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 9
                                [ TablishTable 10 TablishAliasesNone
                                    ( QTableName 11 Nothing "bar" )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT foo.x FROM foo, other.foo;" ~: testVertica "AMBIGUOUSTABLE" defaultTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "x" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8
                                            ( Just
                                                ( QTableName 7 Nothing "foo" )
                                            )
                                            "x"
                                        )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 9
                                [ TablishTable 10 TablishAliasesNone
                                    ( QTableName 11 Nothing "foo" )
                                , TablishTable 12 TablishAliasesNone
                                    ( QTableName 14
                                        (Just (mkNormalSchema "other" 13)) "foo"
                                    )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT bar.a FROM bar, other.bar;" ~: testVertica "AMBIGUOUSTABLECOLUMN" defaultTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "a" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8
                                            ( Just
                                                ( QTableName 7 Nothing "bar" )
                                            )
                                            "a"
                                        )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 9
                                [ TablishTable 10 TablishAliasesNone
                                    ( QTableName 11 Nothing "bar" )
                                , TablishTable 12 TablishAliasesNone
                                    ( QTableName 14
                                        (Just (mkNormalSchema "other" 13)) "bar"
                                    )
                                ]
                            )
                        }
                    )
                )
            )
        , "SELECT missing.qux.* FROM missing.qux;" ~: testVertica "MISSINGSCHEMA" defaultTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectStar 4
                                    ( Just
                                        ( QTableName 6
                                            (Just (mkNormalSchema "missing" 5)) "qux"
                                        )
                                    ) Unused
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 7
                                [ TablishTable 8 TablishAliasesNone
                                    ( QTableName 10
                                        (Just (mkNormalSchema "missing" 9)) "qux"
                                    )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT baz.x FROM (SELECT 1 a) baz;" ~: testVertica "ANOTHERSUBQUERY" defaultTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "x" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8 ( Just ( QTableName 7 Nothing "baz" ) ) "x" )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 9
                                [ TablishSubQuery 10
                                    ( TablishAliasesT ( TableAlias 11 "baz" ( TableAliasId 3 ) ) )
                                    ( QuerySelect 12
                                        ( makeSelect 13
                                            ( SelectColumns 14
                                                [ SelectExpr 15
                                                    [ ColumnAlias 16 "a" ( ColumnAliasId 2 ) ]
                                                    ( ConstantExpr 17 ( NumericConstant 18 "1" ) )
                                                ]
                                            )
                                        )
                                    )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT * FROM foo LEFT SEMI JOIN bar ON (foo.a = bar.a) WHERE bar.b is not null;" ~: testHive "SEMIJOIN" defaultTestCatalog
          -- we're treating invalid semi-join parse rules as *resolve* errors, not parse errors
          ( HiveStandardSqlStatement
              ( QueryStmt
                  ( QuerySelect 1
                      ( makeSelect 2
                          ( SelectColumns 3
                              [ SelectStar 4 Nothing Unused]
                          )
                      )
                      { selectFrom = Just
                          ( SelectFrom 5
                              [ TablishJoin 6
                                   ( JoinSemi 7 )
                                   ( JoinOn
                                       ( BinOpExpr 7
                                           ( Operator "=" )
                                           ( ColumnExpr 8
                                               ( QColumnName 10
                                                   ( Just
                                                       ( QTableName 9 Nothing "foo" )
                                                   )
                                                   "a"
                                               )
                                           )
                                           ( ColumnExpr 11
                                               ( QColumnName 13
                                                   ( Just
                                                       ( QTableName 12 Nothing "bar" )
                                                   )
                                                   "a"
                                               )
                                           )
                                       )
                                   )
                                   ( TablishTable 14 TablishAliasesNone
                                       ( QTableName 15 Nothing "foo" )
                                   )
                                   ( TablishTable 16 TablishAliasesNone
                                       ( QTableName 17 Nothing "bar" )
                                   )
                              ]
                          )
                      , selectWhere = Just
                          ( SelectWhere 18
                              ( UnOpExpr 19
                                  ( Operator "NOT" )
                                  ( UnOpExpr 20
                                      ( Operator "ISNULL" )
                                      ( ColumnExpr 21
                                          ( QColumnName 23
                                              ( Just
                                                  ( QTableName 22 Nothing "bar" )
                                              )
                                              "b"
                                          )
                                      )
                                  )
                              )
                          )
                      })))

        , "SELECT bar.* FROM foo LEFT SEMI JOIN bar ON (foo.a = b);" ~: testHive "SEMIJOIN2" defaultTestCatalog
          -- we're treating invalid semi-join parse rules as *resolve* errors, not parse errors
          ( HiveStandardSqlStatement
              ( QueryStmt
                  ( QuerySelect 1
                      ( makeSelect 2
                          ( SelectColumns 3
                              [ SelectStar 4
                                  ( Just
                                      ( QTableName 5 Nothing "bar" )
                                  )
                                  Unused
                              ]
                          )
                      )
                      { selectFrom = Just
                          ( SelectFrom 6
                              [ TablishJoin 7
                                   ( JoinSemi 8 )
                                   ( JoinOn
                                       ( BinOpExpr 9
                                           ( Operator "=" )
                                           ( ColumnExpr 10
                                               ( QColumnName 12
                                                   ( Just
                                                       ( QTableName 11 Nothing "foo" )
                                                   )
                                                   "a"
                                               )
                                           )
                                           ( ColumnExpr 13
                                               ( QColumnName 14 Nothing "b" )
                                           )
                                       )
                                   )
                                   ( TablishTable 15 TablishAliasesNone
                                       ( QTableName 16 Nothing "foo" )
                                   )
                                   ( TablishTable 17 TablishAliasesNone
                                       ( QTableName 18 Nothing "bar" )
                                   )
                              ]
                          )
                      })))

        , ticket' "T405040"
            [ assertFailure "no golden output - test hasn't passed yet" ]
            [ testVertica "ALIASTABLE.broken" defaultTestCatalog -- "SELECT baz.x FROM bar AS baz;"
                ( VerticaStandardSqlStatement
                    ( QueryStmt
                        ( QuerySelect 1
                            ( makeSelect 2
                                ( SelectColumns 3
                                    [ SelectExpr 4
                                        [ ColumnAlias 5 "x" ( ColumnAliasId 1 ) ]
                                        ( ColumnExpr 6
                                            ( QColumnName 8
                                                ( Just
                                                    ( QTableName 7 Nothing "baz" )
                                                )
                                                "x"
                                            )
                                        )
                                    ]
                                )
                            )
                            { selectFrom = Just
                                ( SelectFrom 9
                                    [ TablishTable 10
                                        ( TablishAliasesT ( TableAlias 11 "baz" ( TableAliasId 2 ) ) )
                                        ( QTableName 12 Nothing "bar" )
                                    ]
                                )
                            }
                        )
                    )
                )

            , testVertica "SUBSELECTALIAS" defaultTestCatalog -- "SELECT baz.x FROM (SELECT * FROM bar) baz;"
                ( VerticaStandardSqlStatement
                    ( QueryStmt
                        ( QuerySelect 1
                            ( makeSelect 2
                                ( SelectColumns 3
                                    [ SelectExpr 4
                                        [ ColumnAlias 5 "x" ( ColumnAliasId 1 ) ]
                                        ( ColumnExpr 6
                                            ( QColumnName 8
                                                ( Just
                                                    ( QTableName 7 Nothing "baz" )
                                                )
                                                "x"
                                            )
                                        )
                                    ]
                                )
                            )
                            { selectFrom = Just
                                ( SelectFrom 9
                                    [ TablishSubQuery 10
                                        ( TablishAliasesT ( TableAlias 11 "baz" ( TableAliasId 2 ) ) )
                                        ( QuerySelect 12
                                            ( makeSelect 13
                                                ( SelectColumns 14
                                                    [ SelectStar 15 Nothing Unused ]
                                                )
                                            )
                                            { selectFrom = Just
                                                ( SelectFrom 16
                                                    [ TablishTable 17 TablishAliasesNone ( QTableName 18 Nothing "bar" ) ]
                                                )
                                            }
                                        )
                                    ]
                                )
                            }
                        )
                    )
                )
            ]

        , "SELECT x y FROM (select 1 x) foo GROUP BY y;" ~: testHive "SUBSELECTGROUPBY" defaultTestCatalog
            ( HiveStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "y" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 7 Nothing "x" )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 8
                                [ TablishSubQuery 9
                                    ( TablishAliasesT ( TableAlias 10 "foo" ( TableAliasId 3 ) ) )
                                    ( QuerySelect 11
                                        ( makeSelect 12
                                            ( SelectColumns 13
                                                [ SelectExpr 14
                                                    [ ColumnAlias 15 "x"
                                                        ( ColumnAliasId 2 )
                                                    ]
                                                    ( ConstantExpr 16
                                                        ( NumericConstant 17 "1" )
                                                    )
                                                ]
                                            )
                                        )
                                    )
                                ]
                            )
                        , selectGroup = Just
                            ( SelectGroup 18
                                [ GroupingElementExpr 19
                                    ( PositionOrExprExpr ( ColumnExpr 19 ( QColumnName 20 Nothing "y" ) ) )
                                ]
                            )
                        }
                    )
                )
            )
        ]

    , "test resolution on some tricky queries" ~:
        [ "SELECT (SELECT foo.x) FROM (SELECT 1 x) foo;" ~: testVertica "TRICKY" (makeCatalog HMS.empty [] (defaultDatabase ()))
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "?column?" $ ColumnAliasId 1 ]
                                    ( SubqueryExpr 6
                                        ( QuerySelect 7
                                            ( makeSelect 8
                                                ( SelectColumns 9
                                                    [ SelectExpr 10
                                                        [ ColumnAlias 11 "?column?" $ ColumnAliasId 2 ]
                                                        ( ColumnExpr 12
                                                            ( QColumnName 14
                                                                ( Just
                                                                    ( QTableName 13 Nothing "foo" )
                                                                )
                                                                "x"
                                                            )
                                                        )
                                                    ]
                                                )
                                            )
                                        )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 15
                                [ TablishSubQuery 16
                                    ( TablishAliasesT ( TableAlias 17 "foo" $ TableAliasId 3 ) )
                                    ( QuerySelect 18
                                        ( makeSelect 19
                                            ( SelectColumns 20
                                                [ SelectExpr 21
                                                    [ ColumnAlias 22 "x" $ ColumnAliasId 4 ]
                                                    ( ConstantExpr 23
                                                        ( NumericConstant 24 "1" )
                                                    )
                                                ]
                                            )
                                        )
                                    )
                                ]
                            )
                        }
                    )
                )
            )
        , "SELECT * FROM foo x JOIN foo y ON x.a = y.a JOIN foo z ON y.a = z.a;" ~: testVertica "TWOJOINS" defaultTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3 [SelectStar 4 Nothing Unused] )
                        )
                        { selectFrom = Just
                            ( SelectFrom 5
                                [ TablishJoin 6
                                    ( JoinInner 7 )
                                    ( JoinOn
                                        ( BinOpExpr 8
                                            ( Operator "=" )
                                            ( ColumnExpr 9
                                                ( QColumnName 11
                                                    ( Just
                                                        ( QTableName 10 Nothing "y" )
                                                    )
                                                    "a"
                                                )
                                            )
                                            ( ColumnExpr 12
                                                ( QColumnName 14
                                                    ( Just
                                                        ( QTableName 13 Nothing "z" )
                                                    )
                                                    "a"
                                                )
                                            )
                                        )
                                    )
                                    ( TablishJoin 15
                                        ( JoinInner 16 )
                                        ( JoinOn
                                            ( BinOpExpr 17
                                                ( Operator "=" )
                                                ( ColumnExpr 18
                                                    ( QColumnName 20
                                                        ( Just
                                                            ( QTableName 19 Nothing "x" )
                                                        )
                                                        "a"
                                                    )
                                                )
                                                ( ColumnExpr 21
                                                    ( QColumnName 23
                                                        ( Just
                                                            ( QTableName 22 Nothing "y" )
                                                        )
                                                        "a"
                                                    )
                                                )
                                            )
                                        )
                                        ( TablishTable 24
                                            ( TablishAliasesT ( TableAlias 25 "x" ( TableAliasId 1 ) ) )
                                            ( QTableName 26 Nothing "foo" )
                                        )
                                        ( TablishTable 27
                                            ( TablishAliasesT ( TableAlias 28 "y" ( TableAliasId 2 ) ) )
                                            ( QTableName 29 Nothing "foo" )
                                        )
                                    )
                                    ( TablishTable 30
                                        ( TablishAliasesT ( TableAlias 31 "z" ( TableAliasId 3 ) ) )
                                        ( QTableName 32 Nothing "foo" )
                                    )
                                ]
                            )
                        }
                    )
                )
            )
        , "SELECT * FROM foo x JOIN foo y ON y.a = z.a JOIN foo z ON x.a = y.a;" ~: testVertica "TWOJOINS2" defaultTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3 [SelectStar 4 Nothing Unused]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 5
                                [ TablishJoin 6
                                    ( JoinInner 7 )
                                    ( JoinOn
                                        ( BinOpExpr 8
                                            ( Operator "=" )
                                            ( ColumnExpr 9
                                                ( QColumnName 11
                                                    ( Just
                                                        ( QTableName 10 Nothing "x" )
                                                    )
                                                    "a"
                                                )
                                            )
                                            ( ColumnExpr 12
                                                ( QColumnName 14
                                                    ( Just
                                                        ( QTableName 13 Nothing "y" )
                                                    )
                                                    "a"
                                                )
                                            )
                                        )
                                    )
                                    ( TablishJoin 15
                                        ( JoinInner 16 )
                                        ( JoinOn
                                            ( BinOpExpr 17
                                                ( Operator "=" )
                                                ( ColumnExpr 18
                                                    ( QColumnName 20
                                                        ( Just
                                                            ( QTableName 19 Nothing "y" )
                                                        )
                                                        "a"
                                                    )
                                                )
                                                ( ColumnExpr 21
                                                    ( QColumnName 23
                                                        ( Just
                                                            ( QTableName 22 Nothing "z" )
                                                        )
                                                        "a"
                                                    )
                                                )
                                            )
                                        )
                                        ( TablishTable 24
                                            ( TablishAliasesT
                                                ( TableAlias 25 "x" ( TableAliasId 1 ) )
                                            )
                                            ( QTableName 26 Nothing "foo" )
                                        )
                                        ( TablishTable 27
                                            ( TablishAliasesT
                                                ( TableAlias 28 "y" ( TableAliasId 2 ) )
                                            )
                                            ( QTableName 29 Nothing "foo" )
                                        )
                                    )
                                    ( TablishTable 30
                                        ( TablishAliasesT
                                            ( TableAlias 31 "z" ( TableAliasId 3 ) )
                                        )
                                        ( QTableName 32 Nothing "foo" )
                                    )
                                ]
                            )
                        }
                    )
                )
            )
        , "SELECT other.foo.b FROM other.foo, foo;" ~: testVertica "OTHERFOO" defaultTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "b" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 9
                                            ( Just
                                                ( QTableName 8
                                                    (Just (mkNormalSchema "other" 7)) "foo"
                                                )
                                            )
                                            "b"
                                        )
                                    )
                                ]
                            )
                        )
                        {  selectFrom = Just
                            ( SelectFrom 10
                                [ TablishTable 11 TablishAliasesNone
                                    ( QTableName 13
                                        (Just (mkNormalSchema "other" 12)) "foo"
                                    )
                                , TablishTable 14 TablishAliasesNone
                                    ( QTableName 15 Nothing "foo" )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT a.x FROM t LEFT SEMI JOIN a ON (t.id = a.id);" ~: testHive "PATHOLOGICALSEMI"
           pathologicalSemiJoinStructAccessorCatalog
           -- this is struct field access: table t, column a, struct field x
           ( HiveStandardSqlStatement
               ( QueryStmt
                   ( QuerySelect 1
                       ( makeSelect 2
                           ( SelectColumns 3
                               [ SelectExpr 4
                                   [ ColumnAlias 5 "_c0" ( ColumnAliasId 1) ]
                                   ( FieldAccessExpr 6
                                       ( ColumnExpr 7
                                           ( QColumnName 8 Nothing "a" )
                                       )
                                       ( StructFieldName 10 "x" )
                                   )
                               ]
                           )
                       )
                       { selectFrom = Just
                           ( SelectFrom 11
                               [ TablishJoin 12
                                   ( JoinSemi 13 )
                                   ( JoinOn
                                       ( BinOpExpr 14
                                           ( Operator "=" )
                                           ( ColumnExpr 15
                                               ( QColumnName 17
                                                   ( Just
                                                       ( QTableName 16 Nothing "t" )
                                                   )
                                                   "id"
                                               )
                                           )
                                           ( ColumnExpr 18
                                               ( QColumnName 20
                                                   ( Just
                                                     ( QTableName 19 Nothing "a" )
                                                   )
                                                   "id"
                                               )
                                           )
                                       )
                                   )
                                   ( TablishTable 21 TablishAliasesNone
                                       ( QTableName 22 Nothing "t" )
                                   )
                                   ( TablishTable 23 TablishAliasesNone
                                       ( QTableName 24 Nothing "a" )
                                   )
                               ]
                          )
                       }
                   )
               )
           )

        , "SELECT a.x FROM a LEFT SEMI JOIN t ON (t.id = a.id);" ~: testHive "PATHOLOGICALSEMI2"
          pathologicalSemiJoinStructAccessorCatalog
          -- this is column access: table a, column x
          ( HiveStandardSqlStatement
              ( QueryStmt
                  ( QuerySelect 1
                      ( makeSelect 2
                          ( SelectColumns 3
                              [ SelectExpr 4
                                  [ ColumnAlias 5 "x" ( ColumnAliasId 1 ) ]
                                  ( ColumnExpr 6
                                      ( QColumnName 8
                                          ( Just
                                              ( QTableName 7 Nothing "a" )
                                          )
                                          "x"
                                      )
                                  )
                              ]
                          )
                      )
                      { selectFrom = Just
                          ( SelectFrom 9
                              [ TablishJoin 10
                                  ( JoinSemi 11 )
                                  ( JoinOn
                                      ( BinOpExpr 12
                                          ( Operator "=" )
                                          ( ColumnExpr 13
                                              ( QColumnName 15
                                                  ( Just
                                                      ( QTableName 14 Nothing "t" )
                                                  )
                                                  "id"
                                              )
                                          )
                                          ( ColumnExpr 16
                                              ( QColumnName 18
                                                  ( Just
                                                      ( QTableName 17 Nothing "a" )
                                                  )
                                                  "id"
                                              )
                                          )
                                      )
                                  )
                                  ( TablishTable 19 TablishAliasesNone
                                      ( QTableName 20 Nothing "a" )
                                  )
                                  ( TablishTable 21 TablishAliasesNone
                                      ( QTableName 22 Nothing "t" )
                                  )
                              ]
                          )
                      }
                  )
              )
          )
        ]

    , "test resolution of Vertica specific queries" ~:
        [ "ALTER TABLE foo, bar, qux RENAME TO qux, foo, bar;" ~: testVertica "ALTERTABLERENAME" defaultTestCatalog
            ( VerticaMultipleRenameStatement
                ( VSQL.MultipleRename 1
                    [ AlterTableRenameTable 2
                        ( QTableName 3 Nothing "foo" )
                        ( QTableName 4 Nothing "qux" )
                    , AlterTableRenameTable 5
                        ( QTableName 6 Nothing "bar" )
                        ( QTableName 7 Nothing "foo" )
                    , AlterTableRenameTable 8
                        ( QTableName 9 Nothing "qux" )
                        ( QTableName 10 Nothing "bar" )
                    ]
                )
            )

        , "CREATE PROJECTION foo_projection AS SELECT * FROM foo SEGMENTED BY HASH(a) ALL NODES KSAFE;" ~: testVertica "CREATEPROJECTION" defaultTestCatalog
            ( VerticaCreateProjectionStatement
                ( VSQL.CreateProjection
                    { createProjectionInfo = 1
                    , createProjectionIfNotExists = Nothing
                    , createProjectionName = VSQL.ProjectionName 2 Nothing "foo_projection"
                    , createProjectionColumns = Nothing
                    , createProjectionQuery = QuerySelect 3
                        ( makeSelect 4
                            ( SelectColumns
                                { selectColumnsInfo = 5
                                , selectColumnsList =
                                    [ SelectStar 6 Nothing Unused ]
                                }
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 7
                                [ TablishTable 8 TablishAliasesNone
                                    ( QTableName 9 Nothing "foo" )
                                ]
                            )
                        }
                    , createProjectionSegmentation = Just
                        ( VSQL.SegmentedBy 10
                            ( FunctionExpr 11
                                ( QFunctionName 12 Nothing "hash" ) notDistinct
                                [ ColumnExpr 13
                                    ( QColumnName 14 Nothing "a" )
                                ]
                                []
                                Nothing
                                Nothing
                            )
                            ( VSQL.AllNodes 15 Nothing )
                        )
                    , createProjectionKSafety = Just ( VSQL.KSafety 16 Nothing )
                    }
                )
            )
        , "ALTER TABLE foo SET SCHEMA empty;" ~: testVertica "SETSCHEMA" defaultTestCatalog
            ( VerticaSetSchemaStatement
                ( VSQL.SetSchema
                    { setSchemaInfo = 1
                    , setSchemaTable = QTableName 2 Nothing "foo"
                    , setSchemaName = mkNormalSchema "empty" 3
                    }
                )
            )
        ]

    , "test resolution of Hive specific queries" ~:
        [ "SELECT foo.* FROM foo LEFT SEMI JOIN bar ON (foo.a = b);" ~: testHive "MORESEMI" defaultTestCatalog
          ( HiveStandardSqlStatement
              ( QueryStmt
                  ( QuerySelect 1
                      ( makeSelect 2
                          ( SelectColumns 3
                              [ SelectStar 4
                                  ( Just
                                      ( QTableName 5 Nothing "foo" )
                                  )
                                  Unused
                              ]
                          )
                      )
                      { selectFrom = Just
                          ( SelectFrom 6
                              [ TablishJoin 7
                                  ( JoinSemi 8 )
                                  ( JoinOn
                                      ( BinOpExpr 9
                                          ( Operator "=" )
                                          ( ColumnExpr 10
                                              ( QColumnName 12
                                                  ( Just
                                                      ( QTableName 11 Nothing "foo" )
                                                  )
                                                  "a"
                                              )
                                          )
                                          ( ColumnExpr 13
                                              ( QColumnName 14 Nothing "b" )
                                          )
                                      )
                                  )
                                  ( TablishTable 15 TablishAliasesNone
                                      ( QTableName 16 Nothing "foo" )
                                  )
                                  ( TablishTable 17 TablishAliasesNone
                                      ( QTableName 18 Nothing "bar" )
                                  )
                              ]
                          )
                      }
                  )
              )
          )

        , "SELECT * FROM foo LEFT SEMI JOIN bar ON (foo.a = b);" ~: testHive "MORESEMI2" defaultTestCatalog
          ( HiveStandardSqlStatement
              ( QueryStmt
                  ( QuerySelect 1
                      ( makeSelect 2
                          ( SelectColumns 3
                              [ SelectStar 4 Nothing Unused ]
                          )
                      )
                      { selectFrom = Just
                          ( SelectFrom 5
                              [ TablishJoin 6
                                  ( JoinSemi 7 )
                                  ( JoinOn
                                      ( BinOpExpr 8
                                          ( Operator "=" )
                                          ( ColumnExpr 9
                                              ( QColumnName 11
                                                  ( Just
                                                      ( QTableName 10 Nothing "foo" )
                                                  )
                                                  "a"
                                              )
                                          )
                                          ( ColumnExpr 12
                                              ( QColumnName 13 Nothing "b" )
                                          )
                                      )
                                  )
                                  ( TablishTable 14 TablishAliasesNone
                                      ( QTableName 15 Nothing "foo" )
                                  )
                                  ( TablishTable 16 TablishAliasesNone
                                      ( QTableName 17 Nothing "bar" )
                                  )
                              ]
                          )
                      }
                  )
              )
          )

        , "SELECT foo.a FROM foo LEFT SEMI JOIN bar ON foo.a = bar.b INNER JOIN baz ON foo.a = bar.b AND foo.a = baz.a;" ~:
          ticket' "T481451"
          [ assertFailure "no golden output - test hasn't passed yet" ]
          [ testHive "MORESEMI3.broken" defaultTestCatalog
            ( HiveStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "a" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8
                                            ( Just
                                                ( QTableName 7 Nothing "foo" )
                                            )
                                            "a"
                                        )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 9
                                [ TablishJoin 10
                                    ( JoinInner 11 )
                                    ( JoinOn
                                        ( BinOpExpr 12
                                            ( Operator "AND" )
                                            ( BinOpExpr 13
                                                ( Operator "=" )
                                                ( ColumnExpr 14
                                                    ( QColumnName 16
                                                        ( Just
                                                            ( QTableName 15 Nothing "foo" )
                                                        )
                                                        "a"
                                                    )
                                                )
                                                ( ColumnExpr 17
                                                    ( QColumnName 19
                                                        ( Just
                                                            ( QTableName 18 Nothing "bar" )
                                                        )
                                                        "b"
                                                    )
                                                )
                                            )
                                            ( BinOpExpr 20
                                                ( Operator "=" )
                                                ( ColumnExpr 21
                                                    ( QColumnName 23
                                                        ( Just
                                                            ( QTableName 22 Nothing "foo" )
                                                        )
                                                        "a"
                                                    )
                                                )
                                                ( ColumnExpr 24
                                                    ( QColumnName 26
                                                        ( Just
                                                            ( QTableName 25 Nothing "baz" )
                                                        )
                                                        "a"
                                                    )
                                                )
                                            )
                                        )
                                    )
                                    ( TablishJoin 27
                                        ( JoinSemi 28 )
                                            ( JoinOn
                                                ( BinOpExpr 29
                                                    ( Operator "=" )
                                                    ( ColumnExpr 30
                                                        ( QColumnName 32
                                                            ( Just
                                                                ( QTableName 31 Nothing "foo" )
                                                            )
                                                            "a"
                                                        )
                                                    )
                                                    ( ColumnExpr 33
                                                        ( QColumnName 35
                                                            ( Just
                                                                ( QTableName 34 Nothing "bar" )
                                                            )
                                                            "b"
                                                        )
                                                    )
                                                )
                                            )
                                        ( TablishTable 36 TablishAliasesNone
                                            ( QTableName 37 Nothing "foo" )
                                        )
                                        ( TablishTable 38 TablishAliasesNone
                                            ( QTableName 39 Nothing "bar" )
                                        )
                                    )
                                    ( TablishTable 40 TablishAliasesNone
                                        ( QTableName 41 Nothing "baz" )
                                    )
                                ]
                            )
                        }
                    )
                )
            )
          ]

        , "SELECT * FROM foo LATERAL VIEW explode(foo.a) f AS x;" ~: testHive "EXPLODE" defaultTestCatalog
            ( HiveStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns
                                { selectColumnsInfo = 3
                                , selectColumnsList = [ SelectStar 4 Nothing Unused ]
                                }
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 5
                                [ TablishLateralView 6
                                    ( LateralView
                                        { lateralViewInfo = 7
                                        , lateralViewOuter = Nothing
                                        , lateralViewExprs =
                                          [ FunctionExpr 8
                                            ( QFunctionName 9 Nothing "explode" )
                                            notDistinct
                                            [ ColumnExpr 10
                                                ( QColumnName 12
                                                    ( Just
                                                        ( QTableName 11 Nothing "foo" )
                                                    )
                                                    "a"
                                                )
                                            ]
                                            []
                                            Nothing
                                            Nothing
                                          ]
                                        , lateralViewWithOrdinality = False
                                        , lateralViewAliases = ( TablishAliasesTC
                                                                 ( TableAlias 13 "f" ( TableAliasId 0 ) )
                                                                 [ ColumnAlias 14 "x" ( ColumnAliasId 1 ) ]
                                                               )
                                        }
                                    )
                                   ( Just ( TablishTable 15 TablishAliasesNone
                                              ( QTableName 16 Nothing "foo" )
                                          )
                                   )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT bar.col FROM foo LATERAL VIEW explode(foo.a) bar;" ~: testHive "EXPLODE2" defaultTestCatalog
          -- test that we resolve the default alias `col` which is specific to the `explode` UDTF
            ( HiveStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "col" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8
                                            ( Just
                                                ( QTableName 7 Nothing "bar" )
                                            )
                                            "col"
                                        )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 9
                                [ TablishLateralView 10
                                    ( LateralView
                                        { lateralViewInfo = 11
                                        , lateralViewOuter = Nothing
                                        , lateralViewExprs =
                                          [ FunctionExpr 12
                                            ( QFunctionName 13 Nothing "explode" )
                                            notDistinct
                                            [ ColumnExpr 14
                                                ( QColumnName 16
                                                    ( Just
                                                        ( QTableName 15 Nothing "foo" )
                                                    )
                                                    "a"
                                                )
                                            ]
                                            []
                                            Nothing
                                            Nothing
                                          ]
                                        , lateralViewWithOrdinality = False
                                        , lateralViewAliases = TablishAliasesT $ TableAlias 17 "bar" ( TableAliasId 0 )
                                        }
                                    )
                                    ( Just ( TablishTable 18 TablishAliasesNone
                                                ( QTableName 19 Nothing "foo" )
                                            )
                                    )
                                ]
                            )
                        }
                    )
                )
            )
        ]
    ]


testDefaulting :: Test
testDefaulting = test
    [ "test resolution on some simple queries" ~:
        [ "SELECT a FROM foo;" ~: testVertica "DEFAULTMISSING" defaultDefaultingTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "a" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 7 Nothing "a" )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 8
                                [ TablishTable 9 TablishAliasesNone
                                    ( QTableName 10 Nothing "foo" )
                                ]
                            )
                        }
                    )
                )
            )
        , "SELECT x y FROM (select 1 x) foo GROUP BY y;" ~: testVertica "DEFAULTGROUP" defaultDefaultingTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "y" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 7 Nothing "x" )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 8
                                [ TablishSubQuery 9
                                    ( TablishAliasesT ( TableAlias 10 "foo" ( TableAliasId 3 ) ) )
                                    ( QuerySelect 11
                                        ( makeSelect 12
                                            ( SelectColumns 13
                                                [ SelectExpr 14
                                                    [ ColumnAlias 15 "x"
                                                        ( ColumnAliasId 2 )
                                                    ]
                                                    ( ConstantExpr 16
                                                        ( NumericConstant 17 "1" )
                                                    )
                                                ]
                                            )
                                        )
                                    )
                                ]
                            )
                        , selectGroup = Just
                            ( SelectGroup 18
                                [ GroupingElementExpr 19
                                    ( PositionOrExprExpr ( ColumnExpr 19 ( QColumnName 20 Nothing "y" ) ))
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT * FROM foo ORDER BY a;" ~: testVertica "ORDER" defaultDefaultingTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QueryOrder 1
                        [ Order 2
                            ( PositionOrExprExpr ( ColumnExpr 3 (QColumnName 4 Nothing "a") ) )
                            ( OrderDesc (Just 5) )
                            ( NullsAuto Nothing)
                        ]
                        ( QuerySelect 6
                            ( makeSelect 7
                                ( SelectColumns
                                    { selectColumnsInfo = 8
                                    , selectColumnsList = [SelectStar 9 Nothing Unused]
                                    }
                                )
                            )
                            { selectFrom = Just
                                ( SelectFrom 10
                                    [ TablishTable 11 TablishAliasesNone
                                        ( QTableName 12 Nothing "foo" )
                                    ]
                                )
                            }
                        )
                    )
                )
            )
        ]

    , "test errors and warnings" ~:
        [ "SELECT b FROM (SELECT 1 a) foo;" ~: testVertica "SUBQUERY2" defaultDefaultingTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "b" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 7 Nothing "b" )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 8
                                [ TablishSubQuery 9
                                    ( TablishAliasesT ( TableAlias 10 "foo" ( TableAliasId 3 ) ) )
                                    ( QuerySelect 11
                                        ( makeSelect 12
                                            ( SelectColumns 13
                                                [ SelectExpr 14
                                                    [ ColumnAlias 15 "a" ( ColumnAliasId 2 ) ]
                                                    ( ConstantExpr 16
                                                        ( NumericConstant 17 "1" )
                                                    )
                                                ]
                                            )
                                        )
                                    )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT foo.b FROM foo;" ~: testVertica "MISSINGCOLUMN" defaultDefaultingTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "b" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8
                                            ( Just
                                                ( QTableName 7 Nothing "foo" )
                                            )
                                            "b"
                                        )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 9
                                [ TablishTable 10 TablishAliasesNone
                                    ( QTableName 11 Nothing "foo" )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT qux.x FROM public.qux;" ~: testVertica "DEFAULTMISSINGTABLE" defaultDefaultingTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "x" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8
                                            ( Just
                                                ( QTableName 7 Nothing "qux" )
                                            )
                                            "x"
                                        )
                                    )
                                ]
                            )
                        )
                        {  selectFrom = Just
                            ( SelectFrom 9
                                [ TablishTable 10 TablishAliasesNone
                                    ( QTableName 12
                                        (Just (mkNormalSchema "public" 11)) "qux"
                                    )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT foo.x FROM bar;" ~: testVertica "DEFAULTUNINTRODUCEDTABLE" defaultDefaultingTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "x" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8
                                            ( Just
                                                ( QTableName 7 Nothing "foo" )
                                            )
                                            "x"
                                        )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 9
                                [ TablishTable 10 TablishAliasesNone
                                    ( QTableName 11 Nothing "bar" )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT foo.x FROM foo, other.foo;" ~: testVertica "DEFAULTAMBIGUOUSTABLE" defaultDefaultingTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "x" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8
                                            ( Just
                                                ( QTableName 7 Nothing "foo" )
                                            )
                                            "x"
                                        )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 9
                                [ TablishTable 10 TablishAliasesNone
                                    ( QTableName 11 Nothing "foo" )
                                , TablishTable 12 TablishAliasesNone
                                    ( QTableName 14
                                        (Just (mkNormalSchema "other" 13)) "foo"
                                    )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT bar.a FROM bar, other.bar;" ~: testVertica "DEFAULTAMBIGUOUSTABLE2" defaultDefaultingTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "a" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8
                                            ( Just
                                                ( QTableName 7 Nothing "bar" )
                                            )
                                            "a"
                                        )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 9
                                [ TablishTable 10 TablishAliasesNone
                                    ( QTableName 11 Nothing "bar" )
                                , TablishTable 12 TablishAliasesNone
                                    ( QTableName 14
                                        (Just (mkNormalSchema "other" 13)) "bar"
                                    )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT missing.qux.* FROM missing.qux;" ~: testVertica "DEFAULTMISSINGSCHEMA" defaultDefaultingTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectStar 4
                                    ( Just
                                        ( QTableName 6
                                            (Just (mkNormalSchema "missing" 5)) "qux"
                                        )
                                    ) Unused
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 7
                                [ TablishTable 8 TablishAliasesNone
                                    ( QTableName 10
                                        (Just (mkNormalSchema "missing" 9)) "qux"
                                    )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT baz.x FROM (SELECT 1 a) baz;" ~: testVertica "DEFAULTMISSINGCOLUMN" defaultDefaultingTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "x" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8 ( Just ( QTableName 7 Nothing "baz" ) )
                                            "x"
                                        )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 9
                                [ TablishSubQuery 10
                                    ( TablishAliasesT ( TableAlias 11 "baz" ( TableAliasId 3 ) ) )
                                    ( QuerySelect 12
                                        ( makeSelect 13
                                            ( SelectColumns 14
                                                [ SelectExpr 15
                                                    [ ColumnAlias 16 "a" ( ColumnAliasId 2 ) ]
                                                    ( ConstantExpr 17 ( NumericConstant 18 "1" ) )
                                                ]
                                            )
                                        )
                                    )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT * FROM foo LEFT SEMI JOIN bar ON (foo.a = bar.a) WHERE bar.b is not null;" ~: testHive "DEFAULTSEMI" defaultDefaultingTestCatalog
          -- we're treating invalid semi-join parse rules as *resolve* errors, not parse errors
          ( HiveStandardSqlStatement
              ( QueryStmt
                  ( QuerySelect 1
                      ( makeSelect 2
                          ( SelectColumns 3
                              [ SelectStar 4 Nothing Unused]
                          )
                      )
                      { selectFrom = Just
                          ( SelectFrom 5
                              [ TablishJoin 6
                                   ( JoinSemi 7 )
                                   ( JoinOn
                                       ( BinOpExpr 7
                                           ( Operator "=" )
                                           ( ColumnExpr 8
                                               ( QColumnName 10
                                                   ( Just
                                                       ( QTableName 9 Nothing "foo" )
                                                   )
                                                   "a"
                                               )
                                           )
                                           ( ColumnExpr 11
                                               ( QColumnName 13
                                                   ( Just
                                                       ( QTableName 12 Nothing "bar" )
                                                   )
                                                   "a"
                                               )
                                           )
                                       )
                                   )
                                   ( TablishTable 14 TablishAliasesNone
                                       ( QTableName 15 Nothing "foo" )
                                   )
                                   ( TablishTable 16 TablishAliasesNone
                                       ( QTableName 17 Nothing "bar" )
                                   )
                              ]
                          )
                      , selectWhere = Just
                          ( SelectWhere 18
                              ( UnOpExpr 19
                                  ( Operator "NOT" )
                                  ( UnOpExpr 20
                                      ( Operator "ISNULL" )
                                      ( ColumnExpr 21
                                          ( QColumnName 23
                                              ( Just
                                                  ( QTableName 22 Nothing "bar" )
                                              )
                                              "b"
                                          )
                                      )
                                  )
                              )
                          )
                      })))


        , "SELECT bar.* FROM foo LEFT SEMI JOIN bar ON (foo.a = b);" ~: testHive "DEFAULTSEMI2" defaultDefaultingTestCatalog
          -- we're treating invalid semi-join parse rules as *resolve* errors, not parse errors
          ( HiveStandardSqlStatement
              ( QueryStmt
                  ( QuerySelect 1
                      ( makeSelect 2
                          ( SelectColumns 3
                              [ SelectStar 4
                                  ( Just
                                      ( QTableName 5 Nothing "bar" )
                                  )
                                  Unused
                              ]
                          )
                      )
                      { selectFrom = Just
                          ( SelectFrom 6
                              [ TablishJoin 7
                                   ( JoinSemi 8 )
                                   ( JoinOn
                                       ( BinOpExpr 9
                                           ( Operator "=" )
                                           ( ColumnExpr 10
                                               ( QColumnName 12
                                                   ( Just
                                                       ( QTableName 11 Nothing "foo" )
                                                   )
                                                   "a"
                                               )
                                           )
                                           ( ColumnExpr 13
                                               ( QColumnName 14 Nothing "b" )
                                           )
                                       )
                                   )
                                   ( TablishTable 15 TablishAliasesNone
                                       ( QTableName 16 Nothing "foo" )
                                   )
                                   ( TablishTable 17 TablishAliasesNone
                                       ( QTableName 18 Nothing "bar" )
                                   )
                              ]
                          )
                      })))
        ]

    , "test resolution on some tricky queries" ~:
        [ "SELECT (SELECT foo.x) FROM (SELECT 1 x) foo;" ~: testVertica "TRICKY2" (makeCatalog HMS.empty [] (defaultDatabase ()))
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "?column?" $ ColumnAliasId 1 ]
                                    ( SubqueryExpr 6
                                        ( QuerySelect 7
                                            ( makeSelect 8
                                                ( SelectColumns 9
                                                    [ SelectExpr 10
                                                        [ ColumnAlias 11 "?column?" $ ColumnAliasId 2 ]
                                                        ( ColumnExpr 12
                                                            ( QColumnName 14
                                                                ( Just
                                                                    ( QTableName 13 Nothing "foo" )
                                                                )
                                                                "x"
                                                            )
                                                        )
                                                    ]
                                                )
                                            )
                                        )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 15
                                [ TablishSubQuery 16
                                    ( TablishAliasesT ( TableAlias 17 "foo" $ TableAliasId 3 ) )
                                    ( QuerySelect 18
                                        ( makeSelect 19
                                            ( SelectColumns 20
                                                [ SelectExpr 21
                                                    [ ColumnAlias 22 "x" $ ColumnAliasId 4 ]
                                                    ( ConstantExpr 23
                                                        ( NumericConstant 24 "1" )
                                                    )
                                                ]
                                            )
                                        )
                                    )
                                ]
                            )
                        }
                    )
                )
            )
        , "SELECT * FROM foo x JOIN foo y ON x.a = y.a JOIN foo z ON y.a = z.a;" ~: testVertica "DEFAULTTWOJOINS" defaultDefaultingTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3 [SelectStar 4 Nothing Unused] )
                        )
                        { selectFrom = Just
                            ( SelectFrom 5
                                [ TablishJoin 6
                                    ( JoinInner 7 )
                                    ( JoinOn
                                        ( BinOpExpr 8
                                            ( Operator "=" )
                                            ( ColumnExpr 9
                                                ( QColumnName 11
                                                    ( Just
                                                        ( QTableName 10 Nothing "y" )
                                                    )
                                                    "a"
                                                )
                                            )
                                            ( ColumnExpr 12
                                                ( QColumnName 14
                                                    ( Just
                                                        ( QTableName 13 Nothing "z" )
                                                    )
                                                    "a"
                                                )
                                            )
                                        )
                                    )
                                    ( TablishJoin 15
                                        ( JoinInner 16 )
                                        ( JoinOn
                                            ( BinOpExpr 17
                                                ( Operator "=" )
                                                ( ColumnExpr 18
                                                    ( QColumnName 20
                                                        ( Just
                                                            ( QTableName 19 Nothing "x" )
                                                        )
                                                        "a"
                                                    )
                                                )
                                                ( ColumnExpr 21
                                                    ( QColumnName 23
                                                        ( Just
                                                            ( QTableName 22 Nothing "y" )
                                                        )
                                                        "a"
                                                    )
                                                )
                                            )
                                        )
                                        ( TablishTable 24
                                            ( TablishAliasesT ( TableAlias 25 "x" ( TableAliasId 1 ) ) )
                                            ( QTableName 26 Nothing "foo" )
                                        )
                                        ( TablishTable 27
                                            ( TablishAliasesT ( TableAlias 28 "y" ( TableAliasId 2 ) ) )
                                            ( QTableName 29 Nothing "foo" )
                                        )
                                    )
                                    ( TablishTable 30
                                        ( TablishAliasesT ( TableAlias 31 "z" ( TableAliasId 3 ) ) )
                                        ( QTableName 32 Nothing "foo" )
                                    )
                                ]
                            )
                        }
                    )
                )
            )
        , "SELECT * FROM foo x JOIN foo y ON y.a = z.a JOIN foo z ON x.a = y.a;" ~: testVertica "DEFAULTTWOJOINS2" defaultDefaultingTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3 [SelectStar 4 Nothing Unused]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 5
                                [ TablishJoin 6
                                    ( JoinInner 7 )
                                    ( JoinOn
                                        ( BinOpExpr 8
                                            ( Operator "=" )
                                            ( ColumnExpr 9
                                                ( QColumnName 11
                                                    ( Just
                                                        ( QTableName 10 Nothing "x" )
                                                    )
                                                    "a"
                                                )
                                            )
                                            ( ColumnExpr 12
                                                ( QColumnName 14
                                                    ( Just
                                                        ( QTableName 13 Nothing "y" )
                                                    )
                                                    "a"
                                                )
                                            )
                                        )
                                    )
                                    ( TablishJoin 15
                                        ( JoinInner 16 )
                                        ( JoinOn
                                            ( BinOpExpr 17
                                                ( Operator "=" )
                                                ( ColumnExpr 18
                                                    ( QColumnName 20
                                                        ( Just
                                                            ( QTableName 19 Nothing "y" )
                                                        )
                                                        "a"
                                                    )
                                                )
                                                ( ColumnExpr 21
                                                    ( QColumnName 23
                                                        ( Just
                                                            ( QTableName 22 Nothing "z" )
                                                        )
                                                        "a"
                                                    )
                                                )
                                            )
                                        )
                                        ( TablishTable 24
                                            ( TablishAliasesT
                                                ( TableAlias 25 "x" ( TableAliasId 1 ) )
                                            )
                                            ( QTableName 26 Nothing "foo" )
                                        )
                                        ( TablishTable 27
                                            ( TablishAliasesT
                                                ( TableAlias 28 "y" ( TableAliasId 2 ) )
                                            )
                                            ( QTableName 29 Nothing "foo" )
                                        )
                                    )
                                    ( TablishTable 30
                                        ( TablishAliasesT
                                            ( TableAlias 31 "z" ( TableAliasId 3 ) )
                                        )
                                        ( QTableName 32 Nothing "foo" )
                                    )
                                ]
                            )
                        }
                    )
                )
            )
        , "SELECT other.foo.b FROM other.foo, foo;" ~: testVertica "DEFAULTOTHERFOO" defaultDefaultingTestCatalog
            ( VerticaStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "b" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 9
                                            ( Just
                                                ( QTableName 8
                                                    (Just (mkNormalSchema "other" 7)) "foo"
                                                )
                                            )
                                            "b"
                                        )
                                    )
                                ]
                            )
                        )
                        {  selectFrom = Just
                            ( SelectFrom 10
                                [ TablishTable 11 TablishAliasesNone
                                    ( QTableName 13
                                        (Just (mkNormalSchema "other" 12)) "foo"
                                    )
                                , TablishTable 14 TablishAliasesNone
                                    ( QTableName 15 Nothing "foo" )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT a.x FROM t LEFT SEMI JOIN a ON (t.id = a.id);" ~: testHive  "PATHOLOGICALSEMI3"
           pathologicalSemiJoinStructAccessorCatalog
           -- this is struct field access: table t, column a, struct field x
           ( HiveStandardSqlStatement
               ( QueryStmt
                   ( QuerySelect 1
                       ( makeSelect 2
                           ( SelectColumns 3
                               [ SelectExpr 4
                                   [ ColumnAlias 5 "_c0" ( ColumnAliasId 1) ]
                                   ( FieldAccessExpr 6
                                       ( ColumnExpr 7
                                           ( QColumnName 8 Nothing "a" )
                                       )
                                       ( StructFieldName 10 "x" )
                                   )
                               ]
                           )
                       )
                       { selectFrom = Just
                           ( SelectFrom 11
                               [ TablishJoin 12
                                   ( JoinSemi 13 )
                                   ( JoinOn
                                       ( BinOpExpr 14
                                           ( Operator "=" )
                                           ( ColumnExpr 15
                                               ( QColumnName 17
                                                   ( Just
                                                       ( QTableName 16 Nothing "t" )
                                                   )
                                                   "id"
                                               )
                                           )
                                           ( ColumnExpr 18
                                               ( QColumnName 20
                                                   ( Just
                                                       ( QTableName 19 Nothing "a" )
                                                   )
                                                   "id"
                                               )
                                           )
                                       )
                                   )
                                   ( TablishTable 21 TablishAliasesNone
                                       ( QTableName 22 Nothing "t" )
                                   )
                                   ( TablishTable 23 TablishAliasesNone
                                       ( QTableName 24 Nothing "a" )
                                   )
                               ]
                          )
                       }
                   )
               )
           )

        , "SELECT a.x FROM a LEFT SEMI JOIN t ON (t.id = a.id);" ~: testHive  "PATHOLOGICALSEMI4"
          pathologicalSemiJoinStructAccessorCatalog
          -- this is column access: table a, column x
          ( HiveStandardSqlStatement
              ( QueryStmt
                  ( QuerySelect 1
                      ( makeSelect 2
                          ( SelectColumns 3
                              [ SelectExpr 4
                                  [ ColumnAlias 5 "x" ( ColumnAliasId 1 ) ]
                                  ( ColumnExpr 6
                                      ( QColumnName 8
                                          ( Just
                                              ( QTableName 7 Nothing "a" )
                                          )
                                          "x"
                                      )
                                  )
                              ]
                          )
                      )
                      { selectFrom = Just
                          ( SelectFrom 9
                              [ TablishJoin 10
                                  ( JoinSemi 11 )
                                  ( JoinOn
                                      ( BinOpExpr 12
                                          ( Operator "=" )
                                          ( ColumnExpr 13
                                              ( QColumnName 15
                                                  ( Just
                                                      ( QTableName 14 Nothing "t" )
                                                  )
                                                  "id"
                                              )
                                          )
                                          ( ColumnExpr 16
                                              ( QColumnName 18
                                                  ( Just
                                                      ( QTableName 17 Nothing "a" )
                                                  )
                                                  "id"
                                              )
                                          )
                                      )
                                  )
                                  ( TablishTable 19 TablishAliasesNone
                                      ( QTableName 20 Nothing "a" )
                                  )
                                  ( TablishTable 21 TablishAliasesNone
                                      ( QTableName 22 Nothing "t" )
                                  )
                              ]
                          )
                      }
                  )
              )
          )
        ]

    , "test resolution of Vertica specific queries" ~:
        [ "ALTER TABLE foo, bar, qux RENAME TO qux, foo, bar;" ~: testVertica "DEFAULTMISSINGALTERTABLERENAME" defaultDefaultingTestCatalog
            ( VerticaMultipleRenameStatement
                ( VSQL.MultipleRename 1
                    [ AlterTableRenameTable 2
                        ( QTableName 3 Nothing "foo" )
                        ( QTableName 4 Nothing "qux" )
                    , AlterTableRenameTable 5
                        ( QTableName 6 Nothing "bar" )
                        ( QTableName 7 Nothing "foo" )
                    , AlterTableRenameTable 8
                        ( QTableName 9 Nothing "qux" )
                        ( QTableName 10 Nothing "bar" )
                    ]
                )
            )

        , "CREATE PROJECTION foo_projection AS SELECT * FROM foo SEGMENTED BY HASH(a) ALL NODES KSAFE;" ~: testVertica "DEFAULTCREATEPROJECTION" defaultDefaultingTestCatalog
            ( VerticaCreateProjectionStatement
                ( VSQL.CreateProjection
                    { createProjectionInfo = 1
                    , createProjectionIfNotExists = Nothing
                    , createProjectionName = VSQL.ProjectionName 2 Nothing "foo_projection"
                    , createProjectionColumns = Nothing
                    , createProjectionQuery = QuerySelect 3
                        ( makeSelect 4
                            ( SelectColumns
                                { selectColumnsInfo = 5
                                , selectColumnsList =
                                    [ SelectStar 6 Nothing Unused ]
                                }
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 7
                                [ TablishTable 8 TablishAliasesNone
                                    ( QTableName 9 Nothing "foo" )
                                ]
                            )
                        }
                    , createProjectionSegmentation = Just
                        ( VSQL.SegmentedBy 10
                            ( FunctionExpr 11
                                ( QFunctionName 12 Nothing "hash" ) notDistinct
                                [ ColumnExpr 13
                                    ( QColumnName 14 Nothing "a" )
                                ]
                                []
                                Nothing
                                Nothing
                            )
                            ( VSQL.AllNodes 15 Nothing )
                        )
                    , createProjectionKSafety = Just ( VSQL.KSafety 16 Nothing )
                    }
                )
            )
        , "ALTER TABLE foo SET SCHEMA empty;" ~: testVertica "DEFAULTSETSCHEMA" defaultDefaultingTestCatalog
            ( VerticaSetSchemaStatement
                ( VSQL.SetSchema
                    { setSchemaInfo = 1
                    , setSchemaTable = QTableName 2 Nothing "foo"
                    , setSchemaName = mkNormalSchema "empty" 3
                    }
                )
            )
        ]

    , "test resolution of Hive specific queries" ~:
        [ "SELECT foo.* FROM foo LEFT SEMI JOIN bar ON (foo.a = b);" ~: testHive "DEFAULTSEMI3" defaultDefaultingTestCatalog
          ( HiveStandardSqlStatement
              ( QueryStmt
                  ( QuerySelect 1
                      ( makeSelect 2
                          ( SelectColumns 3
                              [ SelectStar 4
                                  ( Just
                                      ( QTableName 5 Nothing "foo" )
                                  )
                                  Unused
                              ]
                          )
                      )
                      { selectFrom = Just
                          ( SelectFrom 6
                              [ TablishJoin 7
                                  ( JoinSemi 8 )
                                  ( JoinOn
                                      ( BinOpExpr 9
                                          ( Operator "=" )
                                          ( ColumnExpr 10
                                              ( QColumnName 12
                                                  ( Just
                                                      ( QTableName 11 Nothing "foo" )
                                                  )
                                                  "a"
                                              )
                                          )
                                          ( ColumnExpr 13
                                              ( QColumnName 14 Nothing "b" )
                                          )
                                      )
                                  )
                                  ( TablishTable 15 TablishAliasesNone
                                      ( QTableName 16 Nothing "foo" )
                                  )
                                  ( TablishTable 17 TablishAliasesNone
                                      ( QTableName 18 Nothing "bar" )
                                  )
                              ]
                          )
                      }
                  )
              )
          )

        , "SELECT * FROM foo LEFT SEMI JOIN bar ON (foo.a = b);" ~: testHive "DEFAULTSEMI4" defaultDefaultingTestCatalog
          ( HiveStandardSqlStatement
              ( QueryStmt
                  ( QuerySelect 1
                      ( makeSelect 2
                          ( SelectColumns 3
                              [ SelectStar 4 Nothing Unused ]
                          )
                      )
                      { selectFrom = Just
                          ( SelectFrom 5
                              [ TablishJoin 6
                                  ( JoinSemi 7 )
                                  ( JoinOn
                                      ( BinOpExpr 8
                                          ( Operator "=" )
                                          ( ColumnExpr 9
                                              ( QColumnName 11
                                                  ( Just
                                                      ( QTableName 10 Nothing "foo" )
                                                  )
                                                  "a"
                                              )
                                          )
                                          ( ColumnExpr 12
                                              ( QColumnName 13 Nothing "b" )
                                          )
                                      )
                                  )
                                  ( TablishTable 14 TablishAliasesNone
                                      ( QTableName 15 Nothing "foo" )
                                  )
                                  ( TablishTable 16 TablishAliasesNone
                                      ( QTableName 17 Nothing "bar" )
                                  )
                              ]
                          )
                      }
                  )
              )
          )

        , "SELECT * FROM foo LATERAL VIEW explode(foo.a) f AS x;" ~: testHive "DEFAULTEXPLODE" defaultDefaultingTestCatalog
            ( HiveStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns
                                { selectColumnsInfo = 3
                                , selectColumnsList = [ SelectStar 4 Nothing Unused ]
                                }
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 5
                                [ TablishLateralView 6
                                    ( LateralView
                                        { lateralViewInfo = 7
                                        , lateralViewOuter = Nothing
                                        , lateralViewExprs =
                                          [ FunctionExpr 8
                                            ( QFunctionName 9 Nothing "explode" )
                                            notDistinct
                                            [ ColumnExpr 10
                                                ( QColumnName 12
                                                    ( Just
                                                        ( QTableName 11 Nothing "foo" )
                                                    )
                                                    "a"
                                                )
                                            ]
                                            []
                                            Nothing
                                            Nothing
                                          ]
                                        , lateralViewWithOrdinality = False
                                        , lateralViewAliases = ( TablishAliasesTC
                                                                 ( TableAlias 13 "f" ( TableAliasId 0 ) )
                                                                 [ ColumnAlias 14 "x" ( ColumnAliasId 1 ) ]
                                                               )
                                        }
                                    )
                                    ( Just ( TablishTable 15 TablishAliasesNone
                                                ( QTableName 16 Nothing "foo" )
                                           )
                                    )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT bar.col FROM foo LATERAL VIEW explode(foo.a) bar;" ~: testHive "DEFAULTEXPLODE2" defaultDefaultingTestCatalog
          -- test that we resolve the default alias `col` which is specific to the `explode` UDTF
            ( HiveStandardSqlStatement
                ( QueryStmt
                    ( QuerySelect 1
                        ( makeSelect 2
                            ( SelectColumns 3
                                [ SelectExpr 4
                                    [ ColumnAlias 5 "col" ( ColumnAliasId 1 ) ]
                                    ( ColumnExpr 6
                                        ( QColumnName 8
                                            ( Just
                                                ( QTableName 7 Nothing "bar" )
                                            )
                                            "col"
                                        )
                                    )
                                ]
                            )
                        )
                        { selectFrom = Just
                            ( SelectFrom 9
                                [ TablishLateralView 10
                                    ( LateralView
                                        { lateralViewInfo = 11
                                        , lateralViewOuter = Nothing
                                        , lateralViewExprs =
                                          [ FunctionExpr 12
                                            ( QFunctionName 13 Nothing "explode" )
                                            notDistinct
                                            [ ColumnExpr 14
                                                ( QColumnName 16
                                                    ( Just
                                                        ( QTableName 15 Nothing "foo" )
                                                    )
                                                    "a"
                                                )
                                            ]
                                            []
                                            Nothing
                                            Nothing
                                          ]
                                        , lateralViewWithOrdinality = False
                                        , lateralViewAliases = TablishAliasesT $ TableAlias 17 "bar" ( TableAliasId 0 )
                                        }
                                    )
                                    ( Just ( TablishTable 18 TablishAliasesNone
                                                ( QTableName 19 Nothing "foo" )
                                           )
                                    )
                                ]
                            )
                        }
                    )
                )
            )

        , "SELECT * FROM foo ORDER BY 1;" ~: testHive "ORDER2" defaultDefaultingTestCatalog
            ( HiveStandardSqlStatement
                ( QueryStmt
                    ( QueryOrder 0
                        [ Order 1
                            ( PositionOrExprPosition 2 1 Unused )
                            ( OrderAsc Nothing )
                            ( NullsAuto Nothing )
                        ]
                        ( QuerySelect 3
                            ( makeSelect 4
                                ( SelectColumns
                                    { selectColumnsInfo = 5
                                    , selectColumnsList =
                                        [ SelectStar 6 Nothing Unused ]
                                    }
                                )
                            )
                            { selectFrom = Just
                                ( SelectFrom 7
                                    [ TablishTable 8 TablishAliasesNone
                                        ( QTableName
                                            { tableNameInfo = 9
                                            , tableNameSchema = Nothing
                                            , tableNameName = "foo"
                                            }
                                        )
                                    ]
                                )
                            }
                        )
                    )
                )
            )
        ]
    ]

defaultTestCatalog :: Catalog
defaultTestCatalog = makeCatalog
    ( HMS.singleton (defaultDatabase ()) $ HMS.fromList
        [ ( mkNormalSchema "public" ()
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
            , ( QTableName () None "baz"
              , persistentTable [ QColumnName () None "a" ]
              )
            ]
          )
        , ( mkNormalSchema "other" ()
          , HMS.fromList
            [ ( QTableName () None "foo"
              , persistentTable [ QColumnName () None "b" ]
              )
            , ( QTableName () None "bar"
              , persistentTable [ QColumnName () None "a" ]
              )
            ]
          )
        , ( mkNormalSchema "empty" ()
          , HMS.empty
          )
        ]
    )
    [ mkNormalSchema "public" () ]
    ( defaultDatabase () )

defaultDefaultingTestCatalog :: Catalog
defaultDefaultingTestCatalog = makeDefaultingCatalog (catalogMap defaultTestCatalog) [mkNormalSchema "public" ()] (defaultDatabase ())

pathologicalSemiJoinStructAccessorCatalog :: Catalog
pathologicalSemiJoinStructAccessorCatalog = makeCatalog
    ( HMS.singleton (defaultDatabase ()) $ HMS.fromList
        [ ( mkNormalSchema "public" ()
          , HMS.fromList
            [ ( QTableName () None "t"
              , persistentTable
                  [ QColumnName () None "id"
                  , QColumnName () None "a"]
              )
            , ( QTableName () None "a"
              , persistentTable
                  [ QColumnName () None "id"
                  , QColumnName () None "x"
                  ]
              )
            ]
          )
        ]
    )
    defaultTestPath
    (defaultDatabase ())

tests :: Test
tests = test [ testResolutionOnASTs
             , testNoResolveErrors
             , testDefaulting
             ]
