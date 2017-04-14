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
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Database.Sql.Util.Eval.Test where

import Test.HUnit
import Test.HUnit.Ticket
import qualified Database.Sql.Util.Test as Test
import Database.Sql.Util.Catalog
import Database.Sql.Util.Eval
import Database.Sql.Util.Eval.Concrete
import Database.Sql.Type as SQL
import Database.Sql.Position as SQL
import Database.Sql.Vertica.Type as VSQL
import Database.Sql.Hive.Type as HiveQL
import Database.Sql.Presto.Type as PrestoQL

import Control.Monad (void)

import Data.List (sort)
import           Data.Map (Map)
import qualified Data.Map as M

import qualified Data.Text.Lazy as TL
import           Data.Proxy (Proxy(..))

import Data.Either (isRight)
import Test.QuickCheck


instance (EvalResult Concrete a ~ Eval Concrete 'TableContext (RecordSet Concrete)) => Test.TestableAnalysis (Evaluate Concrete) a where
    type TestResult (Evaluate Concrete) a = Eval Concrete 'TableContext (RecordSet Concrete)
    runTest _ _ = eval (Proxy :: Proxy Concrete)


instance Evaluate Concrete (VerticaStatement ResolvedNames Range) where
    type EvalResult Concrete (VerticaStatement ResolvedNames Range) = Eval Concrete 'TableContext (RecordSet Concrete)
    eval p (VerticaStandardSqlStatement (QueryStmt query)) = eval p query
    eval _ _ = error "can't evaluate"

instance Evaluate Concrete (HiveStatement ResolvedNames Range) where
    type EvalResult Concrete (HiveStatement ResolvedNames Range) = Eval Concrete 'TableContext (RecordSet Concrete)
    eval p (HiveStandardSqlStatement (QueryStmt query)) = eval p query
    eval _ _ = error "can't evaluate"

instance Evaluate Concrete (PrestoStatement ResolvedNames Range) where
    type EvalResult Concrete (PrestoStatement ResolvedNames Range) = Eval Concrete 'TableContext (RecordSet Concrete)
    eval p (PrestoStandardSqlStatement (QueryStmt query)) = eval p query
    eval _ _ = error "can't evaluate"

testHive :: TL.Text -> Catalog -> (Eval Concrete 'TableContext (RecordSet Concrete) -> Assertion) -> [Assertion]
testHive = Test.testResolvedHive (Proxy :: Proxy (Evaluate Concrete))

testVertica :: TL.Text -> Catalog -> (Eval Concrete 'TableContext (RecordSet Concrete) -> Assertion) -> [Assertion]
testVertica = Test.testResolvedVertica (Proxy :: Proxy (Evaluate Concrete))

testPresto :: TL.Text -> Catalog -> (Eval Concrete 'TableContext (RecordSet Concrete) -> Assertion) -> [Assertion]
testPresto = Test.testResolvedPresto (Proxy :: Proxy (Evaluate Concrete))

testAll :: TL.Text -> Catalog -> (Eval Concrete 'TableContext (RecordSet Concrete) -> Assertion) -> [Assertion]
testAll = Test.testResolvedAll (Proxy :: Proxy (Evaluate Concrete))

assertQuickCheck :: Test.QuickCheck.Testable prop => String -> prop -> Assertion
assertQuickCheck lbl prop = quickCheckWithResult args prop >>= \case
    Success{..} -> pure ()
    result -> assertFailure ("quickcheck property did not hold: " ++ lbl ++ "\n" ++ output result)
  where
    args = stdArgs {chatty = False}

lookupNoDB :: RTableName Range -> Maybe (RecordSet Concrete)
lookupNoDB _ = error "touched the DB, shouldn't be necessary"

lookupDB :: Map (FQTableName ()) (RecordSet Concrete) -> RTableName Range -> Maybe (RecordSet Concrete)
lookupDB db (RTableName fqtn _) = M.lookup (void fqtn) db

testEvaluation :: Test
testEvaluation = test
    [ "Evaluate parsed queries" ~: concat
        [ testAll "SELECT 1;" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB @?= Right [[SqlInt 1]]

        , testAll "SELECT 1 WHERE TRUE;" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB @?= Right [[SqlInt 1]]

        , testAll "SELECT 1 WHERE FALSE;" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB @?= Right []

        , testAll "SELECT 1 + 2;" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB  @?= Right [[SqlInt 3]]

        , testVertica "SELECT t.*, 1 y FROM (SELECT 3, 2) t;" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB @?= Right [[SqlInt 3, SqlInt 2, SqlInt 1]]

        , testAll "SELECT x FROM (SELECT 1 x UNION SELECT 1 y) t;" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB  @?= Right [[SqlInt 1]]

        , testAll "SELECT * FROM (SELECT 1 x UNION SELECT 1 y) t;" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB  @?= Right [[SqlInt 1]]

        , testAll "SELECT 1 FROM foo;" defaultTestCatalog $ \ f -> do
            assertQuickCheck "SELECT FROM foo produces number of rows matching foo" $ \ (ConcreteDb db :: ConcreteDb DefaultCatalogType) ->
                let Right rows = recordSetItems <$> runEval f (lookupDB db)
                    Just (RecordSet _ fooRows) = M.lookup (QTableName () (pure $ inDefaultDatabase "public") "foo") db
                 in rows == map (const [SqlInt 1]) fooRows

        , testAll "SELECT 1 FROM foo, foo;" defaultTestCatalog $ \ f -> do
            let fooTable = QTableName () (pure $ inDefaultDatabase "public") "foo"
                aColumn = QColumnName () (pure fooTable) "a"
                db = M.singleton fooTable $ RecordSet [RColumnRef aColumn] $ replicate 3 [SqlInt 1]
            recordSetItems <$> runEval f (lookupDB db) @?= Right (replicate 9 [SqlInt 1])

        , testAll "SELECT 1 FROM foo, (SELECT 1) bar;" defaultTestCatalog $ \ f -> do
            let fooTable = QTableName () (pure $ inDefaultDatabase "public") "foo"
                aColumn = QColumnName () (pure fooTable) "a"
                db = M.singleton fooTable $ RecordSet [RColumnRef aColumn] $ replicate 3 [SqlInt 1]
            recordSetItems <$> runEval f (lookupDB db) @?= Right (replicate 3 [SqlInt 1])

        , testAll "SELECT 1 FROM foo JOIN foo ON FALSE;" defaultTestCatalog $ \ f ->
            assertQuickCheck "inner join on FALSE produces no rows" $ \ (ConcreteDb db :: ConcreteDb DefaultCatalogType) ->
                (recordSetItems <$> runEval f (lookupDB db)) == Right []

        , testAll "SELECT 1 FROM foo, foo LIMIT 5;" defaultTestCatalog $ \ f -> do
            let fooTable = QTableName () (pure $ inDefaultDatabase "public") "foo"
                aColumn = QColumnName () (pure fooTable) "a"
                db = M.singleton fooTable $ RecordSet [RColumnRef aColumn] $ replicate 3 [SqlInt 1]
            recordSetItems <$> runEval f (lookupDB db) @?= Right (replicate 5 [SqlInt 1])

        , testVertica "SELECT 1 FROM foo, foo LIMIT NULL OFFSET 5;" defaultTestCatalog $ \ f -> do
            let fooTable = QTableName () (pure $ inDefaultDatabase "public") "foo"
                aColumn = QColumnName () (pure fooTable) "a"
                db = M.singleton fooTable $ RecordSet [RColumnRef aColumn] $ replicate 3 [SqlInt 1]
            recordSetItems <$> runEval f (lookupDB db) @?= Right (replicate 4 [SqlInt 1])


        , testAll "SELECT 1 FROM foo LEFT JOIN bar ON FALSE;" defaultTestCatalog $ \ f -> do
            assertQuickCheck "left join on false produces number of rows in left table" $ \ (ConcreteDb db :: ConcreteDb DefaultCatalogType) ->
                let Right rows = recordSetItems <$> runEval f (lookupDB db)
                    Just (RecordSet _ fooRows) = M.lookup (QTableName () (pure $ inDefaultDatabase "public") "foo") db
                 in rows == replicate (length fooRows) [SqlInt 1]

        , testAll "SELECT 1 FROM foo FULL OUTER JOIN bar ON FALSE;" defaultTestCatalog $ \ f -> do
            assertQuickCheck "full outer join on false produces sum of rows in constituent tables" $ \ (ConcreteDb db :: ConcreteDb DefaultCatalogType) ->
                let Right rows = recordSetItems <$> runEval f (lookupDB db)
                    Just (RecordSet _ fooRows) = M.lookup (QTableName () (pure $ inDefaultDatabase "public") "foo") db
                    Just (RecordSet _ barRows) = M.lookup (QTableName () (pure $ inDefaultDatabase "public") "bar") db
                 in rows == replicate (length fooRows + length barRows) [SqlInt 1]

        , testAll "SELECT a FROM foo;" defaultTestCatalog $ \ f -> do
            assertQuickCheck "selecting a column from a table produces a matching column" $ \ (ConcreteDb db :: ConcreteDb DefaultCatalogType) ->
                let Right rows = recordSetItems <$> runEval f (lookupDB db)
                    Just (RecordSet _ fooRows) = M.lookup (QTableName () (pure $ inDefaultDatabase "public") "foo") db
                 in rows == fooRows

        , testAll "SELECT * FROM foo ORDER BY a;" defaultTestCatalog $ \ f -> do
            assertQuickCheck "order by works" $ \ (ConcreteDb db :: ConcreteDb DefaultCatalogType) ->
                let Right rows = recordSetItems <$> runEval f (lookupDB db)
                    Just (RecordSet _ fooRows) = M.lookup (QTableName () (pure $ inDefaultDatabase "public") "foo") db
                 in rows == sort fooRows

        , testAll "SELECT 1 FROM foo GROUP BY a;" defaultTestCatalog $ \ f -> do
            assertQuickCheck "can evaluate query with group by clause" $ \ (ConcreteDb db :: ConcreteDb DefaultCatalogType) ->
                isRight $ runEval f (lookupDB db)

        , testVertica "SELECT 1 FROM foo NATURAL JOIN bar;" defaultTestCatalog $ \ f -> do
            assertQuickCheck "can evaluate query with natural join" $ \ (ConcreteDb db :: ConcreteDb DefaultCatalogType) -> isRight $ runEval f (lookupDB db)

        , testVertica "SELECT 1 FROM foo GROUP BY a HAVING a;" defaultTestCatalog $ \ f -> do
            assertQuickCheck "can evaluate query with having clause" $ \ (ConcreteDb db :: ConcreteDb DefaultCatalogType) -> isRight $ runEval f (lookupDB db)

        , testVertica "SELECT * FROM (SELECT 1 UNION SELECT 1) t GROUP BY 1;" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB @?= Right [[SqlInt 1]]

        , testVertica "SELECT * FROM (SELECT 1 x WHERE false) t GROUP BY x;" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB @?= Right []

        , testVertica "SELECT * FROM (SELECT 1 x UNION ALL SELECT 1) t GROUP BY x;" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB @?= Right [[SqlInt 1]]

        , testVertica "SELECT x as y FROM (SELECT 1 x WHERE false) t GROUP BY y;" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB @?= Right []

        , testVertica "SELECT x as y FROM (SELECT 1 x UNION ALL SELECT 1) t GROUP BY y;" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB @?= Right [[SqlInt 1]]
        ]
    ]


type DefaultCatalogType =
    '[ Database "default_db"
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

inDefaultDatabase :: Applicative f => TL.Text -> QSchemaName f ()
inDefaultDatabase name = QSchemaName () (pure defaultDatabase) name NormalSchema

publicSchema :: UQSchemaName ()
publicSchema = mkNormalSchema "public" ()

defaultTestCatalog :: Catalog
defaultTestCatalog = makeCatalog (mkCatalog defaultCatalogProxy) [publicSchema] defaultDatabase

tests :: Test
tests = test
    [ testEvaluation
    , ticket "T628765" $ testHive "SELECT 1 FROM foo LEFT SEMI JOIN bar ON FALSE;" defaultTestCatalog $ \ f -> do
            assertQuickCheck "left semi join on false produces no rows" $ \ (ConcreteDb db :: ConcreteDb DefaultCatalogType) ->
                (recordSetItems <$> runEval f (lookupDB db)) == Right []
    , ticket "T628851" $ testHive "SELECT x FROM foo LATERAL VIEW explode(a) atbl AS x;" defaultTestCatalog $ \ f -> do
            assertQuickCheck "can evaluate query with lateral view" $ \ (ConcreteDb db :: ConcreteDb DefaultCatalogType) -> isRight $ runEval f (lookupDB db)
    , ticket "T636495" $ testVertica "SELECT 'a' LIKE 'a';" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB @?= Right [[SqlBool True]]
    , ticket "T636505" $ testVertica "SELECT ABS(a) FROM foo;" defaultTestCatalog $ \ f -> do
            assertQuickCheck "can evaluate query with function expression" $ \ (ConcreteDb db :: ConcreteDb DefaultCatalogType) -> isRight $ runEval f (lookupDB db)
    , ticket "T636519" $ testVertica "SELECT '2015-01-01' AT TIMEZONE 'US/Pacific';" defaultTestCatalog $ \ f -> do
            assert $ isRight $ runEval f lookupNoDB
    , ticket "T636527" $ testVertica "SELECT 3 :: VARCHAR;" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB @?= Right [[SqlStr "3"]]
    , ticket "T636572" $ testVertica "SELECT INTEGER '3';" defaultTestCatalog $ \ f -> do
            recordSetItems <$> runEval f lookupNoDB @?= Right [[SqlInt 3]]
    , ticket "T637146" $ testVertica "SELECT * FROM (SELECT TIMESTAMP '2015-01-01' a) t TIMESERIES ts AS '1 day' OVER (PARTITION BY a ORDER BY a);" defaultTestCatalog $ \ f -> do
            assert $ isRight $ runEval f lookupNoDB
    , ticket "T637168" $ testAll "SELECT 1.2;" defaultTestCatalog $ \ f -> do
            case recordSetItems <$> runEval f lookupNoDB of
                Left _ -> assertFailure "failed to evaluate"
                Right [[x]] -> assert $ x > SqlInt 1 && x < SqlInt 2
                Right _ -> assertFailure "evaluated to the wrong shape"
    ]
