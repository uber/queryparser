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

{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Sql.Util.Test where

import Test.HUnit (Assertion, assertFailure)
import Control.Exception (throw, AssertionFailed (..))

import Database.Sql.Type
import Database.Sql.Position (Range)
import Database.Sql.Util.Scope (runResolverWError)

import qualified Text.Parsec as P

import qualified Database.Sql.Vertica.Parser as VSQL
import qualified Database.Sql.Hive.Parser as Hive
import qualified Database.Sql.Presto.Parser as Presto

import Database.Sql.Vertica.Type (Vertica, VerticaStatement, resolveVerticaStatement)
import Database.Sql.Hive.Type (Hive, HiveStatement, resolveHiveStatement)
import Database.Sql.Presto.Type (Presto, PrestoStatement, resolvePrestoStatement)

import Data.Proxy
import Data.Text.Lazy (Text)

import GHC.Exts (Constraint)

class TestableAnalysis (q :: * -> Constraint) a where
    type TestResult q a

    runTest :: q a => Proxy q -> Catalog -> a -> TestResult q a

data TestAnalysis q =
    forall d a .
        ( ParseableDialect d
        , TestableAnalysis q a
        , a ~ ParseResult d
        , q (ParseResult d)
        ) => RawTestAnalysis
            { sql :: Text
            , catalog :: Catalog
            , check :: TestResult q a -> Assertion
            , dialect :: Proxy d
            }
    | forall d a .
        ( ParseableDialect d
        , a ~ Resolved (ParseResult d)
        , TestableAnalysis q a
        , Resolvable (ParseResult d)
        , q (Resolved (ParseResult d))
        ) => ResolvedTestAnalysis
            { sql :: Text
            , catalog :: Catalog
            , check :: TestResult q a -> Assertion
            , dialect :: Proxy d
            }

class ParseableDialect d where
    type ParseResult d
    parser :: Proxy d -> (Text -> Either P.ParseError (ParseResult d))

instance ParseableDialect Hive where
    type ParseResult Hive = HiveStatement RawNames Range
    parser _ = Hive.parseAll

instance ParseableDialect Vertica where
    type ParseResult Vertica = VerticaStatement RawNames Range
    parser _ = VSQL.parseAll

instance ParseableDialect Presto where
    type ParseResult Presto = PrestoStatement RawNames Range
    parser _ = Presto.parseAll

class Resolvable a where
    type Resolved a
    resolve :: Catalog -> a -> Resolved a

instance Resolvable (VerticaStatement RawNames Range) where
    type Resolved (VerticaStatement RawNames Range) = VerticaStatement ResolvedNames Range
    resolve catalog stmt = case runResolverWError (resolveVerticaStatement stmt) (Proxy :: Proxy Vertica) catalog of
        Left errors -> throw $ AssertionFailed $ "failed to resolve names in VerticaStatement: " ++ show errors
        Right (stmt', _) -> stmt'

instance Resolvable (HiveStatement RawNames Range) where
    type Resolved (HiveStatement RawNames Range) = HiveStatement ResolvedNames Range
    resolve catalog stmt = case runResolverWError (resolveHiveStatement stmt) (Proxy :: Proxy Hive) catalog of
        Left errors -> throw $ AssertionFailed $ "failed to resolve names in HiveStatement: " ++ show errors
        Right (stmt', _) -> stmt'

instance Resolvable (PrestoStatement RawNames Range) where
    type Resolved (PrestoStatement RawNames Range) = PrestoStatement ResolvedNames Range
    resolve catalog stmt = case runResolverWError (resolvePrestoStatement stmt) (Proxy :: Proxy Presto) catalog of
        Left errors -> throw $ AssertionFailed $ "failed to resolve names in PrestoStatement: " ++ show errors
        Right (stmt', _) -> stmt'

testRawHive :: forall q r a .
    ( a ~ HiveStatement RawNames Range
    , TestableAnalysis q a, q a, r ~ TestResult q a
    ) => Proxy q -> Text -> Catalog -> (r -> Assertion) -> [Assertion]
testRawHive _ sql catalog check =
    [ let dialect = Proxy :: Proxy Hive in runTestAnalysis $ (RawTestAnalysis{..} :: TestAnalysis q) ]

testRawVertica :: forall q r a .
    ( a ~ VerticaStatement RawNames Range
    , TestableAnalysis q a, q a, r ~ TestResult q a
    ) => Proxy q -> Text -> Catalog -> (r -> Assertion) -> [Assertion]
testRawVertica _ sql catalog check =
    [ let dialect = Proxy :: Proxy Vertica in runTestAnalysis $ (RawTestAnalysis{..} :: TestAnalysis q) ]

testRawPresto :: forall q r a .
    ( a ~ PrestoStatement RawNames Range
    , TestableAnalysis q a, q a, r ~ TestResult q a
    ) => Proxy q -> Text -> Catalog -> (r -> Assertion) -> [Assertion]
testRawPresto _ sql catalog check =
    [ let dialect = Proxy :: Proxy Presto in runTestAnalysis $ (RawTestAnalysis{..} :: TestAnalysis q) ]

testRawAll :: forall q r a b c .
    ( a ~ VerticaStatement RawNames Range
    , b ~ HiveStatement RawNames Range
    , c ~ PrestoStatement RawNames Range
    , TestableAnalysis q a, q a, r ~ TestResult q a
    , TestableAnalysis q b, q b, r ~ TestResult q b
    , TestableAnalysis q c, q c, r ~ TestResult q c
    ) => Proxy q -> Text -> Catalog -> (r -> Assertion) -> [Assertion]
testRawAll _ sql catalog check =
    [ let dialect = Proxy :: Proxy Vertica in runTestAnalysis $ (RawTestAnalysis{..} :: TestAnalysis q)
    , let dialect = Proxy :: Proxy Hive in runTestAnalysis $ (RawTestAnalysis{..} :: TestAnalysis q)
    , let dialect = Proxy :: Proxy Presto in runTestAnalysis $ (RawTestAnalysis{..} :: TestAnalysis q)
    ]

testResolvedHive :: forall q r a .
    ( a ~ HiveStatement ResolvedNames Range
    , TestableAnalysis q a, q a, r ~ TestResult q a
    ) => Proxy q -> Text -> Catalog -> (r -> Assertion) -> [Assertion]
testResolvedHive _ sql catalog check =
    [ let dialect = Proxy :: Proxy Hive in runTestAnalysis $ (ResolvedTestAnalysis{..} :: TestAnalysis q) ]

testResolvedVertica :: forall q r a .
    ( TestableAnalysis q a , q a, a ~ VerticaStatement ResolvedNames Range
    , r ~ TestResult q a
    ) => Proxy q -> Text -> Catalog -> (r -> Assertion) -> [Assertion]
testResolvedVertica _ sql catalog check =
    [ let dialect = Proxy :: Proxy Vertica in runTestAnalysis $ (ResolvedTestAnalysis{..} :: TestAnalysis q) ]

testResolvedPresto :: forall q r a .
    ( TestableAnalysis q a , q a, a ~ PrestoStatement ResolvedNames Range
    , r ~ TestResult q a
    ) => Proxy q -> Text -> Catalog -> (r -> Assertion) -> [Assertion]
testResolvedPresto _ sql catalog check =
    [ let dialect = Proxy :: Proxy Presto in runTestAnalysis $ (ResolvedTestAnalysis{..} :: TestAnalysis q) ]

testResolvedAll :: forall q r a b c .
    ( a ~ VerticaStatement ResolvedNames Range
    , b ~ HiveStatement ResolvedNames Range
    , c ~ PrestoStatement ResolvedNames Range
    , TestableAnalysis q a, q a, r ~ TestResult q a
    , TestableAnalysis q b, q b, r ~ TestResult q b
    , TestableAnalysis q c, q c, r ~ TestResult q c
    ) => Proxy q -> Text -> Catalog -> (r -> Assertion) -> [Assertion]
testResolvedAll _ sql catalog check =
    [ let dialect = Proxy :: Proxy Vertica in runTestAnalysis $ (ResolvedTestAnalysis{..} :: TestAnalysis q)
    , let dialect = Proxy :: Proxy Hive in runTestAnalysis $ (ResolvedTestAnalysis{..} :: TestAnalysis q)
    , let dialect = Proxy :: Proxy Presto in runTestAnalysis $ (ResolvedTestAnalysis{..} :: TestAnalysis q)
    ]

defaultTestPath :: Path
defaultTestPath = [mkNormalSchema "public" ()]

defaultDatabase :: DatabaseName ()
defaultDatabase = DatabaseName () "default_db"

runTestAnalysis :: forall q . TestAnalysis q -> Assertion
runTestAnalysis RawTestAnalysis{..} =
    let withParser = ($ parser dialect)
    in withParser $ \ parse ->
        case parse sql of
            (Right stmt) ->
                let result = runTest (Proxy :: Proxy q) catalog stmt
                 in check result

            (Left e) -> assertFailure $ unlines
                [ "failed to parse:"
                , show sql
                , show e
                ]

runTestAnalysis ResolvedTestAnalysis{..} =
    let withParser = ($ parser dialect)
    in withParser $ \ parse ->
        case parse sql of
            (Right stmt) ->
                let result = runTest (Proxy :: Proxy q) catalog $ resolve catalog stmt
                 in check result

            (Left e) -> assertFailure $ unlines
                [ "failed to parse:"
                , show sql
                , show e
                ]

makeSelect :: a -> SelectColumns r a -> Select r a
makeSelect selectInfo selectCols = Select
    { selectFrom = Nothing
    , selectWhere = Nothing
    , selectTimeseries = Nothing
    , selectGroup = Nothing
    , selectHaving = Nothing
    , selectNamedWindow = Nothing
    , selectDistinct = notDistinct
    , ..
    }
