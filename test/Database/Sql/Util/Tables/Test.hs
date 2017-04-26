{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Database.Sql.Util.Tables.Test where

import           Test.HUnit
import           Data.Set (Set)
import qualified Data.Set as S
import qualified Database.Sql.Util.Test as Test
import qualified Data.HashMap.Strict as HMS
import           Database.Sql.Util.Tables
import           Database.Sql.Type as SQL
import qualified Data.Text.Lazy as TL
import           Data.Proxy (Proxy(..))

instance Test.TestableAnalysis HasTables a where
    type TestResult HasTables a = Set FullyQualifiedTableName
    runTest _ _ = getTables

testHive :: TL.Text -> Catalog -> (Set FullyQualifiedTableName -> Assertion) -> [Assertion]
testHive = Test.testResolvedHive (Proxy :: Proxy HasTables)

testVertica :: TL.Text -> Catalog -> (Set FullyQualifiedTableName -> Assertion) -> [Assertion]
testVertica = Test.testResolvedVertica (Proxy :: Proxy HasTables)

testAll :: TL.Text -> Catalog -> (Set FullyQualifiedTableName -> Assertion) -> [Assertion]
testAll = Test.testResolvedAll (Proxy :: Proxy HasTables)

testTableUsage :: Test
testTableUsage = test
      [ "Generate table usages for parsed queries" ~: concat
        [ testAll "SELECT 1;" defaultTestCatalog (@=? S.empty)
        , testAll "SELECT * FROM potato;" defaultTestCatalog
          (@=? S.singleton
           (FullyQualifiedTableName "default_db" "public" "potato"))
        , testAll "INSERT INTO potato SELECT * FROM foobar;" defaultTestCatalog
          (@=? S.fromList
           [ FullyQualifiedTableName "default_db" "public" "potato"
           , FullyQualifiedTableName "default_db" "public" "foobar"
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
            [ ( QTableName () None "potato"
              , persistentTable [ QColumnName () None "a" ]
              )
            , ( QTableName () None "foobar"
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
tests = test [ testTableUsage ]
