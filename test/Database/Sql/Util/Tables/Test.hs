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
    type TestResult HasTables a = Set TableUse
    runTest _ _ = getUsages

testHive :: TL.Text -> Catalog -> (Set TableUse -> Assertion) -> [Assertion]
testHive = Test.testResolvedHive (Proxy :: Proxy HasTables)

testVertica :: TL.Text -> Catalog -> (Set TableUse -> Assertion) -> [Assertion]
testVertica = Test.testResolvedVertica (Proxy :: Proxy HasTables)

testPresto :: TL.Text -> Catalog -> (Set TableUse -> Assertion) -> [Assertion]
testPresto = Test.testResolvedPresto (Proxy :: Proxy HasTables)

testAll :: TL.Text -> Catalog -> (Set TableUse -> Assertion) -> [Assertion]
testAll = Test.testResolvedAll (Proxy :: Proxy HasTables)

tu :: UsageMode -> TL.Text -> TL.Text -> TL.Text -> TableUse
tu um c s t = TableUse um $ FullyQualifiedTableName c s t

testTableUsage :: Test
testTableUsage = test
      [ "Generate table usages for queries" ~: concat
        [ testAll "SELECT 1;" defaultTestCatalog (@?= S.empty)
        , testAll "SELECT * FROM potato;" defaultTestCatalog
          (@?= (S.singleton $ tu ReadData "default_db" "public" "potato"))
        , testAll "SELECT p.x, f.a FROM (SELECT k, a FROM foobar) f JOIN potato p on p.k = f.k;" defaultTestCatalog
          (@?= S.fromList
           [ tu ReadData "default_db" "public" "potato"
           , tu ReadData "default_db" "public" "foobar"
           ]
          )
        , testAll "WITH x AS (SELECT * FROM potato) SELECT * FROM x;" defaultTestCatalog
          (@?= (S.singleton $ tu ReadData "default_db" "public" "potato"))
        ]

      , "Generate table usages for DML queries" ~:
        [ "Insert" ~: concat
          [ testAll "INSERT INTO potato SELECT * FROM foobar;" defaultTestCatalog
            (@?= S.fromList
             [ tu WriteData "default_db" "public" "potato"
             , tu ReadData "default_db" "public" "foobar"
             ]
            )
          ]
        , "Delete" ~: concat
          [ testAll "DELETE FROM potato WHERE x = 'foo';" defaultTestCatalog
            (@?= (S.singleton $ tu WriteData "default_db" "public" "potato"))
          ]
        , "Truncate" ~: concat
          [ testHive "TRUNCATE TABLE potato;" defaultTestCatalog
            (@?= (S.singleton $ tu WriteData "default_db" "public" "potato"))
          , testVertica "TRUNCATE TABLE potato;" defaultTestCatalog
            (@?= (S.singleton $ tu WriteData "default_db" "public" "potato"))
            -- TODO add presto when implemented
          ]
        , "Merge" ~: concat
          [ testVertica
            ( TL.unlines
              [ "MERGE INTO potato t1 USING foobar t2 ON t1.k = t2.k"
              , "WHEN MATCHED THEN UPDATE SET b = t2.b, c = t2.c"
              , "WHEN NOT MATCHED THEN INSERT (b, c) VALUES (t2.b, t2.c);"
              ]
            )
            defaultTestCatalog
            (@?= S.fromList
             [ tu WriteData "default_db" "public" "potato"
             , tu ReadData "default_db" "public" "potato"
             , tu ReadData "default_db" "public" "foobar"
             ]
            )
          ]
        , "Update" ~: ([] :: [Test])
          -- TODO add all to Update when implemented
        ]

      , "Generate table usages for DDL" ~:
        [ "Create table" ~: concat
          [ testHive "CREATE TABLE temp.potato (int a);" defaultTestCatalog
            (@?= (S.singleton $ tu WriteData "default_db" "temp" "potato"))
          , testHive "CREATE TABLE temp.potato AS SELECT * FROM potato;" defaultTestCatalog
            (@?= S.fromList
             [ tu WriteData "default_db" "temp" "potato"
             , tu ReadData "default_db" "public" "potato"
             ]
            )
          , testVertica "CREATE TABLE temp.potato (int a);" defaultTestCatalog
            (@?= (S.singleton $ tu WriteData "default_db" "temp" "potato"))
          , testVertica "CREATE TABLE temp.potato AS SELECT * FROM potato;" defaultTestCatalog
            (@?= S.fromList
             [ tu WriteData "default_db" "temp" "potato"
             , tu ReadData "default_db" "public" "potato"
             ]
            )
            -- TODO add presto when implemented
          ]
        , "Drop table" ~: concat
          [ testAll "DROP TABLE potato;" defaultTestCatalog
            (@?= (S.singleton $ tu WriteData "default_db" "public" "potato"))
          ]
        , "Create view" ~: concat
          [ testHive "CREATE VIEW temp.potato AS SELECT * FROM potato;" defaultTestCatalog
            (@?= S.fromList
             [ tu WriteMeta "default_db" "temp" "potato"
             , tu ReadData "default_db" "public" "potato"
             ]
            )
          , testVertica "CREATE VIEW temp.potato AS SELECT * FROM potato;" defaultTestCatalog
            (@?= S.fromList
             [ tu WriteMeta "default_db" "temp" "potato"
             , tu ReadData "default_db" "public" "potato"
             ]
            )
            -- TODO add presto when implemented
          ]
        , "Drop view" ~: concat
          [ testPresto "DROP VIEW potato;" defaultTestCatalog
            (@?= (S.singleton $ tu WriteMeta "default_db" "public" "potato"))
          , testPresto "DROP VIEW IF EXISTS tomato;" defaultTestCatalog
            (@?= S.empty)
          , testVertica "DROP VIEW potato;" defaultTestCatalog
            (@?= (S.singleton $ tu WriteMeta "default_db" "public" "potato"))
          , testVertica "DROP VIEW IF EXISTS tomato;" defaultTestCatalog
            (@?= S.empty)
          ]
        , "Alter table" ~: concat
          [ testHive "ALTER TABLE potato CHANGE z zz STRING;" defaultTestCatalog
            (@?= (S.singleton $ tu WriteMeta "default_db" "public" "potato"))
          , testHive "ALTER TABLE potato RENAME TO tomato;" defaultTestCatalog
            (@?= S.fromList
             [ tu WriteData "default_db" "public" "tomato"
             , tu WriteData "default_db" "public" "potato"
             , tu ReadData "default_db" "public" "potato"
             ]
            )
          , testHive "ALTER TABLE potato ADD COLUMNS (a STRING);" defaultTestCatalog
            (@?= (S.singleton $ tu WriteData "default_db" "public" "potato"))
          , testVertica "ALTER TABLE potato RENAME TO tomato;" defaultTestCatalog
            (@?= S.fromList
             [ tu WriteData "default_db" "public" "tomato"
             , tu WriteData "default_db" "public" "potato"
             , tu ReadData "default_db" "public" "potato"
             ]
            )
          -- TODO add vertica alter column cases when implemented
          -- TODO add presto when implemented
          ]
        , "Grant/Revoke" ~: ([] :: [Test])
          -- TODO add grant/revoke when fully implemented
        ]
      ]

defaultDatabase :: DatabaseName ()
defaultDatabase = DatabaseName () "default_db"

publicSchema :: UQSchemaName ()
publicSchema = mkNormalSchema "public" ()

tempSchema :: UQSchemaName ()
tempSchema = mkNormalSchema "temp" ()

defaultTestCatalog :: Catalog
defaultTestCatalog = makeCatalog
    ( HMS.singleton defaultDatabase $ HMS.fromList
        [ ( publicSchema
          , HMS.fromList
            [ ( QTableName () None "potato"
              , persistentTable [ QColumnName () None "k"
                                , QColumnName () None "x"
                                , QColumnName () None "y"
                                , QColumnName () None "z"
                                ]
              )
            , ( QTableName () None "foobar"
              , persistentTable
                  [ QColumnName () None "k"
                  , QColumnName () None "a"
                  , QColumnName () None "b"
                  , QColumnName () None "c"
                  ]
              )
            ]
          )
        , ( tempSchema
          , HMS.empty
          )
        ]
    )
    [ publicSchema ]
    defaultDatabase

tests :: Test
tests = test [ testTableUsage ]
