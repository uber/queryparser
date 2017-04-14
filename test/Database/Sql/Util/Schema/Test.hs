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

module Database.Sql.Util.Schema.Test where

import Test.HUnit
import Database.Sql.Util.Schema
import Database.Sql.Type

import qualified Data.HashMap.Strict as HMS

import Debug.FileLocation
import Language.Haskell.TH.Syntax (Loc (..))


applySchemaChangeMatch :: (SchemaChange, Loc, Catalog, (Catalog, [SchemaChangeError])) -> Assertion
applySchemaChangeMatch (change, Loc{..}, oldCatalog, newCatalog) = 
    let newCatalog' = applySchemaChange change oldCatalog
        position = concat $ map (++ ":") [loc_filename, show $ fst loc_start]
     in assertEqual (unwords [position, "catalogs don't match"]) newCatalog newCatalog'

defaultDatabase :: DatabaseName ()
defaultDatabase = DatabaseName () "default_db"

publicSchema :: UQSchemaName ()
publicSchema = mkNormalSchema "public" ()

testApplySchemaChange :: Test
testApplySchemaChange = test
    [ "Apply schema changes to catalogs" ~: map (TestCase . applySchemaChangeMatch)
        [
            let table = QTableName () (pure $ QSchemaName () (pure defaultDatabase) "public" NormalSchema) "foo"
             in ( AddColumn $ QColumnName () (pure table) "c"
                , $__LOC__, defaultTestCatalog
                , ( makeCatalog
                      ( HMS.singleton defaultDatabase $ HMS.singleton publicSchema $ HMS.fromList
                          [ ( QTableName () None "foo"
                            , persistentTable
                                [ QColumnName () None "a"
                                , QColumnName () None "c"
                                ]
                            )
                          , ( QTableName () None "bar"
                            , persistentTable
                                [ QColumnName () None "a"
                                , QColumnName () None "b"
                                ]
                            )
                          ]
                      )
                      [ publicSchema ]
                      defaultDatabase
                  , []
                  )
                )
        ]
    ]

defaultTestCatalog :: Catalog
defaultTestCatalog = makeCatalog
    ( HMS.singleton defaultDatabase $ HMS.singleton publicSchema $ HMS.fromList
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
    [ publicSchema ]
    defaultDatabase


tests :: Test
tests = test [ testApplySchemaChange ]
