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

module Database.Sql.Info.Test where

import Test.HUnit
import Database.Sql.Info
import Database.Sql.Type.Names
import Data.List (sort)
import Data.Functor.Identity

tests :: Test
tests = test
        [ "Info on OQTableName"
          ~: test [ getInfo (QTableName [10] Nothing "") ~?= ([10] :: [Int])
                  , sort (getInfo (QTableName [10] (Just $ QSchemaName [5] Nothing "" NormalSchema) "")) ~?= ([5, 10] :: [Int])
                  , sort (getInfo (QTableName [10] (Just $ QSchemaName [5] (Just $ DatabaseName [15] "") "" NormalSchema) "")) ~?= ([5, 10, 15] :: [Int])
                  ]
        , "Info on FQTableName"
          ~: test [ sort (getInfo (QTableName [10] (Identity $ QSchemaName [5] (Identity $ DatabaseName [1] "") "" NormalSchema) "")) ~?= ([1, 5, 10] :: [Int])
                  ]
        , "Info on OQColumnName"
          ~: test [ getInfo (QColumnName [15] Nothing "") ~?= ([15] :: [Int])
                  , sort (getInfo (QColumnName [15] (Just $ QTableName [10] Nothing "") "")) ~?= ([10, 15] :: [Int])
                  , sort (getInfo (QColumnName [15] (Just $ QTableName [10] (Just $ QSchemaName [5] Nothing "" NormalSchema) "") "")) ~?= ([5, 10, 15] :: [Int])
                  , sort (getInfo (QColumnName [15] (Just $ QTableName [10] (Just $ QSchemaName [5] (Just $ DatabaseName [20] "") "" NormalSchema) "") "")) ~?= ([5, 10, 15, 20] :: [Int])
                  , sort (getInfo (QColumnName [1] (Just $ QTableName [10] (Just $ QSchemaName [5] (Just $ DatabaseName [20] "") "" NormalSchema) "") "")) ~?= ([1, 5, 10, 20] :: [Int])
                  ]
        , "Info on FQColumnName"
          ~: test [ sort (getInfo (QColumnName [15] (Identity $ QTableName [10] (Identity $ QSchemaName [5] (Identity $ DatabaseName [20] "") "" NormalSchema) "") "")) ~?= ([5, 10, 15, 20] :: [Int])
                  , sort (getInfo (QColumnName [1] (Identity $ QTableName [10] (Identity $ QSchemaName [5] (Identity $ DatabaseName [20] "") "" NormalSchema) "") "")) ~?= ([1, 5, 10, 20] :: [Int]) ]
        ]
