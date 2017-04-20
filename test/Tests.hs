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

module Main where

import qualified Database.Sql.Vertica.Parser.Test
import qualified Database.Sql.Vertica.Scanner.Test
import qualified Database.Sql.Hive.Parser.Test
import qualified Database.Sql.Hive.Scanner.Test
import qualified Database.Sql.Presto.Parser.Test
import qualified Database.Sql.Presto.Scanner.Test
import qualified Database.Sql.Info.Test
import qualified Database.Sql.Position.Test
import qualified Database.Sql.Pretty.Test
import qualified Database.Sql.Util.Columns.Test
import qualified Database.Sql.Util.Eval.Test
import qualified Database.Sql.Util.Joins.Test
import qualified Database.Sql.Util.Json.Test
import qualified Database.Sql.Util.Lineage.Table.Test
import qualified Database.Sql.Util.Lineage.ColumnPlus.Test
import qualified Database.Sql.Util.Schema.Test
import qualified Database.Sql.Util.Scope.Test
import qualified Database.Sql.Util.Tables.Test
import qualified Database.Sql.Util.Catalog.Test
import qualified Test.Framework.Providers.API as TestFramework
import qualified Test.Framework.Runners.Console as Test
import qualified Test.Framework.Providers.HUnit as Test
import qualified Test.HUnit as HU

main :: IO ()
main = Test.defaultMain $ hunitTests ++ quickCheckProperties

hunitTests :: [TestFramework.Test]
hunitTests = Test.hUnitTestToTests $ HU.TestList
    [ Database.Sql.Position.Test.tests
    , Database.Sql.Info.Test.tests
    , Database.Sql.Pretty.Test.tests
    , Database.Sql.Util.Columns.Test.tests
    , Database.Sql.Util.Eval.Test.tests
    , Database.Sql.Util.Catalog.Test.tests
    , Database.Sql.Util.Scope.Test.tests
    , Database.Sql.Util.Joins.Test.tests
    , Database.Sql.Util.Lineage.Table.Test.tests
    , Database.Sql.Util.Lineage.ColumnPlus.Test.tests
    , Database.Sql.Util.Schema.Test.tests
    , Database.Sql.Util.Tables.Test.tests
    , Database.Sql.Vertica.Scanner.Test.tests
    , Database.Sql.Vertica.Parser.Test.tests
    , Database.Sql.Hive.Scanner.Test.tests
    , Database.Sql.Hive.Parser.Test.tests
    , Database.Sql.Presto.Scanner.Test.tests
    , Database.Sql.Presto.Parser.Test.tests
    ]

quickCheckProperties :: [TestFramework.Test]
quickCheckProperties = concat
    [ Database.Sql.Util.Json.Test.properties ]
