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

module Database.Sql.Pretty.Test where

import Test.HUnit
import Database.Sql.Pretty
import Database.Sql.Type.Names
import Data.Functor.Identity


defaultDatabase :: DatabaseName ()
defaultDatabase = DatabaseName () "default_db"

tests :: Test
tests = test
        [ "Pretty on OQTableName"
          ~: test [ renderPretty (QTableName () Nothing "foo")
                    ~?= "foo"
                  , renderPretty (QTableName () (Just $ QSchemaName () Nothing "public" NormalSchema) "foo")
                    ~?= "public.foo"
                  , renderPretty (QTableName () (Just $ QSchemaName () (Just defaultDatabase) "public" NormalSchema) "foo")
                    ~?= "default_db.public.foo"
                  , renderPretty (QTableName () (Just $ QSchemaName () Nothing "asdf" SessionSchema) "foo")
                    ~?= "foo"
                  , renderPretty (QTableName () (Just $ QSchemaName () (Just defaultDatabase) "asdf" SessionSchema) "foo")
                    ~?= "foo"
                  ]
        , "Pretty on FQTableName"
          ~: test [ renderPretty (QTableName () (Identity $ QSchemaName () (Identity defaultDatabase) "public" NormalSchema) "foo")
                    ~?= "default_db.public.foo"
                  , renderPretty (QTableName () (Identity $ QSchemaName () (Identity defaultDatabase) "ghjk" SessionSchema) "foo")
                    ~?= "foo"
                  ]
        , "Pretty on OQColumnName"
          ~: test [ renderPretty (QColumnName () Nothing "x")
                    ~?= "x"
                  , renderPretty (QColumnName () (Just $ QTableName () Nothing "foo") "x")
                    ~?= "foo.x"
                  , renderPretty (QColumnName () (Just $ QTableName () (Just $ QSchemaName () Nothing "public" NormalSchema) "foo") "x")
                    ~?= "public.foo.x"
                  , renderPretty (QColumnName () (Just $ QTableName () (Just $ QSchemaName () (Just defaultDatabase) "public" NormalSchema) "foo") "x")
                    ~?= "default_db.public.foo.x"
                  ]
        , "Pretty on FQColumnName"
          ~: test [ renderPretty (QColumnName () (Identity $ QTableName () (Identity $ QSchemaName () (Identity defaultDatabase) "public" NormalSchema) "foo") "x")
                    ~?= "default_db.public.foo.x"
                  ]
        ]
