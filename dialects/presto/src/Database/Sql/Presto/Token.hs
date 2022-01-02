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

module Database.Sql.Presto.Token where

import           Data.Text.Lazy (Text)
import           Data.ByteString.Lazy (ByteString)
import qualified Data.Map as M

data Token = TokWord !Bool !Text
           | TokString !ByteString
           | TokBinary !ByteString
           | TokNumber !Text
           | TokSymbol !Text
           | TokError !String
             deriving (Show, Eq)

data WordInfo = WordInfo
              { wordCanBeSchemaName :: !Bool
              , wordCanBeTableName :: !Bool
              , wordCanBeColumnName :: !Bool
              , wordCanBeFunctionName :: !Bool
              }

wordInfo :: Text -> WordInfo
wordInfo word = maybe (WordInfo True True True True) id $ M.lookup word $ M.fromList
    [ ("all",               WordInfo False False False False)
    , ("alter",             WordInfo False False False False)
    , ("and",               WordInfo False False False False)
    , ("any",               WordInfo False False False False)
    , ("as",                WordInfo False False False False)
    , ("asc",               WordInfo False False False False)
    , ("between",           WordInfo False False False False)
    , ("by",                WordInfo False False False False)
    , ("case",              WordInfo False False False False)
    , ("cast",              WordInfo False False False False)
    , ("constraint",        WordInfo False False False False)
    , ("create",            WordInfo False False False False)
    , ("cross",             WordInfo False False False False)
    , ("cube",              WordInfo False False False False)
    , ("current_date",      WordInfo False False False False)
    , ("current_time",      WordInfo False False False False)
    , ("current_timestamp", WordInfo False False False False)
    , ("deallocate",        WordInfo False False False False)
    , ("delete",            WordInfo False False False False)
    , ("desc",              WordInfo False False False False)
    , ("describe",          WordInfo False False False False)
    , ("distinct",          WordInfo False False False False)
    , ("drop",              WordInfo False False False False)
    , ("else",              WordInfo False False False False)
    , ("end",               WordInfo False False False False)
    , ("escape",            WordInfo False False False False)
    , ("except",            WordInfo False False False False)
    , ("execute",           WordInfo False False False False)
    , ("exists",            WordInfo False False False False)
    , ("extract",           WordInfo False False False False)
    , ("false",             WordInfo False False False False)
    , ("first",             WordInfo False False False False)
    , ("for",               WordInfo False False False False)
    , ("from",              WordInfo False False False False)
    , ("full",              WordInfo False False False False)
    , ("group",             WordInfo False False False False)
    , ("grouping",          WordInfo False False False False)
    , ("having",            WordInfo False False False False)
    , ("in",                WordInfo False False False False)
    , ("inner",             WordInfo False False False False)
    , ("insert",            WordInfo False False False False)
    , ("intersect",         WordInfo False False False False)
    , ("into",              WordInfo False False False False)
    , ("is",                WordInfo False False False False)
    , ("join",              WordInfo False False False False)
    , ("last",              WordInfo False False False False)
    , ("left",              WordInfo False False False False)
    , ("like",              WordInfo False False False False)
    , ("limit",             WordInfo False False False False)
    , ("localtime",         WordInfo False False False False)
    , ("localtimestamp",    WordInfo False False False False)
    , ("natural",           WordInfo False False False False)
    , ("normalize",         WordInfo False False False False)
    , ("not",               WordInfo False False False False)
    , ("null",              WordInfo False False False False)
    , ("nulls",             WordInfo False False False False)
    , ("on",                WordInfo False False False False)
    , ("or",                WordInfo False False False False)
    , ("order",             WordInfo False False False False)
    , ("ordinality",        WordInfo False False False False)
    , ("outer",             WordInfo False False False False)
    , ("prepare",           WordInfo False False False False)
    , ("recursive",         WordInfo False False False False)
    , ("rename",            WordInfo False False False False)
    , ("right",             WordInfo False False False False)
    , ("rollup",            WordInfo False False False False)
    , ("select",            WordInfo False False False False)
    , ("sets",              WordInfo False False False False)
    , ("some",              WordInfo False False False False)
    , ("table",             WordInfo False False False False)
    , ("then",              WordInfo False False False False)
    , ("true",              WordInfo False False False False)
    , ("try_cast",          WordInfo False False False False)
    , ("unbounded",         WordInfo False False False False)
    , ("union",             WordInfo False False False False)
    , ("unnest",            WordInfo False False False False)
    , ("using",             WordInfo False False False False)
    , ("values",            WordInfo False False False False)
    , ("when",              WordInfo False False False False)
    , ("where",             WordInfo False False False False)
    , ("with",              WordInfo False False False False)
    , ("window",            WordInfo False False False False)
    , ("no",                WordInfo True True True True)
    , ("data",              WordInfo True True True True)
    ]
