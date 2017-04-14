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

module Database.Sql.Vertica.Token where

import           Data.Text.Lazy (Text)
import           Data.ByteString.Lazy (ByteString)
import qualified Data.Map as M

data Token = TokWord !Bool !Text
           | TokString !ByteString
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
    [ ("all",                  WordInfo False False False True)
    , ("and",                  WordInfo False False True  False)
    , ("asc",                  WordInfo False False False False)
    , ("as",                   WordInfo False False False False)
    , ("at",                   WordInfo False True  True  False)
    , ("between",              WordInfo False False False False)
    , ("by",                   WordInfo False False False False)
    , ("case",                 WordInfo False False False False)
    , ("comma",                WordInfo False False False False)
    , ("cross",                WordInfo False False False False)
    , ("current_database",     WordInfo False False False True)
    , ("current_date",         WordInfo False False False True)
    , ("current_schema",       WordInfo False False False True)
    , ("current_timestamp",    WordInfo False False False False)
    , ("current_time",         WordInfo False False False False)
    , ("current_user",         WordInfo False False False True)
    , ("desc",                 WordInfo False False False False)
    , ("distinct",             WordInfo False False False False)
    , ("else",                 WordInfo False False False False)
    , ("end",                  WordInfo False False False False)
    , ("except",               WordInfo False False False False)
    , ("false",                WordInfo False False False False)
    , ("from",                 WordInfo False False False False)
    , ("full",                 WordInfo False False False False)
    , ("group",                WordInfo False False True  False)
    , ("having",               WordInfo False False False False)
    , ("ilike",                WordInfo False False False False)
    , ("ilikeb",               WordInfo False False False False)
    , ("inner",                WordInfo False False False False)
    , ("intersect",            WordInfo False False False False)
    , ("interval",             WordInfo False False True  False)
    , ("in",                   WordInfo False False False False)
    , ("is",                   WordInfo False False False False)
    , ("isnull",               WordInfo False False False True)
    , ("join",                 WordInfo False False False False)
    , ("left",                 WordInfo False False False True)
    , ("likeb",                WordInfo False False False False)
    , ("limit",                WordInfo False False False False)
    , ("localtimestamp",       WordInfo False False False False)
    , ("localtime",            WordInfo False False True  False)
    , ("natural",              WordInfo False False False False)
    , ("not",                  WordInfo False False False False)
    , ("notnull",              WordInfo False False False True)
    , ("null",                 WordInfo False False False False)
    , ("on",                   WordInfo False False False False)
    , ("order",                WordInfo False False True  False)
    , ("or",                   WordInfo False False True  False)
    , ("outer",                WordInfo False False False False)
    , ("overlaps",             WordInfo False False False False)
    , ("right",                WordInfo False False False True)
    , ("segmented",            WordInfo False False False False)
    , ("select",               WordInfo False False False False)
    , ("semi",                 WordInfo False False False False)
    , ("session_user",         WordInfo False False False True)
    , ("sysdate",              WordInfo False False False True)
    , ("then",                 WordInfo False False False False)
    , ("time",                 WordInfo False False True  False)
    , ("timezone",             WordInfo False False True  False)
    , ("true",                 WordInfo False False False False)
    , ("union",                WordInfo False False False False)
    , ("unknown",              WordInfo False False False False)
    , ("user",                 WordInfo False False True  True)
    , ("using",                WordInfo False False False False)
    , ("when",                 WordInfo False False False False)
    , ("where",                WordInfo False False False False)
    , ("window",               WordInfo False False False False)
    , ("with",                 WordInfo False False False False)
    , ("zone",                 WordInfo False False True  False)
    ]
