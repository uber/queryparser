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

module Database.Sql.Pretty where

import Database.Sql.Type

import qualified Data.Text.Lazy as TL

import Text.PrettyPrint


renderPretty :: Pretty a => a -> String
renderPretty = render . pretty

class Pretty a where
    pretty :: a -> Doc


dot :: Doc
dot = text "."

instance Pretty (DatabaseName a) where
    pretty (DatabaseName _ name) = text $ TL.unpack name

instance Foldable f => Pretty (QSchemaName f a) where
    pretty (QSchemaName _ _ _ SessionSchema) = empty
    pretty (QSchemaName _ database name NormalSchema) =
        let d = foldMap pretty database
            addDatabase = if isEmpty d then id else ((d <> dot) <>)
         in addDatabase $ text $ TL.unpack name

instance Foldable f => Pretty (QTableName f a) where
    pretty (QTableName _ schema name) =
        let s = foldMap pretty schema
            addSchema = if isEmpty s then id else ((s <> dot) <>)
         in addSchema $ text $ TL.unpack name

instance Foldable f => Pretty (QColumnName f a) where
    pretty (QColumnName _ table name) =
        let t = foldMap pretty table
            addTable = if isEmpty t then id else ((t <> dot) <>)
         in addTable $ text $ TL.unpack name
