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

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.Sql.Presto.Type where

import Database.Sql.Type
import Database.Sql.Position (Range)
import Database.Sql.Util.Columns
import Database.Sql.Util.Joins
import Database.Sql.Util.Lineage.Table
import Database.Sql.Util.Lineage.ColumnPlus
import Database.Sql.Util.Schema
import Database.Sql.Util.Scope
import Database.Sql.Util.Tables

import Data.Aeson as JSON
import Data.Proxy (Proxy (..))

import qualified Data.Set as S
import qualified Data.Map as M

import Data.Data (Data)
import GHC.Generics (Generic)

data Presto

deriving instance Data Presto

dialectProxy :: Proxy Presto
dialectProxy = Proxy

instance Dialect Presto where

    shouldCTEsShadowTables _ = True

    resolveCreateTableExtra _ Unused = pure Unused

    getSelectScope _ fromColumns selectionAliases = SelectScope
        { bindForWhere = bindFromColumns fromColumns
        , bindForGroup = bindFromColumns fromColumns
        , bindForHaving = bindFromColumns fromColumns
        , bindForOrder = bindBothColumns fromColumns selectionAliases
        , bindForNamedWindow = bindColumns fromColumns
        }

    areLcolumnsVisibleInLateralViews _ = True

data PrestoStatement r a = PrestoStandardSqlStatement (Statement Presto r a)
                           | PrestoUnhandledStatement a

deriving instance (ConstrainSNames Data r a, Data r) => Data (PrestoStatement r a)
deriving instance Generic (PrestoStatement r a)
deriving instance ConstrainSNames Eq r a => Eq (PrestoStatement r a)
deriving instance ConstrainSNames Show r a => Show (PrestoStatement r a)
deriving instance ConstrainSASNames Functor r => Functor (PrestoStatement r)
deriving instance ConstrainSASNames Foldable r => Foldable (PrestoStatement r)
deriving instance ConstrainSASNames Traversable r => Traversable (PrestoStatement r)


instance HasJoins (PrestoStatement ResolvedNames a) where
    getJoins (PrestoStandardSqlStatement stmt) = getJoins stmt
    getJoins (PrestoUnhandledStatement _) = S.empty

instance HasTableLineage (PrestoStatement ResolvedNames a) where
    getTableLineage (PrestoStandardSqlStatement stmt) = tableLineage stmt
    getTableLineage (PrestoUnhandledStatement _) = M.empty

instance HasColumnLineage (PrestoStatement ResolvedNames Range) where
    getColumnLineage (PrestoStandardSqlStatement stmt) = columnLineage stmt
    getColumnLineage (PrestoUnhandledStatement _) = returnNothing M.empty

resolvePrestoStatement :: PrestoStatement RawNames a -> Resolver (PrestoStatement ResolvedNames) a
resolvePrestoStatement (PrestoStandardSqlStatement stmt) =
    PrestoStandardSqlStatement <$> resolveStatement stmt
resolvePrestoStatement (PrestoUnhandledStatement info) = pure $ PrestoUnhandledStatement info

instance HasSchemaChange (PrestoStatement ResolvedNames a) where
    getSchemaChange (PrestoStandardSqlStatement stmt) = getSchemaChange stmt
    getSchemaChange (PrestoUnhandledStatement _) = []

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (PrestoStatement r a) where
    toJSON (PrestoStandardSqlStatement stmt) = toJSON stmt
    toJSON (PrestoUnhandledStatement info) = JSON.object
        [ "tag" .= JSON.String "PrestoUnhandledStatement"
        , "info" .= info
        ]

typeExample :: ()
typeExample = const () $ toJSON (undefined :: PrestoStatement ResolvedNames Range)

instance HasTables (PrestoStatement ResolvedNames a) where
    goTables (PrestoStandardSqlStatement s) = goTables s
    goTables (PrestoUnhandledStatement _) = return ()

instance HasColumns (PrestoStatement ResolvedNames a) where
    goColumns (PrestoStandardSqlStatement s) = goColumns s
    goColumns (PrestoUnhandledStatement _) = return ()
