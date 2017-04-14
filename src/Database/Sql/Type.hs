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

{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.Sql.Type
    ( module Database.Sql.Type
    , module Database.Sql.Type.Names
    , module Database.Sql.Type.TableProps
    , module Database.Sql.Type.Schema
    , module Database.Sql.Type.Scope
    , module Database.Sql.Type.Query
    , module Database.Sql.Type.Unused
    ) where

import Database.Sql.Type.Names
import Database.Sql.Type.Schema
import Database.Sql.Type.Query
import Database.Sql.Type.TableProps
import Database.Sql.Type.Scope
import Database.Sql.Type.Unused

import Control.Applicative ((<|>))

import           Data.Aeson (ToJSON (..), FromJSON (..), (.:), (.:?), (.=))
import qualified Data.Aeson as JSON

import           Data.List.NonEmpty (NonEmpty ((:|)), nonEmpty, toList)
import qualified Data.List.NonEmpty as NE
import qualified Data.Text.Lazy.Encoding as TL
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BL

import Data.Proxy (Proxy (..))

import Data.Data (Data)
import GHC.Generics (Generic)
import GHC.Exts (Constraint)

import Test.QuickCheck


type ConstrainSDialectParts (c :: * -> Constraint) d r a = (c a, c (DialectCreateTableExtra d r a), c (DialectColumnDefinitionExtra d a))
type ConstrainSASDialectParts (c :: (* -> *) -> Constraint) d r = (c (DialectCreateTableExtra d r), c (DialectColumnDefinitionExtra d))

type ConstrainSAll (c :: * -> Constraint) d r a = (ConstrainSNames c r a, ConstrainSDialectParts c d r a)
type ConstrainSASAll (c :: (* -> *) -> Constraint) d r = (ConstrainSASNames c r, ConstrainSASDialectParts c d r)

class Dialect d where
    type DialectCreateTableExtra d r :: * -> *
    type DialectCreateTableExtra d r = Unused
    type DialectColumnDefinitionExtra d :: * -> *
    type DialectColumnDefinitionExtra d = Unused

    shouldCTEsShadowTables :: Proxy d -> Bool
    areLcolumnsVisibleInLateralViews :: Proxy d -> Bool
    getSelectScope :: forall a . Proxy d -> FromColumns a -> SelectionAliases a -> SelectScope a
    resolveCreateTableExtra :: Proxy d -> DialectCreateTableExtra d RawNames a -> Resolver (DialectCreateTableExtra d ResolvedNames) a


data Unparsed a = Unparsed a deriving (Show, Eq)

data Statement
    d -- sql dialect
    r -- resolution level (raw or resolved)
    a -- per-node parameters - typically Range or ()
        = QueryStmt (Query r a)
        | InsertStmt (Insert r a)
        | UpdateStmt (Update r a)
        | DeleteStmt (Delete r a)
        | TruncateStmt (Truncate r a)
        | CreateTableStmt (CreateTable d r a)
        | AlterTableStmt (AlterTable r a)
        | DropTableStmt (DropTable r a)
        | CreateViewStmt (CreateView r a)
        | DropViewStmt (DropView r a)
        | CreateSchemaStmt (CreateSchema r a)
        | GrantStmt (Grant a)
        | RevokeStmt (Revoke a)
        | BeginStmt a
        | CommitStmt a
        | RollbackStmt a
        | ExplainStmt a (Statement d r a)
        | EmptyStmt a

deriving instance (ConstrainSAll Data d r a, Data d, Data r) => Data (Statement d r a)
deriving instance Generic (Statement d r a)
deriving instance ConstrainSAll Eq d r a => Eq (Statement d r a)
deriving instance ConstrainSAll Show d r a => Show (Statement d r a)
deriving instance ConstrainSASAll Functor d r => Functor (Statement d r)
deriving instance ConstrainSASAll Foldable d r => Foldable (Statement d r)
deriving instance ConstrainSASAll Traversable d r => Traversable (Statement d r)

data Insert r a = Insert
    { insertInfo :: a
    , insertBehavior :: InsertBehavior a
    , insertTable :: TableName r a
    , insertColumns :: Maybe (NonEmpty (ColumnRef r a))
    , insertValues :: InsertValues r a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (Insert r a)
deriving instance Generic (Insert r a)
deriving instance ConstrainSNames Eq r a => Eq (Insert r a)
deriving instance ConstrainSNames Show r a => Show (Insert r a)
deriving instance ConstrainSASNames Functor r => Functor (Insert r)
deriving instance ConstrainSASNames Foldable r => Foldable (Insert r)
deriving instance ConstrainSASNames Traversable r => Traversable (Insert r)

-- Placeholder for partition type until we figure out
-- how to implement placeholders
type TablePartition = ()

data InsertBehavior a
    = InsertOverwrite a
    | InsertAppend a
    | InsertOverwritePartition a TablePartition
    | InsertAppendPartition a TablePartition
    deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)

data InsertValues r a
    = InsertExprValues a (NonEmpty (NonEmpty (DefaultExpr r a)))
    | InsertSelectValues (Query r a)
    | InsertDefaultValues a
    | InsertDataFromFile a ByteString

deriving instance (ConstrainSNames Data r a, Data r) => Data (InsertValues r a)
deriving instance Generic (InsertValues r a)
deriving instance ConstrainSNames Eq r a => Eq (InsertValues r a)
deriving instance ConstrainSNames Show r a => Show (InsertValues r a)
deriving instance ConstrainSASNames Functor r => Functor (InsertValues r)
deriving instance ConstrainSASNames Foldable r => Foldable (InsertValues r)
deriving instance ConstrainSASNames Traversable r => Traversable (InsertValues r)


data DefaultExpr r a
    = DefaultValue a
    | ExprValue (Expr r a)

deriving instance (ConstrainSNames Data r a, Data r) => Data (DefaultExpr r a)
deriving instance Generic (DefaultExpr r a)
deriving instance ConstrainSNames Eq r a => Eq (DefaultExpr r a)
deriving instance ConstrainSNames Show r a => Show (DefaultExpr r a)
deriving instance ConstrainSASNames Functor r => Functor (DefaultExpr r)
deriving instance ConstrainSASNames Foldable r => Foldable (DefaultExpr r)
deriving instance ConstrainSASNames Traversable r => Traversable (DefaultExpr r)


data Update r a = Update
    { updateInfo :: a
    , updateTable :: TableName r a
    , updateAlias :: Maybe (TableAlias a)
    , updateSetExprs :: NonEmpty (ColumnRef r a, DefaultExpr r a)
    , updateFrom :: Maybe (Tablish r a)
    , updateWhere :: Maybe (Expr r a)
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (Update r a)
deriving instance Generic (Update r a)
deriving instance ConstrainSNames Eq r a => Eq (Update r a)
deriving instance ConstrainSNames Show r a => Show (Update r a)
deriving instance ConstrainSASNames Functor r => Functor (Update r)
deriving instance ConstrainSASNames Foldable r => Foldable (Update r)
deriving instance ConstrainSASNames Traversable r => Traversable (Update r)

data Delete r a
    = Delete a (TableName r a) (Maybe (Expr r a))

deriving instance (ConstrainSNames Data r a, Data r) => Data (Delete r a)
deriving instance Generic (Delete r a)
deriving instance ConstrainSNames Eq r a => Eq (Delete r a)
deriving instance ConstrainSNames Show r a => Show (Delete r a)
deriving instance ConstrainSASNames Functor r => Functor (Delete r)
deriving instance ConstrainSASNames Foldable r => Foldable (Delete r)
deriving instance ConstrainSASNames Traversable r => Traversable (Delete r)

data Truncate r a
    = Truncate a (TableName r a)

deriving instance (ConstrainSNames Data r a, Data r) => Data (Truncate r a)
deriving instance Generic (Truncate r a)
deriving instance ConstrainSNames Eq r a => Eq (Truncate r a)
deriving instance ConstrainSNames Show r a => Show (Truncate r a)
deriving instance ConstrainSASNames Functor r => Functor (Truncate r)
deriving instance ConstrainSASNames Foldable r => Foldable (Truncate r)
deriving instance ConstrainSASNames Traversable r => Traversable (Truncate r)


data CreateTable d r a = CreateTable
    { createTableInfo :: a
    , createTablePersistence :: Persistence a
    , createTableExternality :: Externality a
    , createTableIfNotExists :: Maybe a
    , createTableName :: CreateTableName r a
    , createTableDefinition :: TableDefinition d r a
    , createTableExtra :: Maybe (DialectCreateTableExtra d r a)
    }

deriving instance (ConstrainSAll Data d r a, Data d, Data r) => Data (CreateTable d r a)
deriving instance Generic (CreateTable d r a)
deriving instance ConstrainSAll Eq d r a => Eq (CreateTable d r a)
deriving instance ConstrainSAll Show d r a => Show (CreateTable d r a)
deriving instance ConstrainSASAll Functor d r => Functor (CreateTable d r)
deriving instance ConstrainSASAll Foldable d r => Foldable (CreateTable d r)
deriving instance ConstrainSASAll Traversable d r => Traversable (CreateTable d r)


data AlterTable r a
    = AlterTableRenameTable a (TableName r a) (TableName r a)
    | AlterTableRenameColumn a (TableName r a) (UQColumnName a) (UQColumnName a)
    | AlterTableAddColumns a (TableName r a) (NonEmpty (UQColumnName a))

deriving instance (ConstrainSNames Data r a, Data r) => Data (AlterTable r a)
deriving instance Generic (AlterTable r a)
deriving instance ConstrainSNames Eq r a => Eq (AlterTable r a)
deriving instance ConstrainSNames Show r a => Show (AlterTable r a)
deriving instance ConstrainSASNames Functor r => Functor (AlterTable r)
deriving instance ConstrainSASNames Foldable r => Foldable (AlterTable r)
deriving instance ConstrainSASNames Traversable r => Traversable (AlterTable r)

data DropTable r a = DropTable
    { dropTableInfo :: a
    , dropTableIfExists :: Maybe a
    , dropTableNames :: NonEmpty (DropTableName r a)
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (DropTable r a)
deriving instance Generic (DropTable r a)
deriving instance ConstrainSNames Eq r a => Eq (DropTable r a)
deriving instance ConstrainSNames Show r a => Show (DropTable r a)
deriving instance ConstrainSASNames Functor r => Functor (DropTable r)
deriving instance ConstrainSASNames Foldable r => Foldable (DropTable r)
deriving instance ConstrainSASNames Traversable r => Traversable (DropTable r)

data CreateView r a = CreateView
    { createViewInfo :: a
    , createViewPersistence :: Persistence a
    , createViewIfNotExists :: Maybe a
    , createViewColumns :: Maybe (NonEmpty (UQColumnName a))
    , createViewName :: CreateTableName r a
    , createViewQuery :: Query r a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (CreateView r a)
deriving instance Generic (CreateView r a)
deriving instance ConstrainSNames Eq r a => Eq (CreateView r a)
deriving instance ConstrainSNames Show r a => Show (CreateView r a)
deriving instance ConstrainSASNames Functor r => Functor (CreateView r)
deriving instance ConstrainSASNames Foldable r => Foldable (CreateView r)
deriving instance ConstrainSASNames Traversable r => Traversable (CreateView r)

data DropView r a = DropView
    { dropViewInfo :: a
    , dropViewIfExists :: Maybe a
    , dropViewName :: DropTableName r a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (DropView r a)
deriving instance Generic (DropView r a)
deriving instance ConstrainSNames Eq r a => Eq (DropView r a)
deriving instance ConstrainSNames Show r a => Show (DropView r a)
deriving instance ConstrainSASNames Functor r => Functor (DropView r)
deriving instance ConstrainSASNames Foldable r => Foldable (DropView r)
deriving instance ConstrainSASNames Traversable r => Traversable (DropView r)

data CreateSchema r a = CreateSchema
    { createSchemaInfo :: a
    , createSchemaIfNotExists :: Maybe a
    , createSchemaName :: CreateSchemaName r a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (CreateSchema r a)
deriving instance Generic (CreateSchema r a)
deriving instance ConstrainSNames Eq r a => Eq (CreateSchema r a)
deriving instance ConstrainSNames Show r a => Show (CreateSchema r a)
deriving instance ConstrainSASNames Functor r => Functor (CreateSchema r)
deriving instance ConstrainSASNames Foldable r => Foldable (CreateSchema r)
deriving instance ConstrainSASNames Traversable r => Traversable (CreateSchema r)

data Grant a = Grant a
    deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)

data Revoke a = Revoke a
    deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)

data TableDefinition d r a
    = TableColumns a (NonEmpty (ColumnOrConstraint d r a))
    | TableLike a (TableName r a)
    | TableAs a (Maybe (NonEmpty (UQColumnName a))) (Query r a)  -- TODO what do I do with "AT ..." elsewhere?
    | TableNoColumnInfo a -- Hive permits CREATE TABLEs with custom serializers,
      -- e.g. "CREATE EXTERNAL TABLE foo LOCATION 'hdfs://';"
      -- and "CREATE TABLE foo ROW FORMAT SERDE 'AvroSerDe' TBLPROPERTIES ('avro.schema.url'='hdfs://');"

deriving instance (ConstrainSAll Data d r a, Data d, Data r) => Data (TableDefinition d r a)
deriving instance Generic (TableDefinition d r a)
deriving instance ConstrainSAll Eq d r a => Eq (TableDefinition d r a)
deriving instance ConstrainSAll Show d r a => Show (TableDefinition d r a)
deriving instance ConstrainSASAll Functor d r => Functor (TableDefinition d r)
deriving instance ConstrainSASAll Foldable d r => Foldable (TableDefinition d r)
deriving instance ConstrainSASAll Traversable d r => Traversable (TableDefinition d r)


-- | ColumnOrConstraint
-- Column definition or *table level* constraint
-- Column-level constraints are carried with the column

data ColumnOrConstraint d r a
    = ColumnOrConstraintColumn (ColumnDefinition d r a)
    | ColumnOrConstraintConstraint (ConstraintDefinition a)

deriving instance (ConstrainSAll Data d r a, Data d, Data r) => Data (ColumnOrConstraint d r a)
deriving instance Generic (ColumnOrConstraint d r a)
deriving instance ConstrainSAll Eq d r a => Eq (ColumnOrConstraint d r a)
deriving instance ConstrainSAll Show d r a => Show (ColumnOrConstraint d r a)
deriving instance ConstrainSASAll Functor d r => Functor (ColumnOrConstraint d r)
deriving instance ConstrainSASAll Foldable d r => Foldable (ColumnOrConstraint d r)
deriving instance ConstrainSASAll Traversable d r => Traversable (ColumnOrConstraint d r)


data ColumnDefinition d r a = ColumnDefinition
    { columnDefinitionInfo :: a
    , columnDefinitionName :: UQColumnName a
    , columnDefinitionType :: DataType a
    , columnDefinitionNull :: Maybe (NullConstraint a)
    , columnDefinitionDefault :: Maybe (Expr r a)
    , columnDefinitionExtra :: Maybe (DialectColumnDefinitionExtra d a)
    }

deriving instance (ConstrainSAll Data d r a, Data d, Data r) => Data (ColumnDefinition d r a)
deriving instance Generic (ColumnDefinition d r a)
deriving instance ConstrainSAll Eq d r a => Eq (ColumnDefinition d r a)
deriving instance ConstrainSAll Show d r a => Show (ColumnDefinition d r a)
deriving instance ConstrainSASAll Functor d r => Functor (ColumnDefinition d r)
deriving instance ConstrainSASAll Foldable d r => Foldable (ColumnDefinition d r)
deriving instance ConstrainSASAll Traversable d r => Traversable (ColumnDefinition d r)

data NullConstraint a
    = Nullable a
    | NotNull a
      deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)

data ConstraintDefinition a = ConstraintDefinition
    { constraintDefinitionInfo :: a
    } deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)


instance ConstrainSAll ToJSON d r a => ToJSON (Statement d r a) where
    toJSON (QueryStmt query) = JSON.object
        [ "tag" .= JSON.String "QueryStmt"
        , "query" .= query
        ]

    toJSON (InsertStmt insert) = JSON.object
        [ "tag" .= JSON.String "InsertStmt"
        , "insert" .= insert
        ]

    toJSON (UpdateStmt update) = JSON.object
        [ "tag" .= JSON.String "UpdateStmt"
        , "update" .= update
        ]

    toJSON (DeleteStmt delete) = JSON.object
        [ "tag" .= JSON.String "DeleteStmt"
        , "delete" .= delete
        ]

    toJSON (TruncateStmt truncate') = JSON.object
        [ "tag" .= JSON.String "TruncateStmt"
        , "truncate" .= truncate'
        ]

    toJSON (CreateTableStmt create) = JSON.object
        [ "tag" .= JSON.String "CreateTableStmt"
        , "create" .= create
        ]

    toJSON (AlterTableStmt alter) = JSON.object
        [ "tag" .= JSON.String "AlterTableStmt"
        , "alter" .= alter
        ]

    toJSON (DropTableStmt drop') = JSON.object
        [ "tag" .= JSON.String "DropTableStmt"
        , "drop" .= drop'
        ]

    toJSON (CreateViewStmt create) = JSON.object
        [ "tag" .= JSON.String "CreateViewStmt"
        , "create" .= create
        ]

    toJSON (DropViewStmt drop') = JSON.object
        [ "tag" .= JSON.String "DropViewStmt"
        , "drop" .= drop'
        ]

    toJSON (CreateSchemaStmt create) = JSON.object
        [ "tag" .= JSON.String "CreateSchemaStmt"
        , "create" .= create
        ]

    toJSON (GrantStmt grant) = JSON.object
        [ "tag" .= JSON.String "GrantStmt"
        , "grant" .= grant
        ]

    toJSON (RevokeStmt revoke) = JSON.object
        [ "tag" .= JSON.String "RevokeStmt"
        , "revoke" .= revoke
        ]

    toJSON (BeginStmt begin) = JSON.object
        [ "tag" .= JSON.String "BeginStmt"
        , "begin" .= begin
        ]

    toJSON (CommitStmt info) = JSON.object
        [ "tag" .= JSON.String "CommitStmt"
        , "info" .= info
        ]

    toJSON (RollbackStmt info) = JSON.object
        [ "tag" .= JSON.String "RollbackStmt"
        , "info" .= info
        ]

    toJSON (ExplainStmt info stmt) = JSON.object
        [ "tag" .= JSON.String "ExplainStmt"
        , "info" .= info
        , "stmt" .= stmt
        ]

    toJSON (EmptyStmt info) = JSON.object
        [ "tag" .= JSON.String "EmptyStmt"
        , "info" .= info
        ]

instance ConstrainSAll ToJSON d r a => ToJSON (CreateTable d r a) where
    toJSON CreateTable{..} = JSON.object
        [ "tag" .= JSON.String "CreateTable"
        , "info" .= createTableInfo
        , "persistence" .= createTablePersistence
        , "ifnotexists" .= case createTableIfNotExists of
            Just info -> JSON.object ["info" .= info, "value" .= True]
            Nothing -> JSON.Null

        , "table" .= createTableName
        , "definition" .= createTableDefinition
        , "extra" .= createTableExtra
        ]

instance ( ToJSON a
         , ToJSON (TableName r a)
         ) => ToJSON (AlterTable r a) where
    toJSON (AlterTableRenameTable info from to) = JSON.object
        [ "tag" .= JSON.String "AlterTableRenameTable"
        , "info" .= info
        , "from" .= from
        , "to" .= to
        ]
    toJSON (AlterTableRenameColumn info table from to) = JSON.object
        [ "tag" .= JSON.String "AlterTableRenameColumn"
        , "info" .= info
        , "table" .= table
        , "from" .= from
        , "to" .= to
        ]
    toJSON (AlterTableAddColumns info table (c:|cs)) = JSON.object
        [ "tag" .= JSON.String "AlterTableAddColumns"
        , "info" .= info
        , "table" .= table
        , "columns" .= (c:cs)
        ]

instance (ToJSON a, ToJSON (DropTableName r a)) => ToJSON (DropTable r a) where
    toJSON DropTable{..} = JSON.object
        [ "tag" .= JSON.String "DropTable"
        , "info" .= dropTableInfo
        , "ifexists" .= case dropTableIfExists of
            Just info -> JSON.object ["info" .= info, "value" .= True]
            Nothing -> JSON.Null

        , "tables" .= NE.toList dropTableNames
        ]

instance ConstrainSNames ToJSON r a => ToJSON (CreateView r a) where
    toJSON CreateView{..} = JSON.object
        [ "tag" .= JSON.String "CreateView"
        , "info" .= createViewInfo
        , "persistence" .= createViewPersistence
        , "ifnotexists" .= case createViewIfNotExists of
            Just info -> JSON.object ["info" .= info, "value" .= True]
            Nothing -> JSON.Null

        , "columns" .= case createViewColumns of
            Just (c:|cs) -> toJSON (c:cs)
            Nothing -> JSON.Null

        , "table" .=  createViewName
        , "query" .= createViewQuery
        ]

instance ConstrainSNames ToJSON r a => ToJSON (DropView r a) where
    toJSON DropView{..} = JSON.object
        [ "tag" .= JSON.String "DropView"
        , "info" .= dropViewInfo
        , "ifexists" .= case dropViewIfExists of
            Just info -> JSON.object ["info" .= info, "value" .= True]
            Nothing -> JSON.Null

        , "table" .= dropViewName
        ]


instance (ToJSON a, ToJSON (CreateSchemaName r a)) => ToJSON (CreateSchema r a) where
    toJSON (CreateSchema {..}) = JSON.object
        [ "tag" .= JSON.String "CreateSchema"
        , "info" .= createSchemaInfo
        , "ifnotexists" .= case createSchemaIfNotExists of
            Just info -> JSON.object ["info" .= info, "value" .= True]
            Nothing -> JSON.Null

        , "schema" .= createSchemaName
        ]

instance ToJSON a => ToJSON (Grant a) where
    toJSON (Grant a) = JSON.object
        [ "tag" .= JSON.String "Grant"
        , "info" .= a
        ]

instance ToJSON a => ToJSON (Revoke a) where
    toJSON (Revoke a) = JSON.object
        [ "tag" .= JSON.String "Revoke"
        , "info" .= a
        ]

instance ConstrainSAll ToJSON d r a => ToJSON (TableDefinition d r a) where
    toJSON (TableColumns info (c:|cs)) = JSON.object
        [ "tag" .= JSON.String "TableColumns"
        , "info" .= info
        , "columns" .= (c:cs)
        ]

    toJSON (TableLike info table) = JSON.object
        [ "tag" .= JSON.String "TableLike"
        , "info" .= info
        , "table" .= table
        ]

    toJSON (TableAs info columns query) = JSON.object
        [ "tag" .= JSON.String "TableAs"
        , "info" .= info
        , "columns" .= case columns of
            Just (c:|cs) -> toJSON (c:cs)
            Nothing -> JSON.Null
        , "query" .= query
        ]

    toJSON (TableNoColumnInfo info) = JSON.object
        [ "tag" .= JSON.String "TableNoColumnInfo"
        , "info" .= info
        ]

instance ConstrainSAll ToJSON d r a => ToJSON (ColumnOrConstraint d r a) where
    toJSON (ColumnOrConstraintColumn column) = toJSON column
    toJSON (ColumnOrConstraintConstraint constraint) = toJSON constraint

instance ConstrainSAll ToJSON d r a => ToJSON (ColumnDefinition d r a) where
    toJSON (ColumnDefinition{..}) = JSON.object
        [ "tag" .= JSON.String "ColumnDefinition"
        , "info" .= columnDefinitionInfo
        , "name" .= columnDefinitionName
        , "type" .= columnDefinitionType
        , "nullable" .= columnDefinitionNull
        , "default" .= columnDefinitionDefault
        , "extra" .= columnDefinitionExtra
        ]

instance ToJSON a => ToJSON (NullConstraint a) where
    toJSON (Nullable info) = JSON.object
        [ "tag" .= JSON.String "Nullable"
        , "info" .= info
        ]

    toJSON (NotNull info) = JSON.object
        [ "tag" .= JSON.String "NotNull"
        , "info" .= info
        ]

instance ToJSON a => ToJSON (ConstraintDefinition a) where
    toJSON (ConstraintDefinition{..}) = JSON.object
        [ "tag" .= JSON.String "ConstraintDefinition"
        , "info" .= constraintDefinitionInfo
        ]

instance ConstrainSNames ToJSON r a => ToJSON (Insert r a) where
    toJSON Insert{..} = JSON.object
        [ "tag" .= JSON.String "Insert"
        , "info" .= insertInfo
        , "table" .= insertTable
        , "values" .= insertValues
        , "behavior" .= insertBehavior
        ]

instance ToJSON a => ToJSON (InsertBehavior a) where
    toJSON (InsertOverwrite info) = JSON.object
        [ "tag" .= JSON.String "Overwrite"
        , "info" .= info
        ]
    toJSON (InsertAppend info) = JSON.object
        [ "tag" .= JSON.String "Append"
        , "info" .= info
        ]
    toJSON (InsertOverwritePartition info partition) = JSON.object
        [ "tag" .= JSON.String "OverwritePartition"
        , "info" .= info
        , "partition" .= partition
        ]
    toJSON (InsertAppendPartition info partition) = JSON.object
        [ "tag" .= JSON.String "AppendPartition"
        , "info" .= info
        , "partition" .= partition
        ]

instance ConstrainSNames ToJSON r a => ToJSON (InsertValues r a) where
    toJSON (InsertExprValues info values) = JSON.object
        [ "tag" .= JSON.String "InsertExprValues"
        , "info" .= info
        , "values" .= (map toList $ toList values)
        ]

    toJSON (InsertSelectValues query) = JSON.object
        [ "tag" .= JSON.String "InsertSelectValues"
        , "query" .= query
        ]

    toJSON (InsertDefaultValues info) = JSON.object
        [ "tag" .= JSON.String "InsertDefaultValues"
        , "info" .= info
        ]

    toJSON (InsertDataFromFile info path) = JSON.object
        [ "tag" .= JSON.String "InsertDataFromFile"
        , "info" .= info
        , case TL.decodeUtf8' path of
                Left _ -> "path" .= BL.unpack path
                Right str -> "path" .= str
        ]

instance ConstrainSNames ToJSON r a => ToJSON (DefaultExpr r a) where
    toJSON (DefaultValue info) = JSON.object
        [ "tag" .= JSON.String "DefaultValue"
        , "info" .= info
        ]

    toJSON (ExprValue expr) = JSON.object
        [ "tag" .= JSON.String "ExprValue"
        , "expr" .= expr
        ]

instance ConstrainSNames ToJSON r a => ToJSON (Update r a) where
    toJSON Update{..} = JSON.object
        [ "tag" .= JSON.String "Update"
        , "info" .= updateInfo
        , "table" .= updateTable
        , "alias" .= updateAlias
        , "set_exprs" .= toList updateSetExprs
        , "from" .= updateFrom
        , "where" .= updateWhere
        ]

instance ConstrainSNames ToJSON r a => ToJSON (Delete r a) where
    toJSON (Delete info table expr) = JSON.object
        [ "tag" .= JSON.String "Delete"
        , "info" .= info
        , "table" .= table
        , "expr" .= expr
        ]

instance ConstrainSNames ToJSON r a => ToJSON (Truncate r a) where
    toJSON (Truncate info table) = JSON.object
        [ "tag" .= JSON.String "Truncate"
        , "info" .= info
        , "table" .= table
        ]

instance ConstrainSAll FromJSON d r a => FromJSON (Statement d r a) where
    parseJSON (JSON.Object o) = o .: "tag" >>= \case
        JSON.String "QueryStmt" -> QueryStmt <$> o .: "query"
        JSON.String "InsertStmt" -> InsertStmt <$> o .: "insert"
        JSON.String "UpdateStmt" -> UpdateStmt <$> o .: "update"
        JSON.String "DeleteStmt" -> DeleteStmt <$> o .: "delete"
        JSON.String "TruncateStmt" -> TruncateStmt <$> o .: "truncate"
        JSON.String "CreateTableStmt" -> CreateTableStmt <$> o .: "create"
        JSON.String "AlterTableStmt" -> AlterTableStmt <$> o .: "alter"
        JSON.String "DropTableStmt" -> DropTableStmt <$> o .: "drop"
        JSON.String "CreateSchemaStmt" -> CreateSchemaStmt <$> o .: "create"
        JSON.String "GrantStmt" -> GrantStmt <$> o .: "grant"
        JSON.String "RevokeStmt" -> RevokeStmt <$> o .: "revoke"
        JSON.String "BeginStmt" -> BeginStmt <$> o .: "begin"
        JSON.String "CommitStmt" -> CommitStmt <$> o .: "info"
        JSON.String "RollbackStmt" -> RollbackStmt <$> o .: "info"
        JSON.String "ExplainStmt" -> ExplainStmt <$> o .: "info" <*> o .: "stmt"
        JSON.String "EmptyStmt" -> EmptyStmt <$> o .: "info"
        _ -> fail "unrecognized tag on statement object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Statement:"
        , show v

        ]


instance ConstrainSNames FromJSON r a => FromJSON (Insert r a) where
    parseJSON (JSON.Object o) = do
        JSON.String "Insert" <- o .: "tag"
        insertInfo <- o .: "info"
        insertTable <- o .: "table"
        columns <- o .:? "columns"
        let insertColumns = nonEmpty =<< columns
        insertValues <- o .: "values"
        insertBehavior <- o .: "behavior"
        pure Insert{..}

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Insert:"
        , show v
        ]

instance ConstrainSNames FromJSON r a => FromJSON (Update r a) where
    parseJSON (JSON.Object o) = do
        JSON.String "Update" <- o .: "tag"
        updateInfo <- o .: "info"
        updateTable <- o .: "table"
        updateAlias <- o .:? "alias"
        updateSetExprs <- NE.fromList <$> o .: "set_exprs"
        updateFrom <- o .:? "from"
        updateWhere <- o .:? "where"
        pure Update{..}

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Update:"
        , show v
        ]

instance ConstrainSNames FromJSON r a => FromJSON (Delete r a) where
    parseJSON (JSON.Object o) = do
        JSON.String "Delete" <- o .: "tag"
        info <- o .: "info"
        table <- o .: "table"
        expr <- o .: "expr"
        pure $ Delete info table expr

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Delete:"
        , show v
        ]

instance ConstrainSNames FromJSON r a => FromJSON (Truncate r a) where
    parseJSON (JSON.Object o) = do
        JSON.String "Truncate" <- o .: "tag"
        info <- o .: "info"
        table <- o .: "table"
        pure $ Truncate info table

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Truncate:"
        , show v
        ]

instance ConstrainSAll FromJSON d r a => FromJSON (CreateTable d r a) where
    parseJSON (JSON.Object o) = do
        JSON.String "CreateTable" <- o .: "tag"
        createTableInfo <- o .: "info"
        createTablePersistence <- o .: "persistence"
        createTableExternality <- o .: "externality"
        createTableIfNotExists <- o .:? "ifnotexists" >>= \case
            Just o' -> do
                JSON.Bool True <- o' .: "value"
                o' .: "info"
            Nothing -> pure Nothing

        createTableName <- o .: "table"
        createTableDefinition <- o .: "definition"
        createTableExtra <- o .: "extra"
        pure CreateTable{..}

    parseJSON v = fail $ unwords
        [ "don't know how to parse as CreateTable:"
        , show v
        ]

instance ConstrainSNames FromJSON r a => FromJSON (AlterTable r a) where
    parseJSON (JSON.Object o) = o .: "tag" >>= \case
        JSON.String "AlterTableRenameTable" -> do
            info <- o .: "info"
            from <- o .: "from"
            to <- o .: "to"
            pure $ AlterTableRenameTable info from to
        JSON.String "AlterTableRenameColumn" -> do
            info <- o .: "info"
            table <- o .: "table"
            from <- o .: "from"
            to <- o .: "to"
            pure $ AlterTableRenameColumn info table from to
        JSON.String "AlterTableAddColumns" -> do
            info <- o .: "info"
            table <- o .: "table"
            columns <- o .: "columns" >>= \case
                [] -> fail "expected at least one column in column list for AlterTableAddColumns"
                (c:cs) -> pure (c:|cs)
            pure $ AlterTableAddColumns info table columns
        _ -> fail "unrecognized tag on AlterTable object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as AlterTable:"
        , show v
        ]

instance ConstrainSNames FromJSON r a => FromJSON (DropTable r a) where
    parseJSON (JSON.Object o) = do
        JSON.String "DropTable" <- o .: "tag"
        dropTableInfo <- o .: "info"
        dropTableIfExists <- o .:? "ifexists" >>= \case
            Just o' -> do
                JSON.Bool True <- o' .: "value"
                o' .: "info"
            Nothing -> pure Nothing

        dropTableNames <- NE.fromList <$> o .: "tables"
        pure $ DropTable{..}

    parseJSON v = fail $ unwords
        [ "don't know how to parse as DropTable:"
        , show v
        ]

instance ConstrainSNames FromJSON r a => FromJSON (CreateView r a) where
    parseJSON (JSON.Object o) = do
        JSON.String "CreateView" <- o .: "tag"
        createViewInfo <- o .: "info"
        createViewPersistence <- o .: "persistence"
        createViewIfNotExists <- o .:? "ifnotexists" >>= \case
            Just o' -> do
                JSON.Bool True <- o' .: "value"
                o' .: "info"
            Nothing -> pure Nothing

        createViewColumns <- o .:? "columns" >>= \case
            Just [] -> fail "expected at least one column in column list for CreateView (or no column list)"
            Just (c:cs) -> pure $ Just (c:|cs)
            Nothing -> pure Nothing

        createViewName <- o .: "table"
        createViewQuery <- o .: "query"
        pure $ CreateView{..}

    parseJSON v = fail $ unwords
        [ "don't know how to parse as CreateView:"
        , show v
        ]

instance ConstrainSNames FromJSON r a => FromJSON (DropView r a) where
    parseJSON (JSON.Object o) = do
        JSON.String "DropView" <- o .: "tag"
        dropViewInfo <- o .: "info"
        dropViewIfExists <- o .:? "ifexists" >>= \case
            Just o' -> do
                JSON.Bool True <- o' .: "value"
                o' .: "info"
            Nothing -> pure Nothing

        dropViewName <- o .: "table"
        pure $ DropView{..}

    parseJSON v = fail $ unwords
        [ "don't know how to parse as DropView:"
        , show v
        ]

instance ConstrainSNames FromJSON r a => FromJSON (CreateSchema r a) where

    parseJSON (JSON.Object o) = do
        JSON.String "CreateSchema" <- o .: "tag"
        createSchemaInfo <- o .: "info"
        createSchemaIfNotExists <- o .:? "ifnotexists" >>= \case
            Just o' -> do
                JSON.Bool True <- o' .: "value"
                o' .: "info"
            Nothing -> pure Nothing

        createSchemaName <- o .: "schema"
        pure $ CreateSchema{..}

    parseJSON v = fail $ unwords
        [ "don't know how to parse as CreateSchema:"
        , show v
        ]

instance FromJSON a => FromJSON (Grant a) where

    parseJSON (JSON.Object o) = do
        JSON.String "Grant" <- o .: "tag"
        info <- o .: "info"
        pure $ Grant info

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Grant:"
        , show v
        ]

instance FromJSON a => FromJSON (Revoke a) where

    parseJSON (JSON.Object o) = do
        JSON.String "Revoke" <- o .: "tag"
        info <- o .: "info"
        pure $ Revoke info

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Revoke:"
        , show v
        ]

instance FromJSON a => FromJSON (InsertBehavior a) where

    parseJSON (JSON.Object o) = o .: "tag" >>= \case
        JSON.String "Append" -> do
            info <- o .: "info"
            pure $ InsertAppend info

        JSON.String "Overwrite" -> do
            info <- o .: "info"
            pure $ InsertOverwrite info

        JSON.String "AppendPartition" -> do
            info <- o .: "info"
            partition <- o .: "partition"
            pure $ InsertAppendPartition info partition

        JSON.String "OverwritePartition" -> do
            info <- o .: "info"
            partition <- o .: "partition"
            pure $ InsertOverwritePartition info partition

        _ -> fail "unrecognized tag on InsertBehavior object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as InsertBehavior:"
        , show v
        ]

instance ConstrainSAll FromJSON d r a => FromJSON (TableDefinition d r a) where

    parseJSON (JSON.Object o) = o .: "tag" >>= \case
        JSON.String "TableColumns" -> do
            info <- o .: "info"
            o .: "columns" >>= \case
                [] -> fail "expected at least one column in TableColumns object"
                (c:cs) -> pure $ TableColumns info (c:|cs)

        JSON.String "TableLike" -> do
            info <- o .: "info"
            table <- o .: "table"
            pure $ TableLike info table

        JSON.String "TableAs" -> do
            info <- o .: "info"
            columns <- o .: "columns" >>= \case
                Just [] -> fail "expected at least one column in column list for TableAs (or no column list)"
                Just (c:cs) -> pure $ Just (c:|cs)
                Nothing -> pure Nothing
            query <- o .: "query"
            pure $ TableAs info columns query

        JSON.String "TableNoColumnInfo" -> do
            info <- o .: "info"
            pure $ TableNoColumnInfo info

        _ -> fail "unrecognized tag on table definition object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as TableDefinition:"
        , show v
        ]


instance ConstrainSAll FromJSON d r a => FromJSON (ColumnOrConstraint d r a) where

    parseJSON (JSON.Object o) = o .: "tag" >>= \case
        JSON.String "ColumnDefinition" -> ColumnOrConstraintColumn <$> parseJSON (JSON.Object o)
        JSON.String "ConstraintDefinition" -> ColumnOrConstraintConstraint <$> parseJSON (JSON.Object o)
        _ -> fail "unrecognized tag on column or constraint object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as ColumnOrConstraint:"
        , show v
        ]

instance ConstrainSAll FromJSON d r a => FromJSON (ColumnDefinition d r a) where

    parseJSON (JSON.Object o) = do
        JSON.String "ColumnDefinition" <- o .: "tag"
        columnDefinitionInfo <- o .: "info"
        columnDefinitionName <- o .: "name"
        columnDefinitionType <- o .: "type"
        columnDefinitionNull <- o .: "nullable"
        columnDefinitionDefault <- o .: "default"
        columnDefinitionExtra <- o .: "extra"
        pure ColumnDefinition{..}

    parseJSON v = fail $ unwords
        [ "don't know how to parse as ColumnDefinition:"
        , show v
        ]

instance FromJSON a => FromJSON (NullConstraint a) where
    parseJSON (JSON.Object o) = do
        info <- o .: "info"
        o .: "tag" >>= \case
            JSON.String "Nullable" -> pure $ Nullable info
            JSON.String "NotNull" -> pure $ NotNull info
            _ -> fail "unrecognized tag on null constraint object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as NullConstraint:"
        , show v
        ]

instance FromJSON a => FromJSON (ConstraintDefinition a) where

    parseJSON (JSON.Object o) = do
        JSON.String "ConstraintDefinition" <- o .: "tag"
        constraintDefinitionInfo <- o .: "info"
        pure ConstraintDefinition{..}

    parseJSON v = fail $ unwords
        [ "don't know how to parse as ConstraintDefinition:"
        , show v
        ]

instance ConstrainSNames FromJSON r a => FromJSON (InsertValues r a) where
    parseJSON (JSON.Object o) = o .: "tag" >>= \case
        JSON.String "InsertExprValues" -> do
            info <- o .: "info"
            values <- o .: "values"
            let fromList xs = if null xs
                              then fail "empty list of values for row in insert statement"
                              else NE.fromList xs
                rows = map fromList values
            maybe (fail "empty list of rows for insert statement")
                (pure . InsertExprValues info) $ nonEmpty rows

        JSON.String "InsertSelectValues" -> InsertSelectValues <$> o .: "query"
        JSON.String "InsertDefaultValues" -> InsertDefaultValues <$> o .: "info"
        JSON.String "InsertDataFromFile" -> do
            info <- o .: "info"
            path <- TL.encodeUtf8 <$> o .: "path"
                <|> BL.pack <$> o .: "path"
                <|> fail "expected string or array for path"
            pure $ InsertDataFromFile info path

        _ -> fail "unrecognized tag on insert values object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as InsertValues:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (DefaultExpr r a) where
    parseJSON (JSON.Object o) = o .: "tag" >>= \case
        JSON.String "DefaultValue" -> DefaultValue <$> o .: "info"
        JSON.String "ExprValue" -> ExprValue <$> o .: "expr"
        _ -> fail "unrecognized tag on default expression object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as DefaultExpr:"
        , show v
        ]


instance Arbitrary a => Arbitrary (InsertBehavior a) where
    arbitrary = do
        info <- arbitrary
        elements [ InsertOverwrite info
                 , InsertAppend info
                 , InsertOverwritePartition info ()
                 , InsertAppendPartition info ()
                 ]
    shrink (InsertAppend _) = []
    shrink (InsertOverwrite x) = [InsertAppend x]
    shrink (InsertAppendPartition x _) = [InsertAppend x]
    shrink (InsertOverwritePartition x t) =
        [ InsertAppend x
        , InsertOverwrite x
        , InsertAppendPartition x t
        ]
