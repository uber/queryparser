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
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}

module Database.Sql.Type.Scope where

import Database.Sql.Type.Names
import Database.Sql.Type.TableProps
import Database.Sql.Type.Unused
import Database.Sql.Type.Query

import Control.Monad.Identity
import Control.Monad.Reader
import Control.Monad.State
import Control.Monad.Except
import Control.Monad.Writer

import           Data.Aeson
import qualified Data.HashMap.Strict as HMS
import           Data.HashMap.Strict (HashMap)

import Data.List (subsequences)
import Data.Hashable (Hashable)

import Test.QuickCheck

import Data.Data (Data)
import GHC.Generics (Generic)


-- | A ColumnSet records the table-bindings (if any) of columns.
--
-- Can be used to represent columns that are in ambient scope,
-- which can be referenced, based on arcane and dialect specific rules.
-- The fst component will be Nothing for collections of column
-- aliases bound in a containing select statement (which thus have no
-- corresponding Tablish), or for subqueries/lateral views with no table alias.
--
-- Can also be used to represent "what stars expand into".

type ColumnSet a = [(Maybe (RTableRef a), [RColumnRef a])]


data Bindings a = Bindings
    { boundCTEs :: [(TableAlias a, [RColumnRef a])]
    , boundColumns :: ColumnSet a
    }

emptyBindings :: Bindings a
emptyBindings = Bindings [] []

data SelectScope a = SelectScope
    { bindForHaving :: forall r m . MonadReader (ResolverInfo a) m => m r -> m r
    , bindForWhere :: forall r m . MonadReader (ResolverInfo a) m => m r -> m r
    , bindForOrder :: forall r m . MonadReader (ResolverInfo a) m => m r -> m r
    , bindForGroup :: forall r m . MonadReader (ResolverInfo a) m => m r -> m r
    , bindForNamedWindow :: forall r m . MonadReader (ResolverInfo a) m => m r -> m r
    }

type FromColumns a = ColumnSet a
type SelectionAliases a = [RColumnRef a]

data ResolverInfo a = ResolverInfo
    { catalog :: Catalog
    , onCTECollision :: forall x . (x -> x) -> (x -> x)
    , bindings :: Bindings a
    , lambdaScope :: [[LambdaParam a]]
    , selectScope :: FromColumns a -> SelectionAliases a -> SelectScope a
    , lcolumnsAreVisibleInLateralViews :: Bool
    }

mapBindings :: (Bindings a -> Bindings a) -> ResolverInfo a -> ResolverInfo a
mapBindings f ResolverInfo{..} = ResolverInfo{bindings = f bindings, ..}


bindColumns :: MonadReader (ResolverInfo a) m => ColumnSet a -> m r -> m r
bindColumns columns = local (mapBindings $ \ Bindings{..} -> Bindings{boundColumns = columns ++ boundColumns, ..})

bindLambdaParams :: MonadReader (ResolverInfo a) m => [LambdaParam a] -> m r -> m r
bindLambdaParams params = local (\ResolverInfo{..} -> ResolverInfo{lambdaScope = params:lambdaScope, ..})

bindFromColumns :: MonadReader (ResolverInfo a) m => FromColumns a -> m r -> m r
bindFromColumns = bindColumns

bindAliasedColumns :: MonadReader (ResolverInfo a) m => SelectionAliases a -> m r -> m r
bindAliasedColumns selectionAliases = bindColumns [(Nothing, selectionAliases)]

bindBothColumns :: MonadReader (ResolverInfo a) m => FromColumns a -> SelectionAliases a -> m r -> m r
bindBothColumns fromColumns selectionAliases = bindColumns $ (Nothing, onlyNewAliases selectionAliases) : fromColumns
  where
    onlyNewAliases = filter $ \case
        alias@(RColumnAlias _) -> not $ inFromColumns alias
        RColumnRef _ -> False

    inFromColumns alias =
        let alias' = void alias
            cols = map void (snd =<< fromColumns)
         in alias' `elem` cols

data RawNames
deriving instance Data RawNames
instance Resolution RawNames where
    type TableRef RawNames = OQTableName
    type TableName RawNames = OQTableName
    type CreateTableName RawNames = OQTableName
    type DropTableName RawNames = OQTableName
    type SchemaName RawNames = OQSchemaName
    type CreateSchemaName RawNames = OQSchemaName
    type ColumnRef RawNames = OQColumnName
    type NaturalColumns RawNames = Unused
    type UsingColumn RawNames = UQColumnName
    type StarReferents RawNames = Unused
    type PositionExpr RawNames = Unused
    type ComposedQueryColumns RawNames = Unused

data ResolvedNames
deriving instance Data ResolvedNames
newtype StarColumnNames a = StarColumnNames [RColumnRef a]
    deriving (Generic, Data, Eq, Ord, Show, Functor)

newtype ColumnAliasList a = ColumnAliasList [ColumnAlias a]
    deriving (Generic, Data, Eq, Ord, Show, Functor)

instance Resolution ResolvedNames where
    type TableRef ResolvedNames = RTableRef
    type TableName ResolvedNames = RTableName
    type CreateTableName ResolvedNames = RCreateTableName
    type DropTableName ResolvedNames = RDropTableName
    type SchemaName ResolvedNames = FQSchemaName
    type CreateSchemaName ResolvedNames = RCreateSchemaName
    type ColumnRef ResolvedNames = RColumnRef
    type NaturalColumns ResolvedNames = RNaturalColumns
    type UsingColumn ResolvedNames = RUsingColumn
    type StarReferents ResolvedNames = StarColumnNames
    type PositionExpr ResolvedNames = Expr ResolvedNames
    type ComposedQueryColumns ResolvedNames = ColumnAliasList

type Resolver r a =
    StateT Integer  -- column alias generation (counts down from -1, unlike parse phase)
        (ReaderT (ResolverInfo a)
            (CatalogObjectResolver a))
               (r a)


data SchemaMember = SchemaMember
    { tableType :: TableType
    , persistence :: Persistence ()
    , columnsList :: [UQColumnName ()]
    , viewQuery :: Maybe (Query ResolvedNames ())  -- this will always be Nothing for tables
    } deriving (Generic, Data, Eq, Ord, Show)

persistentTable :: [UQColumnName ()] -> SchemaMember
persistentTable cols = SchemaMember Table Persistent cols Nothing


type SchemaMap = HashMap (UQTableName ()) SchemaMember
type DatabaseMap = HashMap (UQSchemaName ()) SchemaMap
type CatalogMap = HashMap (DatabaseName ()) DatabaseMap
type Path = [UQSchemaName ()]
type CurrentDatabase = DatabaseName ()

data Catalog = Catalog
    { catalogResolveSchemaName :: forall a . OQSchemaName a -> CatalogObjectResolver a (FQSchemaName a)
    , catalogResolveTableName :: forall a . OQTableName a -> CatalogObjectResolver a (RTableName a)
    , catalogHasDatabase :: DatabaseName () -> Existence
    , catalogHasSchema :: UQSchemaName () -> Existence
    , catalogHasTable :: UQTableName () -> Existence  -- | nb DoesNotExist does not imply that we can't resolve to this name (defaulting)
    , catalogResolveTableRef :: forall a . [(TableAlias a, [RColumnRef a])] -> OQTableName a -> CatalogObjectResolver a (WithColumns RTableRef a)
    , catalogResolveCreateSchemaName :: forall a . OQSchemaName a -> CatalogObjectResolver a (RCreateSchemaName a)
    , catalogResolveCreateTableName :: forall a . OQTableName a -> CatalogObjectResolver a (RCreateTableName a)
    , catalogResolveColumnName :: forall a . [(Maybe (RTableRef a), [RColumnRef a])] -> OQColumnName a -> CatalogObjectResolver a (RColumnRef a)
    , overCatalogMap :: forall a . (CatalogMap -> (CatalogMap, a)) -> (Catalog, a)
    , catalogMap :: !CatalogMap
    , catalogWithPath :: Path -> Catalog
    , catalogWithDatabase :: CurrentDatabase -> Catalog
    }

instance Eq Catalog where
    x == y = catalogMap x == catalogMap y

instance Show Catalog where
    show = show . catalogMap


-- returned by methods in Catalog
type CatalogObjectResolver a =
    (ExceptT (ResolutionError a)  -- error
        (Writer [Either (ResolutionError a) (ResolutionSuccess a)])) -- warnings and successes

data ResolutionError a
    = MissingDatabase (DatabaseName a)
    | MissingSchema (OQSchemaName a)
    | MissingTable (OQTableName a)
    | AmbiguousTable (OQTableName a)
    | MissingColumn (OQColumnName a)
    | AmbiguousColumn (OQColumnName a)
    | UnintroducedTable (OQTableName a)
    | UnexpectedTable (FQTableName a)
    | UnexpectedSchema (FQSchemaName a)
    | BadPositionalReference a Int
    | DeleteFromView (FQTableName a)
    | MissingFunctionExprForLateralView
        deriving (Eq, Show, Functor)

data ResolutionSuccess a
    = TableNameResolved (OQTableName a) (RTableName a)
    | TableNameDefaulted (OQTableName a) (RTableName a)
    | CreateTableNameResolved (OQTableName a) (RCreateTableName a)
    | CreateSchemaNameResolved (OQSchemaName a) (RCreateSchemaName a)
    | TableRefResolved (OQTableName a) (RTableRef a)
    | TableRefDefaulted (OQTableName a) (RTableRef a)
    | ColumnRefResolved (OQColumnName a) (RColumnRef a)
    | ColumnRefDefaulted (OQColumnName a) (RColumnRef a)
        deriving (Eq, Show, Functor)

isGuess :: ResolutionSuccess a -> Bool
isGuess (TableNameResolved _ _) = False
isGuess (TableNameDefaulted _ _) = True
isGuess (CreateTableNameResolved _ _) = False
isGuess (CreateSchemaNameResolved _ _) = False
isGuess (TableRefResolved _ _) = False
isGuess (TableRefDefaulted _ _) = True
isGuess (ColumnRefResolved _ _) = False
isGuess (ColumnRefDefaulted _ _) = True

isCertain :: ResolutionSuccess a -> Bool
isCertain = not . isGuess


data WithColumns r a = WithColumns
    { withColumnsValue :: r a
    , withColumnsColumns :: ColumnSet a
    }

data WithColumnsAndOrders r a = WithColumnsAndOrders (r a) (ColumnSet a) [Order ResolvedNames a]

-- R for "resolved"
data RTableRef a
    = RTableRef (FQTableName a) SchemaMember
    | RTableAlias (TableAlias a) [RColumnRef a]
      deriving (Generic, Data, Show, Eq, Ord, Functor, Foldable, Traversable)

getColumnList :: RTableRef a -> [RColumnRef a]
getColumnList (RTableRef fqtn SchemaMember{..}) = 
    let fqcns = map (\uqcn -> uqcn { columnNameInfo = tableNameInfo fqtn, columnNameTable = Identity fqtn }) columnsList
     in map RColumnRef fqcns
getColumnList (RTableAlias _ cols) = cols

resolvedTableHasName :: QTableName f a -> RTableRef a -> Bool
resolvedTableHasName (QTableName _ _ name) (RTableAlias (TableAlias _ name' _) _) = name' == name
resolvedTableHasName (QTableName _ _ name) (RTableRef (QTableName _ _ name') _) = name' == name

resolvedTableHasSchema :: QSchemaName f a -> RTableRef a -> Bool
resolvedTableHasSchema _ (RTableAlias _ _) = False
resolvedTableHasSchema (QSchemaName _ _ name schemaType) (RTableRef (QTableName _ (Identity (QSchemaName _ _ name' schemaType')) _) _) =
    name == name' && schemaType == schemaType'

resolvedTableHasDatabase :: DatabaseName a -> RTableRef a -> Bool
resolvedTableHasDatabase _ (RTableAlias _ _) = False
resolvedTableHasDatabase (DatabaseName _ name) (RTableRef (QTableName _ (Identity (QSchemaName _ (Identity (DatabaseName _ name')) _ _)) _) _) = name' == name


data RTableName a = RTableName (FQTableName a) SchemaMember
    deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

rTableNameToRTableRef :: RTableName a -> RTableRef a
rTableNameToRTableRef (RTableName fqtn sm) = RTableRef fqtn sm

data RDropTableName a
    = RDropExistingTableName (FQTableName a) SchemaMember
    | RDropMissingTableName (OQTableName a)
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data RCreateTableName a = RCreateTableName (FQTableName a) Existence
                          deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data RCreateSchemaName a = RCreateSchemaName (FQSchemaName a) Existence
                           deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)


instance Arbitrary SchemaMember where
    arbitrary = do
        tableType <- arbitrary
        persistence <- arbitrary
        columnsList <- arbitrary
        viewQuery <- pure Nothing  -- TODO holding off til we have arbitrary queries
        pure SchemaMember{..}
    shrink (SchemaMember type_ persistence cols _) =
        [ SchemaMember type_' persistence' cols' Nothing |  -- TODO same
          (type_', persistence', cols') <- shrink (type_, persistence, cols) ]

shrinkHashMap :: (Eq k, Hashable k) => forall v.  HashMap k v -> [HashMap k v]
shrinkHashMap = map HMS.fromList . subsequences . HMS.toList

instance Arbitrary SchemaMap where
    arbitrary = HMS.fromList <$> arbitrary
    shrink = shrinkHashMap

instance Arbitrary DatabaseMap where
    arbitrary = HMS.fromList <$> arbitrary
    shrink = shrinkHashMap

instance Arbitrary CatalogMap where
    arbitrary = HMS.fromList <$> arbitrary
    shrink = shrinkHashMap

instance ToJSON a => ToJSON (RTableRef a) where
    toJSON (RTableRef fqtn _) = object
        [ "tag" .= String "RTableRef"
        , "fqtn" .= fqtn
        ]
    toJSON (RTableAlias alias _) = object
        [ "tag" .= String "RTableAlias"
        , "alias" .= alias
        ]

instance ToJSON a => ToJSON (RTableName a) where
    toJSON (RTableName fqtn _) = object
        [ "tag" .= String "RTableName"
        , "fqtn" .= fqtn
        ]

instance ToJSON a => ToJSON (RDropTableName a) where
    toJSON (RDropExistingTableName fqtn _) = object
        [ "tag" .= String "RDropExistingTableName"
        , "fqtn" .= fqtn
        ]
    toJSON (RDropMissingTableName oqtn) = object
        [ "tag" .= String "RDropMissingTableName"
        , "oqtn" .= oqtn
        ]

instance ToJSON a => ToJSON (RCreateTableName a) where
    toJSON (RCreateTableName fqtn existence) = object
        [ "tag" .= String "RCreateTableName"
        , "fqtn" .= fqtn
        , "existence" .= existence
        ]

instance ToJSON a => ToJSON (RCreateSchemaName a) where
    toJSON (RCreateSchemaName fqsn existence) = object
        [ "tag" .= String "RCreateSchemaName"
        , "fqsn" .= fqsn
        , "existence" .= existence
        ]

instance ToJSON a => ToJSON (StarColumnNames a) where
    toJSON (StarColumnNames cols) = object
        [ "tag" .= String "StarColumnNames"
        , "cols" .= cols
        ]

instance ToJSON a => ToJSON (ColumnAliasList a) where
    toJSON (ColumnAliasList cols) = object
        [ "tag" .= String "ColumnAliasList"
        , "cols" .= cols
        ]
