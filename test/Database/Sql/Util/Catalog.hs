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

{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeOperators #-}


module Database.Sql.Util.Catalog where

import Data.Proxy
import GHC.TypeLits
import           Data.Map (Map)
import qualified Data.Map as M
import qualified Data.HashMap.Strict as HMS
import Test.QuickCheck
import qualified Data.Text.Lazy as TL
import Data.Traversable
import Control.Monad (replicateM)
import Control.Monad.State (execState, modify)

import qualified Database.Sql.Type.TableProps as Props
import Database.Sql.Type.Scope (CatalogMap, SchemaMember (..), SchemaMap)
import Database.Sql.Type.Names ( DatabaseName (..)
                               , QSchemaName (..), UQSchemaName
                               , QTableName (..), FQTableName, UQTableName
                               , RColumnRef (..), QColumnName(..), UQColumnName
                               , No (..)
                               , mkNormalSchema)
import Database.Sql.Util.Eval (RecordSet (..))
import Database.Sql.Util.Eval.Concrete (SqlValue (..), Concrete)


data Column (name :: Symbol) (sqltype :: *) = Column SqlValue deriving Show
data Table (name :: Symbol) (columns :: [*]) = Table (UQTableName ()) [UQColumnName ()] [[SqlValue]] deriving Show
data Schema (name :: Symbol) (tables :: [*])
data Database (name :: Symbol) (schemas :: [*])


mkColumnName :: KnownSymbol name => proxy name -> UQColumnName ()
mkColumnName = QColumnName () None . TL.pack . symbolVal

mkTableName :: KnownSymbol name => proxy name -> UQTableName ()
mkTableName = QTableName () None . TL.pack . symbolVal

mkSchemaName :: KnownSymbol name => proxy name -> UQSchemaName ()
mkSchemaName p = mkNormalSchema (TL.pack $ symbolVal p) ()

mkDatabaseName :: KnownSymbol name => proxy name -> DatabaseName ()
mkDatabaseName = DatabaseName () . TL.pack . symbolVal

class MkColumn a where
    mkColumn :: proxy a -> UQColumnName ()

instance KnownSymbol name => MkColumn (Column name sqltype) where
    mkColumn _ = mkColumnName (Proxy :: Proxy name)

class MkTable a where
    mkTable :: proxy a -> (UQTableName (), [UQColumnName ()])

instance (MkColumn column, MkTable (Table name columns)) => MkTable (Table (name :: Symbol) (column ': columns)) where
    mkTable _ =
        let (name, columns) = mkTable (Proxy :: Proxy (Table name columns))
         in (name, mkColumn (Proxy :: Proxy column) : columns)

instance (KnownSymbol name) => MkTable (Table (name :: Symbol) '[]) where
    mkTable _ = (mkTableName (Proxy :: Proxy name), [])

class MkSchema a where
    mkSchema :: proxy a -> (UQSchemaName (), HMS.HashMap (UQTableName ()) SchemaMember)

instance (MkTable table, MkSchema (Schema name tables)) => MkSchema (Schema (name :: Symbol) (table ': tables)) where
    mkSchema _ =
        let (tableName, columnsList) = mkTable (Proxy :: Proxy table)
            (schemaName, tables) = mkSchema (Proxy :: Proxy (Schema name tables))
            tableType = Props.Table
            persistence = Props.Persistent
            viewQuery = Nothing
         in (schemaName, HMS.insert tableName SchemaMember{..} tables)

instance (KnownSymbol name) => MkSchema (Schema (name :: Symbol) '[]) where
    mkSchema _ =
        let schemaName = mkSchemaName (Proxy :: Proxy name)
         in (schemaName, HMS.empty)

class MkDatabase a where
    mkDatabase :: proxy a -> (DatabaseName (), HMS.HashMap (UQSchemaName ()) SchemaMap)

instance (MkSchema schema, MkDatabase (Database name schemas)) => MkDatabase (Database (name :: Symbol) (schema ': schemas)) where
    mkDatabase _ =
        let (schemaName, tables) = mkSchema (Proxy :: Proxy schema)
            (databaseName, schemas) = mkDatabase (Proxy :: Proxy (Database name schemas))
         in (databaseName, HMS.insert schemaName tables schemas)

instance (KnownSymbol name) => MkDatabase (Database (name :: Symbol) '[]) where
    mkDatabase _ =
        let databaseName = mkDatabaseName (Proxy :: Proxy name)
         in (databaseName, HMS.empty)

class MkCatalog (a :: [*]) where
    mkCatalog :: proxy a -> CatalogMap

instance (MkDatabase database, MkCatalog databases) => MkCatalog (database ': databases) where
    mkCatalog _ =
        let (databaseName, schemas) = mkDatabase (Proxy :: Proxy database)
         in HMS.insert databaseName schemas $ mkCatalog (Proxy :: Proxy databases)

instance MkCatalog '[] where
    mkCatalog _ = HMS.empty

data SqlType = SqlType deriving (Eq, Show)
data Nullable sqltype = Nullable sqltype deriving Eq

data ConcreteDb (catalog :: [*]) = ConcreteDb (Map (FQTableName ()) (RecordSet Concrete)) deriving (Show, Eq)
data SchemaList (schemas :: [*]) = SchemaList [(UQSchemaName (), [(UQTableName (), [UQColumnName ()], [[SqlValue]])])] deriving Show
data TableList (tables :: [*]) = TableList [(UQTableName (), [UQColumnName ()], [[SqlValue]])] deriving Show
data Record (row :: [*]) = Record [SqlValue]

instance Arbitrary (ConcreteDb '[]) where
    arbitrary = pure $ ConcreteDb M.empty
    shrink _ = []

instance (KnownSymbol name, Arbitrary (ConcreteDb databases), Arbitrary (SchemaList schemas)) => Arbitrary (ConcreteDb (Database name schemas ': databases)) where
    arbitrary = do
        let databaseName = mkDatabaseName (Proxy :: Proxy name)
        ConcreteDb concreteDb <- arbitrary :: Gen (ConcreteDb databases)
        SchemaList scms <- arbitrary :: Gen (SchemaList schemas)

        pure $ ConcreteDb $ (`execState` concreteDb) $ forM scms $ \ (uqsn, tbls) -> forM tbls $ \ (uqtn, columns, rows) -> do
            let fqsn = uqsn { schemaNameDatabase = pure databaseName }
                fqtn = uqtn { tableNameSchema = pure fqsn }
            modify $ M.insert fqtn $ RecordSet (map (\ uqcn -> RColumnRef $ uqcn { columnNameTable = pure fqtn }) columns) rows

    shrink (ConcreteDb concreteDb) = M.toList concreteDb >>= \ (table, RecordSet cs rs) ->  do
        set <- RecordSet cs <$> shrinkList (const []) rs
        pure $ ConcreteDb $ M.insert table set concreteDb

instance Arbitrary (SchemaList '[]) where
    arbitrary = pure $ SchemaList []
    shrink _ = []

instance (KnownSymbol name, Arbitrary (SchemaList schemas), Arbitrary (TableList tables)) => Arbitrary (SchemaList (Schema name tables ': schemas)) where
    arbitrary = do
        SchemaList scms <- arbitrary :: Gen (SchemaList schemas)
        let name = mkSchemaName (Proxy :: Proxy name)
        TableList tbls <- arbitrary :: Gen (TableList tables)
        pure $ SchemaList $ (name, tbls) : scms

instance Arbitrary (TableList '[]) where
    arbitrary = pure $ TableList []
    shrink _ = []

instance (Arbitrary (Table name columns), Arbitrary (TableList tables)) => Arbitrary (TableList (Table name columns ': tables)) where
    arbitrary = do
        TableList tbls <- arbitrary :: Gen (TableList tables)
        Table name columns rows <- arbitrary :: Gen (Table name columns)
        pure $ TableList $ (name, columns, rows) : tbls

class MkColumns (a :: [*]) where
    mkColumns :: proxy a -> [(UQColumnName (), [SqlType])]

instance MkColumns '[] where
    mkColumns _ = []

instance (KnownSymbol name, MkColumns columns) => MkColumns (Column name types ': columns) where
    mkColumns _ =
        let columns = mkColumns (Proxy :: Proxy columns)
            columnName = mkColumnName (Proxy :: Proxy name)
            types = [] -- MkTypes (Proxy :: Proxy name)
         in (columnName, types) : columns
        

instance (KnownSymbol name, MkColumns columns, Arbitrary (Record columns)) => Arbitrary (Table name columns) where
    arbitrary = do
        let tableName = mkTableName (Proxy :: Proxy name)
            columns = mkColumns (Proxy :: Proxy columns)
        size <- arbitrary
        rows <- replicateM size $ do
            Record row <- arbitrary :: Gen (Record columns)
            pure row
        pure $ Table tableName (map fst columns) rows


instance Arbitrary (Record '[]) where
    arbitrary = pure $ Record []
    shrink _ = []

instance (Arbitrary (Column name sqltype), Arbitrary (Record columns)) => Arbitrary (Record (Column name sqltype ': columns)) where
    arbitrary = do
        Record columns <- arbitrary :: Gen (Record columns)
        Column column <- arbitrary :: Gen (Column name sqltype)
        pure $ Record $ column : columns


instance Arbitrary (Column name SqlType) where
    arbitrary = Column . SqlInt <$> arbitrary

instance Arbitrary (Column name sqltype) => Arbitrary (Column name (Nullable sqltype)) where
    arbitrary = oneof
        [ do
            Column value <- arbitrary :: Gen (Column name sqltype)
            pure $ Column value
        , pure $ Column SqlNull
        ]
