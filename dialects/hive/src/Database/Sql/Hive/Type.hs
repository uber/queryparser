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
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE RecordWildCards #-}

module Database.Sql.Hive.Type where

import Database.Sql.Type hiding (insertValues, insertInfo)
import Database.Sql.Position
import Database.Sql.Info
import Database.Sql.Util.Columns
import Database.Sql.Util.Joins
import Database.Sql.Util.Lineage.Table
import Database.Sql.Util.Lineage.ColumnPlus
import Database.Sql.Util.Scope
import Database.Sql.Util.Schema
import Database.Sql.Util.Tables

import Control.Monad.Identity
import Control.Monad.Writer (tell)

import Data.Aeson
import qualified Data.Map  as M
import qualified Data.Set  as S
import Data.Text.Lazy (Text)
import qualified Data.Text.Lazy.Encoding as TL
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BL
import Data.Proxy (Proxy (..))

import GHC.Generics (Generic)
import Data.Data (Data)


data Hive

deriving instance Data Hive

dialectProxy :: Proxy Hive
dialectProxy = Proxy

instance Dialect Hive where
    type DialectCreateTableExtra Hive r = HiveCreateTableExtra r

    shouldCTEsShadowTables _ = False

    -- Nothing to resolve in the table extra
    resolveCreateTableExtra _ HiveCreateTableExtra{..} = pure HiveCreateTableExtra{..}

    getSelectScope _ fromColumns selectionAliases = SelectScope
        { bindForWhere = bindFromColumns fromColumns
        , bindForGroup = bindFromColumns fromColumns
        , bindForHaving = bindBothColumns fromColumns selectionAliases
        , bindForOrder = bindAliasedColumns selectionAliases
        , bindForNamedWindow = bindFromColumns fromColumns
        }

    areLcolumnsVisibleInLateralViews _ = False


data HiveStatement r a = HiveStandardSqlStatement (Statement Hive r a)
                         | HiveUseStmt (Use a)
                         | HiveAnalyzeStmt (Analyze r a)
                         | HiveInsertDirectoryStmt (InsertDirectory r a)
                         | HiveTruncatePartitionStmt (TruncatePartition r a)
                         | HiveAlterTableSetLocationStmt (AlterTableSetLocation r a)
                         | HiveAlterPartitionSetLocationStmt (AlterPartitionSetLocation r a)
                         | HiveSetPropertyStmt (SetProperty a)
                         | HiveUnhandledStatement a

deriving instance (ConstrainSNames Data r a, Data r) => Data (HiveStatement r a)
deriving instance Generic (HiveStatement r a)
deriving instance ConstrainSNames Eq r a => Eq (HiveStatement r a)
deriving instance ConstrainSNames Show r a => Show (HiveStatement r a)
deriving instance ConstrainSASNames Functor r => Functor (HiveStatement r)
deriving instance ConstrainSASNames Foldable r => Foldable (HiveStatement r)
deriving instance ConstrainSASNames Traversable r => Traversable (HiveStatement r)

data SetProperty a = SetProperty (SetPropertyDetails a)
                     | PrintProperties a Text
                       deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)

data SetPropertyDetails a = SetPropertyDetails
    { setPropertyDetailsInfo :: a
    , setPropertyDetailsName :: Text
    , setPropertyDetailsValue :: Text
    } deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)

data HiveCreateTableExtra r a = HiveCreateTableExtra
    { hiveCreateTableExtraInfo :: a
    , hiveCreateTableExtraTableProperties :: Maybe (HiveMetadataProperties a)
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (HiveCreateTableExtra r a)
deriving instance Generic (HiveCreateTableExtra r a)
deriving instance ConstrainSNames Eq r a => Eq (HiveCreateTableExtra r a)
deriving instance ConstrainSNames Show r a => Show (HiveCreateTableExtra r a)
deriving instance ConstrainSASNames Functor r => Functor (HiveCreateTableExtra r)
deriving instance ConstrainSASNames Foldable r => Foldable (HiveCreateTableExtra r)
deriving instance ConstrainSASNames Traversable r => Traversable (HiveCreateTableExtra r)

data HiveMetadataProperties a = HiveMetadataProperties
    { hiveMetadataPropertiesInfo :: a
    , hiveMetadataPropertiesProperties :: [HiveMetadataProperty a]
    } deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)

data HiveMetadataProperty a = HiveMetadataProperty
    { hiveMetadataPropertyInfo :: a
    , hiveMetadataPropertyKey :: ByteString
    , hiveMetadataPropertyValue :: ByteString
    } deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)

-- Important terminology note:
-- Hive "databases" are schemas.
-- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Create/Drop/Alter/UseDatabase
data Use a = UseDatabase (UQSchemaName a)
           | UseDefault a
             deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)


data Analyze r a = Analyze
    { analyzeInfo :: a
    , analyzeTable :: TableName r a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (Analyze r a)
deriving instance Generic (Analyze r a)
deriving instance ConstrainSNames Eq r a => Eq (Analyze r a)
deriving instance ConstrainSNames Show r a => Show (Analyze r a)
deriving instance ConstrainSASNames Functor r => Functor (Analyze r)
deriving instance ConstrainSASNames Foldable r => Foldable (Analyze r)
deriving instance ConstrainSASNames Traversable r => Traversable (Analyze r)


data InsertDirectory r a = InsertDirectory
    { insertDirectoryInfo :: a
    , insertDirectoryLocale :: InsertDirectoryLocale a
    , insertDirectoryPath :: Location a
    , insertDirectoryQuery :: Query r a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (InsertDirectory r a)
deriving instance Generic (InsertDirectory r a)
deriving instance ConstrainSNames Eq r a => Eq (InsertDirectory r a)
deriving instance ConstrainSNames Show r a => Show (InsertDirectory r a)
deriving instance ConstrainSASNames Functor r => Functor (InsertDirectory r)
deriving instance ConstrainSASNames Foldable r => Foldable (InsertDirectory r)
deriving instance ConstrainSASNames Traversable r => Traversable (InsertDirectory r)


data Location a = HDFSPath a ByteString
      deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)


data InsertDirectoryLocale a = InsertDirectoryLocal a | InsertDirectoryHDFS
      deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)

data StaticPartitionSpecItem r a = StaticPartitionSpecItem a (ColumnRef r a) (Constant a)

deriving instance (ConstrainSNames Data r a, Data r) => Data (StaticPartitionSpecItem r a)
deriving instance Generic (StaticPartitionSpecItem r a)
deriving instance ConstrainSNames Eq r a => Eq (StaticPartitionSpecItem r a)
deriving instance ConstrainSNames Show r a => Show (StaticPartitionSpecItem r a)
deriving instance ConstrainSASNames Functor r => Functor (StaticPartitionSpecItem r)
deriving instance ConstrainSASNames Foldable r => Foldable (StaticPartitionSpecItem r)
deriving instance ConstrainSASNames Traversable r => Traversable (StaticPartitionSpecItem r)

data DynamicPartitionSpecItem r a = DynamicPartitionSpecItem a (ColumnRef r a)

deriving instance (ConstrainSNames Data r a, Data r) => Data (DynamicPartitionSpecItem r a)
deriving instance Generic (DynamicPartitionSpecItem r a)
deriving instance ConstrainSNames Eq r a => Eq (DynamicPartitionSpecItem r a)
deriving instance ConstrainSNames Show r a => Show (DynamicPartitionSpecItem r a)
deriving instance ConstrainSASNames Functor r => Functor (DynamicPartitionSpecItem r)
deriving instance ConstrainSASNames Foldable r => Foldable (DynamicPartitionSpecItem r)
deriving instance ConstrainSASNames Traversable r => Traversable (DynamicPartitionSpecItem r)

-- Note: We don't track anything at the partition level (for now), so
-- we discard the info on which particular partition is being truncated.
data TruncatePartition r a = TruncatePartition
    { truncatePartitionInfo :: a
    , truncatePartitionTruncate :: Truncate r a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (TruncatePartition r a)
deriving instance Generic (TruncatePartition r a)
deriving instance ConstrainSNames Eq r a => Eq (TruncatePartition r a)
deriving instance ConstrainSNames Show r a => Show (TruncatePartition r a)
deriving instance ConstrainSASNames Functor r => Functor (TruncatePartition r)
deriving instance ConstrainSASNames Foldable r => Foldable (TruncatePartition r)
deriving instance ConstrainSASNames Traversable r => Traversable (TruncatePartition r)

data AlterTableSetLocation r a = AlterTableSetLocation
    { alterTableSetLocationInfo :: a
    , alterTableSetLocationTable :: TableName r a
    , alterTableSetLocationLocation :: Location a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (AlterTableSetLocation r a)
deriving instance ConstrainSNames Eq r a => Eq (AlterTableSetLocation r a)
deriving instance ConstrainSNames Show r a => Show (AlterTableSetLocation r a)
deriving instance ConstrainSASNames Functor r => Functor (AlterTableSetLocation r)
deriving instance ConstrainSASNames Foldable r => Foldable (AlterTableSetLocation r)
deriving instance ConstrainSASNames Traversable r => Traversable (AlterTableSetLocation r)


data AlterPartitionSetLocation r a = AlterPartitionSetLocation
    { alterPartitionSetLocationInfo :: a
    , alterPartitionSetLocationTable :: TableName r a
    , alterPartitionSetLocationPartition :: [StaticPartitionSpecItem r a]
    , alterPartitionSetLocationLocation :: Location a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (AlterPartitionSetLocation r a)
deriving instance ConstrainSNames Eq r a => Eq (AlterPartitionSetLocation r a)
deriving instance ConstrainSNames Show r a => Show (AlterPartitionSetLocation r a)
deriving instance ConstrainSASNames Functor r => Functor (AlterPartitionSetLocation r)
deriving instance ConstrainSASNames Foldable r => Foldable (AlterPartitionSetLocation r)
deriving instance ConstrainSASNames Traversable r => Traversable (AlterPartitionSetLocation r)


instance HasJoins (HiveStatement ResolvedNames a) where
    getJoins (HiveStandardSqlStatement stmt) = getJoins stmt
    getJoins (HiveInsertDirectoryStmt stmt) =
        getJoins (QueryStmt $ insertDirectoryQuery stmt)
    getJoins (HiveUseStmt _) = S.empty
    getJoins (HiveAnalyzeStmt _) = S.empty
    getJoins (HiveTruncatePartitionStmt _) = S.empty
    getJoins (HiveAlterTableSetLocationStmt _) = S.empty
    getJoins (HiveAlterPartitionSetLocationStmt _) = S.empty
    getJoins (HiveSetPropertyStmt _) = S.empty
    getJoins (HiveUnhandledStatement _) = S.empty

instance HasTableLineage (HiveStatement ResolvedNames a) where
    getTableLineage (HiveStandardSqlStatement stmt) = tableLineage stmt
    getTableLineage (HiveUseStmt _) = M.empty
    getTableLineage (HiveAnalyzeStmt _) = M.empty
    getTableLineage (HiveInsertDirectoryStmt _) = M.empty
    getTableLineage (HiveTruncatePartitionStmt (TruncatePartition _ (Truncate _ (RTableName t _)))) =
        let table = mkFQTN t
         in M.singleton table $ S.singleton table
    getTableLineage (HiveAlterTableSetLocationStmt _) = M.empty
    getTableLineage (HiveAlterPartitionSetLocationStmt _) = M.empty
    getTableLineage (HiveSetPropertyStmt _) = M.empty
    getTableLineage (HiveUnhandledStatement _) = M.empty

instance HasColumnLineage (HiveStatement ResolvedNames Range) where
    getColumnLineage (HiveStandardSqlStatement stmt) = columnLineage stmt
    getColumnLineage (HiveUseStmt _) = returnNothing M.empty
    getColumnLineage (HiveAnalyzeStmt _) = returnNothing M.empty
    getColumnLineage (HiveInsertDirectoryStmt _) = returnNothing M.empty
    getColumnLineage (HiveTruncatePartitionStmt (TruncatePartition _ (Truncate _ (RTableName t SchemaMember{..})))) =
        returnNothing
            $ M.insert (Left $ fqtnToFQTN t) (singleTableSet (getInfo t) $ fqtnToFQTN t)
                $ M.fromList $ map ((\ fqcn -> (Right fqcn, singleColumnSet (getInfo t) fqcn)) . fqcnToFQCN . qualifyColumnName t) columnsList
    getColumnLineage (HiveAlterTableSetLocationStmt _) = returnNothing M.empty
    getColumnLineage (HiveAlterPartitionSetLocationStmt _) = returnNothing M.empty
    getColumnLineage (HiveSetPropertyStmt _) = returnNothing M.empty
    getColumnLineage (HiveUnhandledStatement _) = returnNothing M.empty

resolveHiveStatement :: HiveStatement RawNames a -> Resolver (HiveStatement ResolvedNames) a
resolveHiveStatement (HiveStandardSqlStatement stmt) = HiveStandardSqlStatement <$> resolveStatement stmt
resolveHiveStatement (HiveUseStmt stmt) = pure $ HiveUseStmt stmt
resolveHiveStatement (HiveAnalyzeStmt stmt) = HiveAnalyzeStmt <$> resolveAnalyze stmt
resolveHiveStatement (HiveInsertDirectoryStmt stmt) = HiveInsertDirectoryStmt <$> resolveInsertDirectory stmt
resolveHiveStatement (HiveTruncatePartitionStmt stmt) = HiveTruncatePartitionStmt <$> resolveTruncatePartition stmt
resolveHiveStatement (HiveAlterTableSetLocationStmt stmt) = HiveAlterTableSetLocationStmt <$> resolveAlterTableSetLocation stmt
resolveHiveStatement (HiveAlterPartitionSetLocationStmt stmt) = HiveAlterPartitionSetLocationStmt <$> resolveAlterPartitionSetLocation stmt
resolveHiveStatement (HiveSetPropertyStmt stmt) = pure $ HiveSetPropertyStmt stmt
resolveHiveStatement (HiveUnhandledStatement stmt) = pure $ HiveUnhandledStatement stmt

resolveAnalyze :: Analyze RawNames a -> Resolver (Analyze ResolvedNames) a
resolveAnalyze Analyze{..} = do
    analyzeTable' <- resolveTableName analyzeTable
    pure Analyze
        { analyzeTable = analyzeTable'
        , ..
        }

resolveInsertDirectory :: InsertDirectory RawNames a -> Resolver (InsertDirectory ResolvedNames) a
resolveInsertDirectory InsertDirectory{..} = do
    insertDirectoryQuery' <- resolveQuery insertDirectoryQuery
    pure InsertDirectory
        { insertDirectoryQuery = insertDirectoryQuery'
        , ..
        }

resolveTruncatePartition :: TruncatePartition RawNames a -> Resolver (TruncatePartition ResolvedNames) a
resolveTruncatePartition TruncatePartition{..} = do
    truncatePartitionTruncate' <- resolveTruncate truncatePartitionTruncate
    pure TruncatePartition
        { truncatePartitionTruncate = truncatePartitionTruncate'
        , ..
        }

resolveAlterTableSetLocation :: AlterTableSetLocation RawNames a -> Resolver (AlterTableSetLocation ResolvedNames) a
resolveAlterTableSetLocation AlterTableSetLocation{..} = do
    alterTableSetLocationTable' <- resolveTableName alterTableSetLocationTable
    pure AlterTableSetLocation
        { alterTableSetLocationTable = alterTableSetLocationTable'
        , ..
        }

resolveAlterPartitionSetLocation :: AlterPartitionSetLocation RawNames a -> Resolver (AlterPartitionSetLocation ResolvedNames) a
resolveAlterPartitionSetLocation AlterPartitionSetLocation{..} = do
    alterPartitionSetLocationTable'@(RTableName fqtn table@SchemaMember{..}) <- resolveTableName alterPartitionSetLocationTable
    let tableInfo = tableNameInfo fqtn
        columnSet =
            [ ( Just $ RTableRef fqtn table
              , map (RColumnRef . (const tableInfo <$>) . qualifyColumnName fqtn) columnsList
              )
            ]
    alterPartitionSetLocationPartition' <- bindColumns columnSet
        $ forM alterPartitionSetLocationPartition
            $ \ (StaticPartitionSpecItem info column constant) -> do
                column' <- resolveColumnName column
                pure $ StaticPartitionSpecItem info column' constant
    pure AlterPartitionSetLocation
        { alterPartitionSetLocationTable = alterPartitionSetLocationTable'
        , alterPartitionSetLocationPartition = alterPartitionSetLocationPartition'
        , ..
        }

instance HasSchemaChange (HiveStatement ResolvedNames a) where
    getSchemaChange (HiveStandardSqlStatement stmt) = getSchemaChange stmt
    getSchemaChange (HiveUseStmt (UseDefault _)) = [UsePath [QSchemaName () None "default" NormalSchema]]
    getSchemaChange (HiveUseStmt (UseDatabase schema)) = [UsePath [void schema]]
    getSchemaChange (HiveAnalyzeStmt _) = []
    getSchemaChange (HiveInsertDirectoryStmt _) = []
    getSchemaChange (HiveTruncatePartitionStmt _) = []
    getSchemaChange (HiveAlterTableSetLocationStmt _) = []  -- In fact, it may very well have a schema change but
                                                            -- we can't know without hitting the metastore.
    getSchemaChange (HiveAlterPartitionSetLocationStmt _) = []
    getSchemaChange (HiveSetPropertyStmt _) = []
    getSchemaChange (HiveUnhandledStatement _) = []

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (HiveStatement r a) where
    toJSON (HiveStandardSqlStatement stmt) = toJSON stmt
    toJSON (HiveInsertDirectoryStmt stmt) = object
        [ "tag" .= String "InsertDirectoryStmt"
        , "target" .= stmt
        ]
    toJSON (HiveUseStmt stmt) = object
        [ "tag" .= String "UseStmt"
        , "target" .= stmt
        ]
    toJSON (HiveAnalyzeStmt stmt) = object
        [ "tag" .= String "AnalyzeStmt"
        , "target" .= stmt
        ]
    toJSON (HiveTruncatePartitionStmt truncatePartition) = object
        [ "tag" .= String "HiveTruncatePartitionStmt"
        , "statement" .= truncatePartition
        ]
    toJSON (HiveAlterTableSetLocationStmt alterTableSetLocation) = object
        [ "tag" .= String "HiveAlterTableSetLocationStmt"
        , "statement" .= alterTableSetLocation
        ]
    toJSON (HiveAlterPartitionSetLocationStmt alterPartitionSetLocation) = object
        [ "tag" .= String "HiveAlterPartitionSetLocationStmt"
        , "statement" .= alterPartitionSetLocation
        ]
    toJSON (HiveSetPropertyStmt stmt) = object
        [ "tag" .= String "HiveSetPropertyStmt"
        , "info" .= stmt
        ]
    toJSON (HiveUnhandledStatement info) = object
        [ "tag" .= String "HiveUnhandledStatement"
        , "info" .= info
        ]

typeExample :: ()
typeExample = const () $ toJSON (undefined :: HiveStatement ResolvedNames Range)

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (HiveCreateTableExtra r a) where
    toJSON HiveCreateTableExtra{..} = object
        [ "tag" .= String "HiveCreateTableExtra"
        , "info" .= hiveCreateTableExtraInfo
        , "properties" .= hiveCreateTableExtraTableProperties
        ]

instance ToJSON a => ToJSON (HiveMetadataProperties a) where
    toJSON HiveMetadataProperties{..} = object
        [ "tag" .= String "HiveMetadataProperties"
        , "info" .= hiveMetadataPropertiesInfo
        , "properties" .= hiveMetadataPropertiesProperties
        ]

instance ToJSON a => ToJSON (HiveMetadataProperty a) where
    toJSON HiveMetadataProperty{..} = object
        [ "tag" .= String "MetadataProperty"
        , "info" .= hiveMetadataPropertyInfo
        , "key" .= bsToJSON hiveMetadataPropertyKey
        , "value" .= bsToJSON hiveMetadataPropertyValue
        ]

bsToJSON :: ByteString -> Value
bsToJSON bs = case TL.decodeUtf8' bs of
    Left _ -> toJSON $ BL.unpack bs
    Right str -> toJSON str

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (InsertDirectory r a) where
    toJSON stmt = object
        [ "tag" .= String "InsertDirectory"
        , "info" .= insertDirectoryInfo stmt
        , "directory" .= insertDirectoryPath stmt
        , "locale" .= insertDirectoryLocale stmt
        , "values" .= insertDirectoryQuery stmt
        ]

instance ToJSON a => ToJSON (Location a) where
    toJSON (HDFSPath _ p) = bsToJSON p

instance ToJSON a => ToJSON (InsertDirectoryLocale a) where
    toJSON (InsertDirectoryLocal _) = String "Local"
    toJSON InsertDirectoryHDFS = String "HDFS"

instance ToJSON a => ToJSON (Use a) where
    toJSON (UseDatabase dbn) = toJSON dbn
    toJSON (UseDefault _) = String "Default"

instance ToJSON a => ToJSON (SetProperty a) where
    toJSON (SetProperty details) = toJSON details
    toJSON (PrintProperties info t) = object
        [ "tag" .= String "Set"
        , "info" .= info
        , "property" .= t
        ]

instance ToJSON a => ToJSON (SetPropertyDetails a) where
    toJSON d@SetPropertyDetails{} = object
        [ "tag" .= String "Set"
        , "info" .= setPropertyDetailsInfo d
        , "property" .= setPropertyDetailsName d
        , "value" .= setPropertyDetailsValue d
        ]

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (Analyze r a) where
    toJSON (Analyze info tbn) = object
        [ "tag" .= String "Analyze"
        , "info" .= info
        , "table" .= toJSON tbn
        ]

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (TruncatePartition r a) where
    toJSON (TruncatePartition info truncate') = object
        [ "tag" .= String "TruncatePartition"
        , "info" .= info
        , "truncate" .= truncate'
        ]

instance HasInfo (TruncatePartition r a) where
    type Info (TruncatePartition r a) = a
    getInfo (TruncatePartition info _) = info

instance HasInfo (Location a) where
    type Info (Location a) = a
    getInfo (HDFSPath info _) = info

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (AlterTableSetLocation r a) where
    toJSON (AlterTableSetLocation info table location) = object
        [ "tag" .= String "AlterTableSetLocation"
        , "info" .= info
        , "table" .= table
        , "location" .= location
        ]

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (AlterPartitionSetLocation r a) where
    toJSON (AlterPartitionSetLocation info table items location) = object
        [ "tag" .= String "AlterPartitionSetLocation"
        , "info" .= info
        , "table" .= table
        , "items" .= items
        , "location" .= location
        ]

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (StaticPartitionSpecItem r a) where
    toJSON (StaticPartitionSpecItem info column constant) = object
        [ "tag" .= String "StaticPartitionSpecItem"
        , "info" .= info
        , "column" .= column
        , "constant" .= constant
        ]


instance HasTables (HiveStatement ResolvedNames a) where
  goTables (HiveStandardSqlStatement s) = goTables s
  goTables (HiveUseStmt _) = return ()
  goTables (HiveAnalyzeStmt _) = return ()
  goTables (HiveInsertDirectoryStmt s) = goTables s
  goTables (HiveTruncatePartitionStmt s) = goTables s
  goTables (HiveAlterTableSetLocationStmt s) = goTables s
  goTables (HiveAlterPartitionSetLocationStmt (AlterPartitionSetLocation _ (RTableName fqtn _) _ _)) = tell $ S.singleton $ TableUse WriteMeta $ fqtnToFQTN fqtn
  goTables (HiveSetPropertyStmt _) = return ()
  goTables (HiveUnhandledStatement _) = return ()

instance HasTables (InsertDirectory ResolvedNames a) where
  goTables InsertDirectory{..} = goTables insertDirectoryQuery

instance HasTables (TruncatePartition ResolvedNames a) where
  goTables (TruncatePartition _ s) = goTables s

instance HasTables (AlterTableSetLocation ResolvedNames a) where
  goTables (AlterTableSetLocation _ table _) = do
      goTables table


instance HasColumns (HiveStatement ResolvedNames a) where
  goColumns (HiveStandardSqlStatement s) = goColumns s
  goColumns (HiveUseStmt _) = return ()
  goColumns (HiveAnalyzeStmt _) = return ()
  goColumns (HiveInsertDirectoryStmt s) = goColumns s
  goColumns (HiveTruncatePartitionStmt _) = return ()
  goColumns (HiveAlterTableSetLocationStmt _) = return ()
  goColumns (HiveAlterPartitionSetLocationStmt (AlterPartitionSetLocation _ _ items _)) =
      forM_ items $ \ (StaticPartitionSpecItem _ column _) ->
          tell [clauseObservation (void column) "PARTITION"]
  goColumns (HiveSetPropertyStmt _) = return ()
  goColumns (HiveUnhandledStatement _) = return ()

instance HasColumns (InsertDirectory ResolvedNames a) where
  goColumns InsertDirectory{..} = goColumns insertDirectoryQuery
