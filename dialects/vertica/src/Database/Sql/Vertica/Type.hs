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
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.Sql.Vertica.Type where

import Database.Sql.Type
import Database.Sql.Position
import Database.Sql.Info
import Database.Sql.Util.Columns
import Database.Sql.Util.Joins
import Database.Sql.Util.Lineage.Table
import Database.Sql.Util.Lineage.ColumnPlus as ColumnPlus
import Database.Sql.Util.Scope
import Database.Sql.Util.Tables
import Database.Sql.Util.Schema as Schema

import Control.Arrow
import Control.Monad.Reader (asks, local)
import Control.Monad.Writer (listen)
import Control.Monad.Identity

import Data.Either (partitionEithers)
import Data.List (foldl')
import Data.List.NonEmpty (NonEmpty((:|)), toList, fromList)
import Data.Maybe (catMaybes)
import Data.Semigroup
import Data.Text.Lazy (Text)
import Data.Traversable (traverse)

import           Data.Aeson (ToJSON (..), (.=))
import qualified Data.Aeson as JSON

import qualified Data.Foldable as F
import Data.Proxy (Proxy (..))

import           Data.Set (Set)
import qualified Data.Set as S
import           Data.Map (Map)
import qualified Data.Map as M

import Data.Data (Data)
import GHC.Generics (Generic)


data Vertica

deriving instance Data Vertica

dialectProxy :: Proxy Vertica
dialectProxy = Proxy

instance Dialect Vertica where
    type DialectCreateTableExtra Vertica r = TableInfo r

    shouldCTEsShadowTables _ = True

    resolveCreateTableExtra _ = resolveTableInfo

    getSelectScope _ fromColumns selectionAliases = SelectScope
        { bindForWhere = bindFromColumns fromColumns
        , bindForGroup = bindBothColumns fromColumns selectionAliases
        , bindForHaving = bindFromColumns fromColumns
        , bindForOrder = bindBothColumns fromColumns selectionAliases
        , bindForNamedWindow = bindFromColumns fromColumns
        }

    areLcolumnsVisibleInLateralViews _ = False


data TableInfo r a = TableInfo
    { tableInfoInfo :: a
    , tableInfoOrdering :: Maybe [Order r a]
    , tableInfoEncoding :: Maybe (TableEncoding r a)
    , tableInfoSegmentation :: Maybe (Segmentation r a)
    , tableInfoKSafety :: Maybe (KSafety a)
    , tableInfoPartitioning :: Maybe (Partitioning r a)
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (TableInfo r a)
deriving instance Generic (TableInfo r a)
deriving instance ConstrainSNames Eq r a => Eq (TableInfo r a)
deriving instance ConstrainSNames Show r a => Show (TableInfo r a)
deriving instance ConstrainSASNames Functor r => Functor (TableInfo r)
deriving instance ConstrainSASNames Foldable r => Foldable (TableInfo r)
deriving instance ConstrainSASNames Traversable r => Traversable (TableInfo r)

resolveTableInfo :: TableInfo RawNames a -> Resolver (TableInfo ResolvedNames) a
resolveTableInfo TableInfo{..} = do
        tableInfoOrdering' <- traverse (mapM $ resolveOrder []) tableInfoOrdering
        tableInfoEncoding' <- traverse resolveTableEncoding tableInfoEncoding
        tableInfoSegmentation' <- traverse resolveSegmentation tableInfoSegmentation
        tableInfoPartitioning' <- traverse resolvePartitioning tableInfoPartitioning
        pure TableInfo
            { tableInfoOrdering = tableInfoOrdering'
            , tableInfoEncoding = tableInfoEncoding'
            , tableInfoSegmentation = tableInfoSegmentation'
            , tableInfoPartitioning = tableInfoPartitioning'
            , ..
            }


data VerticaStatement r a = VerticaStandardSqlStatement (Statement Vertica r a)
                            | VerticaCreateProjectionStatement (CreateProjection r a)
                            | VerticaMultipleRenameStatement (MultipleRename r a)
                            | VerticaSetSchemaStatement (SetSchema r a)
                            | VerticaMergeStatement (Merge r a)
                            | VerticaUnhandledStatement a

deriving instance (ConstrainSNames Data r a, Data r) => Data (VerticaStatement r a)
deriving instance Generic (VerticaStatement r a)
deriving instance ConstrainSNames Eq r a => Eq (VerticaStatement r a)
deriving instance ConstrainSNames Show r a => Show (VerticaStatement r a)
deriving instance ConstrainSASNames Functor r => Functor (VerticaStatement r)
deriving instance ConstrainSASNames Foldable r => Foldable (VerticaStatement r)
deriving instance ConstrainSASNames Traversable r => Traversable (VerticaStatement r)


data TableEncoding r a = TableEncoding a [(ColumnRef r a, Encoding a)]

deriving instance (ConstrainSNames Data r a, Data r) => Data (TableEncoding r a)
deriving instance Generic (TableEncoding r a)
deriving instance ConstrainSNames Eq r a => Eq (TableEncoding r a)
deriving instance ConstrainSNames Show r a => Show (TableEncoding r a)
deriving instance ConstrainSASNames Functor r => Functor (TableEncoding r)
deriving instance ConstrainSASNames Foldable r => Foldable (TableEncoding r)
deriving instance ConstrainSASNames Traversable r => Traversable (TableEncoding r)

resolveTableEncoding :: TableEncoding RawNames a -> Resolver (TableEncoding ResolvedNames) a
resolveTableEncoding (TableEncoding info encodings) = do
    encodings' <- forM encodings $ \ (column, encoding) -> do
        column' <- resolveColumnName column
        pure (column', encoding)
    pure $ TableEncoding info encodings'


data Segmentation r a = UnsegmentedAllNodes a
                        | UnsegmentedOneNode a (Node a)
                        | SegmentedBy a (Expr r a) (NodeList a)

deriving instance (ConstrainSNames Data r a, Data r) => Data (Segmentation r a)
deriving instance Generic (Segmentation r a)
deriving instance ConstrainSNames Eq r a => Eq (Segmentation r a)
deriving instance ConstrainSNames Show r a => Show (Segmentation r a)
deriving instance ConstrainSASNames Functor r => Functor (Segmentation r)
deriving instance ConstrainSASNames Foldable r => Foldable (Segmentation r)
deriving instance ConstrainSASNames Traversable r => Traversable (Segmentation r)

resolveSegmentation :: Segmentation RawNames a -> Resolver (Segmentation ResolvedNames) a
resolveSegmentation (UnsegmentedAllNodes info) = pure $ UnsegmentedAllNodes info
resolveSegmentation (UnsegmentedOneNode info node) = pure $ UnsegmentedOneNode info node
resolveSegmentation (SegmentedBy info expr nodelist) = do
    expr' <- resolveExpr expr
    pure $ SegmentedBy info expr' nodelist


data Node a = Node a Text
              deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)

data NodeListOffset a = NodeListOffset a Int
                        deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)

data NodeList a = AllNodes a (Maybe (NodeListOffset a))
                | Nodes a (NonEmpty (Node a))
                  deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)

data KSafety a = KSafety a (Maybe Int)
                 deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)

data Partitioning r a = Partitioning a (Expr r a)

deriving instance (ConstrainSNames Data r a, Data r) => Data (Partitioning r a)
deriving instance Generic (Partitioning r a)
deriving instance ConstrainSNames Eq r a => Eq (Partitioning r a)
deriving instance ConstrainSNames Show r a => Show (Partitioning r a)
deriving instance ConstrainSASNames Functor r => Functor (Partitioning r)
deriving instance ConstrainSASNames Foldable r => Foldable (Partitioning r)
deriving instance ConstrainSASNames Traversable r => Traversable (Partitioning r)

resolvePartitioning :: Partitioning RawNames a -> Resolver (Partitioning ResolvedNames) a
resolvePartitioning (Partitioning info expr) = Partitioning info <$> resolveExpr expr


data Encoding a = EncodingAuto a
                | EncodingBlockDict a
                | EncodingBlockDictComp a
                | EncodingBZipComp a
                | EncodingCommonDeltaComp a
                | EncodingDeltaRangeComp a
                | EncodingDeltaVal a
                | EncodingGCDDelta a
                | EncodingGZipComp a
                | EncodingRLE a
                | EncodingNone a
                  deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)


data CreateProjection r a = CreateProjection
    { createProjectionInfo :: a
    , createProjectionIfNotExists :: Maybe a
    , createProjectionName :: ProjectionName a
    , createProjectionColumns :: Maybe (NonEmpty (ProjectionColumn a))
    , createProjectionQuery :: Query r a
    , createProjectionSegmentation :: Maybe (Segmentation r a)
    , createProjectionKSafety :: Maybe (KSafety a)
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (CreateProjection r a)
deriving instance Generic (CreateProjection r a)
deriving instance ConstrainSNames Eq r a => Eq (CreateProjection r a)
deriving instance ConstrainSNames Show r a => Show (CreateProjection r a)
deriving instance ConstrainSASNames Functor r => Functor (CreateProjection r)
deriving instance ConstrainSASNames Foldable r => Foldable (CreateProjection r)
deriving instance ConstrainSASNames Traversable r => Traversable (CreateProjection r)


data ProjectionName a = ProjectionName a (Maybe (QSchemaName Maybe a)) Text
                   deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)


data ProjectionColumn a = ProjectionColumn
    { projectionColumnInfo :: a
    , projectionColumnName :: Text
    , projectionColumnAccessRank :: Maybe (AccessRank a)
    , projectionColumnEncoding :: Maybe (Encoding a)
    } deriving (Generic, Data, Eq, Show, Functor, Foldable, Traversable)

data AccessRank a = AccessRank a Int
    deriving (Generic, Data, Read, Show, Eq, Ord, Functor, Foldable, Traversable)

data MultipleRename r a = MultipleRename a [AlterTable r a]

deriving instance (ConstrainSNames Data r a, Data r) => Data (MultipleRename r a)
deriving instance Generic (MultipleRename r a)
deriving instance ConstrainSNames Eq r a => Eq (MultipleRename r a)
deriving instance ConstrainSNames Show r a => Show (MultipleRename r a)
deriving instance ConstrainSASNames Functor r => Functor (MultipleRename r)
deriving instance ConstrainSASNames Foldable r => Foldable (MultipleRename r)
deriving instance ConstrainSASNames Traversable r => Traversable (MultipleRename r)

data SetSchema r a = SetSchema
    { setSchemaInfo :: a
    , setSchemaTable :: TableName r a
    , setSchemaName :: SchemaName r a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (SetSchema r a)
deriving instance Generic (SetSchema r a)
deriving instance ConstrainSNames Eq r a => Eq (SetSchema r a)
deriving instance ConstrainSNames Show r a => Show (SetSchema r a)
deriving instance ConstrainSASNames Functor r => Functor (SetSchema r)
deriving instance ConstrainSASNames Foldable r => Foldable (SetSchema r)
deriving instance ConstrainSASNames Traversable r => Traversable (SetSchema r)

data Merge r a = Merge
    { mergeInfo :: a
    , mergeTargetTable :: TableName r a
    , mergeTargetAlias :: Maybe (TableAlias a)
    , mergeSourceTable :: TableName r a
    , mergeSourceAlias :: Maybe (TableAlias a)
    , mergeCondition :: Expr r a
    , mergeUpdateDirective :: Maybe (NonEmpty (ColumnRef r a, DefaultExpr r a))
    , mergeInsertDirectiveColumns :: Maybe (NonEmpty (ColumnRef r a))
    , mergeInsertDirectiveValues :: Maybe (NonEmpty (DefaultExpr r a))
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (Merge r a)
deriving instance Generic (Merge r a)
deriving instance ConstrainSNames Eq r a => Eq (Merge r a)
deriving instance ConstrainSNames Show r a => Show (Merge r a)
deriving instance ConstrainSASNames Functor r => Functor (Merge r)
deriving instance ConstrainSASNames Foldable r => Foldable (Merge r)
deriving instance ConstrainSASNames Traversable r => Traversable (Merge r)


decomposeMerge :: forall d a . Merge ResolvedNames a -> NonEmpty (Statement d ResolvedNames a)
decomposeMerge Merge{..} = fromList $ catMaybes [ fmap mkInsert mergeInsertDirectiveValues
                                                , fmap mkUpdate mergeUpdateDirective
                                                ]
  where
    r :: a
    r = mergeInfo

    toAliases :: Maybe (TableAlias a) -> TablishAliases a
    toAliases mAlias = case mAlias of
        Just alias -> TablishAliasesT alias
        Nothing -> TablishAliasesNone

    makeExprAlias :: Expr ResolvedNames a -> [ColumnAlias a]
    makeExprAlias = const []

    lhs :: Tablish ResolvedNames a
    lhs = let RTableName lhsFqtn lhsSchemaMember = mergeTargetTable
           in TablishTable r (toAliases mergeTargetAlias) (RTableRef lhsFqtn lhsSchemaMember)

    rhs :: Tablish ResolvedNames a
    rhs = let RTableName rhsFqtn rhsSchemaMember = mergeSourceTable
           in TablishTable r (toAliases mergeSourceAlias) (RTableRef rhsFqtn rhsSchemaMember)

    mkInsert :: NonEmpty (DefaultExpr ResolvedNames a) -> Statement d ResolvedNames a
    mkInsert insertVals =
        let selectInfo = r

            toSelectExpr (DefaultValue _) = error "don't know how to make SelectExpr from DefaultValue"
            toSelectExpr (ExprValue expr) = SelectExpr r (makeExprAlias expr) expr

            selectCols = SelectColumns r $ map toSelectExpr $ toList insertVals
            selectFrom =
                let join' = TablishJoin r (JoinInner r) (JoinOn $ UnOpExpr r "NOT" mergeCondition) lhs rhs
                 in Just $ SelectFrom r [join']
            selectWhere = Nothing
            selectTimeseries = Nothing
            selectGroup = Nothing
            selectHaving = Nothing
            selectNamedWindow = Nothing
            selectDistinct = Distinct False

            insertInfo = r
            insertBehavior = InsertAppend r
            insertTable = mergeTargetTable
            insertColumns = mergeInsertDirectiveColumns
            insertValues = InsertSelectValues $ QuerySelect r Select{..}
         in InsertStmt Insert{..}

    mkUpdate :: NonEmpty (RColumnRef a, DefaultExpr ResolvedNames a) -> Statement d ResolvedNames a
    mkUpdate setExprs  =
        let updateInfo = r
            updateTable = mergeTargetTable
            updateAlias = mergeTargetAlias
            updateSetExprs = setExprs
            updateFrom = Just $ TablishJoin r (JoinInner r) (JoinOn mergeCondition) lhs rhs
            updateWhere = Nothing
         in UpdateStmt Update{..}


instance HasJoins (VerticaStatement ResolvedNames a) where
    getJoins (VerticaStandardSqlStatement stmt) = getJoins stmt
    getJoins (VerticaCreateProjectionStatement CreateProjection{..}) = getJoins (QueryStmt createProjectionQuery)
    getJoins (VerticaMultipleRenameStatement _) = S.empty
    getJoins (VerticaSetSchemaStatement _) = S.empty
    getJoins (VerticaMergeStatement merge) = foldMap getJoins $ toList $ decomposeMerge merge
    getJoins (VerticaUnhandledStatement _) = S.empty


instance HasTableLineage (VerticaStatement ResolvedNames a) where
    getTableLineage (VerticaStandardSqlStatement stmt) = tableLineage stmt

    -- CREATE PROJECTION does not create a **table** so it has no table lineage.
    getTableLineage (VerticaCreateProjectionStatement _) = M.empty

    getTableLineage (VerticaMultipleRenameStatement (MultipleRename _ renames)) =
        foldl' (\ ls -> squashTableLineage ls . tableLineage . AlterTableStmt) M.empty renames

    getTableLineage (VerticaSetSchemaStatement (SetSchema _ (RTableName fqtn _) (QSchemaName _ (Identity (DatabaseName _ db)) schema schemaType))) = case schemaType of
        NormalSchema ->
            let from@(FullyQualifiedTableName _ _ table) = mkFQTN fqtn
                to = FullyQualifiedTableName db schema table
             in M.fromList [(to, S.singleton from), (from, S.empty)]
        SessionSchema -> error $ "can't set a table's schema to SessionSchema"

    getTableLineage (VerticaMergeStatement merge) = M.unionsWith S.union $ map tableLineage $ toList $ decomposeMerge merge

    getTableLineage (VerticaUnhandledStatement _) = M.empty


instance HasColumnLineage (VerticaStatement ResolvedNames Range) where
    getColumnLineage (VerticaStandardSqlStatement stmt) = columnLineage stmt

    -- CREATE PROJECTION does not create a **table** so it has no column lineage.
    getColumnLineage (VerticaCreateProjectionStatement _) = returnNothing M.empty

    getColumnLineage (VerticaMultipleRenameStatement (MultipleRename _ renames)) =
        returnNothing $ foldl' (\ ls -> squashColumns ls . snd . columnLineage . AlterTableStmt) M.empty renames
      where
        squashColumns :: ColumnLineagePlus -> ColumnLineagePlus -> ColumnLineagePlus
        squashColumns old new =
            -- This gets to be simpler because we know we're dealing with single columns all the way through.
            -- This means we can safely discard the FieldChains and look at the keys as sets.
            let new' = M.map (toColumnPlusSet . M.foldMapWithKey go . fromColumnPlusSet) new
                fromColumnPlusSet :: ColumnPlusSet -> Map (Either FQTN FQCN) (Set Range)
                fromColumnPlusSet ColumnPlusSet{..} =
                    M.fromList $ map (Right *** (S.unions . M.elems)) (M.toList columnPlusColumns)
                        ++ map (first Left) (M.toList columnPlusTables)

                retuple :: (Either a b, c) -> Either (a, c) (b, c)
                retuple (Left x, z) = Left (x, z)
                retuple (Right y, z) = Right (y, z)

                toColumnPlusSet :: Map (Either FQTN FQCN) (Set Range) -> ColumnPlusSet
                toColumnPlusSet ds =
                    let (ts, cs) = partitionEithers $ map retuple $ M.toList ds
                     in ColumnPlusSet (M.singleton (FieldChain M.empty) <$> M.fromList cs) (M.fromList ts)

                go k v = maybe (M.singleton k v) fromColumnPlusSet $ M.lookup k old
             in M.union new' old

    getColumnLineage (VerticaSetSchemaStatement (SetSchema _ (RTableName fqtn SchemaMember{..}) schemaName)) =
        let from = map (qualifyColumnName fqtn) columnsList
            to = map (qualifyColumnName fqtn{tableNameSchema = pure schemaName}) columnsList
         in returnNothing
                $ M.insert (Left $ fqtnToFQTN fqtn) emptyColumnPlusSet
                $ M.insert (Left $ fqtnToFQTN fqtn{tableNameSchema = pure schemaName}) (singleTableSet (getInfo fqtn) $ fqtnToFQTN fqtn)
                $ M.union (ColumnPlus.emptyLineage from) $ M.fromList $ zip (map (Right . fqcnToFQCN) to) $ map (singleColumnSet (getInfo fqtn) . fqcnToFQCN) from

    getColumnLineage (VerticaMergeStatement merge) = returnNothing $
        let x:xs = map (snd . columnLineage) (toList $ decomposeMerge merge)
         in foldr (<>) x xs

    getColumnLineage (VerticaUnhandledStatement _) = returnNothing M.empty



resolveVerticaStatement :: VerticaStatement RawNames a -> Resolver (VerticaStatement ResolvedNames) a
resolveVerticaStatement (VerticaStandardSqlStatement stmt) = VerticaStandardSqlStatement <$> resolveStatement stmt
resolveVerticaStatement (VerticaCreateProjectionStatement CreateProjection{..}) = do
    WithColumns createProjectionQuery' columns <- resolveQueryWithColumns createProjectionQuery
    bindColumns columns $ do
        createProjectionSegmentation' <- traverse resolveSegmentation createProjectionSegmentation
        pure $ VerticaCreateProjectionStatement CreateProjection
            { createProjectionQuery = createProjectionQuery'
            , createProjectionSegmentation = createProjectionSegmentation'
            , ..
            }

resolveVerticaStatement (VerticaMultipleRenameStatement stmt) = VerticaMultipleRenameStatement <$> resolveMultipleRename stmt
resolveVerticaStatement (VerticaSetSchemaStatement stmt) = VerticaSetSchemaStatement <$> resolveSetSchema stmt

resolveVerticaStatement (VerticaMergeStatement Merge{..}) = do
    mergeTargetTable'@(RTableName tFqtn tSchemaMember) <- resolveTableName mergeTargetTable
    mergeSourceTable'@(RTableName sFqtn sSchemaMember) <- resolveTableName mergeSourceTable

    let mkColRefs :: [UQColumnName ()] -> FQTableName a -> [RColumnRef a]
        mkColRefs uqcns fqtn = map (\uqcn -> RColumnRef $ uqcn { columnNameInfo = tableNameInfo fqtn
                                                               , columnNameTable = Identity fqtn
                                                               }) uqcns
        tgtColRefs = mkColRefs (columnsList tSchemaMember) tFqtn
        tgtColSet = case mergeTargetAlias of
            Just alias -> (Just $ RTableAlias alias, tgtColRefs)
            Nothing -> (Just $ RTableRef tFqtn tSchemaMember, tgtColRefs)
        srcColRefs = mkColRefs (columnsList sSchemaMember) sFqtn
        srcColSet = case mergeSourceAlias of
            Just alias -> (Just $ RTableAlias alias, srcColRefs)
            Nothing -> (Just $ RTableRef sFqtn sSchemaMember, srcColRefs)

    mergeCondition' <- bindColumns [srcColSet, tgtColSet] $ resolveExpr mergeCondition

    let resolveColRef oqcn = RColumnRef $ oqcn { columnNameTable = Identity tFqtn }
        resolveSetExpr (oqcn, expr) = do
            expr' <- resolveDefaultExpr expr
            return (resolveColRef oqcn, expr')
    mergeUpdateDirective' <- bindColumns [srcColSet] $ mapM (mapM resolveSetExpr) mergeUpdateDirective

    let mergeInsertDirectiveColumns' = fmap (fmap resolveColRef) mergeInsertDirectiveColumns
    mergeInsertDirectiveValues' <- bindColumns [srcColSet] $ mapM (mapM resolveDefaultExpr) mergeInsertDirectiveValues

    pure $ VerticaMergeStatement Merge
        { mergeTargetTable = mergeTargetTable'
        , mergeSourceTable = mergeSourceTable'
        , mergeCondition = mergeCondition'
        , mergeUpdateDirective = mergeUpdateDirective'
        , mergeInsertDirectiveColumns = mergeInsertDirectiveColumns'
        , mergeInsertDirectiveValues = mergeInsertDirectiveValues'
        , ..
        }

resolveVerticaStatement (VerticaUnhandledStatement info) = pure $ VerticaUnhandledStatement info

resolveMultipleRename :: MultipleRename RawNames a -> Resolver (MultipleRename ResolvedNames) a
resolveMultipleRename (MultipleRename info []) = pure $ MultipleRename info []
resolveMultipleRename (MultipleRename info (a:as)) = do
    -- TODO (part of T416947): apply derived updates based on warnings
    (a', _) <- listen $ resolveAlterTable a
    catalog <- asks catalog


    -- here we're discarding SchemaChangeErrors - I'm not sure what's right
    let merge cat ch = fst $ applySchemaChange ch cat
        catalog' =  foldl' merge catalog $ getSchemaChange a'

    MultipleRename info' as' <- local (\ ri -> ri { catalog = catalog' }) $ resolveMultipleRename $ MultipleRename info as

    pure $ MultipleRename info' (a':as')

resolveSetSchema :: SetSchema RawNames a -> Resolver (SetSchema ResolvedNames) a
resolveSetSchema SetSchema{..} = do
    setSchemaTable' <- resolveTableName setSchemaTable
    setSchemaName' <- resolveSchemaName setSchemaName
    pure SetSchema
        { setSchemaTable = setSchemaTable'
        , setSchemaName = setSchemaName'
        , ..
        }


instance HasSchemaChange (VerticaStatement ResolvedNames a) where
    getSchemaChange (VerticaStandardSqlStatement stmt) = getSchemaChange stmt
    getSchemaChange (VerticaCreateProjectionStatement _) = []
    getSchemaChange (VerticaMultipleRenameStatement stmt) = getSchemaChange stmt
    getSchemaChange (VerticaSetSchemaStatement stmt) = getSchemaChange stmt
    getSchemaChange (VerticaMergeStatement _) = []
    getSchemaChange (VerticaUnhandledStatement _) = []

instance HasSchemaChange (MultipleRename ResolvedNames a) where
    getSchemaChange (MultipleRename _ renames) = renames >>= getSchemaChange

instance HasSchemaChange (SetSchema ResolvedNames a) where
    getSchemaChange (SetSchema _ (RTableName fqtn table) schemaName) =
        [ Schema.DropTable $ void fqtn
        , Schema.CreateTable (void fqtn { tableNameSchema = pure schemaName }) table
        ]


instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (VerticaStatement r a) where
    toJSON (VerticaStandardSqlStatement stmt) = toJSON stmt
    toJSON (VerticaCreateProjectionStatement stmt) = toJSON stmt
    toJSON (VerticaMultipleRenameStatement stmt) = toJSON stmt
    toJSON (VerticaSetSchemaStatement stmt) = toJSON stmt
    toJSON (VerticaMergeStatement stmt) = toJSON stmt

    toJSON (VerticaUnhandledStatement info) = JSON.object
        [ "tag" .= JSON.String "VerticaUnhandledStatement"
        , "info" .= info
        ]

typeExample :: ()
typeExample = const () $ toJSON (undefined :: VerticaStatement ResolvedNames Range)

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (CreateProjection r a) where
    toJSON CreateProjection{..} = JSON.object
        [ "tag" .= JSON.String "CreateProjection"
        , "info" .= createProjectionInfo
        , "ifnotexists" .= createProjectionIfNotExists
        , "name" .= createProjectionName
        , "columns" .= fmap F.toList createProjectionColumns
        , "query" .= createProjectionQuery
        , "segmentation" .= createProjectionSegmentation
        , "ksafety" .= createProjectionKSafety
        ]

instance ToJSON a => ToJSON (ProjectionColumn a) where
    toJSON ProjectionColumn{..} = JSON.object
        [ "tag" .= JSON.String "ProjectionColumn"
        , "info" .= projectionColumnInfo
        , "name" .= projectionColumnName
        , "accessrank" .= projectionColumnAccessRank
        , "encoding" .= projectionColumnEncoding
        ]

instance ToJSON a => ToJSON (ProjectionName a) where
    toJSON (ProjectionName info schema projection) = JSON.object
        [ "tag" .= JSON.String "ProjectionName"
        , "info" .= info
        , "schema" .= schema
        , "projection" .= projection
        ]

instance ToJSON a => ToJSON (AccessRank a) where
    toJSON (AccessRank info rank) = JSON.object
        [ "tag" .= JSON.String "AccessRank"
        , "info" .= info
        , "rank" .= rank
        ]

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (TableInfo r a) where
    toJSON TableInfo{..} = JSON.object
        [ "tag" .= JSON.String "TableInfo"
        , "dialect" .= JSON.String "Vertica"
        , "ordering" .= tableInfoOrdering
        , "encoding" .= tableInfoEncoding
        , "segmentation" .= tableInfoSegmentation
        , "ksafety" .= tableInfoKSafety
        , "partitioning" .= tableInfoPartitioning
        ]

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (TableEncoding r a) where
    toJSON (TableEncoding info encodings) = JSON.object
        [ "tag" .= JSON.String "TableEncoding"
        , "info" .= info
        , "encodings" .= encodings
        ]

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (Segmentation r a) where
    toJSON (UnsegmentedAllNodes info) = JSON.object
        [ "tag" .= JSON.String "UnsegmentedAllNodes"
        , "info" .= info
        ]

    toJSON (UnsegmentedOneNode info node) = JSON.object
        [ "tag" .= JSON.String "UnsegmentedAllNodes"
        , "info" .= info
        , "node" .= node
        ]

    toJSON (SegmentedBy info expr nodes) = JSON.object
        [ "tag" .= JSON.String "UnsegmentedAllNodes"
        , "info" .= info
        , "expr" .= expr
        , "nodes" .= nodes
        ]

instance ToJSON a => ToJSON (KSafety a) where
    toJSON (KSafety info factor) = JSON.object
        [ "tag" .= JSON.String "KSafety"
        , "info" .= info
        , "factor" .= factor
        ]

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (Partitioning r a) where
    toJSON (Partitioning info expr) = JSON.object
        [ "tag" .= JSON.String "Partitioning"
        , "info" .= info
        , "expr" .= expr
        ]


instance ToJSON a => ToJSON (Encoding a) where
    toJSON (EncodingAuto info) = JSON.object
        [ "tag" .= JSON.String "EncodingAuto"
        , "info" .= info
        ]

    toJSON (EncodingBlockDict info) = JSON.object
        [ "tag" .= JSON.String "EncodingBlockDict"
        , "info" .= info
        ]

    toJSON (EncodingBlockDictComp info) = JSON.object
        [ "tag" .= JSON.String "EncodingBlockDictComp"
        , "info" .= info
        ]

    toJSON (EncodingBZipComp info) = JSON.object
        [ "tag" .= JSON.String "EncodingBZipComp"
        , "info" .= info
        ]

    toJSON (EncodingCommonDeltaComp info) = JSON.object
        [ "tag" .= JSON.String "EncodingCommonDeltaComp"
        , "info" .= info
        ]

    toJSON (EncodingDeltaRangeComp info) = JSON.object
        [ "tag" .= JSON.String "EncodingDeltaRangeComp"
        , "info" .= info
        ]

    toJSON (EncodingDeltaVal info) = JSON.object
        [ "tag" .= JSON.String "EncodingDeltaVal"
        , "info" .= info
        ]

    toJSON (EncodingGCDDelta info) = JSON.object
        [ "tag" .= JSON.String "EncodingGCDDelta"
        , "info" .= info
        ]

    toJSON (EncodingGZipComp info) = JSON.object
        [ "tag" .= JSON.String "EncodingGZipComp"
        , "info" .= info
        ]

    toJSON (EncodingRLE info) = JSON.object
        [ "tag" .= JSON.String "EncodingRLE"
        , "info" .= info
        ]

    toJSON (EncodingNone info) = JSON.object
        [ "tag" .= JSON.String "EncodingNone"
        , "info" .= info
        ]

instance ToJSON a => ToJSON (Node a) where
    toJSON (Node info name) = JSON.object
        [ "tag" .= JSON.String "Node"
        , "info" .= info
        , "name" .= name
        ]

instance ToJSON a => ToJSON (NodeList a) where
    toJSON (AllNodes info offset) = JSON.object
        [ "tag" .= JSON.String "AllNodes"
        , "info" .= info
        , "offset" .= offset
        ]

    toJSON (Nodes info (n:|ns)) = JSON.object
        [ "tag" .= JSON.String "Nodes"
        , "info" .= info
        , "nodes" .= (n:ns)
        ]

instance ToJSON a => ToJSON (NodeListOffset a) where
    toJSON (NodeListOffset info offset) = JSON.object
        [ "tag" .= JSON.String "NodeListOffset"
        , "info" .= info
        , "offset" .= offset
        ]

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (MultipleRename r a) where
    toJSON (MultipleRename info renames) = JSON.object
        [ "tag" .= JSON.String "MultipleRename"
        , "info" .= info
        , "renames" .= renames
        ]

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (SetSchema r a) where
    toJSON (SetSchema info table schema) = JSON.object
        [ "tag" .= JSON.String "SetSchema"
        , "info" .= info
        , "table" .= table
        , "schema" .= schema
        ]

instance (ConstrainSNames ToJSON r a, ToJSON a) => ToJSON (Merge r a) where
    toJSON Merge{..} = JSON.object
        [ "tag" .= JSON.String "Merge"
        , "info" .= mergeInfo
        , "mergeTargetTable" .= mergeTargetTable
        , "mergeTargetAlias" .= mergeTargetAlias
        , "mergeSourceTable" .= mergeSourceTable
        , "mergeSourceAlias" .= mergeSourceAlias
        , "mergeCondition" .= mergeCondition
        , "mergeUpdateDirective" .= fmap toList mergeUpdateDirective
        , "mergeInsertDirectiveColumns" .= fmap toList mergeInsertDirectiveColumns
        , "mergeInsertDirectiveValues" .= fmap toList mergeInsertDirectiveValues
        ]

instance HasInfo (TableInfo r a) where
    type Info (TableInfo r a) = a
    getInfo TableInfo{..} = tableInfoInfo

instance HasInfo (TableEncoding r a) where
    type Info (TableEncoding r a) = a
    getInfo (TableEncoding info _) = info

instance HasInfo (Segmentation r a) where
    type Info (Segmentation r a) = a
    getInfo (UnsegmentedAllNodes info) = info
    getInfo (UnsegmentedOneNode info _) = info
    getInfo (SegmentedBy info _ _) = info

instance HasInfo (Partitioning r a) where
    type Info (Partitioning r a) = a
    getInfo (Partitioning info _) = info

instance HasInfo (KSafety a) where
    type Info (KSafety a) = a
    getInfo (KSafety info _) = info

instance HasInfo (NodeList a) where
    type Info (NodeList a) = a
    getInfo (AllNodes info _) = info
    getInfo (Nodes info _) = info

instance HasInfo (NodeListOffset a) where
    type Info (NodeListOffset a) = a
    getInfo (NodeListOffset info _) = info

instance HasInfo (Node a) where
    type Info (Node a) = a
    getInfo (Node info _) = info

instance HasInfo (AccessRank a) where
    type Info (AccessRank a) = a
    getInfo (AccessRank info _) = info

instance HasInfo (Encoding a) where
    type Info (Encoding a) = a
    getInfo (EncodingAuto info) = info
    getInfo (EncodingBlockDict info) = info
    getInfo (EncodingBlockDictComp info) = info
    getInfo (EncodingBZipComp info) = info
    getInfo (EncodingCommonDeltaComp info) = info
    getInfo (EncodingDeltaRangeComp info) = info
    getInfo (EncodingDeltaVal info) = info
    getInfo (EncodingGCDDelta info) = info
    getInfo (EncodingGZipComp info) = info
    getInfo (EncodingRLE info) = info
    getInfo (EncodingNone info) = info

instance HasInfo (ProjectionName a) where
    type Info (ProjectionName a) = a
    getInfo (ProjectionName info _ _) = info

instance HasInfo (MultipleRename r a) where
    type Info (MultipleRename r a) = a
    getInfo (MultipleRename info _) = info

instance HasInfo (SetSchema r a) where
    type Info (SetSchema r a) = a
    getInfo (SetSchema info _ _) = info

instance HasInfo (Merge r a) where
    type Info (Merge r a) = a
    getInfo Merge{..} = mergeInfo

instance HasTables (VerticaStatement ResolvedNames a) where
  goTables (VerticaStandardSqlStatement s) = goTables s
  goTables (VerticaCreateProjectionStatement _) = return ()
  goTables (VerticaMultipleRenameStatement mr) = goTables mr
  goTables (VerticaSetSchemaStatement _) = return ()
  goTables (VerticaMergeStatement merge) = goTables merge
  goTables (VerticaUnhandledStatement _) = return ()

instance HasTables (MultipleRename ResolvedNames a) where
  goTables (MultipleRename _ alters) = mapM_ goTables alters

instance HasTables (Merge ResolvedNames a) where
  goTables merge = mapM_ goTables $ toList $ decomposeMerge merge

instance HasColumns (VerticaStatement ResolvedNames a) where
    goColumns (VerticaStandardSqlStatement s) = goColumns s
    goColumns (VerticaCreateProjectionStatement s) = goColumns s
    goColumns (VerticaMultipleRenameStatement _) = return ()
    goColumns (VerticaSetSchemaStatement _) = return ()
    goColumns (VerticaMergeStatement m) = goColumns m
    goColumns (VerticaUnhandledStatement _) = return ()

instance HasColumns (CreateProjection ResolvedNames a) where
    goColumns CreateProjection{..} = bindClause "CREATE" $ do
        goColumns createProjectionQuery
        goColumns createProjectionSegmentation

instance HasColumns (Segmentation ResolvedNames a) where
    goColumns (UnsegmentedAllNodes _) = return ()
    goColumns (UnsegmentedOneNode _ _) = return ()
    goColumns (SegmentedBy _ expr _) = goColumns expr

instance HasColumns (Merge ResolvedNames a) where
    goColumns merge = bindClause "MERGE" $ mapM_ goColumns $ toList $ decomposeMerge merge
