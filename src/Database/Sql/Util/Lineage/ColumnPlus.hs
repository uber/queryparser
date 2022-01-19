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

{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.Sql.Util.Lineage.ColumnPlus where

import Database.Sql.Type
import Database.Sql.Position
import Database.Sql.Info

import Database.Sql.Util.Eval

import           Data.List.NonEmpty (NonEmpty ((:|)))
import           Data.Maybe (mapMaybe, maybeToList)
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Set (Set)
import qualified Data.Set as S
import           Data.Proxy
import           Data.Semigroup
import           Control.Arrow
import           Control.Applicative
import           Control.Monad.Identity
import           Control.Monad.Except
import           Control.Monad.Writer (Writer, runWriter, execWriter, writer, tell)
import           Data.Foldable


-- | ColumnLineagePlus is a set of descendants, each with an associated set of ancestors.
-- Descendents may be a column (representing values in that column)
-- or a table (representing row-count).
--
-- Ancestors are the same, but that ancestor columns may be further
-- specialized by field path.
--
-- Tracking impacts on row-count is necessary because row-count can
-- have impacts on data without any columns being involved. For a clear
-- example, consider `CREATE TABLE foo AS SELECT COUNT(1) FROM BAR;`
--
-- It also gives us something coherent to speak about with respect to
-- `EXISTS` - the value depends on the row count of the subquery.
--
-- N.b. While it looks like we're talking about "tables", this is *not*
-- the same thing as table-level lineage.  Changes to values in existing
-- rows does not impact row count.  Following an UPDATE, if we ask "has
-- this table changed, such that we need to rerun things downstream?",
-- the answer is clearly yes. If we ask "has the row-count of this
-- table changed, such that we need to rerun things that depend only on
-- row-count?" the answer is clearly not.

type ColumnLineagePlus = Map (Either FQTN FQCN) ColumnPlusSet

class HasColumnLineage q where
  getColumnLineage :: q -> (RecordSet ColumnLineage, ColumnLineagePlus)

instance HasColumnLineage (Statement d ResolvedNames Range) where
  getColumnLineage stmt = columnLineage stmt


emptyLineage :: [FQColumnName ()] -> ColumnLineagePlus
emptyLineage = M.fromList . map (, emptyColumnPlusSet) . map (Right . fqcnToFQCN)

data ColumnLineage

data ColumnPlusSet = ColumnPlusSet
    { columnPlusColumns :: Map FQCN (Map FieldChain (Set Range))
    , columnPlusTables :: Map FQTN (Set Range)
    } deriving (Eq, Show)

instance Semigroup ColumnPlusSet where
    ColumnPlusSet m s <> ColumnPlusSet n t = ColumnPlusSet (M.unionWith (M.unionWith S.union) m n) (M.unionWith S.union s t)

instance Monoid ColumnPlusSet where
    mempty = ColumnPlusSet mempty mempty
    mappend = (<>)

emptyColumnPlusSet :: ColumnPlusSet
emptyColumnPlusSet = ColumnPlusSet M.empty M.empty

singleColumnSet :: Range -> FullyQualifiedColumnName -> ColumnPlusSet
singleColumnSet info c = ColumnPlusSet (M.singleton c $ M.singleton (FieldChain M.empty) $ S.singleton info) M.empty

singleTableSet :: Range -> FullyQualifiedTableName -> ColumnPlusSet
singleTableSet info t = ColumnPlusSet M.empty (M.singleton t $ S.singleton info)

mergeLineages :: Writer ColumnPlusSet [ColumnPlusSet] -> Writer ColumnPlusSet [ColumnPlusSet] -> EvalT ColumnLineage 'TableContext Identity (Writer ColumnPlusSet [ColumnPlusSet])
mergeLineages = (pure .) . liftA2 (zipWith (<>))

instance Evaluation ColumnLineage where
    type EvalValue ColumnLineage = ColumnPlusSet
    type EvalRow ColumnLineage = Writer ColumnPlusSet
    type EvalMonad ColumnLineage = Identity
    addItems _ = mergeLineages
    removeItems _ = mergeLineages
    unionItems _ = mergeLineages
    intersectItems _ = mergeLineages
    distinctItems _ = id
    offsetItems _ _ = id
    limitItems _ _ = id

    filterBy expr (RecordSet cs rs) = do
        let p = Proxy :: Proxy ColumnLineage
        let (r, rowCount) = runWriter rs
        x <- exprToTable (eval p expr) $ makeRowMap cs r
        pure $ makeRecordSet (Proxy :: Proxy ColumnLineage) cs $ writer (map (x <>) r, x <> rowCount)

    inList x xs = pure $ foldl' (<>) x xs
    inSubquery x xs =
        let (ys, y) = runWriter xs
         in pure $ foldl' (<>) x (y:ys)
    existsSubquery rs = pure $ execWriter rs

    atTimeZone ts tz = pure $ ts <> tz

    handleConstant _ _ = pure emptyColumnPlusSet

    handleCases p cases else_ = do
        cases' :: [ColumnPlusSet] <- mapM (\ (when_, then_) -> (<>) <$> eval p when_ <*> eval p then_) cases
        else_' <- maybe (pure emptyColumnPlusSet) (eval p) else_
        pure $ foldl' (<>) else_' cases'

    -- TODO: incorporate over
    handleFunction p _ _ args params filter' _ = mconcat <$> mapM (eval p)
        ( args
            ++ map snd params
            ++ map filterExpr (maybeToList filter')
        )

    handleLambdaParam _ _ = pure emptyColumnPlusSet

    handleLambda p _ body = eval p body

    handleGroups cs gs =
        pure $ RecordSet cs $ do
            (g, rs) <- gs
            r <- rs
            mapM_ tell g
            pure $ map (\ v -> sconcat (v:|g)) r

    handleLike p (Operator _) escape pattern expr = do
        let addEscape = maybe id ((:) . escapeExpr) escape
            exprs = addEscape [patternExpr pattern, expr]
        mconcat <$> mapM (eval p) exprs

    handleOrder _ _ = pure

    handleSubquery xs = case runWriter xs of
        ([x], y) -> pure (x<>y)
        (_, _) -> throwError "wrong number of columns from subquery"

    handleJoin p joinType cond x y = clip joinType <$> eval p cond x y
      where
        clip (JoinSemi _) = \ set ->
            let n = length $ recordSetLabels x
             in RecordSet
                    { recordSetLabels = take n $ recordSetLabels set
                    , recordSetItems = fmap (take n) $ recordSetItems set
                    }
        clip _ = id

    handleStructField expr field = go expr (FieldChain $ M.singleton (void field) $ FieldChain M.empty, getInfo expr)
      where
        go (ColumnExpr _ (RColumnRef col)) (chain, info) = pure ColumnPlusSet
            { columnPlusColumns = M.singleton (fqcnToFQCN col) $ M.singleton chain $ S.singleton info
            , columnPlusTables = mempty
            }

        go (FieldAccessExpr _ expr' field') chain = go expr' $ first (FieldChain . M.singleton (void field')) chain
        go expr' _ = eval (Proxy :: Proxy ColumnLineage) expr'

    handleTypeCast _ expr _ = eval (Proxy :: Proxy ColumnLineage) expr

    binop _ _ = Just $ (pure .) . (<>)

    unop _ _ = Just $ pure . id


ancestorsForTableName :: RTableName Range -> Maybe (RecordSet ColumnLineage)
ancestorsForTableName (RTableName fqtn SchemaMember{..}) =
    let recordSetLabels = map RColumnRef columns
        info = getInfo fqtn
        recordSetItems = writer (map (singleColumnSet info . fqcnToFQCN) columns, singleTableSet info $ fqtnToFQTN $ void fqtn)
        columns = map (qualifyColumnName fqtn) columnsList
     in Just RecordSet{..}

truncateTableLineage :: FQTableName a -> [FQColumnName ()] -> ColumnLineagePlus
truncateTableLineage tableName columns = M.insert (Left $ fqtnToFQTN tableName) emptyColumnPlusSet $ emptyLineage columns

evalDefaultExpr :: DefaultExpr ResolvedNames Range -> EvalResult ColumnLineage (Expr ResolvedNames Range)
evalDefaultExpr (DefaultValue _) = pure emptyColumnPlusSet
evalDefaultExpr (ExprValue expr) = eval (Proxy :: Proxy ColumnLineage) expr

returnNothing :: ColumnLineagePlus -> (RecordSet ColumnLineage, ColumnLineagePlus)
returnNothing = (emptyRecordSet Proxy,)

columnLineage :: Statement d ResolvedNames Range -> (RecordSet ColumnLineage, ColumnLineagePlus)
columnLineage (QueryStmt query) =
    let p = Proxy :: Proxy ColumnLineage
     in case runEval (eval p query) ancestorsForTableName of
            Left err -> error $ "failed to evaluate column lineage for query: " ++ err
            Right r -> (r, mempty)

columnLineage (InsertStmt Insert{insertTable = RTableName tableName SchemaMember{..}, ..}) =
    returnNothing $
        let columns :: [Either FQTN FQCN]
            columns = case insertColumns of
                Just (c:|cs) -> map (columnNameToKey . extractColumnRef) $ c : cs
                Nothing -> map (columnNameToKey . qualifyColumnName tableName) columnsList
         in mergeExisting $ case insertValues of
                InsertExprValues _ exprs -> case runEval (mapM (mapM evalDefaultExpr) exprs) ancestorsForTableName of
                    Left err -> error $ "failed to evaluate column lineage for insert exprs: " ++ err
                    Right rows -> M.fromList $ zip columns $ foldl1 (zipWith (<>)) $ map toList $ toList rows
                InsertSelectValues query -> case runEval (eval p query) ancestorsForTableName of
                    Left err -> error $ "failed to evaluate column lineage for insert query: " ++ err
                    Right RecordSet{..} ->
                        let (columnsLineage, tableLineage) = runWriter recordSetItems
                         in M.insert (Left $ fqtnToFQTN tableName) tableLineage $ M.fromList $ zip columns columnsLineage
                InsertDefaultValues _ -> M.empty
                InsertDataFromFile _ _ -> M.empty
  where
    p = Proxy :: Proxy ColumnLineage
    mergeExisting :: ColumnLineagePlus -> ColumnLineagePlus
    mergeExisting = M.unionWith (<>) existing
    existing :: ColumnLineagePlus
    existing = importExistingTable existingColumns
    existingColumns = M.mapKeysMonotonic Right $ M.fromSet importExistingColumns $ S.fromList $ map (fqcnToFQCN . qualifyColumnName tableName) columnsList
    extractColumnRef :: RColumnRef Range -> FQColumnName Range
    extractColumnRef (RColumnRef fqcn) = fqcn
    extractColumnRef (RColumnAlias _) = error "this shouldn't happen"
    columnNameToKey :: FQColumnName a -> Either FQTN FQCN
    columnNameToKey = Right . fqcnToFQCN
    (importExistingColumns, importExistingTable) = case insertBehavior of -- only INSERT OVERWRITE discards the entire contents of the table
            InsertOverwrite _ -> (const emptyColumnPlusSet, id)
            _ ->
                ( singleColumnSet insertInfo
                , M.insert (Left $ fqtnToFQTN tableName) (singleTableSet (getInfo tableName) $ fqtnToFQTN tableName)
                )

columnLineage (UpdateStmt Update{..}) =
    returnNothing $
        let lineagesFromSetExprs :: ColumnLineagePlus = case runEval (mapM evalDefaultExpr exprs) ancestorsForTableName of
                Left err -> error $ "failed to evaluate column lineage for update exprs: " ++ err
                Right lins -> M.fromList $ zip columns lins

            ancestorsFromWhere :: ColumnPlusSet = case updateWhere of
                Just expr -> case runEval (filterBy expr table) ancestorsForTableName of
                    Left err -> error $ "failed to evaluate column lineage for update expr: " ++ err
                    Right lins -> execWriter $ recordSetItems lins
                Nothing -> mempty

         in mergeExisting $ M.map (<> ancestorsFromWhere) lineagesFromSetExprs
  where
    table :: RecordSet ColumnLineage
    Just table = ancestorsForTableName updateTable

    existing :: ColumnLineagePlus
    existing = M.mapKeysMonotonic Right $ M.fromSet (singleColumnSet updateInfo) $ S.fromList $ map (fqcnToFQCN) fqcns

    mergeExisting :: ColumnLineagePlus -> ColumnLineagePlus
    mergeExisting = M.unionWith (<>) existing

    columns = map (Right . fqcnToFQCN) fqcns
    fqcns = map (\(RColumnRef fqcn) -> fqcn) colRefs
    (colRefs, exprs) = unzip $ toList updateSetExprs

columnLineage (DeleteStmt (Delete _ (RTableName _ SchemaMember{viewQuery = Just _}) _)) = error "delete statement targeting view"
columnLineage (DeleteStmt (Delete _ (RTableName tableName SchemaMember{viewQuery = Nothing, ..}) maybeExpr)) =
    returnNothing $
        let columns = map (qualifyColumnName tableName) columnsList
         in case maybeExpr of
            Nothing -> truncateTableLineage tableName columns
            Just expr ->
                let lineages = map (singleColumnSet (getInfo tableName) . fqcnToFQCN) columns
                    table :: RecordSet ColumnLineage
                    table = RecordSet (map RColumnRef columns) (writer (lineages, singleTableSet (getInfo tableName) $ fqtnToFQTN tableName))
                 in case runEval (filterBy expr table) ancestorsForTableName of
                    Left err -> error $ "failed to evaluate column lineage for delete query: " ++ err
                    Right RecordSet{..} ->
                        let (columnsLineage, tableLineage) = runWriter recordSetItems
                         in M.insert (Left $ fqtnToFQTN tableName) (singleTableSet (getInfo tableName) (fqtnToFQTN tableName) <> tableLineage) $ M.fromList $ zip (map (Right . fqcnToFQCN) columns) columnsLineage

columnLineage (TruncateStmt (Truncate _ (RTableName _ SchemaMember{viewQuery = Just _}))) = error "truncate statement targeting view"
columnLineage (TruncateStmt (Truncate _ (RTableName tableName SchemaMember{viewQuery = Nothing, ..}))) =
    returnNothing $
        let columns = map (qualifyColumnName tableName) columnsList
         in truncateTableLineage tableName columns

columnLineage (CreateTableStmt CreateTable{createTableName = RCreateTableName _ Exists}) = returnNothing M.empty
columnLineage (CreateTableStmt CreateTable{createTableName = RCreateTableName tableName DoesNotExist, ..}) =
    returnNothing $
        case createTableDefinition of
            TableColumns _ (c:|cs) ->
                M.insert (Left $ fqtnToFQTN tableName) emptyColumnPlusSet
                    $ emptyLineage $ (`mapMaybe` (c:cs)) $ \case
                        ColumnOrConstraintConstraint _ -> Nothing
                        ColumnOrConstraintColumn ColumnDefinition{..} -> Just $ qualifyColumnName tableName columnDefinitionName
            TableLike _ (RTableName _ SchemaMember{..}) ->
                M.insert (Left $ fqtnToFQTN tableName) emptyColumnPlusSet
                    $ emptyLineage $ map (qualifyColumnName tableName) columnsList
            TableAs _ maybeColumns query -> case runEval (eval (Proxy :: Proxy ColumnLineage) query) ancestorsForTableName of
                Left err -> error $ "failed to evaluate column lineage for create table statement: " ++ err
                Right RecordSet{..} ->
                    let columns = maybe queryColumns proc maybeColumns
                        proc = map (qualifyColumnName tableName) . toList
                        queryColumns = map (qualifyColumnName tableName . resolvedColumnName) recordSetLabels
                        resolvedColumnName (RColumnRef fqtn) = fqtn{columnNameTable = None}
                        resolvedColumnName (RColumnAlias (ColumnAlias _ name _)) = QColumnName () None name
                        (columnsLineage, tableLineage) = runWriter recordSetItems
                     in M.insert (Left $ fqtnToFQTN tableName) tableLineage $ M.fromList $ zip (map (Right . fqcnToFQCN) columns) columnsLineage
            TableNoColumnInfo _ -> M.empty

columnLineage (DropTableStmt DropTable{dropTableNames = tables}) =
    returnNothing $
        M.unions $ map
          (\case
              RDropExistingTableName tableName SchemaMember{..} ->
                    let columns = map (qualifyColumnName tableName) columnsList
                     in truncateTableLineage tableName columns
              RDropMissingTableName _ -> M.empty
          ) $ toList tables

columnLineage (AlterTableStmt (AlterTableRenameTable _ (RTableName from SchemaMember{..}) (RTableName to _))) =
    returnNothing $
        let as = map (qualifyColumnName from) columnsList
            ds = map (qualifyColumnName to) columnsList
         in M.insert (Left $ fqtnToFQTN to) (singleTableSet (getInfo from) $ fqtnToFQTN from)
                $ M.insert (Left $ fqtnToFQTN from) emptyColumnPlusSet
                $ M.union (emptyLineage as) $ M.fromList $ zip (map (Right . fqcnToFQCN) ds) $ map (singleColumnSet (getInfo from) . fqcnToFQCN) as

columnLineage (AlterTableStmt (AlterTableRenameColumn _ (RTableName table _) from to)) =
    returnNothing $
        let a = fqcnToFQCN $ qualifyColumnName table from
            d = fqcnToFQCN $ qualifyColumnName table to
         in M.fromList [(Right d, singleColumnSet (getInfo table) a), (Right a, emptyColumnPlusSet)]

columnLineage (AlterTableStmt (AlterTableAddColumns _ (RTableName table _) (c:|cs))) =
    returnNothing $ emptyLineage $ map (qualifyColumnName table) (c:cs)

columnLineage (CreateViewStmt _) = returnNothing M.empty
columnLineage (DropViewStmt _) = returnNothing M.empty
-- TODO T590907

columnLineage (CreateSchemaStmt _) = returnNothing M.empty
columnLineage (GrantStmt _) = returnNothing M.empty
columnLineage (RevokeStmt _) = returnNothing M.empty
columnLineage (BeginStmt _) = returnNothing M.empty
columnLineage (CommitStmt _) = returnNothing M.empty
columnLineage (RollbackStmt _) = returnNothing M.empty
columnLineage (ExplainStmt _ _) = returnNothing M.empty
columnLineage (EmptyStmt _) = returnNothing M.empty
