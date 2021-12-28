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

{-# LANGUAGE CPP #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Database.Sql.Util.Joins (HasJoins(..), JoinsResult) where

import Database.Sql.Type

import qualified Data.Map as M
import Data.Map (Map)

import qualified Data.Set as S
import Data.Set (Set)

#if !(MIN_VERSION_base(4,11,0))
import Data.Semigroup
#endif

import Data.Functor.Identity
import Data.Foldable
import Control.Monad (void, when)
import Control.Monad.Writer (Writer, execWriter, tell)

data Result = Result
    { resultBindings :: Map ColumnAliasId (Map (RColumnRef ()) FieldChain)
    , resultColumns :: Set (Map (RColumnRef ()) FieldChain)
    }

instance Semigroup Result where
    (Result bindings columns) <> (Result bindings' columns') = Result (bindings <> bindings') (columns <> columns')

instance Monoid Result where
    mempty = Result mempty mempty
    mappend = (<>)

-- Relationship observed between two columns
type Join = ((FullyQualifiedColumnName, [StructFieldName ()]), (FullyQualifiedColumnName, [StructFieldName ()]))
type Scoped a = Writer Result a
type JoinsResult = Set Join

class HasJoins q where
    getJoins :: q -> Set Join

instance HasJoins (Statement d ResolvedNames a) where
    getJoins stmt =
        let Result{..} = execWriter $ getJoinsStatement stmt
            unalias :: Map (RColumnRef ()) FieldChain -> Map (FQColumnName ()) FieldChain
            unalias m = M.fromList $ M.toList m >>= \case
                (RColumnRef fqcn, chain) -> [(fqcn, chain)]
                (RColumnAlias (ColumnAlias _ _ aliasId), _) -> maybe [] (M.toList . unalias) $ M.lookup aliasId resultBindings
            sets = S.map unalias resultColumns

            toPairs m
                | M.null m = []
                | otherwise = do
                    let ((c@(QColumnName _ (Identity table) _), chain), m') = M.deleteFindMin m
                        pairs = do
                            (c'@(QColumnName _ (Identity table') _), chain') <- M.toList m'
                            fields <- expandChain chain
                            fields' <- expandChain chain'
                            if table /= table'
                             then [((fqcnToFQCN c, fields), (fqcnToFQCN c', fields'))]
                             else []
                    pairs ++ toPairs m'

         in S.fromList $ toPairs =<< S.toList sets
      where
        expandChain (FieldChain m)
            | M.null m = [[]]
            | otherwise = do
                (k, v) <- M.toList m
                (k:) <$> expandChain v


getJoinsStatement :: Statement d ResolvedNames a -> Scoped ()
getJoinsStatement (QueryStmt query) = void $ getJoinsQuery query
getJoinsStatement (InsertStmt insert) = getJoinsInsert insert
getJoinsStatement (UpdateStmt update) = getJoinsUpdate update
getJoinsStatement (DeleteStmt delete) = getJoinsDelete delete
getJoinsStatement (TruncateStmt _) = pure ()
getJoinsStatement (CreateTableStmt create) = getJoinsCreateTable create
getJoinsStatement (AlterTableStmt _) = pure ()
getJoinsStatement (DropTableStmt _) = pure ()
getJoinsStatement (CreateViewStmt create) = void $ getJoinsQuery $ createViewQuery create
getJoinsStatement (DropViewStmt _) = pure ()
getJoinsStatement (CreateSchemaStmt _) = pure ()
getJoinsStatement (GrantStmt _) = pure ()
getJoinsStatement (RevokeStmt _) = pure ()
getJoinsStatement (BeginStmt _) = pure ()
getJoinsStatement (CommitStmt _) = pure ()
getJoinsStatement (RollbackStmt _) = pure ()
getJoinsStatement (ExplainStmt _ _) = pure ()
getJoinsStatement (EmptyStmt _) = pure ()


queryColumns :: Query ResolvedNames a -> [RColumnRef a]
queryColumns (QueryExcept _ _ query _) = queryColumns query
queryColumns (QueryUnion _ _ _ query _) = queryColumns query
queryColumns (QueryIntersect _ _ query _) = queryColumns query
queryColumns (QueryWith _ _ query) = queryColumns query
queryColumns (QueryOrder _ _ query) = queryColumns query
queryColumns (QueryLimit _ _ query) = queryColumns query
queryColumns (QueryOffset _ _ query) = queryColumns query
queryColumns (QuerySelect _ Select{selectCols = SelectColumns _ selections}) = selections >>= \case
    SelectExpr _ aliases _ -> map RColumnAlias aliases
    SelectStar _ _ (StarColumnNames cols) -> cols


getJoinsCreateTable :: CreateTable d ResolvedNames a -> Scoped ()
getJoinsCreateTable CreateTable{..} = getJoinsTableDefinition createTableDefinition

-- TODO - "join" for columns producing columns?  Assuming no...
-- note that defaults cannot reference other tables in Vertica, possibly in other dialects
getJoinsTableDefinition :: TableDefinition d ResolvedNames a -> Scoped ()
getJoinsTableDefinition (TableColumns _ _) = pure ()
getJoinsTableDefinition (TableLike _ _) = pure ()
getJoinsTableDefinition (TableAs _ _ query) = void $ getJoinsQuery query
getJoinsTableDefinition (TableNoColumnInfo _) = pure ()


getJoinsInsert :: Insert ResolvedNames a -> Scoped ()
getJoinsInsert Insert{..} = case insertValues of
    InsertDefaultValues _ -> pure ()
    InsertExprValues _ values -> mapM_ (mapM_ getJoinsDefaultExpr) values
    InsertSelectValues query -> void $ getJoinsQuery query
    InsertDataFromFile _ _ -> pure ()

getJoinsDefaultExpr :: DefaultExpr ResolvedNames a -> Scoped ()
getJoinsDefaultExpr (DefaultValue _) = pure ()
getJoinsDefaultExpr (ExprValue expr) = void $ getJoinsExpr expr

getJoinsUpdate :: Update ResolvedNames a -> Scoped ()
getJoinsUpdate Update{..} = do
    mapM_ (getJoinsDefaultExpr . snd) updateSetExprs
    mapM_ getJoinsTablish updateFrom
    mapM_ getJoinsExpr updateWhere

getJoinsDelete :: Delete ResolvedNames a -> Scoped ()
getJoinsDelete (Delete _ _ (Just expr)) = void $ getJoinsExpr expr
getJoinsDelete (Delete _ _ Nothing) = pure ()

zipColumns :: Query ResolvedNames a -> Query ResolvedNames a -> Scoped ()
zipColumns lhs rhs = do
    let lcolumns = queryColumns lhs
        rcolumns = queryColumns rhs
    forM_ (zip lcolumns rcolumns) $ \ (lcol, rcol) -> emit $ M.fromSet (const $ FieldChain M.empty) $ S.fromList [void lcol, void rcol]

getJoinsQuery :: Query ResolvedNames a -> Scoped ()
getJoinsQuery (QuerySelect _ select) = getJoinsSelect select
getJoinsQuery (QueryExcept _ _ lhs rhs) = do
    getJoinsQuery lhs
    getJoinsQuery rhs
    zipColumns lhs rhs

getJoinsQuery (QueryUnion _ _ _ lhs rhs) = do
    getJoinsQuery lhs
    getJoinsQuery rhs
    zipColumns lhs rhs

getJoinsQuery (QueryIntersect _ _ lhs rhs) = do
    getJoinsQuery lhs
    getJoinsQuery rhs
    zipColumns lhs rhs

getJoinsQuery (QueryWith _ ctes query) = do
    mapM_ getJoinsCTE ctes
    getJoinsQuery query

getJoinsQuery (QueryOrder _ orders query) = do
    mapM_ getJoinsOrder orders
    getJoinsQuery query

getJoinsQuery (QueryLimit _ _ query) = getJoinsQuery query
getJoinsQuery (QueryOffset _ _ query) = getJoinsQuery query


getJoinsSelect :: Select ResolvedNames a -> Scoped ()
getJoinsSelect (Select{..}) = do
    getJoinsSelectCols selectCols
    maybe (pure ()) getJoinsSelectFrom selectFrom
    maybe (pure ()) getJoinsSelectWhere selectWhere
    maybe (pure ()) getJoinsSelectTimeseries selectTimeseries
    maybe (pure ()) getJoinsSelectGroup selectGroup
    maybe (pure ()) getJoinsSelectHaving selectHaving
    maybe (pure ()) getJoinsSelectNamedWindow selectNamedWindow

getJoinsSelectFrom :: SelectFrom ResolvedNames a -> Scoped ()
getJoinsSelectFrom (SelectFrom _ tablishes) = mapM_ getJoinsTablish tablishes

getJoinsSelectCols :: SelectColumns ResolvedNames a -> Scoped ()
getJoinsSelectCols (SelectColumns _ selections) = mapM_ getJoinsSelection selections

getJoinsSelectWhere :: SelectWhere ResolvedNames a -> Scoped ()
getJoinsSelectWhere (SelectWhere _ expr) = void $ getJoinsExpr expr

getJoinsSelectTimeseries :: SelectTimeseries ResolvedNames a -> Scoped ()
getJoinsSelectTimeseries (SelectTimeseries _ _ _ partition expr) = do
    maybe (pure ()) getJoinsPartition partition
    void $ getJoinsExpr expr

getJoinsPositionOrExpr :: PositionOrExpr ResolvedNames a -> Scoped ()
getJoinsPositionOrExpr (PositionOrExprPosition _ _ _) = pure ()
getJoinsPositionOrExpr (PositionOrExprExpr expr) = void $ getJoinsExpr expr

getJoinsGroupingElement :: GroupingElement ResolvedNames a -> Scoped ()
getJoinsGroupingElement (GroupingElementExpr _ posOrExpr) = getJoinsPositionOrExpr posOrExpr
getJoinsGroupingElement (GroupingElementSet _ exprs) = mapM_ getJoinsExpr exprs

getJoinsSelectGroup :: SelectGroup ResolvedNames a -> Scoped ()
getJoinsSelectGroup (SelectGroup _ groupingElements) =
    mapM_ getJoinsGroupingElement groupingElements

getJoinsSelectHaving :: SelectHaving ResolvedNames a -> Scoped ()
getJoinsSelectHaving (SelectHaving _ exprs) = mapM_ getJoinsExpr exprs

getJoinsSelectNamedWindow :: SelectNamedWindow ResolvedNames a -> Scoped ()
getJoinsSelectNamedWindow (SelectNamedWindow _ windows) = mapM_ joins windows
  where
    joins (NamedWindowExpr _ _ windowExpr) = getJoinsWindowExpr windowExpr
    joins (NamedPartialWindowExpr _ _ partialWindowExpr) = getJoinsPartialWindowExpr partialWindowExpr

emit :: Map (RColumnRef ()) FieldChain -> Scoped ()
emit cols = tell $ mempty { resultColumns = S.singleton cols }

bind :: ColumnAliasId -> Map (RColumnRef ()) FieldChain -> Scoped ()
bind alias cols = tell $ mempty { resultBindings = M.singleton alias cols }

getJoinsExpr :: Expr ResolvedNames a -> Scoped (Map (RColumnRef ()) FieldChain)
getJoinsExpr (BinOpExpr _ op lhs rhs) = do
    lcols <- getJoinsExpr lhs
    rcols <- getJoinsExpr rhs

    let allcols = M.unionWith (<>) lcols rcols

    when (op `elem` ["=", "!=", "<>", "<=>", "==", "<", ">", "<=", ">="]) $ do
        emit allcols

    return allcols

getJoinsExpr (CaseExpr _ cases else_) = do
    cols <- mapM (\ (when_, then_) -> getJoinsExpr when_ *> getJoinsExpr then_) cases
    col <- maybe (pure M.empty) getJoinsExpr else_

    return $ M.unionsWith (<>) $ col : cols

getJoinsExpr (LikeExpr _ _ escape pattern expr) = do
    void $ maybe (pure mempty) (getJoinsExpr . escapeExpr) escape

    lcols <- getJoinsExpr $ patternExpr pattern
    rcols <- getJoinsExpr expr

    let allcols = M.unionWith (<>) lcols rcols

    emit allcols

    return allcols

getJoinsExpr (UnOpExpr _ _ expr) = getJoinsExpr expr
getJoinsExpr (ConstantExpr _ _) = return M.empty
getJoinsExpr (ColumnExpr _ column) = return $ M.singleton (void column) $ FieldChain M.empty
getJoinsExpr (InListExpr _ exprs expr) = do
    cols <- M.unionsWith (<>) <$> mapM getJoinsExpr (expr:exprs)
    emit cols
    return cols

getJoinsExpr (InSubqueryExpr _ query expr) = do
    getJoinsQuery query
    let [column] = queryColumns query
    columns <- getJoinsExpr expr
    let columns' = M.insert (void column) (FieldChain M.empty) columns
    emit columns'
    return columns'

getJoinsExpr (BetweenExpr _ expr start end) = M.unionsWith (<>) <$> mapM getJoinsExpr [expr, start, end]
getJoinsExpr (OverlapsExpr _ (r1start, r1end) (r2start, r2end)) = M.unionsWith (<>) <$> mapM getJoinsExpr [r1start, r1end, r2start, r2end]
getJoinsExpr (FunctionExpr _ _ _ args params mFilter mOver) = do
    cols <- M.unionsWith (<>) <$> mapM getJoinsExpr (args ++ map snd params)
    maybe (pure mempty) getJoinsFilter mFilter
    maybe (pure mempty) getJoinsOverSubExpr mOver
    return cols

getJoinsExpr (AtTimeZoneExpr _ ts tz) = M.unionWith (<>) <$> getJoinsExpr ts <*> getJoinsExpr tz
getJoinsExpr (SubqueryExpr _ query) = do
    getJoinsQuery query
    let [column] = queryColumns query
    pure $ M.singleton (void column) $ FieldChain M.empty

getJoinsExpr (ExistsExpr _ query) = do
    _ <- getJoinsQuery query
    return M.empty

getJoinsExpr (ArrayExpr _ values) = M.unionsWith (<>) <$> mapM getJoinsExpr values
getJoinsExpr (FieldAccessExpr _ expr field) = go expr $ FieldChain $ M.singleton (void field) $ FieldChain M.empty
  where
    go (ColumnExpr _ ref@(RColumnRef _)) chain = return $ M.singleton (void ref) chain
    go (FieldAccessExpr _ expr' field') chain = go expr' $ FieldChain $ M.singleton (void field') chain
    go expr' _ = getJoinsExpr expr'


getJoinsExpr (ArrayAccessExpr _ expr index) = M.unionsWith (<>) <$> mapM getJoinsExpr [expr, index]
getJoinsExpr (TypeCastExpr _ _ expr _) = getJoinsExpr expr
getJoinsExpr (VariableSubstitutionExpr _) = return M.empty
getJoinsExpr (LambdaParamExpr _ _) = return M.empty
getJoinsExpr (LambdaExpr _ _ body) = getJoinsExpr body

getJoinsFilter :: Filter ResolvedNames a -> Scoped ()
getJoinsFilter (Filter _ expr) = void $ getJoinsExpr expr

getJoinsOverSubExpr :: OverSubExpr ResolvedNames a -> Scoped ()
getJoinsOverSubExpr (OverWindowExpr _ windowExpr) = getJoinsWindowExpr windowExpr
getJoinsOverSubExpr (OverWindowName _ _) = pure ()
getJoinsOverSubExpr (OverPartialWindowExpr _ partial) = getJoinsPartialWindowExpr partial

getJoinsWindowExpr :: WindowExpr ResolvedNames a -> Scoped ()
getJoinsWindowExpr (WindowExpr _ p os _) = do
    maybe (pure ()) getJoinsPartition p
    mapM_ getJoinsOrder os

getJoinsPartialWindowExpr :: PartialWindowExpr ResolvedNames a -> Scoped ()
getJoinsPartialWindowExpr (PartialWindowExpr _ _ p os _) = do
    maybe (pure ()) getJoinsPartition p
    mapM_ getJoinsOrder os

getJoinsPartition :: Partition ResolvedNames a -> Scoped ()
getJoinsPartition (PartitionBy _ es) = mapM_ getJoinsExpr es
getJoinsPartition (PartitionBest _) = return ()
getJoinsPartition (PartitionNodes _) = return ()

getJoinsOrder :: Order ResolvedNames a -> Scoped ()
getJoinsOrder (Order _ posOrExpr _ _) = void $ getJoinsPositionOrExpr posOrExpr

getJoinsTablish :: Tablish ResolvedNames a -> Scoped ()
getJoinsTablish (TablishTable _ _ _) = pure ()
getJoinsTablish (TablishLateralView _ LateralView{..} lhs) = do
    maybe (pure ()) getJoinsTablish lhs
    mapM_ getJoinsExpr lateralViewExprs

getJoinsTablish (TablishSubQuery _ _ query) = getJoinsQuery query
getJoinsTablish (TablishParenthesizedRelation _ _ relation) = getJoinsTablish relation
getJoinsTablish (TablishJoin _ _ (JoinNatural _ (RNaturalColumns columns)) lhs rhs) = do
    getJoinsTablish lhs
    getJoinsTablish rhs
    forM_ columns $ \ (RUsingColumn lcol rcol) -> do
        emit $ M.fromSet (const $ FieldChain M.empty) $ S.fromList [void lcol, void rcol]

getJoinsTablish (TablishJoin _ _ (JoinOn expr) lhs rhs) = do
    getJoinsTablish lhs
    getJoinsTablish rhs
    void $ getJoinsExpr expr

getJoinsTablish (TablishJoin _ _ (JoinUsing _ columns) lhs rhs) = do
    getJoinsTablish lhs
    getJoinsTablish rhs
    forM_ columns $ \ (RUsingColumn lcol rcol) -> do
        emit $ M.fromSet (const $ FieldChain M.empty) $ S.fromList [void lcol, void rcol]


getJoinsCTE :: CTE ResolvedNames a -> Scoped ()
getJoinsCTE (CTE _ _ _ query) = getJoinsQuery query


getJoinsSelection :: Selection ResolvedNames a -> Scoped ()
getJoinsSelection (SelectStar _ _ _) = pure ()
getJoinsSelection (SelectExpr _ aliases expr) = do
    cols <- getJoinsExpr expr
    forM_ aliases $ \ (ColumnAlias _ _ aliasId) -> bind aliasId cols
