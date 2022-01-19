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

{-# LANGUAGE FlexibleContexts #-}
module Database.Sql.Util.Columns ( Clause, ColumnAccess
                                 , HasColumns(..), getColumns
                                 , bindClause, clauseObservation
                                 ) where

import           Data.Either
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.List.NonEmpty (NonEmpty(..))
import           Data.List (transpose)
import           Data.Set (Set)
import qualified Data.Set as S
import           Data.Text.Lazy (Text)

import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.Writer

import           Database.Sql.Type
import           Database.Sql.Util.Scope (queryColumnNames, tablishColumnNames)

type Clause = Text  -- SELECT, WHERE, GROUPBY, etc... for nested clauses,
                    -- report the innermost clause.
type ColumnAccess = (FQCN, Clause)

-- To support dereferencing of column aliases, employ the following algorithm:
--
-- Traverse the resolved AST to write two maps.
--
-- 1. "alias map" which is Map ColumnAlias (Set RColumnRef)
--
-- To populate the alias map, emit at the site of every alias definition,
-- i.e. for every SelectExpr. The key is always the ColumnAlias. The value is
-- the set of columns/aliases referenced in the expr.
--
-- 2. "clause map" which is Map RColumnRef (Set Clause)
--
-- To populate the clause map, emit the current-clause for every RColumnRef.
--
-- Then at the end, stitch the results together by walking over the clause
-- map. If the key is an RColumnRef/FQColumnName, emit the column, for every
-- clause. If the key is an RColumnAlias/ColumnAlias, look it up recursively
-- into the alias map until everything is an RColumnRef/FQColumnName, and then
-- emit every column for every clause.
type AliasInfo = (ColumnAliasId, Set (RColumnRef ()))
type AliasMap = Map ColumnAliasId (Set (RColumnRef ()))
type ClauseInfo = (RColumnRef (), Set Clause)
type ClauseMap = Map (RColumnRef ()) (Set Clause)
type Observation = Either AliasInfo ClauseInfo -- Stuff both info-types into an Either, so we only traverse the AST once.

aliasObservation :: ColumnAlias a -> Set (RColumnRef b) -> Observation
aliasObservation (ColumnAlias _ _ cid) refs = Left (cid, S.map void refs)

clauseObservation :: RColumnRef a -> Clause -> Observation
clauseObservation ref clause = Right (void ref, S.singleton clause)

toAliasMap :: [Observation] -> AliasMap
toAliasMap = M.fromListWith S.union . lefts

toClauseMap :: [Observation] -> ClauseMap
toClauseMap = M.fromListWith S.union . rights

type Observer = ReaderT Clause (Writer [Observation]) ()

class HasColumns q where
    goColumns :: q -> Observer

baseClause :: Clause
baseClause = "BASE"

bindClause :: MonadReader Clause m => Clause -> m r -> m r
bindClause clause = local (const clause)

getColumns :: HasColumns q => q -> Set ColumnAccess
getColumns q = foldMap columnAccesses $ M.toList clauseMap
  where
    observations = execWriter $ runReaderT (goColumns q) baseClause
    aliasMap = toAliasMap observations
    clauseMap = toClauseMap observations

    columnAccesses :: ClauseInfo -> Set ColumnAccess
    columnAccesses (ref, clauses) =
        S.fromList [(fqcn, clause) | fqcn <- S.toList $ getAllFQCNs ref
                                   , clause <- S.toList clauses]

    getAllFQCNs :: RColumnRef () -> Set FQCN
    getAllFQCNs ref = recur [ref] [] S.empty

    --recur :: refsToVisit   -> allRefsVisited  -> fqcnsVisited -> all the fqcns!
    recur :: [RColumnRef ()] -> [RColumnRef ()] -> Set FQCN     -> Set FQCN
    recur [] _ fqcns = fqcns
    recur (ref:refs) visited fqcns =
        if ref `elem` visited
        then recur refs visited fqcns
        else case ref of
            RColumnRef fqcn -> recur refs (ref:visited) (S.insert (fqcnToFQCN fqcn) fqcns)
            RColumnAlias (ColumnAlias _ _ cid) -> case M.lookup cid aliasMap of
                Nothing -> error $ "column alias missing from aliasMap: " ++ show ref ++ ", " ++ show aliasMap
                Just moarRefs -> recur (refs ++ S.toList moarRefs) (ref:visited) fqcns


instance HasColumns a => HasColumns (NonEmpty a) where
    goColumns ne = mapM_ goColumns ne

instance HasColumns a => HasColumns (Maybe a) where
    goColumns Nothing = return ()
    goColumns (Just a) = goColumns a


instance HasColumns (Statement d ResolvedNames a) where
    goColumns (QueryStmt q) = goColumns q
    goColumns (InsertStmt i) = goColumns i
    goColumns (UpdateStmt u) = goColumns u
    goColumns (DeleteStmt d) = goColumns d
    goColumns (TruncateStmt _) = return ()
    goColumns (CreateTableStmt c) = goColumns c
    goColumns (AlterTableStmt a) = goColumns a
    goColumns (DropTableStmt _) = return ()
    goColumns (CreateViewStmt c) = goColumns c
    goColumns (DropViewStmt _) = return ()
    goColumns (CreateSchemaStmt _) = return ()
    goColumns (GrantStmt _) = return ()
    goColumns (RevokeStmt _) = return ()
    goColumns (BeginStmt _) = return ()
    goColumns (CommitStmt _) = return ()
    goColumns (RollbackStmt _) = return ()
    goColumns (ExplainStmt _ s) = goColumns s
    goColumns (EmptyStmt _) = return ()

instance HasColumns (Query ResolvedNames a) where
    goColumns (QuerySelect _ select) = goColumns select
    goColumns (QueryExcept _ cc lhs rhs) = goColumnsComposed cc [lhs, rhs]
    goColumns (QueryUnion _ _ cc lhs rhs) = goColumnsComposed cc [lhs, rhs]
    goColumns (QueryIntersect _ cc lhs rhs) = goColumnsComposed cc [lhs, rhs]
    goColumns (QueryWith _ ctes query) = goColumns query >> mapM_ goColumns ctes
    goColumns (QueryOrder _ orders query) = sequence_
        [ bindClause "ORDER" $ mapM_ (handleOrderTopLevel query) orders
        , goColumns query
        ]
    goColumns (QueryLimit _ _ query) = goColumns query
    goColumns (QueryOffset _ _ query) = goColumns query

goColumnsComposed :: ColumnAliasList a -> [Query ResolvedNames a] -> Observer
goColumnsComposed (ColumnAliasList as) qs = do
    mapM_ goColumns qs
    let deps = map S.unions $ transpose $ map queryColumnDeps qs
    tell $ zipWith aliasObservation as deps

handleOrderTopLevel :: Query ResolvedNames a -> Order ResolvedNames a -> Observer
handleOrderTopLevel query (Order _ posOrExpr _ _) = case posOrExpr of
    PositionOrExprPosition _ pos _ -> handlePos pos query
    PositionOrExprExpr expr -> goColumns expr

handlePos :: Int -> Query ResolvedNames a -> Observer
handlePos pos (QuerySelect _ select) = do
    let selections = selectColumnsList $ selectCols select
        starsConcatted = selections >>= (\case
                                             SelectStar _ _ (StarColumnNames refs) -> refs
                                             SelectExpr _ cAliases _ -> map RColumnAlias cAliases
                                        )
        posRef = starsConcatted !! (pos - 1)  -- SQL is 1 indexed, Haskell is 0 indexed
    clause <- ask
    tell $ [clauseObservation posRef clause]
handlePos pos (QueryExcept _ _ lhs rhs) = handlePos pos lhs >> handlePos pos rhs
handlePos pos (QueryUnion _ _ _ lhs rhs) = handlePos pos lhs >> handlePos pos rhs
handlePos pos (QueryIntersect _ _ lhs rhs) = handlePos pos lhs >> handlePos pos rhs
handlePos pos (QueryWith _ _ q) = handlePos pos q
handlePos pos (QueryOrder _ _ q) = handlePos pos q
handlePos pos (QueryLimit _ _ q) = handlePos pos q
handlePos pos (QueryOffset _ _ q) = handlePos pos q


instance HasColumns (CTE ResolvedNames a) where
    goColumns CTE{..} = do
        -- recurse to emit clause infos
        goColumns cteQuery

        -- also emit alias infos
        case cteColumns of
            [] -> return ()
            aliases -> tell $ zipWith aliasObservation aliases (queryColumnDeps cteQuery)

-- for every column returned by the query, what columns did it depend on?
queryColumnDeps :: Query ResolvedNames a -> [Set (RColumnRef ())]
queryColumnDeps = map (S.singleton . void) . queryColumnNames

instance HasColumns (Insert ResolvedNames a) where
    goColumns Insert{..} = bindClause "INSERT" $ goColumns insertValues

instance HasColumns (InsertValues ResolvedNames a) where
    goColumns (InsertExprValues _ e) = goColumns e
    goColumns (InsertSelectValues q) = goColumns q
    goColumns (InsertDefaultValues _) = return ()
    goColumns (InsertDataFromFile _ _) = return ()

instance HasColumns (DefaultExpr ResolvedNames a) where
    goColumns (DefaultValue _) = return ()
    goColumns (ExprValue e) = goColumns e


instance HasColumns (Update ResolvedNames a) where
    goColumns Update{..} = bindClause "UPDATE" $ do
        mapM_ (goColumns . snd) updateSetExprs
        mapM_ goColumns updateFrom
        mapM_ goColumns updateWhere


instance HasColumns (Delete ResolvedNames a) where
    goColumns (Delete _ _ expr) = bindClause "WHERE" $ goColumns expr


instance HasColumns (CreateTable d ResolvedNames a) where
    goColumns CreateTable{..} = bindClause "CREATE" $ do
        -- TODO handle createTableExtra, and the dialect instances
        goColumns createTableDefinition

instance HasColumns (TableDefinition d ResolvedNames a) where
    goColumns (TableColumns _ cs) = goColumns cs
    goColumns (TableLike _ _) = return ()
    goColumns (TableAs _ _ query) = goColumns query
    goColumns (TableNoColumnInfo _) = return ()

instance HasColumns (ColumnOrConstraint d ResolvedNames a) where
    goColumns (ColumnOrConstraintColumn c) = goColumns c
    goColumns (ColumnOrConstraintConstraint _) = return ()

instance HasColumns (ColumnDefinition d ResolvedNames a) where
    goColumns ColumnDefinition{..} = goColumns columnDefinitionDefault


instance HasColumns (AlterTable ResolvedNames a) where
    goColumns (AlterTableRenameTable _ _ _) = return ()
    goColumns (AlterTableRenameColumn _ _ _ _) = return ()
    goColumns (AlterTableAddColumns _ _ _) = return ()


instance HasColumns (CreateView ResolvedNames a) where
    goColumns CreateView{..} = bindClause "CREATE" $ goColumns createViewQuery


instance HasColumns (Select ResolvedNames a) where
    goColumns select@(Select {..}) = sequence_
        [ bindClause "SELECT" $ goColumns $ selectCols
        , bindClause "FROM" $ goColumns selectFrom
        , bindClause "WHERE" $ goColumns selectWhere
        , bindClause "TIMESERIES" $ goColumns selectTimeseries
        , bindClause "GROUPBY" $ handleGroup select selectGroup
        , bindClause "HAVING" $ goColumns selectHaving
        , bindClause "NAMEDWINDOW" $ goColumns selectNamedWindow
        ]

instance HasColumns (SelectColumns ResolvedNames a) where
    goColumns (SelectColumns _ selections) = mapM_ goColumns selections

instance HasColumns (SelectFrom ResolvedNames a) where
    goColumns (SelectFrom _ tablishes) = mapM_ goColumns tablishes

instance HasColumns (SelectWhere ResolvedNames a) where
    goColumns (SelectWhere _ condition) = goColumns condition

instance HasColumns (SelectTimeseries ResolvedNames a) where
    goColumns (SelectTimeseries _ alias _ partition order) = do
        -- recurse to emit clause infos
        goColumns partition
        bindClause "ORDER" $ goColumns order

        -- also emit alias infos
        clause <- ask
        let observations = execWriter $ runReaderT (goColumns order) clause
            cols = S.fromList $ map fst $ rights observations
        tell $ [aliasObservation alias cols]

instance HasColumns (Partition ResolvedNames a) where
    goColumns (PartitionBy _ exprs) = bindClause "PARTITION" $ mapM_ goColumns exprs
    goColumns (PartitionBest _) = return ()
    goColumns (PartitionNodes _) = return ()

handleGroup :: Select ResolvedNames a -> Maybe (SelectGroup ResolvedNames a) -> Observer
handleGroup _ Nothing = return ()
handleGroup select (Just (SelectGroup _ groupingElements)) = mapM_ handleElement groupingElements
  where
    handleElement (GroupingElementExpr _ (PositionOrExprExpr expr)) =
        goColumns expr
    handleElement (GroupingElementExpr _ (PositionOrExprPosition _ pos _)) =
        handlePos pos $ QuerySelect (selectInfo select) select
    handleElement (GroupingElementSet _ exprs) =
        mapM_ goColumns exprs

instance HasColumns (SelectHaving ResolvedNames a) where
    goColumns (SelectHaving _ havings) = mapM_ goColumns havings

instance HasColumns (SelectNamedWindow ResolvedNames a) where
    goColumns (SelectNamedWindow _ windowExprs) = mapM_ goColumns windowExprs


instance HasColumns (Selection ResolvedNames a) where
    goColumns (SelectStar _ _ starColumns) = goColumns starColumns
    goColumns (SelectExpr _ aliases expr) = do
        -- recurse to emit clause infos
        goColumns expr

        -- also emit alias infos
        clause <- ask
        let observations = execWriter $ runReaderT (goColumns expr) clause
            cols = S.fromList $ map fst $ rights observations
        tell $ map (\a -> aliasObservation a cols) aliases

instance HasColumns (StarColumnNames a) where
    goColumns (StarColumnNames rColumnRefs) = mapM_ goColumns rColumnRefs

instance HasColumns (RColumnRef a) where
    -- treat RColumnRef and RColumnAlias the same, here :)
    goColumns ref = do
        clause <- ask
        tell $ [clauseObservation ref clause]


instance HasColumns (Tablish ResolvedNames a) where
    goColumns (TablishTable _ tablishAliases tableRef) = do
        -- no clause infos to emit
        -- but there are potentially alias infos
        case tablishAliases of
            TablishAliasesNone -> return ()
            TablishAliasesT _ -> return ()
            TablishAliasesTC _ cAliases -> 
                let cRefSets = map S.singleton $ getColumnList tableRef
                 in tell $ zipWith aliasObservation cAliases cRefSets

    goColumns (TablishSubQuery _ tablishAliases query) = do
        -- recurse to emit clause infos
        bindClause "SUBQUERY" $ goColumns query

        -- also emit alias infos (if any)
        case tablishAliases of
            TablishAliasesNone -> return ()
            TablishAliasesT _ -> return ()
            TablishAliasesTC _ cAliases ->
                tell $ zipWith aliasObservation cAliases (queryColumnDeps query)

    goColumns (TablishParenthesizedRelation _ tablishAliases relation) = do
        goColumns relation

        case tablishAliases of
            TablishAliasesNone -> return ()
            TablishAliasesT _ -> return ()
            TablishAliasesTC _ cAliases ->
                let cRefSets = map S.singleton $ tablishColumnNames relation
                 in tell $ zipWith aliasObservation cAliases cRefSets

    goColumns (TablishJoin _ _ cond lhs rhs) = do
        bindClause "JOIN" $ goColumns cond
        goColumns lhs
        goColumns rhs

    goColumns (TablishLateralView _ LateralView{..} lhs) = do
        -- recurse to emit clause infos
        bindClause "LATERALVIEW" $ do
            goColumns lhs
            mapM_ goColumns lateralViewExprs

        -- also emit alias infos (if any)
        --
        -- NB this is tricky. In general, lateral views (like UNNEST) can
        -- expand their input exprs into variable numbers of columns. E.g. in
        -- Presto, UNNEST will expand arrays into 1 col and maps into 2
        -- cols. Since we don't keep track of column types, we can't map column
        -- aliases to the (Set RColumnRefs) they refer to in the general case
        -- :-( So let's just handle the particular case where lateralViewExpr
        -- is a singleton list :-)
        case lateralViewAliases of
            TablishAliasesNone -> return ()
            TablishAliasesT _ -> return ()
            TablishAliasesTC _ cAliases -> case lateralViewExprs of
                [FunctionExpr _ _ _ [e] _ _ _] ->
                       let observations = execWriter $ runReaderT (goColumns e) baseClause
                           refs = S.fromList $ map fst $ rights observations
                        in tell $ zipWith aliasObservation cAliases (repeat refs)
                _ -> return () -- alas, the general case

instance HasColumns (LateralView ResolvedNames a) where
    goColumns (LateralView _ _ exprs _ _) = mapM_ goColumns exprs

instance HasColumns (JoinCondition ResolvedNames a) where
    goColumns (JoinNatural _ cs) = goColumns cs
    goColumns (JoinOn expr) = goColumns expr
    goColumns (JoinUsing _ cs) = mapM_ goColumns cs

instance HasColumns (RNaturalColumns a) where
    goColumns (RNaturalColumns cs) = mapM_ goColumns cs

instance HasColumns (RUsingColumn a) where
    goColumns (RUsingColumn c1 c2) = goColumns c1 >> goColumns c2


instance HasColumns (NamedWindowExpr ResolvedNames a) where
    goColumns (NamedWindowExpr _ _ expr) = goColumns expr
    goColumns (NamedPartialWindowExpr _ _ expr) = goColumns expr


handleOrderForWindow :: Order ResolvedNames a -> Observer
handleOrderForWindow (Order _ (PositionOrExprPosition _ _ _) _ _) = error "unexpected positional reference"
handleOrderForWindow (Order _ (PositionOrExprExpr expr) _ _) = goColumns expr

instance HasColumns (WindowExpr ResolvedNames a) where
    goColumns (WindowExpr _ partition orders _) = do
        goColumns partition
        bindClause "ORDER" $ mapM_ handleOrderForWindow orders

instance HasColumns (PartialWindowExpr ResolvedNames a) where
    goColumns (PartialWindowExpr _ _ partition orders _) = do
        goColumns partition
        bindClause "ORDER" $ mapM_ handleOrderForWindow orders


instance HasColumns (Expr ResolvedNames a) where
    goColumns (BinOpExpr _ _ lhs rhs) = mapM_ goColumns [lhs, rhs]
    goColumns (CaseExpr _ whens else') = do
        mapM_ ( \ (when', then') -> goColumns when' >> goColumns then') whens
        goColumns else'
    goColumns (UnOpExpr _ _ expr) = goColumns expr
    goColumns (LikeExpr _ _ escape pattern expr) = do
        goColumns escape
        goColumns pattern
        goColumns expr
    goColumns (ConstantExpr _ _) = return ()
    goColumns (ColumnExpr _ c) = goColumns c
    goColumns (InListExpr _ exprs expr) = mapM_ goColumns (expr:exprs)
    goColumns (InSubqueryExpr _ query expr) = do
        goColumns query
        goColumns expr
    goColumns (BetweenExpr _ expr start end) = mapM_ goColumns [expr, start, end]
    goColumns (OverlapsExpr _ (e1, e2) (e3, e4)) = mapM_ goColumns [e1, e2, e3, e4]
    goColumns (FunctionExpr _ _ _ exprs params filter' over) = do
        mapM_ goColumns exprs
        mapM_ (goColumns . snd) params
        goColumns filter'
        goColumns over
    goColumns (AtTimeZoneExpr _ expr tz) = mapM_ goColumns [expr, tz]
    goColumns (SubqueryExpr _ query) = bindClause "SUBQUERY" $ goColumns query
    goColumns (ArrayExpr _ exprs) = mapM_ goColumns exprs
    goColumns (ExistsExpr _ query) = goColumns query
    goColumns (FieldAccessExpr _ expr _) = goColumns expr -- NB we aren't emitting any special info about field access (for now)
    goColumns (ArrayAccessExpr _ expr idx) = mapM_ goColumns [expr, idx] -- NB we aren't emitting any special info about array access (for now)
    goColumns (TypeCastExpr _ _ expr _) = goColumns expr
    goColumns (VariableSubstitutionExpr _) = return ()
    goColumns (LambdaParamExpr _ _) = return ()
    goColumns (LambdaExpr _ _ body) = goColumns body

instance HasColumns (Escape ResolvedNames a) where
    goColumns (Escape expr) = goColumns expr

instance HasColumns (Pattern ResolvedNames a) where
    goColumns (Pattern expr) = goColumns expr

instance HasColumns (Filter ResolvedNames a) where
    goColumns (Filter _ expr) = goColumns expr

instance HasColumns (OverSubExpr ResolvedNames a) where
    goColumns (OverWindowExpr _ expr) = goColumns expr
    goColumns (OverWindowName _ _) = return ()
    goColumns (OverPartialWindowExpr _ expr) = goColumns expr
