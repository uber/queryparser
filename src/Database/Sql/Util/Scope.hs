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
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Sql.Util.Scope
    ( runResolverWarn, runResolverWError, runResolverNoWarn
    , WithColumns (..)
    , queryColumnNames, tablishColumnNames
    , resolveStatement, resolveQuery, resolveQueryWithColumns, resolveSelectAndOrders, resolveCTE, resolveInsert
    , resolveInsertValues, resolveDefaultExpr, resolveDelete, resolveTruncate
    , resolveCreateTable, resolveTableDefinition, resolveColumnOrConstraint
    , resolveColumnDefinition, resolveAlterTable, resolveDropTable
    , resolveSelectColumns, resolvedTableHasName, resolvedTableHasSchema
    , resolveSelection, resolveExpr, resolveTableName, resolveDropTableName
    , resolveCreateSchemaName, resolveSchemaName
    , resolveTableRef, resolveColumnName, resolvePartition, resolveSelectFrom
    , resolveTablish, resolveJoinCondition, resolveSelectWhere, resolveSelectTimeseries
    , resolveSelectGroup, resolveSelectHaving, resolveOrder
    , selectionNames, mkTableSchemaMember
    ) where

import Data.Maybe (mapMaybe)
import Data.Either (lefts, rights)
import Data.List (find)
import Database.Sql.Type

import qualified Data.List.NonEmpty as NonEmpty
import qualified Data.Text.Lazy as TL
import           Data.Text.Lazy (Text)

import Control.Applicative (liftA2)
import Control.Monad.State
import Control.Monad.Reader
import Control.Monad.Writer
import Control.Monad.Except
import Control.Monad.Identity

import Control.Arrow (first, (&&&))

import Data.Proxy (Proxy (..))


makeResolverInfo :: Dialect d => Proxy d -> Catalog -> ResolverInfo a
makeResolverInfo dialect catalog = ResolverInfo
    { bindings = emptyBindings
    , onCTECollision =
        if shouldCTEsShadowTables dialect
         then \ f x -> f x
         else \ _ x -> x
    , lambdaScope = []
    , selectScope = getSelectScope dialect
    , lcolumnsAreVisibleInLateralViews = areLcolumnsVisibleInLateralViews dialect
    , ..
    }

makeColumnAlias :: a -> Text -> Resolver ColumnAlias a
makeColumnAlias r alias = ColumnAlias r alias . ColumnAliasId <$> getNextCounter
  where
    getNextCounter = modify (subtract 1) >> get

runResolverWarn :: Dialect d => Resolver r a -> Proxy d -> Catalog -> (Either (ResolutionError a) (r a), [Either (ResolutionError a) (ResolutionSuccess a)])
runResolverWarn resolver dialect catalog = runWriter $ runExceptT $ runReaderT (evalStateT resolver 0) $ makeResolverInfo dialect catalog


runResolverWError :: Dialect d => Resolver r a -> Proxy d -> Catalog -> Either [ResolutionError a] ((r a), [ResolutionSuccess a])
runResolverWError resolver dialect catalog =
    let (result, warningsSuccesses) = runResolverWarn resolver dialect catalog
        warnings = lefts warningsSuccesses
        successes = rights warningsSuccesses
     in case (result, warnings) of
            (Right x, []) -> Right (x, successes)
            (Right _, ws) -> Left ws
            (Left e, ws) -> Left (e:ws)


runResolverNoWarn :: Dialect d => Resolver r a -> Proxy d -> Catalog -> Either (ResolutionError a) (r a)
runResolverNoWarn resolver dialect catalog = fst $ runResolverWarn resolver dialect catalog


resolveStatement :: Dialect d => Statement d RawNames a -> Resolver (Statement d ResolvedNames) a
resolveStatement (QueryStmt stmt) = QueryStmt <$> resolveQuery stmt
resolveStatement (InsertStmt stmt) = InsertStmt <$> resolveInsert stmt
resolveStatement (UpdateStmt stmt) = UpdateStmt <$> resolveUpdate stmt
resolveStatement (DeleteStmt stmt) = DeleteStmt <$> resolveDelete stmt
resolveStatement (TruncateStmt stmt) = TruncateStmt <$> resolveTruncate stmt
resolveStatement (CreateTableStmt stmt) = CreateTableStmt <$> resolveCreateTable stmt
resolveStatement (AlterTableStmt stmt) = AlterTableStmt <$> resolveAlterTable stmt
resolveStatement (DropTableStmt stmt) = DropTableStmt <$> resolveDropTable stmt
resolveStatement (CreateViewStmt stmt) = CreateViewStmt <$> resolveCreateView stmt
resolveStatement (DropViewStmt stmt) = DropViewStmt <$> resolveDropView stmt
resolveStatement (CreateSchemaStmt stmt) = CreateSchemaStmt <$> resolveCreateSchema stmt
resolveStatement (GrantStmt stmt) = pure $ GrantStmt stmt
resolveStatement (RevokeStmt stmt) = pure $ RevokeStmt stmt
resolveStatement (BeginStmt info) = pure $ BeginStmt info
resolveStatement (CommitStmt info) = pure $ CommitStmt info
resolveStatement (RollbackStmt info) = pure $ RollbackStmt info
resolveStatement (ExplainStmt info stmt) = ExplainStmt info <$> resolveStatement stmt
resolveStatement (EmptyStmt info) = pure $ EmptyStmt info

resolveQuery :: Query RawNames a -> Resolver (Query ResolvedNames) a
resolveQuery = (withColumnsValue <$>) . resolveQueryWithColumns

resolveQueryWithColumns :: Query RawNames a -> Resolver (WithColumns (Query ResolvedNames)) a
resolveQueryWithColumns (QuerySelect info select) = do
    WithColumnsAndOrders select' columns _  <- resolveSelectAndOrders select []
    pure $ WithColumns (QuerySelect info select') columns
resolveQueryWithColumns (QueryExcept info Unused lhs rhs) = do
    WithColumns lhs' columns <- resolveQueryWithColumns lhs
    rhs' <- resolveQuery rhs
    cs <- forM (queryColumnNames lhs') $ \case
        RColumnRef QColumnName{..} -> makeColumnAlias columnNameInfo columnNameName
        RColumnAlias (ColumnAlias aliasInfo name _) -> makeColumnAlias aliasInfo name
    pure $ WithColumns (QueryExcept info (ColumnAliasList cs) lhs' rhs') columns

resolveQueryWithColumns (QueryUnion info distinct Unused lhs rhs) = do
    WithColumns lhs' columns <- resolveQueryWithColumns lhs
    rhs' <- resolveQuery rhs
    cs <- forM (queryColumnNames lhs') $ \case
        RColumnRef QColumnName{..} -> makeColumnAlias columnNameInfo columnNameName
        RColumnAlias (ColumnAlias aliasInfo name _) -> makeColumnAlias aliasInfo name
    pure $ WithColumns (QueryUnion info distinct (ColumnAliasList cs) lhs' rhs') columns

resolveQueryWithColumns (QueryIntersect info Unused lhs rhs) = do
    WithColumns lhs' columns <- resolveQueryWithColumns lhs
    rhs' <- resolveQuery rhs
    cs <- forM (queryColumnNames lhs') $ \case
        RColumnRef QColumnName{..} -> makeColumnAlias columnNameInfo columnNameName
        RColumnAlias (ColumnAlias aliasInfo name _) -> makeColumnAlias aliasInfo name
    pure $ WithColumns (QueryIntersect info (ColumnAliasList cs) lhs' rhs') columns

resolveQueryWithColumns (QueryWith info [] query) = overWithColumns (QueryWith info []) <$> resolveQueryWithColumns query
resolveQueryWithColumns (QueryWith info (cte:ctes) query) = do
    cte' <- resolveCTE cte
    Catalog{..} <- asks catalog

    let TableAlias _ alias _ = cteAlias cte'

    updateBindings <- fmap ($ local (mapBindings $ bindCTE cte')) $
        case catalogHasTable $ QTableName () None alias of
            Exists -> asks onCTECollision
            DoesNotExist -> pure id

    ~(WithColumns (QueryWith _ ctes' query') columns) <- updateBindings $ resolveQueryWithColumns $ QueryWith info ctes query
    pure $ WithColumns (QueryWith info (cte':ctes') query') columns

resolveQueryWithColumns (QueryOrder info orders query) = do
    WithColumns query' columns <- resolveQueryWithColumns query

    ResolvedOrders orders' <- resolveOrders query orders

    pure $ WithColumns (QueryOrder info orders' query') columns

resolveQueryWithColumns (QueryLimit info limit query) = overWithColumns (QueryLimit info limit) <$> resolveQueryWithColumns query
resolveQueryWithColumns (QueryOffset info offset query) = overWithColumns (QueryOffset info offset) <$> resolveQueryWithColumns query


newtype ResolvedOrders a = ResolvedOrders [Order ResolvedNames a]

resolveOrders :: Query RawNames a -> [Order RawNames a] -> Resolver ResolvedOrders a
resolveOrders query orders = case query of
    QuerySelect _ s -> do
        -- dispatch to dialect specific binding rules :)
        WithColumnsAndOrders _ _ os <- resolveSelectAndOrders s orders
        pure $ ResolvedOrders os
    q@(QueryExcept _ _ _ _) -> do
        ~(q'@(QueryExcept _ (ColumnAliasList cs) _ _)) <- resolveQuery q
        let exprs = map (\ c@(ColumnAlias info _ _) -> ColumnExpr info $ RColumnAlias c) cs
        bindAliasedColumns (queryColumnNames q') $ ResolvedOrders <$> mapM (resolveOrder exprs) orders
    q@(QueryUnion _ _ _ _ _) -> do
        ~(q'@(QueryUnion _ _ (ColumnAliasList cs) _ _)) <- resolveQuery q
        let exprs = map (\ c@(ColumnAlias info _ _) -> ColumnExpr info $ RColumnAlias c) cs
        bindAliasedColumns (queryColumnNames q') $ ResolvedOrders <$> mapM (resolveOrder exprs) orders
    q@(QueryIntersect _ _ _ _) -> do
        ~(q'@(QueryIntersect _ (ColumnAliasList cs) _ _)) <- resolveQuery q
        let exprs = map (\ c@(ColumnAlias info _ _) -> ColumnExpr info $ RColumnAlias c) cs
        bindAliasedColumns (queryColumnNames q') $ ResolvedOrders <$> mapM (resolveOrder exprs) orders
    QueryWith _ _ _ -> error "unexpected AST: QueryOrder enclosing QueryWith"
    QueryOrder _ _ q -> do
        -- this case (nested orders) is possible in presto, but not vertica or hive
        resolveOrders q orders
    QueryLimit _ _ q -> resolveOrders q orders
    QueryOffset _ _ q -> resolveOrders q orders


bindCTE :: CTE ResolvedNames a -> Bindings a -> Bindings a
bindCTE CTE{..} =
    let columns =
            case cteColumns of
                [] -> queryColumnNames cteQuery
                cs -> map RColumnAlias cs
        cte = (cteAlias, columns)
     in \ Bindings{..} -> Bindings{boundCTEs = cte:boundCTEs, ..}


selectionNames :: Selection ResolvedNames a -> [RColumnRef a]
selectionNames (SelectExpr _ [alias] (ColumnExpr _ ref)) =
    let refName = case ref of
            RColumnRef (QColumnName _ _ name) -> name
            RColumnAlias (ColumnAlias _ name _) -> name
        ColumnAlias _ aliasName _ = alias
     in if (refName == aliasName) then [ref] else [RColumnAlias alias]
selectionNames (SelectExpr _ aliases _) = map RColumnAlias aliases
selectionNames (SelectStar _ _ (StarColumnNames referents)) = referents


selectionExprs :: Selection ResolvedNames a -> [Expr ResolvedNames a]
selectionExprs (SelectExpr info aliases _) = map (ColumnExpr info . RColumnAlias) aliases
selectionExprs (SelectStar info _ (StarColumnNames referents)) = map (ColumnExpr info) referents


queryColumnNames :: Query ResolvedNames a -> [RColumnRef a]
queryColumnNames (QuerySelect _ Select{selectCols = SelectColumns _ cols}) = cols >>= selectionNames
queryColumnNames (QueryExcept _ (ColumnAliasList cs) _ _) = map RColumnAlias cs
queryColumnNames (QueryUnion _ _ (ColumnAliasList cs) _ _) = map RColumnAlias cs
queryColumnNames (QueryIntersect _ (ColumnAliasList cs) _ _) = map RColumnAlias cs
queryColumnNames (QueryWith _ _ query) = queryColumnNames query
queryColumnNames (QueryOrder _ _ query) = queryColumnNames query
queryColumnNames (QueryLimit _ _ query) = queryColumnNames query
queryColumnNames (QueryOffset _ _ query) = queryColumnNames query


tablishColumnNames :: Tablish ResolvedNames a -> [RColumnRef a]
tablishColumnNames (TablishTable _ tablishAliases tableRef) =
    case tablishAliases of
        TablishAliasesNone -> getColumnList tableRef
        TablishAliasesT _ -> getColumnList tableRef
        TablishAliasesTC _ cAliases -> map RColumnAlias cAliases

tablishColumnNames (TablishSubQuery _ tablishAliases query) =
    case tablishAliases of
        TablishAliasesNone -> queryColumnNames query
        TablishAliasesT _ -> queryColumnNames query
        TablishAliasesTC _ cAliases -> map RColumnAlias cAliases

tablishColumnNames (TablishParenthesizedRelation _ tablishAliases relation) =
    case tablishAliases of
        TablishAliasesNone -> tablishColumnNames relation
        TablishAliasesT _ -> tablishColumnNames relation
        TablishAliasesTC _ cAliases -> map RColumnAlias cAliases

tablishColumnNames (TablishJoin _ _ _ lhs rhs) =
    tablishColumnNames lhs ++ tablishColumnNames rhs

tablishColumnNames (TablishLateralView _ LateralView{..} lhs) =
    let cols = maybe [] tablishColumnNames lhs in
    case lateralViewAliases of
        TablishAliasesNone -> cols
        TablishAliasesT _ -> cols
        TablishAliasesTC _ cAliases -> cols ++ map RColumnAlias cAliases
    

resolveSelectAndOrders :: Select RawNames a -> [Order RawNames a] -> Resolver (WithColumnsAndOrders (Select ResolvedNames)) a
resolveSelectAndOrders Select{..} orders = do
    (selectFrom', columns) <- traverse resolveSelectFrom selectFrom >>= \case
        Nothing -> pure (Nothing, [])
        Just (WithColumns selectFrom' columns) -> pure (Just selectFrom', columns)

    selectTimeseries' <- traverse (bindColumns columns . resolveSelectTimeseries) selectTimeseries

    maybeBindTimeSlice selectTimeseries' $ do
        selectCols' <- bindColumns columns $ resolveSelectColumns selectCols

        let selectedAliases = selectionNames =<< selectColumnsList selectCols'
            selectedExprs = selectionExprs =<< selectColumnsList selectCols'

        SelectScope{..} <- (\ f -> f columns selectedAliases) <$> asks selectScope

        selectHaving' <- bindForHaving $ traverse resolveSelectHaving selectHaving
        selectWhere' <- bindForWhere $ traverse resolveSelectWhere selectWhere
        selectGroup' <- bindForGroup $ traverse (resolveSelectGroup selectedExprs) selectGroup
        selectNamedWindow' <- bindForNamedWindow $ traverse resolveSelectNamedWindow selectNamedWindow
        orders' <- bindForOrder $ mapM (resolveOrder selectedExprs) orders
        let select = Select { selectCols = selectCols'
                            , selectFrom = selectFrom'
                            , selectWhere = selectWhere'
                            , selectTimeseries = selectTimeseries'
                            , selectGroup = selectGroup'
                            , selectHaving = selectHaving'
                            , selectNamedWindow = selectNamedWindow'
                            , ..
                            }
        pure $ WithColumnsAndOrders select columns orders'
  where
    maybeBindTimeSlice Nothing = id
    maybeBindTimeSlice (Just timeseries) = bindColumns [(Nothing, [RColumnAlias $ selectTimeseriesSliceName timeseries])]


resolveCTE :: CTE RawNames a -> Resolver (CTE ResolvedNames) a
resolveCTE CTE{..} = do
    cteQuery' <- resolveQuery cteQuery
    pure $ CTE
        { cteQuery = cteQuery'
        , ..
        }

resolveInsert :: Insert RawNames a -> Resolver (Insert ResolvedNames) a
resolveInsert Insert{..} = do
    insertTable'@(RTableName fqtn _) <- resolveTableName insertTable
    let insertColumns' = fmap (fmap (\uqcn -> RColumnRef $ uqcn { columnNameTable = Identity fqtn })) insertColumns
    insertValues' <- resolveInsertValues insertValues
    pure $ Insert
        { insertTable = insertTable'
        , insertColumns = insertColumns'
        , insertValues = insertValues'
        , ..
        }

resolveInsertValues :: InsertValues RawNames a -> Resolver (InsertValues ResolvedNames) a
resolveInsertValues (InsertExprValues info exprs) = InsertExprValues info <$> mapM (mapM resolveDefaultExpr) exprs
resolveInsertValues (InsertSelectValues query) = InsertSelectValues <$> resolveQuery query
resolveInsertValues (InsertDefaultValues info) = pure $ InsertDefaultValues info
resolveInsertValues (InsertDataFromFile info path) = pure $ InsertDataFromFile info path

resolveDefaultExpr :: DefaultExpr RawNames a -> Resolver (DefaultExpr ResolvedNames) a
resolveDefaultExpr (DefaultValue info) = pure $ DefaultValue info
resolveDefaultExpr (ExprValue expr) = ExprValue <$> resolveExpr expr

resolveUpdate :: Update RawNames a -> Resolver (Update ResolvedNames) a
resolveUpdate Update{..} = do
    updateTable'@(RTableName fqtn _) <- resolveTableName updateTable

    let tgtTableRef = rTableNameToRTableRef updateTable'
        tgtColRefs = getColumnList tgtTableRef
        tgtColSet = case updateAlias of
            Just alias -> (Just $ RTableAlias alias tgtColRefs, tgtColRefs)
            Nothing -> (Just tgtTableRef, tgtColRefs)

    (updateFrom', srcColSet) <- case updateFrom of
        Just tablish -> resolveTablish tablish >>= (\ (WithColumns t cs) -> return (Just t, cs))
        Nothing -> return (Nothing, [])

    updateSetExprs' <- bindColumns srcColSet $
        mapM (\(uqcn, expr) -> (RColumnRef uqcn { columnNameTable = Identity fqtn},) <$> resolveDefaultExpr expr) updateSetExprs

    updateWhere' <- bindColumns (tgtColSet:srcColSet) $ mapM resolveExpr updateWhere

    pure $ Update
        { updateTable = updateTable'
        , updateSetExprs = updateSetExprs'
        , updateFrom = updateFrom'
        , updateWhere = updateWhere'
        , ..
        }

resolveDelete :: forall a . Delete RawNames a -> Resolver (Delete ResolvedNames) a
resolveDelete (Delete info tableName expr) = do
    tableName'@(RTableName fqtn table@SchemaMember{..}) <- resolveTableName tableName
    when (tableType /= Table) $ throwError $ DeleteFromView fqtn
    let QTableName tableInfo _ _ = tableName
    bindColumns [(Just $ RTableRef fqtn table, map (\ (QColumnName () None column) -> RColumnRef $ QColumnName tableInfo (pure fqtn) column) columnsList)] $ do
        expr' <- traverse resolveExpr expr
        pure $ Delete info tableName' expr'


resolveTruncate :: Truncate RawNames a -> Resolver (Truncate ResolvedNames) a
resolveTruncate (Truncate info name) = do
    name' <- resolveTableName name
    pure $ Truncate info name'


resolveCreateTable :: forall d a . (Dialect d) => CreateTable d RawNames a -> Resolver (CreateTable d ResolvedNames) a
resolveCreateTable CreateTable{..} = do
    createTableName'@(RCreateTableName fqtn _) <- resolveCreateTableName createTableName createTableIfNotExists

    WithColumns createTableDefinition' columns <- resolveTableDefinition fqtn createTableDefinition
    bindColumns columns $ do
        createTableExtra' <- traverse (resolveCreateTableExtra (Proxy :: Proxy d)) createTableExtra
        pure $ CreateTable
            { createTableName = createTableName'
            , createTableDefinition = createTableDefinition'
            , createTableExtra = createTableExtra'
            , ..
            }



mkTableSchemaMember :: [UQColumnName ()] -> SchemaMember
mkTableSchemaMember columnsList = SchemaMember{..}
  where
    tableType = Table
    persistence = Persistent
    viewQuery = Nothing

resolveTableDefinition :: FQTableName a -> TableDefinition d RawNames a -> Resolver (WithColumns (TableDefinition d ResolvedNames)) a
resolveTableDefinition fqtn (TableColumns info cs) = do
    cs' <- mapM resolveColumnOrConstraint cs
    let columns = mapMaybe columnOrConstraintToColumn $ NonEmpty.toList cs'
        table = mkTableSchemaMember $ map (\ c -> c{columnNameInfo = (), columnNameTable = None}) columns
    pure $ WithColumns (TableColumns info cs') [(Just $ RTableRef fqtn table, map RColumnRef columns)]
  where
    columnOrConstraintToColumn (ColumnOrConstraintConstraint _) = Nothing
    columnOrConstraintToColumn (ColumnOrConstraintColumn ColumnDefinition{columnDefinitionName = QColumnName columnInfo None name}) =
        Just $ QColumnName columnInfo (pure fqtn) name


resolveTableDefinition _ (TableLike info name) = do
    name' <- resolveTableName name
    pure $ WithColumns (TableLike info name') []

resolveTableDefinition fqtn (TableAs info cols query) = do
    query' <- resolveQuery query
    let columns = queryColumnNames query'
        table = mkTableSchemaMember $ map toUQCN columns
        toUQCN (RColumnRef fqcn) = fqcn{columnNameInfo = (), columnNameTable = None}
        toUQCN (RColumnAlias (ColumnAlias _ cn _)) = QColumnName{..}
          where
            columnNameInfo = ()
            columnNameName = cn
            columnNameTable = None
    pure $ WithColumns (TableAs info cols query') [(Just $ RTableRef fqtn table, columns)]

resolveTableDefinition _ (TableNoColumnInfo info) = do
    pure $ WithColumns (TableNoColumnInfo info) []


resolveColumnOrConstraint :: ColumnOrConstraint d RawNames a -> Resolver (ColumnOrConstraint d ResolvedNames) a
resolveColumnOrConstraint (ColumnOrConstraintColumn column) = ColumnOrConstraintColumn <$> resolveColumnDefinition column
resolveColumnOrConstraint (ColumnOrConstraintConstraint constraint) = pure $ ColumnOrConstraintConstraint constraint


resolveColumnDefinition :: ColumnDefinition d RawNames a -> Resolver (ColumnDefinition d ResolvedNames) a
resolveColumnDefinition ColumnDefinition{..} = do
    columnDefinitionDefault' <- traverse resolveExpr columnDefinitionDefault
    pure $ ColumnDefinition
        { columnDefinitionDefault = columnDefinitionDefault'
        , ..
        }


resolveAlterTable :: AlterTable RawNames a -> Resolver (AlterTable ResolvedNames) a
resolveAlterTable (AlterTableRenameTable info old new) = do
    old'@(RTableName (QTableName _ (Identity oldSchema@(QSchemaName _ (Identity oldDb@(DatabaseName _ _)) _ oldSchemaType)) _) table) <- resolveTableName old

    let new'@(RTableName (QTableName _ (Identity (QSchemaName _ _ _ newSchemaType)) _) _) = case new of
            QTableName tInfo (Just (QSchemaName sInfo (Just db) s sType)) t ->
                RTableName (QTableName tInfo (pure (QSchemaName sInfo (pure db) s sType)) t) table

            QTableName tInfo (Just (QSchemaName sInfo Nothing s sType)) t ->
                RTableName (QTableName tInfo (pure (QSchemaName sInfo (pure oldDb) s sType)) t) table

            QTableName tInfo Nothing t ->
                RTableName (QTableName tInfo (pure oldSchema) t) table

    case (oldSchemaType, newSchemaType) of
        (NormalSchema, NormalSchema) -> pure ()
        (SessionSchema, SessionSchema) -> pure ()
        (NormalSchema, SessionSchema) -> error "can't rename a table into the session schema"
        (SessionSchema, NormalSchema) -> error "can't rename a table out of the session schema"

    pure $ AlterTableRenameTable info old' new'

resolveAlterTable (AlterTableRenameColumn info table old new) = do
    table' <- resolveTableName table
    pure $ AlterTableRenameColumn info table' old new
resolveAlterTable (AlterTableAddColumns info table columns) = do
    table' <- resolveTableName table
    pure $ AlterTableAddColumns info table' columns


resolveDropTable :: DropTable RawNames a -> Resolver (DropTable ResolvedNames) a
resolveDropTable DropTable{..} = do
    dropTableNames' <- mapM resolveDropTableName dropTableNames
    pure $ DropTable
        { dropTableNames = dropTableNames'
        , ..
        }


resolveCreateView :: CreateView RawNames a -> Resolver (CreateView ResolvedNames) a
resolveCreateView CreateView{..} = do
    createViewName' <- resolveCreateTableName createViewName createViewIfNotExists
    createViewQuery' <- resolveQuery createViewQuery
    pure $ CreateView
        { createViewName = createViewName'
        , createViewQuery = createViewQuery'
        , ..
        }


resolveDropView :: DropView RawNames a -> Resolver (DropView ResolvedNames) a
resolveDropView DropView{..} = do
    dropViewName' <- resolveDropTableName dropViewName
    pure $ DropView
        { dropViewName = dropViewName'
        , ..
        }


resolveCreateSchema :: CreateSchema RawNames a -> Resolver (CreateSchema ResolvedNames) a
resolveCreateSchema CreateSchema{..} = do
    createSchemaName' <- resolveCreateSchemaName createSchemaName createSchemaIfNotExists
    pure $ CreateSchema
        { createSchemaName = createSchemaName'
        , ..
        }


resolveSelectColumns :: SelectColumns RawNames a -> Resolver (SelectColumns ResolvedNames) a
resolveSelectColumns (SelectColumns info selections) = SelectColumns info <$> mapM resolveSelection selections


qualifiedOnly :: [(Maybe a, b)] -> [(a, b)]
qualifiedOnly = mapMaybe (\(mTable, cs) -> case mTable of
                              (Just t) -> Just (t, cs)
                              Nothing -> Nothing)

resolveSelection :: Selection RawNames a -> Resolver (Selection ResolvedNames) a
resolveSelection (SelectStar info Nothing Unused) = do
    columns <- asks (boundColumns . bindings)
    pure $ SelectStar info Nothing $ StarColumnNames $ map (const info <$>) $ snd =<< columns

resolveSelection (SelectStar info (Just oqtn@(QTableName _ (Just schema) _)) Unused) = do
    columns <- asks (boundColumns . bindings)
    let qualifiedColumns = qualifiedOnly columns
    case filter ((liftA2 (&&) (resolvedTableHasSchema schema) (resolvedTableHasName oqtn)) . fst) qualifiedColumns of
        [] -> throwError $ UnintroducedTable oqtn
        [(t, cs)] -> pure $ SelectStar info (Just t) $ StarColumnNames $ map (const info <$>) cs
        _ -> throwError $ AmbiguousTable oqtn

resolveSelection (SelectStar info (Just oqtn@(QTableName tableInfo Nothing table)) Unused) = do
    columns <- asks (boundColumns . bindings)
    let qualifiedColumns = qualifiedOnly columns
    case filter (resolvedTableHasName oqtn . fst) qualifiedColumns of
        [] -> throwError $ UnintroducedTable $ QTableName tableInfo Nothing table
        [(t, cs)] -> pure $ SelectStar info (Just t) $ StarColumnNames $ map (const info <$>) cs
        _ -> throwError $ AmbiguousTable $ QTableName tableInfo Nothing table

resolveSelection (SelectExpr info alias expr) = SelectExpr info alias <$> resolveExpr expr


resolveExpr :: Expr RawNames a -> Resolver (Expr ResolvedNames) a
resolveExpr (BinOpExpr info op lhs rhs) = BinOpExpr info op <$> resolveExpr lhs <*> resolveExpr rhs

resolveExpr (CaseExpr info whens else_) = CaseExpr info <$> mapM resolveWhen whens <*> traverse resolveExpr else_
  where
    resolveWhen (when_, then_) = (,) <$> resolveExpr when_ <*> resolveExpr then_

resolveExpr (UnOpExpr info op expr) = UnOpExpr info op <$> resolveExpr expr
resolveExpr (LikeExpr info op escape pattern expr) = do
    escape' <- traverse (fmap Escape . resolveExpr . escapeExpr) escape
    pattern' <- Pattern <$> resolveExpr (patternExpr pattern)
    expr' <- resolveExpr expr
    pure $ LikeExpr info op escape' pattern' expr'

resolveExpr (ConstantExpr info constant) = pure $ ConstantExpr info constant
resolveExpr (ColumnExpr info column) = resolveLambdaParamOrColumnName info column
resolveExpr (InListExpr info list expr) = InListExpr info <$> mapM resolveExpr list <*> resolveExpr expr
resolveExpr (InSubqueryExpr info query expr) = do
    query' <- resolveQuery query
    expr' <- resolveExpr expr
    pure $ InSubqueryExpr info query' expr'

resolveExpr (BetweenExpr info expr start end) =
    BetweenExpr info <$> resolveExpr expr <*> resolveExpr start <*> resolveExpr end

resolveExpr (OverlapsExpr info range1 range2) = OverlapsExpr info <$> resolveRange range1 <*> resolveRange range2
  where
    resolveRange (from, to) = (,) <$> resolveExpr from <*> resolveExpr to

resolveExpr (FunctionExpr info name distinct args params filter' over) =
    FunctionExpr info name distinct <$> mapM resolveExpr args <*> mapM resolveParam params <*> traverse resolveFilter filter' <*> traverse resolveOverSubExpr over
  where
    resolveParam (param, expr) = (param,) <$> resolveExpr expr
    -- T482568: expand named windows on resolve
    resolveOverSubExpr (OverWindowExpr i window) =
      OverWindowExpr i <$> resolveWindowExpr window
    resolveOverSubExpr (OverWindowName i windowName) =
      pure $ OverWindowName i windowName
    resolveOverSubExpr (OverPartialWindowExpr i partWindow) =
      OverPartialWindowExpr i <$> resolvePartialWindowExpr partWindow
    resolveFilter (Filter i expr) =
      Filter i <$> resolveExpr expr

resolveExpr (AtTimeZoneExpr info expr tz) = AtTimeZoneExpr info <$> resolveExpr expr <*> resolveExpr tz
resolveExpr (SubqueryExpr info query) = SubqueryExpr info <$> resolveQuery query
resolveExpr (ArrayExpr info array) = ArrayExpr info <$> mapM resolveExpr array
resolveExpr (ExistsExpr info query) = ExistsExpr info <$> resolveQuery query
resolveExpr (FieldAccessExpr info expr field) = FieldAccessExpr info <$> resolveExpr expr <*> pure field
resolveExpr (ArrayAccessExpr info expr idx) = ArrayAccessExpr info <$> resolveExpr expr <*> resolveExpr idx
resolveExpr (TypeCastExpr info onFail expr type_) = TypeCastExpr info onFail <$> resolveExpr expr <*> pure type_
resolveExpr (VariableSubstitutionExpr info) = pure $ VariableSubstitutionExpr info
resolveExpr (LambdaParamExpr info param) = pure $ LambdaParamExpr info param
resolveExpr (LambdaExpr info params body) = do
    expr <- bindLambdaParams params $ resolveExpr body
    pure $ LambdaExpr info params expr

resolveOrder :: [Expr ResolvedNames a]
             -> Order RawNames a
             -> Resolver (Order ResolvedNames) a
resolveOrder exprs (Order i posOrExpr direction nullPos) =
    Order i <$> resolvePositionOrExpr exprs posOrExpr <*> pure direction <*> pure nullPos

resolveWindowExpr :: WindowExpr RawNames a
                  -> Resolver (WindowExpr ResolvedNames) a
resolveWindowExpr WindowExpr{..} =
  do
    windowExprPartition' <- traverse resolvePartition windowExprPartition
    windowExprOrder' <- mapM (resolveOrder []) windowExprOrder
    pure $ WindowExpr
        { windowExprPartition = windowExprPartition'
        , windowExprOrder = windowExprOrder'
        , ..
        }

resolvePartialWindowExpr :: PartialWindowExpr RawNames a
                         -> Resolver (PartialWindowExpr ResolvedNames) a
resolvePartialWindowExpr PartialWindowExpr{..} =
  do
    partWindowExprOrder' <- mapM (resolveOrder []) partWindowExprOrder
    partWindowExprPartition' <- mapM resolvePartition partWindowExprPartition
    pure $ PartialWindowExpr
        { partWindowExprOrder = partWindowExprOrder'
        , partWindowExprPartition = partWindowExprPartition'
        , ..
        }

resolveNamedWindowExpr :: NamedWindowExpr RawNames a
                       -> Resolver (NamedWindowExpr ResolvedNames) a
resolveNamedWindowExpr (NamedWindowExpr info name window) =
  NamedWindowExpr info name <$> resolveWindowExpr window
resolveNamedWindowExpr (NamedPartialWindowExpr info name partWindow) =
  NamedPartialWindowExpr info name <$> resolvePartialWindowExpr partWindow

resolveTableName :: OQTableName a -> Resolver RTableName a
resolveTableName table = do
    Catalog{..} <- asks catalog
    lift $ lift $ catalogResolveTableName table

resolveCreateTableName :: CreateTableName RawNames a -> Maybe a -> Resolver (CreateTableName ResolvedNames) a
resolveCreateTableName tableName ifNotExists = do
    Catalog{..} <- asks catalog
    tableName'@(RCreateTableName fqtn existence) <- lift $ lift $ catalogResolveCreateTableName tableName

    when ((existence, void ifNotExists) == (Exists, Nothing)) $ tell [ Left $ UnexpectedTable fqtn ]

    pure $ tableName'

resolveDropTableName :: DropTableName RawNames a -> Resolver (DropTableName ResolvedNames) a
resolveDropTableName tableName = do
    (getName <$> resolveTableName tableName)
        `catchError` handleMissing
  where
    getName (RTableName name table) = RDropExistingTableName name table
    handleMissing (MissingTable name) = pure $ RDropMissingTableName name
    handleMissing e = throwError e


resolveCreateSchemaName :: CreateSchemaName RawNames a -> Maybe a -> Resolver (CreateSchemaName ResolvedNames) a
resolveCreateSchemaName schemaName ifNotExists = do
    Catalog{..} <- asks catalog
    schemaName'@(RCreateSchemaName fqsn existence) <- lift $ lift $ catalogResolveCreateSchemaName schemaName
    when ((existence, void ifNotExists) == (Exists, Nothing)) $ tell [ Left $ UnexpectedSchema fqsn ]
    pure schemaName'

resolveSchemaName :: SchemaName RawNames a -> Resolver (SchemaName ResolvedNames) a
resolveSchemaName schemaName = do
    Catalog{..} <- asks catalog
    lift $ lift $ catalogResolveSchemaName schemaName


resolveTableRef :: OQTableName a -> Resolver (WithColumns RTableRef) a
resolveTableRef tableName = do
    ResolverInfo{catalog = Catalog{..}, bindings = Bindings{..}} <- ask
    lift $ lift $ catalogResolveTableRef boundCTEs tableName

resolveColumnName :: forall a . OQColumnName a -> Resolver RColumnRef a
resolveColumnName columnName = do
    (Catalog{..}, Bindings{..}) <- asks (catalog &&& bindings)
    lift $ lift $ catalogResolveColumnName boundColumns columnName

resolveLambdaParamOrColumnName :: forall a . a -> OQColumnName a -> Resolver (Expr ResolvedNames) a
resolveLambdaParamOrColumnName info columnName = do
    params <- asks lambdaScope
    case isParam params columnName of 
        Just name -> pure $ LambdaParamExpr info name
        Nothing -> ColumnExpr info <$> resolveColumnName columnName
  where
    isParam :: [[LambdaParam a]] -> OQColumnName a -> Maybe (LambdaParam a)
    isParam _ (QColumnName _ (Just _) _) = Nothing
    isParam params (QColumnName _ Nothing name) = find (\(LambdaParam _ pname _) -> pname == name) $ concat params


resolvePartition :: Partition RawNames a -> Resolver (Partition ResolvedNames) a
resolvePartition (PartitionBy info exprs) = PartitionBy info <$> mapM resolveExpr exprs
resolvePartition (PartitionBest info) = pure $ PartitionBest info
resolvePartition (PartitionNodes info) = pure $ PartitionNodes info


resolveSelectFrom :: SelectFrom RawNames a -> Resolver (WithColumns (SelectFrom ResolvedNames)) a
resolveSelectFrom (SelectFrom info tablishes) = do
    tablishesWithColumns <- mapM resolveTablish tablishes
    let (tablishes', css) = unzip $ map (\ (WithColumns t cs) -> (t, cs)) tablishesWithColumns
    pure $ WithColumns (SelectFrom info tablishes') $ concat css


resolveTablish :: forall a . Tablish RawNames a -> Resolver (WithColumns (Tablish ResolvedNames)) a
resolveTablish (TablishTable info aliases name) = do
    WithColumns name' columns <- resolveTableRef name

    let columns' = case aliases of
            TablishAliasesNone -> columns
            TablishAliasesT t -> map (first $ const $ Just $ RTableAlias t (getColumnList name')) columns
            TablishAliasesTC t cs -> [(Just $ RTableAlias t (getColumnList name'), map RColumnAlias cs)]

    pure $ WithColumns (TablishTable info aliases name') columns'


resolveTablish (TablishSubQuery info aliases query) = do
    query' <- resolveQuery query
    let columns = queryColumnNames query'
        (tAlias, cAliases) = case aliases of
            TablishAliasesNone -> (Nothing, columns)
            TablishAliasesT t -> (Just $ RTableAlias t columns, columns)
            TablishAliasesTC t cs -> (Just $ RTableAlias t columns, map RColumnAlias cs)

    pure $ WithColumns (TablishSubQuery info aliases query') [(tAlias, cAliases)]

resolveTablish (TablishParenthesizedRelation info aliases relation) = do
    WithColumns relation' columns <- resolveTablish relation
    let colRefs = concatMap snd columns
        columns' = case aliases of
            TablishAliasesNone -> columns
            TablishAliasesT t -> map (first $ const $ Just $ RTableAlias t colRefs) columns
            TablishAliasesTC t cs -> [(Just $ RTableAlias t colRefs, map RColumnAlias cs)]

    pure $ WithColumns (TablishParenthesizedRelation info aliases relation') columns'

resolveTablish (TablishJoin info joinType cond lhs rhs) = do
    WithColumns lhs' lcolumns <- resolveTablish lhs

    -- special case for Presto
    lcolumnsAreVisible <- asks lcolumnsAreVisibleInLateralViews
    let bindForRhs = case (lcolumnsAreVisible, rhs) of
          (True, TablishLateralView _ _ _) -> bindColumns lcolumns
          _ -> id

    WithColumns rhs' rcolumns <- bindForRhs $ resolveTablish rhs
    let colsForRestOfQuery = case joinType of
          -- for LEFT SEMI JOIN (Hive), the rhs is only in scope in the expr, nowhere else in the query
          JoinSemi _ -> lcolumns
          _ -> lcolumns ++ rcolumns
    bindColumns (lcolumns ++ rcolumns) $ do
        cond' <- resolveJoinCondition cond lcolumns rcolumns
        pure $ WithColumns (TablishJoin info joinType cond' lhs' rhs') colsForRestOfQuery

resolveTablish (TablishLateralView info LateralView{..} lhs) = do
    (lhs', lcolumns) <- case lhs of
        Nothing -> return (Nothing, [])
        Just tablish -> do
                            WithColumns lhs' lcolumns <- resolveTablish tablish
                            return (Just lhs', lcolumns)

    bindColumns lcolumns $ do
        lateralViewExprs' <- mapM resolveExpr lateralViewExprs
        let view = LateralView
                { lateralViewExprs = lateralViewExprs'
                , ..
                }

        defaultCols <- map RColumnAlias . concat <$> mapM defaultAliases lateralViewExprs'
        let rcolumns = case lateralViewAliases of
                TablishAliasesNone -> [(Nothing, defaultCols)]
                TablishAliasesT t -> [(Just $ RTableAlias t defaultCols, defaultCols)]
                TablishAliasesTC t cs -> [(Just $ RTableAlias t defaultCols, map RColumnAlias cs)]

        pure $ WithColumns (TablishLateralView info view lhs') $ lcolumns ++ rcolumns
  where
    defaultAliases (FunctionExpr r (QFunctionName _ _ rawName) _ args _ _ _) = do
        let argsLessOne = length args - 1

            alias = makeColumnAlias r

            prependAlias :: Text -> Int -> Resolver ColumnAlias a
            prependAlias prefix int = alias $ prefix `TL.append` (TL.pack $ show int)

            name = TL.toLower rawName

            functionSpecificLookups
              | name == "explode" = map alias [ "col", "key", "val" ]
              | name == "inline" = map alias [ "col1", "col2" ]
              | name == "json_tuple" = map (prependAlias "c") $ take argsLessOne [0..]
              | name == "parse_url_tuple" = map (prependAlias "c") $ take argsLessOne [0..]
              | name == "posexplode" = map alias [ "pos", "val" ]
              | name == "stack" =
                  let n = case head args of
                            (ConstantExpr _ (NumericConstant _ nText)) -> read $ TL.unpack nText
                            _ -> argsLessOne -- this should never happen, but if it does, this is a reasonable guess
                      k = argsLessOne
                      len = (k `div` n) + (if k `mod` n == 0 then 0 else 1)
                   in map (prependAlias "col") $ take len [0..]
              | otherwise = []

        sequence functionSpecificLookups

    defaultAliases _ = throwError MissingFunctionExprForLateralView


resolveJoinCondition :: JoinCondition RawNames a -> ColumnSet a -> ColumnSet a -> Resolver (JoinCondition ResolvedNames) a
resolveJoinCondition (JoinNatural info _) lhs rhs = do
    let name (RColumnRef (QColumnName _ _ column)) = column
        name (RColumnAlias (ColumnAlias _ alias _)) = alias
        columns = RNaturalColumns $ do
            l <- snd =<< lhs
            r <- snd =<< rhs
            if name l == name r
             then [RUsingColumn l r]
             else []
    pure $ JoinNatural info columns

resolveJoinCondition (JoinOn expr) _ _ = JoinOn <$> resolveExpr expr
resolveJoinCondition (JoinUsing info cols) lhs rhs = JoinUsing info <$> mapM resolveColumn cols
  where
    resolveColumn (QColumnName columnInfo _ column) = do
        let resolveIn columns =
                case filter hasName $ snd =<< columns of
                    [] -> throwError $ MissingColumn $ QColumnName columnInfo Nothing column
                    [c] -> pure c
                    _ -> throwError $ AmbiguousColumn $ QColumnName columnInfo Nothing column
            hasName (RColumnRef (QColumnName _ _ column')) = column' == column
            hasName (RColumnAlias (ColumnAlias _ column' _)) = column' == column
        l <- resolveIn lhs
        r <- resolveIn rhs
        pure $ RUsingColumn l r


resolveSelectWhere :: SelectWhere RawNames a -> Resolver (SelectWhere ResolvedNames) a
resolveSelectWhere (SelectWhere info expr) = SelectWhere info <$> resolveExpr expr

resolveSelectTimeseries :: SelectTimeseries RawNames a -> Resolver (SelectTimeseries ResolvedNames) a
resolveSelectTimeseries SelectTimeseries{..} = do
    selectTimeseriesPartition' <- traverse resolvePartition selectTimeseriesPartition
    selectTimeseriesOrder' <- resolveExpr selectTimeseriesOrder
    pure $ SelectTimeseries
        { selectTimeseriesPartition = selectTimeseriesPartition'
        , selectTimeseriesOrder = selectTimeseriesOrder'
        , ..
        }

resolvePositionOrExpr :: [Expr ResolvedNames a] -> PositionOrExpr RawNames a -> Resolver (PositionOrExpr ResolvedNames) a
resolvePositionOrExpr _ (PositionOrExprExpr expr) = PositionOrExprExpr <$> resolveExpr expr
resolvePositionOrExpr exprs (PositionOrExprPosition info pos Unused)
    | pos < 1 = throwError $ BadPositionalReference info pos
    | otherwise =
        case drop (pos - 1) exprs of
            expr:_ -> pure $ PositionOrExprPosition info pos expr
            [] -> throwError $ BadPositionalReference info pos

resolveGroupingElement :: [Expr ResolvedNames a] -> GroupingElement RawNames a -> Resolver (GroupingElement ResolvedNames) a
resolveGroupingElement exprs (GroupingElementExpr info posOrExpr) =
    GroupingElementExpr info <$> resolvePositionOrExpr exprs posOrExpr
resolveGroupingElement _ (GroupingElementSet info exprs) =
    GroupingElementSet info <$> mapM resolveExpr exprs

resolveSelectGroup :: [Expr ResolvedNames a] -> SelectGroup RawNames a -> Resolver (SelectGroup ResolvedNames) a
resolveSelectGroup exprs SelectGroup{..} = do
    selectGroupGroupingElements' <- mapM (resolveGroupingElement exprs) selectGroupGroupingElements
    pure $ SelectGroup
        { selectGroupGroupingElements = selectGroupGroupingElements'
        , ..
        }

resolveSelectHaving :: SelectHaving RawNames a -> Resolver (SelectHaving ResolvedNames) a
resolveSelectHaving (SelectHaving info exprs) = SelectHaving info <$> mapM resolveExpr exprs

resolveSelectNamedWindow :: SelectNamedWindow RawNames a
                         -> Resolver (SelectNamedWindow ResolvedNames) a
resolveSelectNamedWindow (SelectNamedWindow info windows) =
  SelectNamedWindow info <$> mapM resolveNamedWindowExpr windows
