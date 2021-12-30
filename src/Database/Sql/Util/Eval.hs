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
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.Sql.Util.Eval where

import Database.Sql.Type.Query
import Database.Sql.Type.Names
import Database.Sql.Type.Scope
import Database.Sql.Position

import Database.Sql.Util.Scope (selectionNames)

import qualified Data.Text.Lazy as TL
import           Data.Foldable
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Map (Map)
import qualified Data.Map as M
import           Control.Monad.Except
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Writer
import           Data.Proxy


data RecordSet e = RecordSet
    { recordSetLabels :: [RColumnRef ()]
    , recordSetItems :: EvalRow e [EvalValue e]
    }

data EvalContext e = EvalContext
    { evalAliasMap :: Map TableAliasId (RecordSet e)
    , evalFromTable :: RTableName Range -> Maybe (RecordSet e)
    , evalRow :: Map (RColumnRef ()) (EvalValue e)
    }

data ContextType = ExprContext | TableContext


exprToTable :: Evaluation e => EvalT e 'ExprContext (EvalMonad e) a -> Map (RColumnRef ()) (EvalValue e) -> EvalT e 'TableContext (EvalMonad e) a
exprToTable (EvalT e) r = EvalT $ local (\ EvalContext{..} -> EvalContext{evalRow = M.union evalRow r, ..}) e

tableToExpr :: Evaluation e => EvalT e 'TableContext (EvalMonad e) a -> EvalT e 'ExprContext (EvalMonad e) a
tableToExpr (EvalT e) = EvalT e

newtype EvalT e (t :: ContextType) m a = EvalT (ReaderT (EvalContext e) (ExceptT String m) a)
    deriving (Functor, Applicative, Monad, MonadReader (EvalContext e), MonadError String, MonadWriter w, MonadState s)

type Eval e t = EvalT e t Identity

runEval :: Evaluation e => Eval e t a -> (RTableName Range -> Maybe (RecordSet e)) -> Either String a
runEval = (runIdentity .) . runEvalT

runEvalT :: Evaluation e => EvalT e t m a -> (RTableName Range -> Maybe (RecordSet e)) -> m (Either String a)
runEvalT (EvalT e) evalFromTable = runExceptT $ runReaderT e $ EvalContext{..}
  where
    evalAliasMap = M.empty
    evalRow = M.empty

class Evaluate e q where
    type EvalResult e q :: *
    eval :: Proxy e -> q -> EvalResult e q

introduceAlias :: Evaluation e => Proxy e -> TableAlias () -> RecordSet e -> EvalT e 'TableContext (EvalMonad e) a -> EvalT e 'TableContext (EvalMonad e) a
introduceAlias _ (TableAlias _ _ alias) tbl = local $ \ EvalContext{..} -> EvalContext{evalAliasMap = M.insert alias tbl evalAliasMap, ..}

makeRecordSet :: (Evaluation e, Foldable (EvalRow e)) => Proxy e -> [RColumnRef ()] -> EvalRow e [EvalValue e] -> RecordSet e
makeRecordSet _ cols rows =
    let numColumns = length cols
     in if any ((/= numColumns) . length) rows
         then error "wrong number of columns in record when constructing RecordSet"
         else RecordSet cols rows

emptyRecordSet :: Evaluation e => Proxy e -> RecordSet e
emptyRecordSet p = makeRecordSet p [] $ pure []

class (Monad (EvalRow e), Monad (EvalMonad e), Traversable (EvalRow e)) => Evaluation e where
    type EvalValue e :: *
    type EvalRow e :: * -> *
    type EvalMonad e :: * -> *
    addItems :: Proxy e -> EvalRow e [EvalValue e] -> EvalRow e [EvalValue e] -> EvalT e 'TableContext (EvalMonad e) (EvalRow e [EvalValue e])
    removeItems :: Proxy e -> EvalRow e [EvalValue e] -> EvalRow e [EvalValue e] -> EvalT e 'TableContext (EvalMonad e) (EvalRow e [EvalValue e])
    unionItems :: Proxy e -> EvalRow e [EvalValue e] -> EvalRow e [EvalValue e] -> EvalT e 'TableContext (EvalMonad e) (EvalRow e [EvalValue e])
    intersectItems :: Proxy e -> EvalRow e [EvalValue e] -> EvalRow e [EvalValue e] -> EvalT e 'TableContext (EvalMonad e) (EvalRow e [EvalValue e])
    distinctItems :: Proxy e -> EvalRow e [EvalValue e] -> EvalRow e [EvalValue e]
    offsetItems :: Proxy e -> Int -> RecordSet e -> RecordSet e
    limitItems :: Proxy e -> Int -> RecordSet e -> RecordSet e
    filterBy :: Expr ResolvedNames Range -> RecordSet e -> EvalT e 'TableContext (EvalMonad e) (RecordSet e)
    inList :: EvalValue e -> [EvalValue e] -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    inSubquery :: EvalValue e -> EvalRow e [EvalValue e] -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    existsSubquery :: EvalRow e [EvalValue e] -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    atTimeZone :: EvalValue e -> EvalValue e -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    handleConstant :: Proxy e -> Constant a -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    handleCases :: Proxy e -> [(Expr ResolvedNames Range, Expr ResolvedNames Range)] -> Maybe (Expr ResolvedNames Range) -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    handleFunction :: Proxy e -> FunctionName Range -> Distinct -> [Expr ResolvedNames Range] -> [(ParamName Range, Expr ResolvedNames Range)] -> Maybe (Filter ResolvedNames Range) -> Maybe (OverSubExpr ResolvedNames Range) -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    handleLambdaParam :: Proxy e -> LambdaParam Range -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    handleLambda :: Proxy e -> [LambdaParam Range] -> Expr ResolvedNames Range -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    handleGroups ::  [RColumnRef ()] -> EvalRow e ([EvalValue e], EvalRow e [EvalValue e]) -> EvalRow e (RecordSet e)
    handleLike :: Proxy e -> Operator a -> Maybe (Escape ResolvedNames Range) -> Pattern ResolvedNames Range -> Expr ResolvedNames Range -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    handleOrder :: Proxy e -> [Order ResolvedNames Range] -> RecordSet e -> EvalT e 'TableContext (EvalMonad e) (RecordSet e)
    handleSubquery :: EvalRow e [EvalValue e] -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    handleJoin :: Proxy e -> JoinType a -> JoinCondition ResolvedNames Range -> RecordSet e -> RecordSet e -> EvalT e 'TableContext (EvalMonad e) (RecordSet e)
    handleStructField :: Expr ResolvedNames Range -> StructFieldName a -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    handleTypeCast :: CastFailureAction -> Expr ResolvedNames Range -> DataType a -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    binop :: Proxy e -> TL.Text -> Maybe (EvalValue e -> EvalValue e -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e))
    unop :: Proxy e -> TL.Text -> Maybe (EvalValue e -> EvalT e 'ExprContext (EvalMonad e) (EvalValue e))

instance Evaluation e => Evaluate e (Query ResolvedNames Range) where
    type EvalResult e (Query ResolvedNames Range) = EvalT e 'TableContext (EvalMonad e) (RecordSet e)
    eval p (QuerySelect _ select) = eval p select
    eval p (QueryExcept _ (ColumnAliasList cs) lhs rhs) = do
        exclude <- recordSetItems <$> eval p rhs
        RecordSet{recordSetItems = unfiltered} <- eval p lhs
        let labels = map (RColumnAlias . void) cs
        makeRecordSet p labels <$> removeItems p exclude unfiltered

    eval p (QueryUnion _ (Distinct False) (ColumnAliasList cs) lhs rhs) = do
        RecordSet{recordSetItems = lhsRows} <- eval p lhs
        RecordSet{recordSetItems = rhsRows} <- eval p rhs
        let labels = map (RColumnAlias . void) cs
        makeRecordSet p labels <$> unionItems p lhsRows rhsRows

    eval p (QueryUnion info (Distinct True) cs lhs rhs) = do
        result@RecordSet{recordSetItems} <- eval p (QueryUnion info (Distinct False) cs lhs rhs)
        pure $ result{recordSetItems = distinctItems p recordSetItems}

    eval p (QueryIntersect _ (ColumnAliasList cs) lhs rhs) = do
        RecordSet{recordSetItems = litems} <- eval p lhs
        ritems <- recordSetItems <$> eval p rhs
        let labels = map (RColumnAlias . void) cs
        makeRecordSet p labels <$> intersectItems p litems ritems

    eval p (QueryWith _ [] query) = eval p query
    eval p (QueryWith info (CTE{..}:ctes) query) = do
        RecordSet{..} <- eval p cteQuery
        columns <- override cteColumns recordSetLabels
        let result = makeRecordSet p columns recordSetItems
        introduceAlias p (void cteAlias) result $ eval p $ QueryWith info ctes query
      where
        override [] ys = pure ys
        override (alias:xs) (_:ys) = do
            ys' <- override xs ys
            pure $ (RColumnAlias $ void alias) : ys'
        override _ [] = throwError "more aliases than columns in CTE"

    eval p (QueryLimit _ limit query) = eval p limit <$> eval p query
    eval p (QueryOffset _ offset query) = eval p offset <$> eval p query
    eval p (QueryOrder _ orders query) = eval p query >>= handleOrder p orders

instance Evaluation e => Evaluate e (Select ResolvedNames Range) where
    type EvalResult e (Select ResolvedNames Range) = EvalT e 'TableContext (EvalMonad e) (RecordSet e)
    eval p Select{..} = do
        -- nb. if we handle named windows at resolution time (T637160)
        -- then we shouldn't need to do anything with them here
        unfiltered <- maybe (pure $ emptyRecordSet p) (eval p) selectFrom
        filtered <- maybe pure (eval p) selectWhere unfiltered
        interpolated <- maybe pure (eval p) selectTimeseries filtered
        groups <- maybe (const $ pure . pure) (eval p) selectGroup selectCols interpolated
        having <- maybe pure (eval p) selectHaving groups
        records <- mapM (eval p selectCols) having
        let rows = recordSetItems =<< records
            labels = map void $ selectionNames =<< selectColumnsList selectCols
            indistinct = makeRecordSet p labels rows

        pure $ case selectDistinct of
             Distinct True -> indistinct { recordSetItems = distinctItems p $ recordSetItems indistinct }
             Distinct False -> indistinct

instance Evaluation e => Evaluate e (SelectFrom ResolvedNames Range) where
    type EvalResult e (SelectFrom ResolvedNames Range) = EvalT e 'TableContext (EvalMonad e) (RecordSet e)
    eval p (SelectFrom _ []) = pure $ emptyRecordSet p
    eval p (SelectFrom info (t:ts)) = do
        RecordSet rcols rrows <- eval p $ SelectFrom info ts
        RecordSet lcols lrows <- eval p t
        pure $ makeRecordSet p (lcols ++ rcols) $ (++) <$> lrows <*> rrows


appendRecordSets :: Evaluation e => Proxy e -> NonEmpty (RecordSet e) -> EvalT e 'TableContext (EvalMonad e) (RecordSet e)
appendRecordSets p (RecordSet cs rs :| sets) = makeRecordSet p cs <$> foldM (addItems p) rs (map recordSetItems sets)


instance Evaluation e => Evaluate e (Tablish ResolvedNames Range) where
    type EvalResult e (Tablish ResolvedNames Range) = EvalT e 'TableContext (EvalMonad e) (RecordSet e)
    eval _ (TablishTable _ _ (RTableRef tableName table)) = asks evalFromTable <*> pure (RTableName tableName table) >>= \case
        Nothing -> throwError $ "missing table: " ++ show (void tableName)
        Just result -> pure result
    eval _ (TablishTable _ _ (RTableAlias (TableAlias _ aliasName alias) _)) = asks (M.lookup alias . evalAliasMap) >>= \case
        Nothing -> throwError $ "missing table alias: " ++ show aliasName
        Just result -> pure result

    eval p (TablishSubQuery _ _ query) = eval p query
    eval p (TablishParenthesizedRelation _ _ relation) = eval p relation
    eval p (TablishJoin _ joinType cond lhs rhs) = do
        x <- eval p lhs
        y <- eval p rhs
        handleJoin p joinType cond x y

    eval _ (TablishLateralView _ _ _) = error "lateral view not yet supported"


instance Evaluation e => Evaluate e (JoinCondition ResolvedNames Range) where
    type EvalResult e (JoinCondition ResolvedNames Range) = RecordSet e -> RecordSet e -> EvalT e 'TableContext (EvalMonad e) (RecordSet e)
    eval p (JoinOn expr) = \ (RecordSet lcols lrows) (RecordSet rcols rrows) -> do
        filterBy expr $ makeRecordSet p (lcols ++ rcols) $ (++) <$> lrows <*> rrows

    eval p (JoinUsing info columns) = \ (RecordSet lcols lrows) (RecordSet rcols rrows) -> do
        fmap (adjust columns) $ filterBy (mkExpr columns) $ makeRecordSet p (lcols ++ rcols) $ (++) <$> lrows <*> rrows
      where
        mkExpr :: [RUsingColumn Range] -> Expr ResolvedNames Range
        mkExpr [] = ConstantExpr info $ BooleanConstant info True
        mkExpr (RUsingColumn l r : cs) =
            BinOpExpr info "AND"
                (BinOpExpr info ("=") (ColumnExpr info l) (ColumnExpr info r))
                (mkExpr cs)

        adjust :: [RUsingColumn Range] -> RecordSet e -> RecordSet e
        adjust [] = id
        adjust (c:cs) = adjust cs . adjust' c
        adjust' (RUsingColumn l r) = \ RecordSet{..} ->
            RecordSet
                { recordSetLabels = map fst . remove r . skip l . zip recordSetLabels $ repeat ()
                , recordSetItems = fmap (map snd . remove r . skip l . zip recordSetLabels) recordSetItems
                }

        match column (label, _) = label == void column
        skip column = break $ match column
        remove column (skipped, next:rest) =
            case break (match column) rest of
                (skipped2, _:rest2) -> skipped ++ [next] ++ skipped2 ++ rest2
                (_, []) -> error "failed to find rhs using column"
        remove _ (_, []) = error "failed to find lhs using column"

    eval p (JoinNatural info (RNaturalColumns columns)) = eval p (JoinUsing info columns :: JoinCondition ResolvedNames Range)

makeRowMap :: [RColumnRef ()] -> [a] -> Map (RColumnRef ()) a
makeRowMap = (M.fromList .) . zip


-- | SelectColumns tells us how to map from the records provided by the
-- FROM to (unfiltered, &c) records provided by our select.  Evaluating it
-- gives us that function.
instance Evaluation e => Evaluate e (SelectColumns ResolvedNames Range) where
    type EvalResult e (SelectColumns ResolvedNames Range) = RecordSet e -> EvalT e 'TableContext (EvalMonad e) (RecordSet e)
    eval p (SelectColumns _ columns) (RecordSet cs rs) = do
        let cs' = map void $ selectionNames =<< columns
        rs' <- forM rs $ \ r -> do
            r' <- forM columns $ \ column ->
                exprToTable (eval p column) $ makeRowMap cs r
            pure $ concat r'
        pure $ makeRecordSet p cs' rs'

instance Evaluation e => Evaluate e (SelectWhere ResolvedNames Range) where
    type EvalResult e (SelectWhere ResolvedNames Range) = RecordSet e -> EvalT e 'TableContext (EvalMonad e) (RecordSet e)
    eval _ (SelectWhere _ expr) = filterBy expr

instance Evaluation e => Evaluate e (SelectGroup ResolvedNames Range) where
    type EvalResult e (SelectGroup ResolvedNames Range) = SelectColumns ResolvedNames Range -> RecordSet e -> EvalT e 'TableContext (EvalMonad e) (EvalRow e (RecordSet e))
    eval _ (SelectGroup _ elts) columns (RecordSet cs rs) = do
        gs <- forM rs $ \ r -> do
            RecordSet{..} <- eval Proxy columns $ RecordSet cs $ pure r
            let selectMap = makeRowMap recordSetLabels (head $ toList recordSetItems)
            g <- exprToTable (mapM (eval Proxy) (eltToExprs =<< elts)) $ M.union selectMap $ makeRowMap cs r
            pure (g, pure r)
        pure $ handleGroups cs gs
      where
        eltToExprs (GroupingElementSet _ exprs) = exprs
        eltToExprs (GroupingElementExpr _ (PositionOrExprExpr expr)) = [expr]
        eltToExprs (GroupingElementExpr _ (PositionOrExprPosition _ _ expr)) = [expr]

instance Evaluation e => Evaluate e (SelectHaving ResolvedNames Range) where
    type EvalResult e (SelectHaving ResolvedNames Range) = EvalRow e (RecordSet e) -> EvalT e 'TableContext (EvalMonad e) (EvalRow e (RecordSet e))
    eval _ (SelectHaving _ exprs) = foldl (>=>) pure $ map (mapM . filterBy) exprs

instance Evaluation e => Evaluate e (SelectTimeseries ResolvedNames Range) where
    type EvalResult e (SelectTimeseries ResolvedNames Range) = RecordSet e -> EvalT e 'TableContext (EvalMonad e) (RecordSet e)
    eval _ (SelectTimeseries _ sliceName interval partition over) = error "timeseries not yet supported" sliceName interval partition over

data Direction a = Ascending a | Descending a deriving Eq

instance Ord a => Ord (Direction a) where
    compare (Descending x) (Descending y) = compare y x
    compare (Ascending x) (Ascending y) = compare x y
    compare _ _ = error "comparing ascending to descending - this shouldn't happen"

instance Evaluation e => Evaluate e (Limit a) where
    type EvalResult e (Limit a) = RecordSet e -> RecordSet e
    eval p (Limit _ limit) = limitItems p (read $ TL.unpack limit)

instance Evaluation e => Evaluate e (Offset a) where
    type EvalResult e (Offset a) = RecordSet e -> RecordSet e
    eval p (Offset _ offset) = offsetItems p (read $ TL.unpack offset)

instance Evaluation e => Evaluate e (Selection ResolvedNames Range) where
    type EvalResult e (Selection ResolvedNames Range) = EvalT e 'ExprContext (EvalMonad e) [EvalValue e]
    eval p (SelectExpr _ _ expr) = pure <$> eval p expr
    eval p (SelectStar info _ (StarColumnNames cols)) = forM cols $ \ col ->
        let expr :: Expr ResolvedNames Range
            expr = ColumnExpr info col
         in eval p expr


instance Evaluation e => Evaluate e (Expr ResolvedNames Range) where
    type EvalResult e (Expr ResolvedNames Range) = EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    eval p (BinOpExpr _ (Operator op) lhs rhs) = do
        x <- eval p lhs
        y <- eval p rhs
        case binop p op of
            Nothing -> throwError $ "unhandled operator: " ++ show op
            Just f -> f x y

    eval p (CaseExpr _ cases else_) = handleCases p cases else_

    eval p (UnOpExpr _ (Operator op) expr) = do
        x <- eval p expr
        case unop p op of
            Nothing -> throwError $ "unhandled operator: " ++ show op
            Just f -> f x

    eval p (LikeExpr _ op escape pattern expr) = handleLike p op escape pattern expr
    eval p (ConstantExpr _ expr) = eval p expr
    eval _ (ColumnExpr _ col) = do
        row <- asks evalRow
        case M.lookup (void col) row of
            Just x -> pure x
            Nothing -> throwError $ "failure looking up column: " ++ show (void col) ++ " in " ++ show (M.keys row)

    eval p (InListExpr _ list expr) = do
        x <- eval p expr
        xs <- mapM (eval p) list
        inList x xs

    eval p (InSubqueryExpr _ query expr) = do
        x <- eval p expr
        RecordSet{..} <- tableToExpr $ eval p query
        case length recordSetLabels of
         1 -> inSubquery x recordSetItems
         0 -> throwError "no columns returned from subquery for IN"
         _ -> throwError "multiple columns returned from subquery for IN"

    eval p (BetweenExpr info expr start end) = eval p $ BinOpExpr info (Operator "AND")
        (BinOpExpr info (Operator "<=") start expr)
        (BinOpExpr info (Operator "<=") expr end)

    eval p (OverlapsExpr info (lstart, lend) (rstart, rend)) = eval p $ BinOpExpr info (Operator "AND")
        (BinOpExpr info (Operator "<") lstart rend)
        (BinOpExpr info (Operator "<") rstart lend)

    eval p (FunctionExpr _ fn isDistinct args parms filter' over) = handleFunction p fn isDistinct args parms filter' over
    eval p (AtTimeZoneExpr _ expr tz) = join $ atTimeZone <$> eval p expr <*> eval p tz
    eval p (SubqueryExpr _ query) = do
        RecordSet{..} <- tableToExpr $ eval p query
        handleSubquery recordSetItems

    eval p (ArrayExpr _ exprs) = error "array expression not yet supported" <$> mapM (eval p) exprs -- T636472
    eval p (ExistsExpr _ query) = do
        RecordSet{..} <- tableToExpr (eval p query)
        existsSubquery recordSetItems

    eval _ (FieldAccessExpr _ struct field) = handleStructField struct field

    eval _ (ArrayAccessExpr _ array idx) = error "array indexing not yet supported" array idx -- T636558
    eval _ (TypeCastExpr _ onFail expr type_) = handleTypeCast onFail expr type_
    eval _ (VariableSubstitutionExpr _) = throwError "no way to evaluate unsubstituted variable"
    eval p (LambdaParamExpr _ param) = handleLambdaParam p param
    eval p (LambdaExpr _ params body) = handleLambda p params body

instance Evaluation e => Evaluate e (Constant a) where
    type EvalResult e (Constant a) = EvalT e 'ExprContext (EvalMonad e) (EvalValue e)
    eval p constant = handleConstant p constant
