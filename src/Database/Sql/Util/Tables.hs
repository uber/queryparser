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
module Database.Sql.Util.Tables where

import           Data.Foldable

import           Data.Set (Set)
import qualified Data.Set as S

import           Data.Map (Map)
import qualified Data.Map as M

import           Data.Ord
import           Data.Maybe (catMaybes)

import           Data.List.NonEmpty (NonEmpty(..))
import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL

import           Control.Monad.Identity
import           Control.Monad.Writer
import           Control.Monad.Reader
import           Control.Monad.State

import           Database.Sql.Type
import           Database.Sql.Position


-- This describes the usage of a particular table, whether it was read from or
-- written to. Note that if a table is used in both modes, it will show up
-- twice as each mode.
data UsageMode = ReadData | ReadMeta | WriteData | WriteMeta | Unknown
  deriving (Show, Eq, Ord)

data TableUse = TableUse UsageMode FullyQualifiedTableName
  deriving (Show, Eq, Ord)


getTables :: HasTables q => q -> Set FullyQualifiedTableName
getTables = S.map tableFromUsage . getUsages
  where
   tableFromUsage (TableUse _ t) = t

getUsages :: HasTables q => q -> Set TableUse
getUsages = execWriter . flip runReaderT Unknown . goTables

class HasTables q where
    goTables :: q -> ReaderT UsageMode (Writer (Set TableUse)) ()

-- Note that vertica and hive statements have their own table usage instances
-- in their type file. Changes made here should also reflect there.
instance HasTables (Statement d ResolvedNames a) where
  goTables (QueryStmt q) = goTables q
  goTables (InsertStmt i) = goTables i
  goTables (UpdateStmt u) = goTables u
  goTables (DeleteStmt d) = goTables d
  goTables (TruncateStmt t) = goTables t
  goTables (CreateTableStmt c) = goTables c
  goTables (AlterTableStmt a) = goTables a
  goTables (DropTableStmt d) = goTables d
  goTables (CreateViewStmt c) = goTables c
  goTables (DropViewStmt d) = goTables d
  goTables (CreateSchemaStmt _) = return ()
  goTables (GrantStmt _) = return ()
  goTables (RevokeStmt _) = return ()
  goTables (BeginStmt _) = return ()
  goTables (CommitStmt _) = return ()
  goTables (RollbackStmt _) = return ()
  goTables (ExplainStmt _ s) = goTables s
  goTables (EmptyStmt _) = return ()

instance HasTables (Query ResolvedNames a) where
    goTables (QuerySelect _ select) = goTables select
    goTables (QueryExcept _ _ lhs rhs) = mapM_ goTables [lhs, rhs]
    goTables (QueryUnion _ _ _ lhs rhs) = mapM_ goTables [lhs, rhs]
    goTables (QueryIntersect _ _ lhs rhs) = mapM_ goTables [lhs, rhs]
    goTables (QueryWith _ ctes query) = mapM_ goTables $ query : map cteQuery ctes
    goTables (QueryOrder _ orders query) = mapM_ goTables orders >> goTables query
    goTables (QueryLimit _ _ query) = goTables query
    goTables (QueryOffset _ _ query) = goTables query

instance HasTables (Select ResolvedNames a) where
    goTables (Select {..}) = sequence_
        [ goTables selectCols
        , maybe (return ()) goTables selectFrom
        , maybe (return ()) goTables selectWhere
        , maybe (return ()) goTables selectTimeseries
        , maybe (return ()) goTables selectGroup
        , maybe (return ()) goTables selectHaving
        , maybe (return ()) goTables selectNamedWindow
        ]


emitTable :: (MonadWriter (Set TableUse) m,
              MonadReader UsageMode m)
          => FQTableName a -> m ()
emitTable t = do
  r <- ask
  tell . S.singleton $ TableUse r $ fqtnToFQTN t

instance (HasTables a, HasTables b) => HasTables (a, b) where
    goTables (a, b) = goTables a >> goTables b

instance HasTables (RColumnRef a) where
    goTables (RColumnRef fqcn) = emitTable . runIdentity $ columnNameTable fqcn
    goTables (RColumnAlias _) = return () -- the table that introduced it will get iterated over anyway. (shrug)

instance HasTables (FQTableName a) where
    goTables fqtn = emitTable fqtn

instance HasTables (RTableName a) where
    goTables (RTableName table _) = emitTable table

instance HasTables (SelectColumns ResolvedNames a) where
    goTables (SelectColumns _ columns) = mapM_ goTables columns

instance HasTables (SelectFrom ResolvedNames a) where
    goTables (SelectFrom _ tablishes) = local (\_ -> ReadData) $ mapM_ goTables tablishes

instance HasTables (SelectWhere ResolvedNames a) where
    goTables (SelectWhere _ condition) = goTables condition

instance HasTables (SelectTimeseries ResolvedNames a) where
    goTables (SelectTimeseries _ _ _ partition expr) = do
        goTables partition
        goTables expr

instance HasTables (PositionOrExpr ResolvedNames a) where
    goTables (PositionOrExprPosition _ _ _) = return ()
    goTables (PositionOrExprExpr expr) = goTables expr

instance HasTables (GroupingElement ResolvedNames a) where
    goTables (GroupingElementExpr _ posOrExpr) = goTables posOrExpr
    goTables (GroupingElementSet _ exprs) = mapM_ goTables exprs

instance HasTables (SelectGroup ResolvedNames a) where
    goTables (SelectGroup _ groupingElements) = mapM_ goTables groupingElements

instance HasTables (SelectHaving ResolvedNames a) where
    goTables (SelectHaving _ havings) = mapM_ goTables havings

instance HasTables (SelectNamedWindow ResolvedNames a) where
    goTables (SelectNamedWindow _ windows) = mapM_ goTables windows

instance HasTables (NamedWindowExpr ResolvedNames a) where
    goTables (NamedWindowExpr _ _ windowExpr) = goTables windowExpr
    goTables (NamedPartialWindowExpr _ _ partial) = goTables partial

instance HasTables (WindowExpr ResolvedNames a) where
    goTables (WindowExpr _ mPartition orders _) = do
        goTables mPartition
        mapM_ goTables orders

instance HasTables (PartialWindowExpr ResolvedNames a) where
    goTables (PartialWindowExpr _ _ mPartition orders _) = do
        goTables mPartition
        mapM_ goTables orders

instance HasTables (Selection ResolvedNames a) where
    goTables (SelectStar _ _ _) = return ()
    goTables (SelectExpr _ _ expr) = goTables expr

instance HasTables (Insert ResolvedNames a) where
    goTables Insert{..} = do
        local (\_ -> WriteData) $ goTables insertTable
        goTables insertValues

instance HasTables (InsertValues ResolvedNames a) where
    goTables (InsertExprValues _ e) = goTables e
    goTables (InsertSelectValues q) = goTables q
    goTables (InsertDefaultValues _) = return ()
    goTables (InsertDataFromFile _ _) = return ()

instance HasTables (DefaultExpr ResolvedNames a) where
    goTables (DefaultValue _) = return ()
    goTables (ExprValue e) = goTables e

instance HasTables a => HasTables (NonEmpty a) where
    goTables ne = mapM_ goTables ne

instance HasTables a => HasTables (Maybe a) where
    goTables Nothing = return ()
    goTables (Just a) = goTables a

instance HasTables (Update ResolvedNames a) where
    goTables Update{..} = do
        local (\_ -> WriteData) $ goTables updateTable
        local (\_ -> ReadData) $ goTables updateSetExprs
        local (\_ -> ReadData) $ goTables updateFrom
        goTables updateWhere

instance HasTables (Delete ResolvedNames a) where
    goTables (Delete _ table expr) = do
        local (\_ -> WriteData) $ goTables table
        goTables expr

instance HasTables (CreateTable d ResolvedNames a) where
    goTables CreateTable{createTableName = RCreateTableName table _, ..} = do
        -- TODO handle createTableExtra, and the dialect instances
        local (\_ -> WriteData) $ emitTable table
        goTables createTableDefinition

instance HasTables (TableDefinition d ResolvedNames a) where
    goTables (TableColumns _ s) = goTables s
    goTables (TableLike _ table) = goTables table
    goTables (TableAs _ _ query) = goTables query
    goTables (TableNoColumnInfo _) = return ()

instance HasTables (ColumnOrConstraint d ResolvedNames a) where
    goTables (ColumnOrConstraintColumn c) = goTables c
    goTables (ColumnOrConstraintConstraint _) = return ()

instance HasTables (ColumnDefinition d ResolvedNames a) where
    goTables ColumnDefinition{..} = goTables columnDefinitionDefault

instance HasTables (Truncate ResolvedNames a) where
    goTables (Truncate _ tn) = local (\_ -> WriteData) $ goTables tn

instance HasTables (AlterTable ResolvedNames a) where
    goTables (AlterTableRenameTable _ tl tr) = do
      -- Since renaming table both reads and writes (drops) the source,
      -- we report twice.
      local (\_ -> ReadData) $ goTables tl
      local (\_ -> WriteData) $ goTables tl
      local (\_ -> WriteData) $ goTables tr
    goTables (AlterTableRenameColumn _ t _ _) =
      local (\_ -> WriteMeta) $ goTables t
    goTables (AlterTableAddColumns _ t _) =
      local (\_ -> WriteData) $ goTables t

instance HasTables (DropTable ResolvedNames a) where
    goTables DropTable{dropTableNames = tables} =
      mapM_
      (\case
          RDropExistingTableName table _ -> local (\_ -> WriteData) $ emitTable table
          RDropMissingTableName _ -> pure ()
      )
      tables

instance HasTables (CreateView ResolvedNames a) where
    goTables CreateView{createViewName = RCreateTableName view _, ..} = do
        local (\_ -> WriteMeta) $ emitTable view
        goTables createViewQuery

instance HasTables (DropView ResolvedNames a) where
    goTables DropView{dropViewName = RDropExistingTableName view _} = local (\_ -> WriteMeta) $ emitTable view
    goTables DropView{dropViewName = RDropMissingTableName _} = pure ()

instance HasTables (Tablish ResolvedNames a) where
    goTables (TablishTable _ _ (RTableRef fqtn _)) = emitTable fqtn
    goTables (TablishTable _ _ (RTableAlias _ _)) = return ()
    goTables (TablishSubQuery _ _ query) = goTables query
    goTables (TablishParenthesizedRelation _ _ relation) = goTables relation
    goTables (TablishLateralView _ LateralView{..} lhs) = goTables lhs >> mapM_ goTables lateralViewExprs
    goTables (TablishJoin _ _ cond outer inner) = do
        case cond of
            JoinOn expr -> goTables expr
            JoinNatural _ _ -> return ()
            JoinUsing _ _ -> return ()

        goTables outer
        goTables inner

instance HasTables (Expr ResolvedNames a) where
    goTables (BinOpExpr _ _ lhs rhs) = mapM_ goTables [lhs, rhs]
    goTables (CaseExpr _ whens else_) =
        let whens' = foldl' (\ xs (x, y) -> x:y:xs) [] whens
         in mapM_ goTables (maybe id (:) else_ whens')

    goTables (UnOpExpr _ _ operand) = goTables operand
    goTables (LikeExpr _ _ escape pattern expr) = mapM_ goTables $ catMaybes
        [ fmap escapeExpr escape
        , Just (patternExpr pattern)
        , Just expr
        ]

    goTables (ConstantExpr _ _) = return ()
    goTables (ColumnExpr _ _) = return ()
    goTables (InListExpr _ exprs expr) = mapM_ goTables $ expr : exprs
    goTables (InSubqueryExpr _ query expr) = do
        goTables expr
        goTables query

    goTables (BetweenExpr _ expr start end) =
        mapM_ goTables [expr, start, end]

    goTables (OverlapsExpr _ (s1, e1) (s2, e2)) =
        mapM_ goTables [s1, e1, s2, e2]

    goTables (FunctionExpr _ _ _ args params mFilter mOver) = do
        mapM_ goTables $ args ++ map snd params
        maybe (return ()) goTables mFilter
        maybe (return ()) goTables mOver

    goTables (AtTimeZoneExpr _ expr tz) = mapM_ goTables [expr, tz]
    goTables (SubqueryExpr _ query) = goTables query
    goTables (ArrayExpr _ values) = mapM_ goTables values
    goTables (ExistsExpr _ query) = goTables query
    goTables (FieldAccessExpr _ expr _) = goTables expr
    goTables (ArrayAccessExpr _ expr subscript) = do
        goTables expr
        goTables subscript
    goTables (TypeCastExpr _ _ expr _) = goTables expr
    goTables (VariableSubstitutionExpr _) = return ()
    goTables (LambdaParamExpr _ _) = return ()
    goTables (LambdaExpr _ _ body) = goTables body

instance HasTables (Filter ResolvedNames a) where
    goTables (Filter _ expr) = goTables expr

instance HasTables (OverSubExpr ResolvedNames a) where
    goTables (OverWindowExpr _ windowExpr) = goTables windowExpr
    goTables (OverWindowName _ _) = return ()
    goTables (OverPartialWindowExpr _ partial) = goTables partial

instance HasTables (Partition ResolvedNames a) where
    goTables (PartitionBy _ exprs) = mapM_ goTables exprs
    goTables (PartitionBest _) = return ()
    goTables (PartitionNodes _) = return ()

instance HasTables (Order ResolvedNames a) where
    goTables (Order _ posOrExpr _ _) = goTables posOrExpr


data Open = Open { openRange :: Range
                 , openNumber :: RangeNumber
                 } deriving (Eq, Show)

data Close = Close { closeRange :: Range
                   , closeNumber :: RangeNumber
                   } deriving (Eq, Show)

class Positioned a where
    position :: a -> Position

instance Ord Open where
    compare = comparing (start . openRange)
                <> flip (comparing $ end . openRange)

instance Positioned Open where
    position = start . openRange

instance Ord Close where
    compare = comparing (end . closeRange)
                <> flip (comparing $ start . closeRange)

instance Positioned Close where
    position = end . closeRange


newtype RangeNumber = RangeNumber Integer deriving (Eq, Ord, Show)
newtype NodeNumber = NodeNumber Integer deriving (Eq, Ord, Show)

getRanges :: Query RawNames Range
        -> ( Query RawNames NodeNumber
           , Set Open, Set Close
           , Map NodeNumber RangeNumber )
getRanges query =
    let (query', (_, m)) = runState (mapM numberNodes query) (0, M.empty)
        (ranges, (_, mapping)) =
            runState (mapM numberRanges m) (0, M.empty)

        (opens, closes) =
            let makeOpenAndClose r rnum =
                    ( S.singleton (Open r rnum)
                    , S.singleton (Close r rnum)
                    )
             in M.foldMapWithKey makeOpenAndClose ranges

     in (query', opens, closes, mapping)

  where
    numberNodes r = state $ \ (i, m) ->
        let m' = M.insertWith S.union r (S.singleton $ NodeNumber i) m
         in (NodeNumber i, (i+1, m'))

    numberRanges s = state $ \ (i, m) ->
        let m' = M.union (M.fromSet (const $ RangeNumber i) s) m
         in (RangeNumber i, (i+1, m'))


spliceMarkers :: Monoid a => (Open -> a)
                          -> (Close -> a)
                          -> (Text -> a)
                          -> Set Open
                          -> Set Close
                          -> Text
                          -> a

spliceMarkers renderOpen renderClose renderText = go 0
  where
    go offset opens closes text =
        case (S.minView opens, S.minView closes) of
            (Just (o, opens'), Just (c, closes'))
                | offset == positionOffset (position c) ->
                    renderClose c <> go offset opens closes' text

                | offset == positionOffset (position o) ->
                    renderOpen o <> go offset opens' closes text

                | otherwise ->
                    let offset' = min (positionOffset $ position c)
                                      (positionOffset $ position o)
                        (chunk, rest) = TL.splitAt (offset' - offset) text
                     in renderText chunk <> go offset' opens closes rest

            (Just _, Nothing) -> error $ unwords
                [ "remaining opens when all closes are exhausted"
                , "- this should not be possible"
                ]

            (Nothing, Just (c, closes'))
                | offset == positionOffset (position c) ->
                    renderClose c <> go offset opens closes' text

                | otherwise ->
                    let offset' = positionOffset $ position c
                        (chunk, rest) = TL.splitAt (offset' - offset) text
                     in renderText chunk <> go offset' opens closes rest

            (Nothing, Nothing) -> renderText text
