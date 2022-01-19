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

{-# LANGUAGE TypeFamilies #-}

module Database.Sql.Info where

import Database.Sql.Type

class HasInfo a where
    type Info a
    getInfo :: a -> Info a


instance HasInfo (Statement d r a) where
    type Info (Statement d r a) = a
    getInfo (QueryStmt query) = getInfo query
    getInfo (InsertStmt insert) = getInfo insert
    getInfo (UpdateStmt update) = getInfo update
    getInfo (DeleteStmt delete) = getInfo delete
    getInfo (TruncateStmt truncate') = getInfo truncate'
    getInfo (CreateTableStmt create) = getInfo create
    getInfo (AlterTableStmt alter) = getInfo alter
    getInfo (DropTableStmt drop') = getInfo drop'
    getInfo (CreateViewStmt create) = getInfo create
    getInfo (DropViewStmt drop') = getInfo drop'
    getInfo (CreateSchemaStmt create) = getInfo create
    getInfo (GrantStmt grant) = getInfo grant
    getInfo (RevokeStmt revoke) = getInfo revoke
    getInfo (BeginStmt info) = info
    getInfo (CommitStmt info) = info
    getInfo (RollbackStmt info) = info
    getInfo (ExplainStmt info _) = info
    getInfo (EmptyStmt info) = info

instance HasInfo (CreateTable d r a) where
    type Info (CreateTable d r a) = a
    getInfo = createTableInfo

instance HasInfo (AlterTable r a) where
    type Info (AlterTable r a) = a
    getInfo (AlterTableRenameTable info _ _) = info
    getInfo (AlterTableRenameColumn info _ _ _) = info
    getInfo (AlterTableAddColumns info _ _) = info

instance HasInfo (TableDefinition d r a) where
    type Info (TableDefinition d r a) = a
    getInfo (TableColumns info _) = info
    getInfo (TableLike info _) = info
    getInfo (TableAs info _ _) = info
    getInfo (TableNoColumnInfo info) = info

instance HasInfo (Insert r a) where
    type Info (Insert r a) = a
    getInfo = insertInfo

instance HasInfo (InsertValues r a) where
    type Info (InsertValues r a) = a
    getInfo (InsertExprValues info _) = info
    getInfo (InsertSelectValues query) = getInfo query
    getInfo (InsertDefaultValues info) = info
    getInfo (InsertDataFromFile info _) = info

instance HasInfo (Update r a) where
    type Info (Update r a) = a
    getInfo = updateInfo

instance HasInfo (Delete r a) where
    type Info (Delete r a) = a
    getInfo (Delete info _ _) = info

instance HasInfo (Truncate r a) where
    type Info (Truncate r a) = a
    getInfo (Truncate info _) = info

instance HasInfo (DropTable r a) where
    type Info (DropTable r a) = a
    getInfo = dropTableInfo

instance HasInfo (CreateView r a) where
    type Info (CreateView r a) = a
    getInfo = createViewInfo

instance HasInfo (DropView r a) where
    type Info (DropView r a) = a
    getInfo = dropViewInfo

instance HasInfo (CreateSchema r a) where
    type Info (CreateSchema r a) = a
    getInfo = createSchemaInfo

instance HasInfo (Grant a) where
    type Info (Grant a) = a
    getInfo (Grant info) = info

instance HasInfo (Revoke a) where
    type Info (Revoke a) = a
    getInfo (Revoke info) = info

instance HasInfo (Query r a) where
    type Info (Query r a) = a
    getInfo (QuerySelect info _) = info
    getInfo (QueryExcept info _ _ _) = info
    getInfo (QueryUnion info _ _ _ _) = info
    getInfo (QueryIntersect info _ _ _) = info
    getInfo (QueryWith info _ _) = info
    getInfo (QueryOrder info _ _) = info
    getInfo (QueryLimit info _ _) = info
    getInfo (QueryOffset info _ _) = info


instance HasInfo (DatabaseName a) where
    type Info (DatabaseName a) = a
    getInfo (DatabaseName info _) = info

instance (Foldable f, Functor f, Semigroup a) => HasInfo (QSchemaName f a) where
    type Info (QSchemaName f a) = a
    getInfo (QSchemaName info database _ _) = foldl (<>) info $ getInfo <$> database

instance (Foldable f, Functor f, Semigroup a) => HasInfo (QTableName f a) where
    type Info (QTableName f a) = a
    getInfo (QTableName info schema _) = foldl (<>) info $ getInfo <$> schema

instance HasInfo (TableAlias a) where
    type Info (TableAlias a) = a
    getInfo (TableAlias info _ _) = info

instance Semigroup a => HasInfo (RTableRef a) where
    type Info (RTableRef a) = a
    getInfo (RTableRef name _) = getInfo name
    getInfo (RTableAlias alias _) = getInfo alias

instance Semigroup a => HasInfo (RTableName a) where
    type Info (RTableName a) = a
    getInfo (RTableName name _) = getInfo name

instance (Foldable f, Functor f, Semigroup a) => HasInfo (QFunctionName f a) where
    type Info (QFunctionName f a) = a
    getInfo (QFunctionName info _ _) = info

instance (Foldable f, Functor f, Semigroup a) => HasInfo (QColumnName f a) where
    type Info (QColumnName f a) = a
    getInfo (QColumnName info table _) = foldl (<>) info $ getInfo <$> table

instance HasInfo (ColumnAlias a) where
    type Info (ColumnAlias a) = a
    getInfo (ColumnAlias info _ _) = info

instance Semigroup a => HasInfo (RColumnRef a) where
    type Info (RColumnRef a) = a
    getInfo (RColumnRef name) = getInfo name
    getInfo (RColumnAlias alias) = getInfo alias

instance HasInfo (ParamName a) where
    type Info (ParamName a) = a
    getInfo (ParamName info _) = info

instance HasInfo (DefaultExpr r a) where
    type Info (DefaultExpr r a) = a
    getInfo (DefaultValue info) = info
    getInfo (ExprValue expr) = getInfo expr

instance HasInfo (Expr r a) where
    type Info (Expr r a) = a
    getInfo (BinOpExpr info _ _ _) = info
    getInfo (CaseExpr info _ _) = info
    getInfo (UnOpExpr info _ _) = info
    getInfo (LikeExpr info _ _ _ _) = info
    getInfo (ConstantExpr info _) = info
    getInfo (ColumnExpr info _) = info
    getInfo (InListExpr info _ _) = info
    getInfo (InSubqueryExpr info _ _) = info
    getInfo (BetweenExpr info _ _ _) = info
    getInfo (OverlapsExpr info _ _) = info
    getInfo (FunctionExpr info _ _ _ _ _ _) = info
    getInfo (AtTimeZoneExpr info _ _) = info
    getInfo (SubqueryExpr info _) = info
    getInfo (ArrayExpr info _) = info
    getInfo (ExistsExpr info _) = info
    getInfo (FieldAccessExpr info _ _) = info
    getInfo (ArrayAccessExpr info _ _) = info
    getInfo (TypeCastExpr info _ _ _) = info
    getInfo (VariableSubstitutionExpr info) = info
    getInfo (LambdaParamExpr info _) = info
    getInfo (LambdaExpr info _ _) = info

instance HasInfo (Filter r a) where
    type Info (Filter r a) = a
    getInfo (Filter info _) = info

instance HasInfo (NamedWindowExpr r a) where
    type Info (NamedWindowExpr r a) = a
    getInfo (NamedWindowExpr info _ _) = info
    getInfo (NamedPartialWindowExpr info _ _) = info

instance HasInfo (OverSubExpr r a) where
    type Info (OverSubExpr r a) = a
    getInfo (OverWindowExpr info _) = info
    getInfo (OverWindowName info _) = info
    getInfo (OverPartialWindowExpr info _) = info

instance HasInfo (WindowExpr r a) where
    type Info (WindowExpr r a) = a
    getInfo (WindowExpr info _ _ _) = info

instance HasInfo (PartialWindowExpr r a) where
    type Info (PartialWindowExpr r a) = a
    getInfo (PartialWindowExpr info _ _ _ _) = info

instance HasInfo (Partition r a) where
    type Info (Partition r a) = a
    getInfo (PartitionBy info _) = info
    getInfo (PartitionBest info) = info
    getInfo (PartitionNodes info) = info

instance HasInfo (Order r a) where
    type Info (Order r a) = a
    getInfo (Order info _ _ _) = info

instance HasInfo (WindowName a) where
    type Info (WindowName a) = a
    getInfo (WindowName info _) = info

instance HasInfo (StructFieldName a) where
    type Info (StructFieldName a) = a
    getInfo (StructFieldName info _) = info

instance HasInfo (ArrayIndex a) where
    type Info (ArrayIndex a) = a
    getInfo (ArrayIndex info _) = info

instance HasInfo (DataType a) where
    type Info (DataType a) = a
    getInfo (PrimitiveDataType info _ _) = info
    getInfo (ArrayDataType info _) = info
    getInfo (MapDataType info _ _) = info
    getInfo (StructDataType info _) = info
    getInfo (UnionDataType info _) = info

instance HasInfo (DataTypeParam a) where
    type Info (DataTypeParam a) = a
    getInfo (DataTypeParamConstant c) = getInfo c
    getInfo (DataTypeParamType t) = getInfo t

instance HasInfo (Select r a) where
    type Info (Select r a) = a
    getInfo = selectInfo


instance HasInfo (SelectColumns r a) where
    type Info (SelectColumns r a) = a
    getInfo (SelectColumns info _) = info


instance HasInfo (SelectFrom r a) where
    type Info (SelectFrom r a) = a
    getInfo (SelectFrom info _) = info


instance HasInfo (SelectWhere r a) where
    type Info (SelectWhere r a) = a
    getInfo (SelectWhere info _) = info

instance HasInfo (SelectTimeseries r a) where
    type Info (SelectTimeseries r a) = a
    getInfo = selectTimeseriesInfo

instance HasInfo (PositionOrExpr r a) where
    type Info (PositionOrExpr r a) = a
    getInfo (PositionOrExprPosition info _ _) = info
    getInfo (PositionOrExprExpr expr) = getInfo expr

instance HasInfo (GroupingElement r a) where
    type Info (GroupingElement r a) = a
    getInfo (GroupingElementExpr info _) = info
    getInfo (GroupingElementSet info _) = info

instance HasInfo (SelectGroup r a) where
    type Info (SelectGroup r a) = a
    getInfo (SelectGroup info _) = info

instance HasInfo (SelectHaving r a) where
    type Info (SelectHaving r a) = a
    getInfo (SelectHaving info _) = info

instance HasInfo (SelectNamedWindow r a) where
    type Info (SelectNamedWindow r a) = a
    getInfo (SelectNamedWindow info _) = info


instance HasInfo (FrameType a) where
    type Info (FrameType a) = a
    getInfo (RowFrame info) = info
    getInfo (RangeFrame info) = info


instance HasInfo (FrameBound a) where
    type Info (FrameBound a) = a
    getInfo (Unbounded info) = info
    getInfo (CurrentRow info) = info
    getInfo (Preceding info _) = info
    getInfo (Following info _) = info


instance HasInfo (Frame a) where
    type Info (Frame a) = a
    getInfo (Frame info _ _ _) = info


instance HasInfo (Constant a) where
    type Info (Constant a) = a
    getInfo (StringConstant info _) = info
    getInfo (NumericConstant info _) = info
    getInfo (NullConstant info) = info
    getInfo (BooleanConstant info _) = info
    getInfo (TypedConstant info _ _) = info
    getInfo (ParameterConstant info) = info


instance HasInfo (JoinCondition r a) where
    type Info (JoinCondition r a) = a
    getInfo (JoinNatural info _) = info
    getInfo (JoinOn expr) = getInfo expr
    getInfo (JoinUsing info _) = info

instance HasInfo (JoinType a) where
    type Info (JoinType a) = a
    getInfo (JoinInner info) = info
    getInfo (JoinLeft info) = info
    getInfo (JoinRight info) = info
    getInfo (JoinFull info) = info
    getInfo (JoinSemi info) =  info

instance HasInfo (Tablish r a) where
    type Info (Tablish r a) = a
    getInfo (TablishTable info _ _) = info
    getInfo (TablishSubQuery info _ _) = info
    getInfo (TablishParenthesizedRelation info _ _) = info
    getInfo (TablishJoin info _ _ _ _) = info
    getInfo (TablishLateralView info _ _) = info


instance HasInfo (Selection r a) where
    type Info (Selection r a) = a
    getInfo (SelectStar info _ _) = info
    getInfo (SelectExpr info _ _) = info


instance HasInfo (OrderDirection a) where
    type Info (OrderDirection a) = a
    getInfo (OrderAsc info) = info
    getInfo (OrderDesc info) = info

instance HasInfo (NullPosition a) where
    type Info (NullPosition a) = a
    getInfo (NullsFirst info) = info
    getInfo (NullsLast info) = info
    getInfo (NullsAuto info) = info

instance HasInfo (Offset a) where
    type Info (Offset a) = a
    getInfo (Offset info _) = info

instance HasInfo (Limit a) where
    type Info (Limit a) = a
    getInfo (Limit info _) = info

instance HasInfo (Escape r a) where
    type Info (Escape r a) = a
    getInfo = getInfo . escapeExpr

instance HasInfo (Pattern r a) where
    type Info (Pattern r a) = a
    getInfo = getInfo . patternExpr

instance HasInfo (LambdaParam a) where
    type Info (LambdaParam a) = a
    getInfo (LambdaParam info _ _) = info
