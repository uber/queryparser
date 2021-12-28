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
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.Sql.Type.Query where

import Database.Sql.Type.Names

import Control.Applicative ((<|>))

import qualified Data.Char as Char
import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BL

import Data.Aeson
import Data.Foldable (toList)
import Data.String (IsString (..))

import Data.Data (Data)
import GHC.Generics (Generic)

import Test.QuickCheck


data Query r a
    = QuerySelect a (Select r a)
    | QueryExcept a (ComposedQueryColumns r a) (Query r a) (Query r a)
    | QueryUnion a Distinct (ComposedQueryColumns r a) (Query r a) (Query r a)
    | QueryIntersect a (ComposedQueryColumns r a) (Query r a) (Query r a)
    | QueryWith a [CTE r a] (Query r a)
    | QueryOrder a [Order r a] (Query r a)
    | QueryLimit a (Limit a) (Query r a)
    | QueryOffset a (Offset a) (Query r a)

deriving instance (ConstrainSNames Data r a, Data r) => Data (Query r a)
deriving instance Generic (Query r a)
deriving instance ConstrainSNames Eq r a => Eq (Query r a)
deriving instance ConstrainSNames Ord r a => Ord (Query r a)
deriving instance ConstrainSNames Show r a => Show (Query r a)
deriving instance ConstrainSASNames Functor r => Functor (Query r)
deriving instance ConstrainSASNames Foldable r => Foldable (Query r)
deriving instance ConstrainSASNames Traversable r => Traversable (Query r)

newtype Distinct = Distinct Bool
  deriving (Data, Generic, Eq, Ord, Show)

notDistinct :: Distinct
notDistinct = Distinct False

data CTE r a = CTE
    { cteInfo :: a
    , cteAlias :: TableAlias a
    , cteColumns :: [ColumnAlias a]
    , cteQuery :: Query r a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (CTE r a)
deriving instance Generic (CTE r a)
deriving instance ConstrainSNames Eq r a => Eq (CTE r a)
deriving instance ConstrainSNames Ord r a => Ord (CTE r a)
deriving instance ConstrainSNames Show r a => Show (CTE r a)
deriving instance ConstrainSASNames Functor r => Functor (CTE r)
deriving instance ConstrainSASNames Foldable r => Foldable (CTE r)
deriving instance ConstrainSASNames Traversable r => Traversable (CTE r)


data Select r a = Select
    { selectInfo :: a
    , selectCols :: SelectColumns r a
    , selectFrom :: Maybe (SelectFrom r a)
    , selectWhere :: Maybe (SelectWhere r a)
    , selectTimeseries :: Maybe (SelectTimeseries r a)
    , selectGroup :: Maybe (SelectGroup r a)
    , selectHaving :: Maybe (SelectHaving r a)
    , selectNamedWindow :: Maybe (SelectNamedWindow r a)
    , selectDistinct :: Distinct
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (Select r a)
deriving instance Generic (Select r a)
deriving instance ConstrainSNames Eq r a => Eq (Select r a)
deriving instance ConstrainSNames Ord r a => Ord (Select r a)
deriving instance ConstrainSNames Show r a => Show (Select r a)
deriving instance ConstrainSASNames Functor r => Functor (Select r)
deriving instance ConstrainSASNames Foldable r => Foldable (Select r)
deriving instance ConstrainSASNames Traversable r => Traversable (Select r)


data SelectColumns r a = SelectColumns
    { selectColumnsInfo :: a
    , selectColumnsList :: [Selection r a]
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (SelectColumns r a)
deriving instance Generic (SelectColumns r a)
deriving instance ConstrainSNames Eq r a => Eq (SelectColumns r a)
deriving instance ConstrainSNames Ord r a => Ord (SelectColumns r a)
deriving instance ConstrainSNames Show r a => Show (SelectColumns r a)
deriving instance ConstrainSASNames Functor r => Functor (SelectColumns r)
deriving instance ConstrainSASNames Foldable r => Foldable (SelectColumns r)
deriving instance ConstrainSASNames Traversable r => Traversable (SelectColumns r)


data SelectFrom r a
    = SelectFrom a [Tablish r a]

deriving instance (ConstrainSNames Data r a, Data r) => Data (SelectFrom r a)
deriving instance Generic (SelectFrom r a)
deriving instance ConstrainSNames Eq r a => Eq (SelectFrom r a)
deriving instance ConstrainSNames Ord r a => Ord (SelectFrom r a)
deriving instance ConstrainSNames Show r a => Show (SelectFrom r a)
deriving instance ConstrainSASNames Functor r => Functor (SelectFrom r)
deriving instance ConstrainSASNames Foldable r => Foldable (SelectFrom r)
deriving instance ConstrainSASNames Traversable r => Traversable (SelectFrom r)


data TablishAliases a
    = TablishAliasesNone
    | TablishAliasesT (TableAlias a)
    | TablishAliasesTC (TableAlias a) [ColumnAlias a]
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)


-- Tablish is a Table, Query, or join of 2 Tablishs
data Tablish r a
    = TablishTable a (TablishAliases a) (TableRef r a)
    | TablishSubQuery a (TablishAliases a) (Query r a)
    | TablishJoin a (JoinType a) (JoinCondition r a)
            (Tablish r a) (Tablish r a)
    | TablishLateralView a (LateralView r a) (Maybe (Tablish r a))
    | TablishParenthesizedRelation a (TablishAliases a) (Tablish r a)

deriving instance (ConstrainSNames Data r a, Data r) => Data (Tablish r a)
deriving instance Generic (Tablish r a)
deriving instance ConstrainSNames Eq r a => Eq (Tablish r a)
deriving instance ConstrainSNames Ord r a => Ord (Tablish r a)
deriving instance ConstrainSNames Show r a => Show (Tablish r a)
deriving instance ConstrainSASNames Functor r => Functor (Tablish r)
deriving instance ConstrainSASNames Foldable r => Foldable (Tablish r)
deriving instance ConstrainSASNames Traversable r => Traversable (Tablish r)


data JoinType a
    = JoinInner a  -- JoinInner also encompasses CROSS JOIN (see D508260)
    | JoinLeft a
    | JoinRight a
    | JoinFull a
    | JoinSemi a
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)


data JoinCondition r a
    = JoinNatural a (NaturalColumns r a)
    | JoinOn (Expr r a)
    | JoinUsing a [UsingColumn r a]

deriving instance (ConstrainSNames Data r a, Data r) => Data (JoinCondition r a)
deriving instance Generic (JoinCondition r a)
deriving instance ConstrainSNames Eq r a => Eq (JoinCondition r a)
deriving instance ConstrainSNames Ord r a => Ord (JoinCondition r a)
deriving instance ConstrainSNames Show r a => Show (JoinCondition r a)
deriving instance ConstrainSASNames Functor r => Functor (JoinCondition r)
deriving instance ConstrainSASNames Foldable r => Foldable (JoinCondition r)
deriving instance ConstrainSASNames Traversable r => Traversable (JoinCondition r)

data LateralView r a = LateralView
    { lateralViewInfo :: a
    , lateralViewOuter :: Maybe a
    , lateralViewExprs :: [Expr r a]
    , lateralViewWithOrdinality :: Bool
    , lateralViewAliases :: TablishAliases a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (LateralView r a)
deriving instance Generic (LateralView r a)
deriving instance ConstrainSNames Eq r a => Eq (LateralView r a)
deriving instance ConstrainSNames Ord r a => Ord (LateralView r a)
deriving instance ConstrainSNames Show r a => Show (LateralView r a)
deriving instance ConstrainSASNames Functor r => Functor (LateralView r)
deriving instance ConstrainSASNames Foldable r => Foldable (LateralView r)
deriving instance ConstrainSASNames Traversable r => Traversable (LateralView r)



data SelectWhere r a
    = SelectWhere a (Expr r a)

deriving instance (ConstrainSNames Data r a, Data r) => Data (SelectWhere r a)
deriving instance Generic (SelectWhere r a)
deriving instance ConstrainSNames Eq r a => Eq (SelectWhere r a)
deriving instance ConstrainSNames Ord r a => Ord (SelectWhere r a)
deriving instance ConstrainSNames Show r a => Show (SelectWhere r a)
deriving instance ConstrainSASNames Functor r => Functor (SelectWhere r)
deriving instance ConstrainSASNames Foldable r => Foldable (SelectWhere r)
deriving instance ConstrainSASNames Traversable r => Traversable (SelectWhere r)


data SelectTimeseries r a = SelectTimeseries
    { selectTimeseriesInfo :: a
    , selectTimeseriesSliceName :: ColumnAlias a
    , selectTimeseriesInterval :: Constant a
    , selectTimeseriesPartition :: Maybe (Partition r a)
    , selectTimeseriesOrder :: Expr r a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (SelectTimeseries r a)
deriving instance Generic (SelectTimeseries r a)
deriving instance ConstrainSNames Eq r a => Eq (SelectTimeseries r a)
deriving instance ConstrainSNames Ord r a => Ord (SelectTimeseries r a)
deriving instance ConstrainSNames Show r a => Show (SelectTimeseries r a)
deriving instance ConstrainSASNames Functor r => Functor (SelectTimeseries r)
deriving instance ConstrainSASNames Foldable r => Foldable (SelectTimeseries r)
deriving instance ConstrainSASNames Traversable r => Traversable (SelectTimeseries r)


data PositionOrExpr r a
    = PositionOrExprPosition a Int (PositionExpr r a)
    | PositionOrExprExpr (Expr r a)

deriving instance (ConstrainSNames Data r a, Data r) => Data (PositionOrExpr r a)
deriving instance Generic (PositionOrExpr r a)
deriving instance ConstrainSNames Eq r a => Eq (PositionOrExpr r a)
deriving instance ConstrainSNames Ord r a => Ord (PositionOrExpr r a)
deriving instance ConstrainSNames Show r a => Show (PositionOrExpr r a)
deriving instance ConstrainSASNames Functor r => Functor (PositionOrExpr r)
deriving instance ConstrainSASNames Foldable r => Foldable (PositionOrExpr r)
deriving instance ConstrainSASNames Traversable r => Traversable (PositionOrExpr r)

data GroupingElement r a
    = GroupingElementExpr a (PositionOrExpr r a)
    | GroupingElementSet a [Expr r a]

deriving instance (ConstrainSNames Data r a, Data r) => Data (GroupingElement r a)
deriving instance Generic (GroupingElement r a)
deriving instance ConstrainSNames Eq r a => Eq (GroupingElement r a)
deriving instance ConstrainSNames Ord r a => Ord (GroupingElement r a)
deriving instance ConstrainSNames Show r a => Show (GroupingElement r a)
deriving instance ConstrainSASNames Functor r => Functor (GroupingElement r)
deriving instance ConstrainSASNames Foldable r => Foldable (GroupingElement r)
deriving instance ConstrainSASNames Traversable r => Traversable (GroupingElement r)


data SelectGroup r a = SelectGroup
    { selectGroupInfo :: a
    , selectGroupGroupingElements :: [GroupingElement r a]
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (SelectGroup r a)
deriving instance Generic (SelectGroup r a)
deriving instance ConstrainSNames Eq r a => Eq (SelectGroup r a)
deriving instance ConstrainSNames Ord r a => Ord (SelectGroup r a)
deriving instance ConstrainSNames Show r a => Show (SelectGroup r a)
deriving instance ConstrainSASNames Functor r => Functor (SelectGroup r)
deriving instance ConstrainSASNames Foldable r => Foldable (SelectGroup r)
deriving instance ConstrainSASNames Traversable r => Traversable (SelectGroup r)


data SelectHaving r a
    = SelectHaving a [Expr r a]

deriving instance (ConstrainSNames Data r a, Data r) => Data (SelectHaving r a)
deriving instance Generic (SelectHaving r a)
deriving instance ConstrainSNames Eq r a => Eq (SelectHaving r a)
deriving instance ConstrainSNames Ord r a => Ord (SelectHaving r a)
deriving instance ConstrainSNames Show r a => Show (SelectHaving r a)
deriving instance ConstrainSASNames Functor r => Functor (SelectHaving r)
deriving instance ConstrainSASNames Foldable r => Foldable (SelectHaving r)
deriving instance ConstrainSASNames Traversable r => Traversable (SelectHaving r)

data SelectNamedWindow r a
    = SelectNamedWindow a [NamedWindowExpr r a ]

deriving instance (ConstrainSNames Data r a, Data r) => Data (SelectNamedWindow r a)
deriving instance Generic (SelectNamedWindow r a)
deriving instance ConstrainSNames Eq r a => Eq (SelectNamedWindow r a)
deriving instance ConstrainSNames Ord r a => Ord (SelectNamedWindow r a)
deriving instance ConstrainSNames Show r a => Show (SelectNamedWindow r a)
deriving instance ConstrainSASNames Functor r => Functor (SelectNamedWindow r)
deriving instance ConstrainSASNames Foldable r => Foldable (SelectNamedWindow r)
deriving instance ConstrainSASNames Traversable r => Traversable (SelectNamedWindow r)


data Order r a
    = Order a (PositionOrExpr r a) (OrderDirection (Maybe a)) (NullPosition (Maybe a))

deriving instance (ConstrainSNames Data r a, Data r) => Data (Order r a)
deriving instance Generic (Order r a)
deriving instance ConstrainSNames Eq r a => Eq (Order r a)
deriving instance ConstrainSNames Ord r a => Ord (Order r a)
deriving instance ConstrainSNames Show r a => Show (Order r a)
deriving instance ConstrainSASNames Functor r => Functor (Order r)
deriving instance ConstrainSASNames Foldable r => Foldable (Order r)
deriving instance ConstrainSASNames Traversable r => Traversable (Order r)

data OrderDirection a
    = OrderAsc a
    | OrderDesc a
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data NullPosition a
    = NullsFirst a
    | NullsLast a
    | NullsAuto a
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)


data Offset a
    = Offset a Text
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data Limit a
    = Limit a Text
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)


data Selection r a
    = SelectStar a (Maybe (TableRef r a)) (StarReferents r a)
    | SelectExpr a [ColumnAlias a] (Expr r a)

deriving instance (ConstrainSNames Data r a, Data r) => Data (Selection r a)
deriving instance Generic (Selection r a)
deriving instance ConstrainSNames Eq r a => Eq (Selection r a)
deriving instance ConstrainSNames Ord r a => Ord (Selection r a)
deriving instance ConstrainSNames Show r a => Show (Selection r a)
deriving instance ConstrainSASNames Functor r => Functor (Selection r)
deriving instance ConstrainSASNames Foldable r => Foldable (Selection r)
deriving instance ConstrainSASNames Traversable r => Traversable (Selection r)


data Constant a
    = StringConstant a ByteString
    -- ^ nb: Encoding *probably* matches server encoding, but there are ways to cram arbitrary byte sequences into strings on both Hive and Vertica.
    | NumericConstant a Text
    | NullConstant a
    | BooleanConstant a Bool
    | TypedConstant a Text (DataType a)
    | ParameterConstant a
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data DataTypeParam a
    = DataTypeParamConstant (Constant a)
    | DataTypeParamType (DataType a)
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data DataType a
    = PrimitiveDataType a Text [DataTypeParam a]
    | ArrayDataType a (DataType a)
    | MapDataType a (DataType a) (DataType a)
    | StructDataType a [(Text, DataType a)]
    | UnionDataType a [DataType a]
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)


data Operator a
    = Operator Text
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

instance IsString (Operator a) where
    fromString = Operator . TL.pack

data ArrayIndex a = ArrayIndex a Text
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data Expr r a
    = BinOpExpr a (Operator a) (Expr r a) (Expr r a)
    | CaseExpr a [(Expr r a, Expr r a)] (Maybe (Expr r a))
    | UnOpExpr a (Operator a) (Expr r a)
    | LikeExpr a (Operator a) (Maybe (Escape r a)) (Pattern r a) (Expr r a)
    | ConstantExpr a (Constant a)
    | ColumnExpr a (ColumnRef r a)
    | InListExpr a [Expr r a] (Expr r a)
    | InSubqueryExpr a (Query r a) (Expr r a)
    | BetweenExpr a (Expr r a) (Expr r a) (Expr r a)
    | OverlapsExpr a (Expr r a, Expr r a) (Expr r a, Expr r a)

    | FunctionExpr a
        (FunctionName a)
        Distinct
        [Expr r a]
        [(ParamName a, Expr r a)]
        (Maybe (Filter r a))
        (Maybe (OverSubExpr r a))

    | AtTimeZoneExpr a (Expr r a) (Expr r a)

    | SubqueryExpr a (Query r a)
    | ArrayExpr a [Expr r a]
    | ExistsExpr a (Query r a)
    | FieldAccessExpr a (Expr r a) (StructFieldName a)
    | ArrayAccessExpr a (Expr r a) (Expr r a)
    | TypeCastExpr a CastFailureAction (Expr r a) (DataType a)
    | VariableSubstitutionExpr a
    | LambdaParamExpr a (LambdaParam a)
    | LambdaExpr a [LambdaParam a] (Expr r a)

deriving instance (ConstrainSNames Data r a, Data r) => Data (Expr r a)
deriving instance Generic (Expr r a)
deriving instance ConstrainSNames Eq r a => Eq (Expr r a)
deriving instance ConstrainSNames Ord r a => Ord (Expr r a)
deriving instance ConstrainSNames Show r a => Show (Expr r a)
deriving instance ConstrainSASNames Functor r => Functor (Expr r)
deriving instance ConstrainSASNames Foldable r => Foldable (Expr r)
deriving instance ConstrainSASNames Traversable r => Traversable (Expr r)


data CastFailureAction
    = CastFailureToNull
    | CastFailureError
      deriving (Generic, Data, Eq, Ord, Show, ToJSON, FromJSON)


newtype Escape r a = Escape
    { escapeExpr :: Expr r a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (Escape r a)
deriving instance Generic (Escape r a)
deriving instance ConstrainSNames Eq r a => Eq (Escape r a)
deriving instance ConstrainSNames Ord r a => Ord (Escape r a)
deriving instance ConstrainSNames Show r a => Show (Escape r a)
deriving instance ConstrainSASNames Functor r => Functor (Escape r)
deriving instance ConstrainSASNames Foldable r => Foldable (Escape r)
deriving instance ConstrainSASNames Traversable r => Traversable (Escape r)


newtype Pattern r a = Pattern
    { patternExpr :: Expr r a
    }

deriving instance (ConstrainSNames Data r a, Data r) => Data (Pattern r a)
deriving instance Generic (Pattern r a)
deriving instance ConstrainSNames Eq r a => Eq (Pattern r a)
deriving instance ConstrainSNames Ord r a => Ord (Pattern r a)
deriving instance ConstrainSNames Show r a => Show (Pattern r a)
deriving instance ConstrainSASNames Functor r => Functor (Pattern r)
deriving instance ConstrainSASNames Foldable r => Foldable (Pattern r)
deriving instance ConstrainSASNames Traversable r => Traversable (Pattern r)

data Filter r a
    = Filter
        { filterInfo :: a
        , filterExpr :: Expr r a
        }

deriving instance (ConstrainSNames Data r a, Data r) => Data (Filter r a)
deriving instance Generic (Filter r a)
deriving instance ConstrainSNames Eq r a => Eq (Filter r a)
deriving instance ConstrainSNames Ord r a => Ord (Filter r a)
deriving instance ConstrainSNames Show r a => Show (Filter r a)
deriving instance ConstrainSASNames Functor r => Functor (Filter r)
deriving instance ConstrainSASNames Foldable r => Foldable (Filter r)
deriving instance ConstrainSASNames Traversable r => Traversable (Filter r)


data Partition r a
    = PartitionBy a [Expr r a]
    | PartitionBest a
    | PartitionNodes a

deriving instance (ConstrainSNames Data r a, Data r) => Data (Partition r a)
deriving instance Generic (Partition r a)
deriving instance ConstrainSNames Eq r a => Eq (Partition r a)
deriving instance ConstrainSNames Ord r a => Ord (Partition r a)
deriving instance ConstrainSNames Show r a => Show (Partition r a)
deriving instance ConstrainSASNames Functor r => Functor (Partition r)
deriving instance ConstrainSASNames Foldable r => Foldable (Partition r)
deriving instance ConstrainSASNames Traversable r => Traversable (Partition r)

data FrameType a
    = RowFrame a
    | RangeFrame a
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data FrameBound a
    = Unbounded a
    | CurrentRow a
    | Preceding a (Constant a)
    | Following a (Constant a)
      deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data Frame a = Frame
    { frameInfo :: a
    , frameType :: FrameType a
    , frameStart :: FrameBound a
    , frameEnd :: Maybe (FrameBound a)
    } deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data OverSubExpr r a
    = OverWindowExpr        a (WindowExpr r a)
    | OverWindowName        a (WindowName a)
    | OverPartialWindowExpr a (PartialWindowExpr r a)
deriving instance (ConstrainSNames Data r a, Data r) => Data (OverSubExpr r a)
deriving instance Generic (OverSubExpr r a)
deriving instance ConstrainSNames Eq r a => Eq (OverSubExpr r a)
deriving instance ConstrainSNames Ord r a => Ord (OverSubExpr r a)
deriving instance ConstrainSNames Show r a => Show (OverSubExpr r a)
deriving instance ConstrainSASNames Functor r => Functor (OverSubExpr r)
deriving instance ConstrainSASNames Foldable r => Foldable (OverSubExpr r)
deriving instance ConstrainSASNames Traversable r => Traversable (OverSubExpr r)

data WindowExpr r a
    = WindowExpr
    { windowExprInfo :: a
    , windowExprPartition :: Maybe (Partition r a)
    , windowExprOrder :: [Order r a]
    , windowExprFrame :: Maybe (Frame a)
    }
deriving instance (ConstrainSNames Data r a, Data r) => Data (WindowExpr r a)
deriving instance Generic (WindowExpr r a)
deriving instance ConstrainSNames Eq r a => Eq (WindowExpr r a)
deriving instance ConstrainSNames Ord r a => Ord (WindowExpr r a)
deriving instance ConstrainSNames Show r a => Show (WindowExpr r a)
deriving instance ConstrainSASNames Functor r => Functor (WindowExpr r)
deriving instance ConstrainSASNames Foldable r => Foldable (WindowExpr r)
deriving instance ConstrainSASNames Traversable r => Traversable (WindowExpr r)

data PartialWindowExpr r a
    = PartialWindowExpr
    { partWindowExprInfo :: a
    , partWindowExprInherit :: WindowName a
    , partWindowExprPartition :: Maybe (Partition r a)
    , partWindowExprOrder :: [Order r a]
    , partWindowExprFrame :: Maybe (Frame a)
    }
deriving instance (ConstrainSNames Data r a, Data r) => Data (PartialWindowExpr r a)
deriving instance Generic (PartialWindowExpr r a)
deriving instance ConstrainSNames Eq r a => Eq (PartialWindowExpr r a)
deriving instance ConstrainSNames Ord r a => Ord (PartialWindowExpr r a)
deriving instance ConstrainSNames Show r a => Show (PartialWindowExpr r a)
deriving instance ConstrainSASNames Functor r => Functor (PartialWindowExpr r)
deriving instance ConstrainSASNames Foldable r => Foldable (PartialWindowExpr r)
deriving instance ConstrainSASNames Traversable r => Traversable (PartialWindowExpr r)

data WindowName a = WindowName a Text
  deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data NamedWindowExpr r a
    = NamedWindowExpr        a (WindowName a) (WindowExpr r a)
    | NamedPartialWindowExpr a (WindowName a) (PartialWindowExpr r a)
deriving instance (ConstrainSNames Data r a, Data r) => Data (NamedWindowExpr r a)
deriving instance Generic (NamedWindowExpr r a)
deriving instance ConstrainSNames Eq r a => Eq (NamedWindowExpr r a)
deriving instance ConstrainSNames Ord r a => Ord (NamedWindowExpr r a)
deriving instance ConstrainSNames Show r a => Show (NamedWindowExpr r a)
deriving instance ConstrainSASNames Functor r => Functor (NamedWindowExpr r)
deriving instance ConstrainSASNames Foldable r => Foldable (NamedWindowExpr r)
deriving instance ConstrainSASNames Traversable r => Traversable (NamedWindowExpr r)

-- ToJSON

instance ConstrainSNames ToJSON r a => ToJSON (Query r a) where
    toJSON (QuerySelect info select) = object
        [ "tag" .= String "QuerySelect"
        , "info" .= info
        , "select" .= select
        ]

    toJSON (QueryExcept info columns lhs rhs) = object
        [ "tag" .= String "Except"
        , "info" .= info
        , "columns" .= columns
        , "lhs" .= lhs
        , "rhs" .= rhs
        ]

    toJSON (QueryUnion info distinct columns lhs rhs) = object
        [ "tag" .= String "Union"
        , "info" .= info
        , "distinct" .= distinct
        , "columns" .= columns
        , "lhs" .= lhs
        , "rhs" .= rhs
        ]

    toJSON (QueryIntersect info columns lhs rhs) = object
        [ "tag" .= String "Intersect"
        , "info" .= info
        , "columns" .= columns
        , "lhs" .= lhs
        , "rhs" .= rhs
        ]

    toJSON (QueryWith info ctes query) = object
        [ "tag" .= String "With"
        , "info" .= info
        , "ctes" .= ctes
        , "query" .= query
        ]

    toJSON (QueryOrder info orders query) = object
        [ "tag" .= String "QueryOrder"
        , "info" .= info
        , "orders" .= orders
        , "query" .= query
        ]

    toJSON (QueryLimit info limit query) = object
        [ "tag" .= String "QueryLimit"
        , "info" .= info
        , "limit" .= limit
        , "query" .= query
        ]

    toJSON (QueryOffset info offset query) = object
        [ "tag" .= String "QueryOffset"
        , "info" .= info
        , "offset" .= offset
        , "query" .= query
        ]


instance ToJSON Distinct where
    toJSON (Distinct bool) = toJSON bool


instance ConstrainSNames ToJSON r a => ToJSON (CTE r a) where
    toJSON (CTE {..}) = object
        [ "tag" .= String "CTE"
        , "alias" .= cteAlias
        , "columns" .= cteColumns
        , "query" .= cteQuery
        ]


instance ConstrainSNames ToJSON r a => ToJSON (Select r a) where
    toJSON (Select {..}) = object
         [ "tag" .= String "Select"
         , "cols" .= selectCols
         , "from" .= selectFrom
         , "where" .= selectWhere
         , "timeseries" .= selectTimeseries
         , "group" .= selectGroup
         , "having" .= selectHaving
         , "window" .= selectNamedWindow
         , "distinct" .= selectDistinct
         ]


instance ConstrainSNames ToJSON r a => ToJSON (SelectColumns r a) where
    toJSON (SelectColumns info columns) = object
        [ "tag" .= String "SelectColumns"
        , "info" .= info
        , "columns" .= columns
        ]


instance ConstrainSNames ToJSON r a => ToJSON (SelectFrom r a) where
    toJSON (SelectFrom info tables) = object
        [ "tag" .= String "SelectFrom"
        , "info" .= info
        , "tables" .= tables
        ]


instance ConstrainSNames ToJSON r a => ToJSON (SelectWhere r a) where
    toJSON (SelectWhere info conditions) = object
        [ "tag" .= String "SelectWhere"
        , "info" .= info
        , "conditions" .= conditions
        ]


instance ConstrainSNames ToJSON r a=> ToJSON (SelectTimeseries r a) where
    toJSON SelectTimeseries{..} = object
        [ "tag" .= String "SelectTimeseries"
        , "info" .= selectTimeseriesInfo
        , "slice_name" .= selectTimeseriesSliceName
        , "interval" .= selectTimeseriesInterval
        , "partition" .= selectTimeseriesPartition
        , "order" .= selectTimeseriesOrder
        ]


instance ConstrainSNames ToJSON r a => ToJSON (PositionOrExpr r a) where
    toJSON (PositionOrExprPosition info pos expr) = object
        [ "tag" .= String "PositionOrExprPosition"
        , "info" .= info
        , "position" .= pos
        , "expr" .= expr
        ]
    toJSON (PositionOrExprExpr expr) = object
        [ "tag" .= String "PositionOrExprExpr"
        , "expr" .= expr
        ]


instance ConstrainSNames ToJSON r a => ToJSON (GroupingElement r a) where
    toJSON (GroupingElementExpr info posOrExpr) = object
        [ "tag" .= String "GroupingElementExpr"
        , "info" .= info
        , "position_or_expr" .= posOrExpr
        ]
    toJSON (GroupingElementSet info exprs) = object
        [ "tag" .= String "GroupingElementSet"
        , "info" .= info
        , "exprs" .= exprs
        ]


instance ConstrainSNames ToJSON r a => ToJSON (SelectGroup r a) where
    toJSON SelectGroup{..} = object
        [ "tag" .= String "SelectGroup"
        , "info" .= selectGroupInfo
        , "grouping_elements" .= selectGroupGroupingElements
        ]


instance ConstrainSNames ToJSON r a => ToJSON (SelectHaving r a) where
    toJSON (SelectHaving info conditions) = object
        [ "tag" .= String "SelectHaving"
        , "info" .= info
        , "conditions" .= conditions
        ]


instance ConstrainSNames ToJSON r a => ToJSON (SelectNamedWindow r a) where
    toJSON (SelectNamedWindow info windows) = object
        [ "tag" .= String "SelectNamedWindow"
        , "info" .= info
        , "windows" .= windows
        ]


instance ConstrainSNames ToJSON r a => ToJSON (Selection r a) where
    toJSON (SelectStar info table referents) = object
        [ "tag" .= String "SelectStar"
        , "info" .= info
        , "table" .= table
        , "referents" .= referents
        ]

    toJSON (SelectExpr info aliases expr) = object
        [ "tag" .= String "SelectExpr"
        , "info" .= info
        , "aliases" .= aliases
        , "expr" .= expr
        ]


instance ToJSON a => ToJSON (Constant a) where
    toJSON (StringConstant info value) = object
        [ "tag" .= String "StringConstant"
        , "info" .= info
        , case TL.decodeUtf8' value of
            Left _ -> "value" .= BL.unpack value
            Right str -> "value" .= str
        ]

    toJSON (NumericConstant info value) = object
        [ "tag" .= String "NumericConstant"
        , "info" .= info
        , "value" .= value
        ]

    toJSON (NullConstant info) = object
        [ "tag" .= String "NullConstant"
        , "info" .= info
        ]

    toJSON (BooleanConstant info value) = object
        [ "tag" .= String "BooleanConstant"
        , "info" .= info
        , "value" .= value
        ]

    toJSON (TypedConstant info value type_) = object
        [ "tag" .= String "TypedConstant"
        , "info" .= info
        , "value" .= value
        , "type" .= type_
        ]

    toJSON (ParameterConstant info) = object
        [ "tag" .= String "ParameterConstant"
        , "info" .= info
        ]


instance ConstrainSNames ToJSON r a => ToJSON (Expr r a) where

    toJSON (BinOpExpr info op lhs rhs) = object
        [ "tag" .= String "BinOpExpr"
        , "info" .= info
        , "op" .= op
        , "lhs" .= lhs
        , "rhs" .= rhs
        ]

    toJSON (UnOpExpr info op expr) = object
        [ "tag" .= String "UnOpExpr"
        , "info" .= info
        , "op" .= op
        , "expr" .= expr
        ]

    toJSON (LikeExpr info op escape pattern expr) = object
        [ "tag" .= String "LikeExpr"
        , "info" .= info
        , "op" .= op
        , "escape" .= fmap escapeExpr escape
        , "pattern" .= patternExpr pattern
        , "expr" .= expr
        ]

    toJSON (CaseExpr info whens melse) = object
        [ "tag" .= String "CaseExpr"
        , "info" .= info

        , "whens" .=
            let conditionToJSON (c, r) =
                    object ["condition" .= c
                           , "result" .= r
                           ]
             in map conditionToJSON whens

        , "else" .= melse
        ]

    toJSON (ConstantExpr info constant) = object
        [ "tag" .= String "ConstantExpr"
        , "info" .= info
        , "constant" .= constant
        ]

    toJSON (ColumnExpr info column) = object
        [ "tag" .= String "ColumnExpr"
        , "info" .= info
        , "column" .= column
        ]

    toJSON (InListExpr info array expr) = object
        [ "tag" .= String "InListExpr"
        , "info" .= info
        , "array" .= array
        , "expr" .= expr
        ]

    toJSON (InSubqueryExpr info query expr) = object
        [ "tag" .= String "InSubqueryExpr"
        , "info" .= info
        , "query" .= query
        , "expr" .= expr
        ]

    toJSON (BetweenExpr info expr start end) = object
        [ "tag" .= String "BetweenExpr"
        , "info" .= info
        , "expr" .= expr
        , "start" .= start
        , "end" .= end
        ]

    toJSON (OverlapsExpr info lhs rhs) = object
        [ "tag" .= String "OverlapsExpr"
        , "info" .= info
        , "ranges" .=
            let rangeToJSON (start, end) =
                    object [ "start" .= start, "end" .= end ]
             in map rangeToJSON [lhs, rhs]
        ]

    toJSON (AtTimeZoneExpr info expr tz) = object
        [ "tag" .= String "AtTimeZoneExpr"
        , "info" .= info
        , "expr" .= expr
        , "tz" .= tz
        ]

    toJSON (SubqueryExpr info query) = object
        [ "tag" .= String "SubqueryExpr"
        , "info" .= info
        , "query" .= query
        ]

    toJSON (FunctionExpr info fn distinct args params filter' over) = object
        [ "tag" .= String "FunctionExpr"
        , "info" .= info
        , "function" .= fn
        , "distinct" .= distinct
        , "args" .= args
        , "params" .= params
        , "filter" .= filter'
        , "over" .= over
        ]

    toJSON (ExistsExpr info query) = object
        [ "tag" .= String "ExistsExpr"
        , "info" .= info
        , "query" .= query
        ]

    toJSON (ArrayExpr info values) = object
        [ "tag" .= String "ArrayExpr"
        , "info" .= info
        , "values" .= values
        ]

    toJSON (FieldAccessExpr info struct field) = object
        [ "tag" .= String "FieldAccessExpr"
        , "info" .= info
        , "struct" .= struct
        , "field" .= field
        ]

    toJSON (ArrayAccessExpr info expr idx) = object
        [ "tag" .= String "ArrayAccessExpr"
        , "info" .= info
        , "expr" .= expr
        , "idx" .= idx
        ]

    toJSON (TypeCastExpr info onFail expr type_) = object
        [ "tag" .= String "TypeCastExpr"
        , "info" .= info
        , "onfail" .= onFail
        , "expr" .= expr
        , "type" .= type_
        ]

    toJSON (VariableSubstitutionExpr info) = object
        [ "tag" .= String "VariableSubstitutionExpr"
        , "info" .= info
        ]

    toJSON (LambdaParamExpr info param) = object
        [ "tag" .= String "LambdaParamExpr"
        , "info" .= info
        , "param" .= param
        ]

    toJSON (LambdaExpr info params body) = object
        [ "tag" .= String "LambdaExpr"
        , "info" .= info
        , "params" .= params
        , "body" .= body
        ]

instance ToJSON a => ToJSON (ArrayIndex a) where
    toJSON (ArrayIndex info value) = object
        [ "tag" .= String "ArrayIndex"
        , "info" .= info
        , "value" .= value
        ]


instance ToJSON a => ToJSON (DataTypeParam a) where
    toJSON (DataTypeParamConstant constant) = object
        [ "tag" .= String "DataTypeParamConstant"
        , "param" .= constant
        ]
    toJSON (DataTypeParamType type_) = object
        [ "tag" .= String "DataTypeParamType"
        , "param" .= type_
        ]


instance ToJSON a => ToJSON (DataType a) where
    toJSON (PrimitiveDataType info name args) = object
        [ "tag" .= String "PrimitiveDataType"
        , "info" .= info
        , "name" .= name
        , "args" .= args
        ]
    toJSON (ArrayDataType info itemType) = object
        [ "tag" .= String "ArrayDataType"
        , "info" .= info
        , "itemType" .= itemType
        ]
    toJSON (MapDataType info keyType valueType) = object
        [ "tag" .= String "MapDataType"
        , "info" .= info
        , "keyType" .= keyType
        , "valueType" .= valueType
        ]
    toJSON (StructDataType info fields) = object
        [ "tag" .= String "StructDataType"
        , "info" .= info
        , "fields" .= fields
        ]
    toJSON (UnionDataType info types) = object
        [ "tag" .= String "UnionDataType"
        , "info" .= info
        , "types" .= types
        ]


instance ConstrainSNames ToJSON r a => ToJSON (Filter r a) where
    toJSON (Filter info expr) = object
        [ "tag" .= String "Filter"
        , "info" .= info
        , "expr" .= expr
        ]


instance ConstrainSNames ToJSON r a => ToJSON (OverSubExpr r a) where
    toJSON (OverWindowExpr info windowExpr) = object
        [ "tag" .= String "OverWindowExpr"
        , "info" .= info
        , "windowExpr" .= windowExpr
        ]

    toJSON (OverWindowName info windowName) = object
        [ "tag" .= String "OverWindowName"
        , "info" .= info
        , "windowName" .= windowName
        ]

    toJSON (OverPartialWindowExpr info partialWindowExpr) = object
        [ "tag" .= String "OverPartialWindowExpr"
        , "info" .= info
        , "partialWindowExpr" .= partialWindowExpr
        ]

instance ConstrainSNames ToJSON r a => ToJSON (WindowExpr r a) where
    toJSON (WindowExpr info partition order frame) = object
        [ "tag" .= String "WindowExpr"
        , "info" .= info
        , "partition" .= partition
        , "order" .= order
        , "frame" .= frame
        ]

instance ConstrainSNames ToJSON r a => ToJSON (PartialWindowExpr r a) where
    toJSON (PartialWindowExpr info inherit partition order frame) = object
        [ "tag" .= String "PartialWindowExpr"
        , "info" .= info
        , "inherit" .= inherit
        , "partition" .= partition
        , "order" .= order
        , "frame" .= frame
        ]

instance ToJSON a => ToJSON (WindowName a) where
    toJSON (WindowName info name) = object
        [ "tag" .= String "WindowName"
        , "info" .= info
        , "name" .= name
        ]


instance ConstrainSNames ToJSON r a => ToJSON (NamedWindowExpr r a) where
    toJSON (NamedWindowExpr info name window) = object
        [ "tag" .= String "NamedWindowExpr"
        , "info" .= info
        , "name" .= name
        , "window" .= window
        ]
    toJSON (NamedPartialWindowExpr info name partialWindow) = object
        [ "tag" .= String "NamedWindowExpr"
        , "info" .= info
        , "name" .= name
        , "partialWindow" .= partialWindow
        ]


instance ConstrainSNames ToJSON r a => ToJSON (Partition r a) where
    toJSON (PartitionBest info) = object
        [ "tag" .= String "PartitionBest"
        , "info" .= info
        ]

    toJSON (PartitionNodes info) = object
        [ "tag" .= String "PartitionNodes"
        , "info" .= info
        ]

    toJSON (PartitionBy info expr) = object
        [ "tag" .= String "PartitionBy"
        , "info" .= info
        , "expr" .= expr
        ]

instance ConstrainSNames ToJSON r a => ToJSON (Order r a) where
    toJSON (Order info posOrExpr direction nullPos) = object
        [ "tag" .= String "Order"
        , "info" .= info
        , "position_or_expr" .= posOrExpr
        , "direction" .= direction
        , "nullPos" .= nullPos
        ]

instance ToJSON a => ToJSON (FrameType a) where
    toJSON (RowFrame info) = object
        [ "tag" .= String "RowFrame"
        , "info" .= info
        ]

    toJSON (RangeFrame info) = object
        [ "tag" .= String "RangeFrame"
        , "info" .= info
        ]


instance ToJSON a => ToJSON (FrameBound a) where
    toJSON (Unbounded info) = object
        [ "tag" .= String "Unbounded"
        , "info" .= info
        ]

    toJSON (CurrentRow info) = object
        [ "tag" .= String "CurrentRow"
        , "info" .= info
        ]

    toJSON (Preceding info bound) = object
        [ "tag" .= String "Preceding"
        , "info" .= info
        , "bound" .= bound
        ]

    toJSON (Following info bound) = object
        [ "tag" .= String "Following"
        , "info" .= info
        , "bound" .= bound
        ]


instance ToJSON a => ToJSON (Frame a) where
    toJSON (Frame info ftype start end) = object
        [ "tag" .= String "Frame"
        , "info" .= info
        , "ftype" .= ftype
        , "start" .= start
        , "end" .= end
        ]


instance ToJSON a => ToJSON (Offset a) where
    toJSON (Offset info offset) = object
        [ "tag" .= String "Offset"
        , "info" .= info
        , "offset" .= offset
        ]


instance ToJSON a => ToJSON (Limit a) where
    toJSON (Limit info limit) = object
        [ "tag" .= String "Limit"
        , "info" .= info
        , "limit" .= limit
        ]


instance ToJSON (Operator a) where
    toJSON (Operator op) = object
        [ "tag" .= String "Operator"
        , "operator" .= op
        ]


instance ToJSON a => ToJSON (JoinType a) where
    toJSON (JoinInner info) = object
        [ "tag" .= String "JoinInner"
        , "info" .= info
        ]
    toJSON (JoinLeft info) = object
        [ "tag" .= String "JoinLeft"
        , "info" .= info
        ]
    toJSON (JoinRight info) = object
        [ "tag" .= String "JoinRight"
        , "info" .= info
        ]
    toJSON (JoinFull info) = object
        [ "tag" .= String "JoinFull"
        , "info" .= info
        ]
    toJSON (JoinSemi info) = object
        [ "tag" .= String "JoinSemi"
        , "info" .= info
        ]


instance ConstrainSNames ToJSON r a => ToJSON (JoinCondition r a) where
    toJSON (JoinNatural info columns) = object [ "tag" .= String "JoinNatural", "info" .= info, "columns" .= columns ]
    toJSON (JoinOn expr) = object
        [ "tag" .= String "JoinOn"
        , "expr" .= expr
        ]
    toJSON (JoinUsing info columns) = object
        [ "tag" .= String "JoinUsing"
        , "info" .= info
        , "columns" .= columns
        ]


instance ToJSON a => ToJSON (TablishAliases a) where
    toJSON (TablishAliasesNone) = object
        [ "tag" .= String "TablishAliasesNone"
        ]
    toJSON (TablishAliasesT table) = object
        [ "tag" .= String "TablishAliasesT"
        , "table" .= table
        ]
    toJSON (TablishAliasesTC table columns) = object
        [ "tag" .= String "TablishAliasesTC"
        , "table" .= table
        , "columns" .= columns
        ]


instance ConstrainSNames ToJSON r a => ToJSON (Tablish r a) where
    toJSON (TablishTable info aliases table) = object
        [ "tag" .= String "TablishTable"
        , "info" .= info
        , "aliases" .= aliases
        , "table" .= table
        ]

    toJSON (TablishSubQuery info alias query) = object
        [ "tag" .= String "TablishSubQuery"
        , "info" .= info
        , "alias" .= alias
        , "query" .= query
        ]

    toJSON (TablishParenthesizedRelation info alias relation) = object
        [ "tag" .= String "TablishParenthesizedRelation"
        , "info" .= info
        , "alias" .= alias
        , "realtion" .= relation
        ]

    toJSON (TablishJoin info join condition outer inner) = object
        [ "tag" .= String "TablishJoin"
        , "info" .= info
        , "join" .= join
        , "condition" .= condition
        , "outer" .= outer
        , "inner" .= inner
        ]

    toJSON (TablishLateralView info view lhs) = object
        [ "tag" .= String "TablishLateralView"
        , "info" .= info
        , "view" .= view
        , "lhs" .= lhs
        ]


instance ConstrainSNames ToJSON r a => ToJSON (LateralView r a) where
    toJSON LateralView{..} = object
        [ "tag" .= String "LateralView"
        , "info" .= lateralViewInfo
        , "outer" .= lateralViewOuter
        , "exprs" .= lateralViewExprs
        , "with_ordinality" .= lateralViewWithOrdinality
        , "aliases" .= lateralViewAliases
        ]


instance ToJSON a => ToJSON (OrderDirection a) where
    toJSON (OrderAsc info) = object
        [ "tag" .= String "OrderAsc"
        , "info" .= info
        ]

    toJSON (OrderDesc info) = object
        [ "tag" .= String "OrderDesc"
        , "info" .= info
        ]


instance ToJSON a => ToJSON (NullPosition a) where
    toJSON (NullsFirst info) = object
        [ "tag" .= String "NullsFirst"
        , "info" .= info
        ]

    toJSON (NullsLast info) = object
        [ "tag" .= String "NullsLast"
        , "info" .= info
        ]

    toJSON (NullsAuto info) = object
        [ "tag" .= String "NullsAuto"
        , "info" .= info
        ]


-- FromJSON

instance ConstrainSNames FromJSON r a => FromJSON (Query r a) where
    parseJSON (Object o) = o .: "tag" >>= \case
            String "QuerySelect" -> QuerySelect
                <$> o .: "info"
                <*> o .: "select"

            String "QueryExcept" -> QueryExcept
                <$> o .: "info"
                <*> o .: "columns"
                <*> o .: "lhs"
                <*> o .: "rhs"

            String "QueryUnion" -> QueryUnion
                <$> o .: "info"
                <*> o .: "distinct"
                <*> o .: "columns"
                <*> o .: "lhs"
                <*> o .: "rhs"

            String "QueryIntersect" -> QueryIntersect
                <$> o .: "info"
                <*> o .: "columns"
                <*> o .: "lhs"
                <*> o .: "rhs"

            String "QueryWith" -> QueryWith
                <$> o .: "info"
                <*> o .: "ctes"
                <*> o .: "query"

            String "QueryOrder" -> QueryOrder
                <$> o .: "info"
                <*> o .: "orders"
                <*> o .: "query"

            String "QueryLimit" -> QueryLimit
                <$> o .: "info"
                <*> o .: "limit"
                <*> o .: "query"

            String "QueryOffset" -> QueryOffset
                <$> o .: "info"
                <*> o .: "offset"
                <*> o .: "query"

            _ -> fail "unrecognized tag on query object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Query:"
        , show v
        ]


instance FromJSON Distinct where
    parseJSON (Bool bool) = return $ Distinct bool
    parseJSON v = fail $ unwords
        [ "don't know how to parse as Distinct:"
        , show v
        ]

instance ConstrainSNames FromJSON r a => FromJSON (CTE r a) where
    parseJSON (Object o) = do
        String "CTE" <- o .: "tag"
        cteInfo <- o .: "info"
        cteAlias <- o .: "alias"
        cteColumns <- o .: "columns"
        cteQuery <- o .: "query"
        return CTE{..}

    parseJSON v = fail $ unwords
        [ "don't know how to parse as CTE:"
        , show v
        ]


instance FromJSON a => FromJSON (OrderDirection a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "OrderAsc" -> OrderAsc <$> o .: "info"
        String "OrderDesc" -> OrderDesc <$> o .: "info"
        _ -> fail "unrecognized tag on order direction object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as OrderDirection:"
        ,  show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (Selection r a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "SelectStar" ->
            SelectStar
                <$> o .: "info"
                <*> o .: "table"
                <*> o .: "referents"

        String "SelectExpr" ->
            SelectExpr
                <$> o .: "info"
                <*> o .: "aliases"
                <*> o .: "expr"

        _ -> fail "unrecognized tag on selection object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Selection:"
        , show v
        ]


instance FromJSON a => FromJSON (Constant a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "StringConstant" -> do
            info <- o .: "info"
            value <- TL.encodeUtf8 <$> o .: "value"
                <|> BL.pack <$> o .: "value"
                <|> fail "expected string or array for StringConstant value"
            pure $ StringConstant info value

        String "NumericConstant" ->
            NumericConstant
                <$> o .: "info"
                <*> o .: "value"

        String "NullConstant" ->
            NullConstant <$> o .: "info"

        String "BooleanConstant" ->
            BooleanConstant
                <$> o .: "info"
                <*> o .: "value"

        String "TypedConstant" ->
            TypedConstant
                <$> o .: "info"
                <*> o .: "value"
                <*> o .: "type"

        String "ParameterConstant" ->
            ParameterConstant <$> o .: "info"

        _ -> fail "unrecognized tag on constant object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Constant:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (Expr r a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "BinOpExpr" ->
            BinOpExpr
                <$> o .: "info"
                <*> o .: "op"
                <*> o .: "lhs"
                <*> o .: "rhs"

        String "UnOpExpr" ->
            UnOpExpr
                <$> o .: "info"
                <*> o .: "op"
                <*> o .: "expr"

        String "LikeExpr" ->
            LikeExpr
                <$> o .: "info"
                <*> o .: "op"
                <*> (fmap Escape <$> o .: "escape")
                <*> (Pattern <$> o .: "pattern")
                <*> o .: "expr"

        String "CaseExpr" -> do
            let jsonToWhen (Object w) =
                    (,) <$> w .: "condition"
                        <*> w .: "result"

                jsonToWhen v = fail $ unwords
                    [ "don't know how to parse as (Expr, Expr):"
                    , show v
                    ]

            whens <- mapM jsonToWhen =<< o .: "whens"

            CaseExpr <$> o .: "info" <*> pure whens <*> o .: "else"

        String "ConstantExpr" ->
            ConstantExpr
                <$> o .: "info"
                <*> o .: "constant"

        String "ColumnExpr" ->
            ColumnExpr
                <$> o .: "info"
                <*> o .: "column"

        String "InListExpr" ->
            InListExpr
                <$> o .: "info"
                <*> o .: "array"
                <*> o .: "expr"

        String "InSubqueryExpr" ->
            InSubqueryExpr
                <$> o .: "info"
                <*> o .: "query"
                <*> o .: "expr"

        String "BetweenExpr" ->
            BetweenExpr
                <$> o .: "info"
                <*> o .: "expr"
                <*> o .: "start"
                <*> o .: "end"

        String "OverlapsExpr" -> do
            info <- o .: "info"
            Array ranges <- o .: "ranges"

            let jsonToRange (Object r) = (,) <$> r .: "start"
                                             <*> r .: "end"
                jsonToRange v = fail $ unwords
                    [ "don't know how to parse as (Expr, Expr):"
                    , show v
                    ]

            [range1, range2] <- mapM jsonToRange $ toList ranges

            return $ OverlapsExpr info range1 range2

        String "FunctionExpr" ->
            FunctionExpr
                <$> o .: "info"
                <*> o .: "function"
                <*> o .: "distinct"
                <*> o .: "args"
                <*> o .: "params"
                <*> o .: "filter"
                <*> o .: "over"

        String "AtTimeZoneExpr" ->
            AtTimeZoneExpr
                <$> o .: "info"
                <*> o .: "expr"
                <*> o .: "tz"

        String "ArrayExpr" ->
            ArrayExpr
                <$> o .: "info"
                <*> o .: "values"

        String "SubqueryExpr" ->
            SubqueryExpr
                <$> o .: "info"
                <*> o .: "query"

        String "ExistsExpr" ->
            ExistsExpr
                <$> o .: "info"
                <*> o .: "query"

        String "FieldAccessExpr" ->
            FieldAccessExpr
                <$> o .: "info"
                <*> o .: "struct"
                <*> o .: "field"

        String "ArrayAccessExpr" ->
            ArrayAccessExpr
                <$> o .: "info"
                <*> o .: "expr"
                <*> o .: "idx"

        String "TypeCastExpr" ->
            TypeCastExpr
                <$> o .: "info"
                <*> o .: "onfailure"
                <*> o .: "expr"
                <*> o .: "type"

        String "VariableSubstitutionExpr" ->
            VariableSubstitutionExpr <$> o .: "info"

        _ -> fail "unrecognized tag on expression object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Expr:"
        , show v
        ]


instance FromJSON a => FromJSON (ArrayIndex a) where
    parseJSON (Object o) =  do
        String "ArrayIndex" <- o .: "tag"
        ArrayIndex
            <$> o .: "info"
            <*> o .: "value"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as ArrayIndex:"
        , show v
        ]


instance FromJSON a => FromJSON (DataTypeParam a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "DataTypeParamConstant" ->
            DataTypeParamConstant <$> o .: "param"
        String "DataTypeParamType" ->
            DataTypeParamType <$> o .: "param"

        _ -> fail "unrecognized tag on data type param object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as DataTypeParam:"
        , show v
        ]

instance FromJSON a => FromJSON (DataType a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "PrimitiveDataType" ->
            PrimitiveDataType
                <$> o .: "info"
                <*> o .: "name"
                <*> o .: "args"
        String "ArrayDataType" ->
            ArrayDataType
                <$> o .: "info"
                <*> o .: "itemType"
        String "MapDataType" ->
            MapDataType
                <$> o .: "info"
                <*> o .: "keyType"
                <*> o .: "valueType"
        String "StructDataType" ->
            StructDataType
                <$> o .: "info"
                <*> o .: "fields"
        String "UnionDataType" ->
            UnionDataType
                <$> o .: "info"
                <*> o .: "types"

        _ -> fail "unrecognized tag on data type object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as DataType:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (Filter r a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "Filter" ->
          Filter
            <$> o .: "info"
            <*> o .: "expr"
        _ -> fail "unrecognized tag on Filter object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Filter:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (OverSubExpr r a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "OverWindowExpr" ->
          OverWindowExpr
            <$> o .: "info"
            <*> o .: "windowExpr"
        String "OverWindowName" ->
          OverWindowName
            <$> o .: "info"
            <*> o .: "windowName"
        String "OverPartialWindowExpr" ->
          OverPartialWindowExpr
            <$> o .: "info"
            <*> o .: "partialWindowExpr"
        _ -> fail "unrecognized tag on OverSubExpr object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as OverSubExpr:"
        , show v
        ]

instance ConstrainSNames FromJSON r a => FromJSON (WindowExpr r a) where
    parseJSON (Object o) = do
        String "WindowExpr" <- o .: "tag"
        WindowExpr
            <$> o .: "info"
            <*> o .: "partition"
            <*> o .: "order"
            <*> o .: "frame"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as WindowExpr:"
        , show v
        ]

instance ConstrainSNames FromJSON r a => FromJSON (PartialWindowExpr r a) where
    parseJSON (Object o) =
      do
        String "PartialWindowExpr" <- o .: "tag"
        PartialWindowExpr
            <$> o .: "info"
            <*> o .: "inherit"
            <*> o .: "partition"
            <*> o .: "order"
            <*> o .: "frame"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as PartialWindowExpr:"
        , show v
        ]

instance FromJSON a => FromJSON (WindowName a) where
    parseJSON (Object o) =
      do
        String "WindowName" <- o .: "tag"
        WindowName
            <$> o .: "info"
            <*> o .: "name"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as WindowName:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (NamedWindowExpr r a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "NamedWindowExpr" ->
          NamedWindowExpr
            <$> o .: "info"
            <*> o .: "name"
            <*> o .: "window"
        String "NamedPartialWindowExpr" ->
          NamedPartialWindowExpr
            <$> o .: "info"
            <*> o .: "name"
            <*> o .: "partialWindow"
        _ -> fail "unrecognized tag on NamedWindowExpr object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as NamedWindowExpr:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (Partition r a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "PartitionBest" -> PartitionBest <$> o .: "info"
        String "PartitionNodes" -> PartitionNodes <$> o .: "info"
        String "PartitionBy" -> PartitionBy <$> o .: "info" <*> o .: "expr"

        _ -> fail "unrecognized tag on partition object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Partition:"
        , show v
        ]

instance ConstrainSNames FromJSON r a => FromJSON (Order r a) where
    parseJSON (Object o) = do
      String "Order" <- o .: "tag"
      Order
          <$> o .: "info"
          <*> o .: "position_or_expr"
          <*> o .: "direction"
          <*> o .: "nullPos"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Order:"
        , show v
        ]


instance FromJSON a => FromJSON (NullPosition a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "NullsFirst" -> NullsFirst <$> o .: "info"
        String "NullsLast" -> NullsLast <$> o .: "info"
        String "NullsAuto" -> NullsAuto <$> o .: "info"

        _ -> fail "unrecognized tag on null position object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as NullPosition:"
        , show v
        ]


instance FromJSON a => FromJSON (FrameType a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "RowFrame" -> RowFrame <$> o .: "info"
        String "RangeFrame" -> RangeFrame <$> o .: "info"

        _ -> fail "unrecognized tag on frame type object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as FrameType:"
        , show v
        ]


instance FromJSON a => FromJSON (FrameBound a) where
    parseJSON (Object o) = o .: "tag" >>= \case
         String "Unbounded" -> Unbounded <$> o .: "info"
         String "CurrentRow" -> CurrentRow <$> o .: "info"
         String "Preceding" -> Preceding <$> o .: "info" <*> o .: "bound"
         String "Following" -> Following <$> o .: "info" <*> o .: "bound"

         _ -> fail "unrecognized tag on frame bound object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as FrameBound:"
        , show v
        ]


instance FromJSON a => FromJSON (Frame a) where
    parseJSON (Object o) = do
        String "Frame" <- o .: "tag"
        Frame <$> o .: "info"
              <*> o .: "ftype"
              <*> o .: "start"
              <*> o .: "end"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Frame:"
        , show v
        ]


instance FromJSON a => FromJSON (Offset a) where
    parseJSON (Object o) = do
        String "Offset" <- o .: "tag"
        Offset <$> o .: "info" <*> o .: "offset"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Offset:"
        , show v
        ]


instance FromJSON a => FromJSON (Limit a) where
    parseJSON (Object o) = do
        String "Limit" <- o .: "tag"
        Limit <$> o .: "info" <*> o .: "limit"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Limit:"
        , show v
        ]


instance FromJSON (Operator a) where
    parseJSON (Object o) = do
        String "Operator" <- o .: "tag"
        Operator <$> o .: "operator"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Operator:"
        , show v
        ]


instance FromJSON a => FromJSON (JoinType a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "JoinInner" -> JoinInner <$> o .: "info"
        String "JoinLeft" -> JoinLeft <$> o .: "info"
        String "JoinRight" -> JoinRight <$> o .: "info"
        String "JoinFull" -> JoinFull <$> o .: "info"
        String "JoinSemi" -> JoinSemi <$> o .: "info"
        _ -> fail "unrecognized tag on JoinType object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as JoinType:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (JoinCondition r a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "JoinNatural" -> JoinNatural <$> o .: "info" <*> o .: "columns"
        String "JoinOn" -> JoinOn <$> o .: "expr"
        String "JoinUsing" -> JoinUsing <$> o .: "info" <*> o .: "columns"
        _ -> fail "unrecognized tag on JoinCondition object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as JoinCondition:"
        , show v
        ]


instance FromJSON a => FromJSON (TablishAliases a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "TablishAliasesNone" -> return TablishAliasesNone
        String "TablishAliasesT" -> TablishAliasesT <$> o .: "table"
        String "TablishAliasesTC" -> TablishAliasesTC <$> o .: "table" <*> o .: "columns"
        _ -> fail "unrecognized tag on TablishAliases object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as TablishAliases:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (Tablish r a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "TablishTable" ->
            TablishTable
                <$> o .: "info"
                <*> o .: "aliases"
                <*> o .: "table"

        String "TablishSubQuery" ->
            TablishSubQuery
                <$> o .: "info"
                <*> o .: "alias"
                <*> o .: "query"

        String "TablishJoin" ->
            TablishJoin
                <$> o .: "info"
                <*> o .: "join"
                <*> o .: "condition"
                <*> o .: "outer"
                <*> o .: "inner"

        String "TablishLateralView" ->
            TablishLateralView
                <$> o .: "info"
                <*> o .: "view"
                <*> o .: "lhs"

        _ -> fail "unrecognized tag on tablish object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Tablish:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (LateralView r a) where
    parseJSON (Object o) = do
        String "LateralView" <- o .: "tag"
        lateralViewInfo <- o .: "info"
        lateralViewOuter <- o .: "outer"
        lateralViewExprs <- o .: "exprs"
        lateralViewWithOrdinality <- o .: "with_ordinality"
        lateralViewAliases <- o .: "aliases"
        pure LateralView{..}

    parseJSON v = fail $ unwords
        [ "don't know how to parse as LateralView:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (Select r a) where
    parseJSON (Object o) = do
        String "Select" <- o .: "tag"
        selectInfo <- o .: "info"
        selectCols <- o .: "cols"
        selectFrom <- o .: "from"
        selectWhere <- o .: "where"
        selectTimeseries <- o .:? "timeseries"
        selectGroup <- o .: "group"
        selectHaving <- o .: "having"
        selectNamedWindow <- o .: "window"
        selectDistinct <- o .: "distinct"
        return Select{..}

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Select:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (SelectColumns r a) where
    parseJSON (Object o) = do
        String "SelectColumns" <- o .: "tag"
        SelectColumns <$> o .: "info" <*> o .: "columns"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as SelectColumns:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (SelectFrom r a) where
    parseJSON (Object o) = do
        String "SelectFrom" <- o .: "tag"
        SelectFrom <$> o .: "info" <*> o .: "tables"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as SelectFrom:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (SelectWhere r a) where
    parseJSON (Object o) = do
        String "SelectWhere" <- o .: "tag"
        SelectWhere <$> o .: "info" <*> o .: "conditions"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as SelectWhere:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (SelectTimeseries r a) where
    parseJSON (Object o) = do
        String "SelectTimeseries" <- o .: "tag"
        selectTimeseriesInfo <- o .: "info"
        selectTimeseriesSliceName <- o .: "slice_name"
        selectTimeseriesInterval <- o .: "interval"
        selectTimeseriesPartition <- o .: "partition"
        selectTimeseriesOrder <- o .: "order"
        pure SelectTimeseries{..}

    parseJSON v = fail $ unwords
        [ "don't know how to parse as SelectTimeseries:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (PositionOrExpr r a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "PositionOrExprPosition" -> PositionOrExprPosition <$> o .: "info" <*> o .: "position" <*> o .: "expr"
        String "PositionOrExprExpr" -> PositionOrExprExpr <$> o .: "expr"
        _ -> fail "unrecognized tag on PositionOrExpr object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as PositionOrExpr:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (GroupingElement r a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "GroupingElementExpr" -> GroupingElementExpr <$> o .: "info" <*> o .: "position_or_expr"
        String "GroupingElementSet" -> GroupingElementSet <$> o .: "info" <*> o .: "exprs"
        _ -> fail "unrecognized tag on GroupingElement object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as GroupingElement:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (SelectGroup r a) where
    parseJSON (Object o) = do
        String "SelectGroup" <- o .: "tag"
        selectGroupInfo <- o .: "info"
        selectGroupGroupingElements <- o .: "grouping_elements"
        pure $ SelectGroup{..}

    parseJSON v = fail $ unwords
        [ "don't know how to parse as SelectGroup:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (SelectHaving r a) where
    parseJSON (Object o) = do
        String "SelectHaving" <- o .: "tag"
        SelectHaving <$> o .: "info" <*> o .: "conditions"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as SelectHaving:"
        , show v
        ]


instance ConstrainSNames FromJSON r a => FromJSON (SelectNamedWindow r a) where
    parseJSON (Object o) = do
        String "SelectNamedWindow" <- o .: "tag"
        SelectNamedWindow
            <$> o .: "info"
            <*> o .: "windows"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as SelectNamedWindow:"
        , show v
        ]


-- Arbitrary helpers

arbitraryDate :: Gen Text
arbitraryDate = do
    y <- choose (0,3000) :: Gen Int
    m <- choose (1,12) :: Gen Int
    d <- choose (1,28) :: Gen Int
    pure $ TL.intercalate "-" $ map (TL.pack . show) [y,m,d]

arbitraryTime :: Gen Text
arbitraryTime = do
    h <- choose (0,23) :: Gen Int
    m <- choose (0,59) :: Gen Int
    s <- choose (0,59) :: Gen Int
    pure $ TL.intercalate ":" $ map (TL.pack . show) [h,m,s]

arbitraryTimestamp :: Gen Text
arbitraryTimestamp = do
    d <- arbitraryDate
    t <- arbitraryTime
    pure $ TL.unwords [d, t]

arbitraryInterval :: Gen Text
arbitraryInterval = do
    n <- (TL.pack . show) <$> (arbitrary :: Gen Int)
    period <- elements ["days", "weeks", "hours"]
    pure $ TL.unwords [n, period]

arbitraryText :: Gen Text
arbitraryText = TL.pack <$> oneof
    [ arbitrary
    , listOf $ arbitrary `suchThat` Char.isPrint
    ]

arbitraryByteString :: Gen ByteString
arbitraryByteString = oneof
    [ BL.pack <$> arbitrary
    , TL.encodeUtf8 <$> arbitraryText
    ]

shrinkByteString :: ByteString -> [ByteString]
shrinkByteString str
    | BL.null str = []
    | BL.length str == 1 = [""]
    | otherwise =
        [ ""
        , BL.take halfLen str
        , BL.drop halfLen str
        ] ++ if any (not . Char.isPrint . Char.chr . fromIntegral) $ BL.unpack str
              then [BL.filter (Char.isPrint . Char.chr . fromIntegral) str]
              else []
  where
    halfLen = BL.length str `div` 2


-- Arbitrary queries

instance Arbitrary a => Arbitrary (Constant a) where
    arbitrary = oneof
        [ StringConstant <$> arbitrary
                         <*> arbitraryByteString
        , NumericConstant <$> arbitrary <*> oneof
            [ fmap (TL.pack . show) (arbitrary :: Gen Int)
            , fmap (TL.pack . show) (arbitrary :: Gen Float)
            ]
        , NullConstant <$> arbitrary
        , BooleanConstant <$> arbitrary <*> arbitrary
        , do
            info <- arbitrary
            (text, dataType) <- oneof
                [ do
                    value <- arbitraryText
                    info' <- arbitrary
                    let name = "VARCHAR"
                    pure (value, PrimitiveDataType info' name [])
                , do
                    value <- arbitraryText
                    info' <- arbitrary
                    len <- arbitrary `suchThat` (>0) :: Gen Int
                    let name = "VARCHAR"
                    lenConst <- NumericConstant <$> arbitrary <*> pure (TL.pack $ show len)
                    pure (value, PrimitiveDataType info' name [DataTypeParamConstant lenConst])
                , do
                    value <- arbitraryTimestamp
                    info' <- arbitrary
                    name <- elements ["TIMESTAMP", "TIMESTAMP WITH TIME ZONE"]
                    pure (value, PrimitiveDataType info' name [])
                , do
                    value <- fmap (TL.pack . show) $ (arbitrary :: Gen Int)
                    info' <- arbitrary
                    let name = "INT"
                    pure (value, PrimitiveDataType info' name [])
                , do
                    value <- arbitraryInterval
                    info' <- arbitrary
                    let name = "INTERVAL"
                    pure (value, PrimitiveDataType info' name [])
                ]
            pure $ TypedConstant info text dataType
        , ParameterConstant <$> arbitrary
        ]
    shrink (StringConstant info s) = StringConstant info <$> shrinkByteString s

    shrink (NumericConstant info n) =
        case reads $ TL.unpack n of
            [] -> []
            (prefix :: Float, _):_ -> NumericConstant info . TL.pack . show <$> shrink prefix

    shrink (NullConstant _) = []

    shrink (BooleanConstant _ True) = []
    shrink (BooleanConstant info False) = [BooleanConstant info True]

    shrink (TypedConstant _ _ (PrimitiveDataType _ "VARCHAR" [])) = []
    shrink (TypedConstant info value (PrimitiveDataType info' "TIMESTAMP WITH TIME ZONE" args)) =
        [TypedConstant info value (PrimitiveDataType info' "TIMESTAMP" args)]
    shrink (TypedConstant info value (PrimitiveDataType info' _ _)) =
        [TypedConstant info value (PrimitiveDataType info' "VARCHAR" [])]
    shrink (TypedConstant _ _ _) =
        fail "TODO: shrink for complex data types"

    shrink (ParameterConstant _) = []

scaleDown :: Int -> Gen a -> Gen a
scaleDown n = scale (`div` n)

instance (Arbitrary (PositionExpr r a), Arbitrary a) => Arbitrary (Partition r a) where
    arbitrary = oneof
        [ PartitionBy <$> arbitrary <*> (scaleDown 5 arbitrary)
        , PartitionBest <$> arbitrary
        , PartitionNodes <$> arbitrary
        ]
    shrink (PartitionBest _) = []
    shrink (PartitionNodes info) = [PartitionBest info]
    shrink (PartitionBy info _) = [PartitionBest info]

instance (Arbitrary (PositionExpr r a), Arbitrary a) => Arbitrary (PositionOrExpr r a) where
    arbitrary = oneof
        [ PositionOrExprPosition <$> arbitrary <*> arbitrary <*> arbitrary
        , PositionOrExprExpr <$> arbitrary
        ]
    shrink (PositionOrExprPosition info _ expr) = PositionOrExprPosition info 1 <$> shrink expr
    shrink (PositionOrExprExpr expr) = PositionOrExprExpr <$> shrink expr

instance (Arbitrary (PositionExpr r a), Arbitrary a) => Arbitrary (Order r a) where
    arbitrary = Order <$> arbitrary <*> (scaleDown 5 arbitrary) <*> arbitrary <*> arbitrary

instance Arbitrary a => Arbitrary (OrderDirection a) where
    arbitrary = oneof
        [ OrderAsc <$> arbitrary
        , OrderDesc <$> arbitrary
        ]
    shrink (OrderAsc _) = []
    shrink (OrderDesc info) = [OrderAsc info]

instance Arbitrary a => Arbitrary (NullPosition a) where
    arbitrary = oneof
        [ NullsFirst <$> arbitrary
        , NullsLast <$> arbitrary
        , NullsAuto <$> arbitrary
        ]
    shrink (NullsFirst _) = []
    shrink (NullsLast info) = [NullsFirst info]
    shrink (NullsAuto info) = [NullsFirst info]

instance Arbitrary a => Arbitrary (FrameType a) where
    arbitrary = oneof
        [ RowFrame <$> arbitrary
        , RangeFrame <$> arbitrary
        ]
    shrink (RowFrame _) = []
    shrink (RangeFrame info) = [RowFrame info]

instance Arbitrary a => Arbitrary (FrameBound a) where
    arbitrary = oneof
        [ Unbounded <$> arbitrary
        , CurrentRow <$> arbitrary
        , Preceding <$> arbitrary <*> arbitrary
        , Following <$> arbitrary <*> arbitrary
        ]
    shrink (Unbounded _) = []
    shrink (CurrentRow info) = [Unbounded info]
    shrink (Preceding info _) = [Unbounded info]
    shrink (Following info _) = [Unbounded info]

instance Arbitrary a => Arbitrary (Frame a) where
    arbitrary = do
        frameInfo <- arbitrary
        frameType <- arbitrary
        frameStart <- arbitrary
        frameEnd <- arbitrary
        pure $ Frame{..}
    shrink (Frame i t s e) = [Frame i t' s' e' | (t', s', e') <- shrink (t, s, e)]

instance (Arbitrary (PositionExpr r a), Arbitrary a) => Arbitrary (OverSubExpr r a) where
    arbitrary = do
      info <- arbitrary
      oneof
          [ OverWindowExpr info <$> arbitrary
          , OverWindowName info <$> arbitrary
          , OverPartialWindowExpr info <$> arbitrary
          ]
    shrink (OverWindowExpr info e) = OverWindowExpr info <$> shrink e
    shrink (OverWindowName _ _) = [] -- Ideally resolve name, once implemented
    shrink (OverPartialWindowExpr info e) =
      OverPartialWindowExpr info <$> shrink e

instance (Arbitrary (PositionExpr r a), Arbitrary a) => Arbitrary (WindowExpr r a) where
    arbitrary = do
        windowExprInfo <- arbitrary
        windowExprPartition <- arbitrary
        windowExprOrder <- scaleDown 5 arbitrary
        windowExprFrame <- arbitrary
        pure $ WindowExpr{..}
    shrink (WindowExpr i p o f) = [WindowExpr i p' o' f' | (p', o', f') <- shrink (p, o, f)]

instance Arbitrary a => Arbitrary (WindowName a) where
    arbitrary =
      do
        info <- arbitrary
        name <- TL.pack <$> arbitrary
        pure $ WindowName info name

instance (Arbitrary (PositionExpr r a), Arbitrary a) => Arbitrary (PartialWindowExpr r a) where
    arbitrary =
      do
        partWindowExprInfo <- arbitrary
        partWindowExprInherit <- arbitrary
        partWindowExprPartition <- arbitrary
        partWindowExprOrder <- scaleDown 5 arbitrary
        partWindowExprFrame <- arbitrary
        pure $ PartialWindowExpr{..}

    shrink (PartialWindowExpr i n p o f) =
      [PartialWindowExpr i n p' o' f' | (p', o', f') <- shrink (p, o, f)]

instance Arbitrary Distinct where
    arbitrary = Distinct <$> arbitrary
    shrink (Distinct bool) = Distinct <$> shrink bool

instance (Arbitrary (PositionExpr r a), Arbitrary a) => Arbitrary (Filter r a) where
    arbitrary = Filter <$> arbitrary <*> arbitrary
    shrink (Filter info expr) = Filter info <$> shrink expr

instance (Arbitrary (PositionExpr r a), Arbitrary a) => Arbitrary (Expr r a) where
    arbitrary = sized $ \size -> do
        info <- arbitrary
        frequency
            [ (5, ConstantExpr info <$> arbitrary)
            , (1, FunctionExpr info <$> arbitrary <*> arbitrary <*> (scaleDown 5 arbitrary) <*> (scaleDown 5 arbitrary) <*> (if size > 5 then arbitrary else pure Nothing) <*> (if size > 5 then arbitrary else pure Nothing))
            ]
    shrink (ConstantExpr info c) = ConstantExpr info <$> shrink c
    shrink (FunctionExpr i n d e p f o) =
        [FunctionExpr i n' d' e' p' f' o' | (n', d', e', p', (f', o')) <- shrink (n, d, e, p, (f, o))]
    shrink _ = []  -- NB shrinking is only partially implemented; stubbing this
                   -- out to avoid compiler warnings about non-exhaustive
                   -- pattern matching
