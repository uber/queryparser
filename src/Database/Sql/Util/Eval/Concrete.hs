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
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.Sql.Util.Eval.Concrete where

import Database.Sql.Util.Eval

import Database.Sql.Type.Names
import Database.Sql.Type.Query

import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text.Lazy as TL
import           Data.List (nub, sort)
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Map (Map)
import qualified Data.Map as M
import qualified Data.Set as S
import           Control.Monad.Identity
import           Control.Monad.Except
import           Data.Proxy


data Concrete

deriving instance Eq (RecordSet Concrete)
deriving instance Show (RecordSet Concrete)

data SqlValue
    = SqlInt Integer
    | SqlStr ByteString
    | SqlBool Bool
    | SqlStruct (Map (StructFieldName ()) SqlValue)
    | SqlNull
      deriving (Eq, Ord, Show)


-- | truthy tells us whether our Haskell code should consider a SqlValue
-- "true", mostly for the purpose of filtering.  It should not be used to
-- construct a SqlValue, as it does not handle NULL correctly for that
-- purpose.

truthy :: SqlValue -> Bool
truthy (SqlBool bool) = bool
truthy (SqlInt int) = int /= 0
truthy (SqlStr str) = not $ BL.null str
truthy (SqlStruct _) = True -- pigeon?
truthy SqlNull = False


instance Evaluation Concrete where
    type EvalValue Concrete = SqlValue
    type EvalRow Concrete = []
    type EvalMonad Concrete = Identity
    addItems _ = (pure .) . (++)
    removeItems _ exclude unfiltered = pure $ filter (not . (`S.member` S.fromList exclude)) unfiltered
    unionItems _ xs ys = pure $ S.toList $ S.union (S.fromList xs) (S.fromList ys)
    intersectItems _ xs ys = pure $ S.toList $ S.intersection (S.fromList xs) (S.fromList ys)
    distinctItems _ = nub
    offsetItems p offset RecordSet{..} = makeRecordSet p recordSetLabels $ drop offset recordSetItems
    limitItems p limit RecordSet{..} = makeRecordSet p recordSetLabels $ take limit recordSetItems

    filterBy expr (RecordSet cs rs) = do
        rs' <- filterM ((truthy <$>) . exprToTable (eval Proxy expr) . makeRowMap cs) rs
        pure $ makeRecordSet Proxy cs rs'

    handleGroups cs gs = map (makeRecordSet Proxy cs) $ M.elems $ M.fromListWith (++) gs

    inList x xs = pure $ SqlBool $ elem x xs
    inSubquery x xss = pure $ SqlBool $ elem x $ concat xss
    existsSubquery = pure . SqlBool . not . null

    atTimeZone _ _ = throwError "AT TIME ZONE not yet handled in concrete evaluation"
    handleConstant _ (StringConstant _ str) = pure $ SqlStr str
    handleConstant _ (NumericConstant _ num) = pure $ SqlInt $ read $ TL.unpack num
    handleConstant _ (NullConstant _) = pure SqlNull
    handleConstant _ (BooleanConstant _ bool) = pure $ SqlBool bool
    handleConstant _ (TypedConstant _ text dataType) = error "typed constant expression not yet supported" text dataType
    handleConstant _ (ParameterConstant _) = throwError "no way to evaluate unsubstituted parameter"

    handleCases p ((when_, then_):cases) else_ = do
        truthy <$> eval p when_ >>= \case
            True -> eval p then_
            False -> handleCases p cases else_

    handleCases _ [] Nothing = throwError "fell through case with no else"
    handleCases p [] (Just expr) = eval p expr

    handleFunction _ _ _ _ _ _ _ = throwError "function exprs not yet supported"

    handleLambdaParam _ _ = error "unreachable, lambda should be handled inside a function"

    handleLambda _ _ _ = error "unreachable, lambda should be handled inside a function"

    handleLike _ _ _ _ _ = throwError "concrete evaluation for LIKE expressions not yet supported"

    handleOrder p orders (RecordSet cs rs) = do
        pairs <- forM rs $ \ r -> do
            k <- (`exprToTable` makeRowMap cs r) $ forM orders $ \case
                Order _ (PositionOrExprPosition _ pos _) (OrderAsc _) _ -> Ascending <$> return (r !! (pos - 1))
                Order _ (PositionOrExprPosition _ pos _) (OrderDesc _) _ -> Descending <$> return (r !! (pos - 1))
                Order _ (PositionOrExprExpr expr) (OrderAsc _) _ -> Ascending <$> eval p expr
                Order _ (PositionOrExprExpr expr) (OrderDesc _) _ -> Descending <$> eval p expr
            pure (k, r)
        pure $ makeRecordSet p cs $ map snd $ sort pairs

    handleSubquery [[x]] = pure x
    handleSubquery [] = throwError "no rows returned from subquery"
    handleSubquery [_] = throwError "wrong number of columns from subquery"
    handleSubquery _ = throwError "multiple rows returned from subquery"

    handleJoin p (JoinInner _) cond x y = eval p cond x y
    handleJoin p (JoinLeft _) cond x y = do
        case x of
            RecordSet _ [] -> eval p cond x y
            RecordSet lcols lrows -> do
                ~(set:sets) <- forM lrows $ \ lrow -> do
                    let x' = makeRecordSet p lcols [lrow]
                    eval p cond x' y >>= \case
                        RecordSet cols [] -> pure $ makeRecordSet p cols [extendWithNulls cols lrow]
                        set -> pure set
                appendRecordSets p (set:|sets)

    handleJoin p (JoinRight _) cond x y = do
        case y of
            RecordSet _ [] -> eval p cond x y
            RecordSet rcols rrows -> do
                ~(set:sets) <- forM rrows $ \ rrow -> do
                    let y' = makeRecordSet p rcols [rrow]
                    eval p cond x y' >>= \case
                        RecordSet cols [] -> pure $ makeRecordSet p cols [reverse $ extendWithNulls cols $ reverse rrow]
                        set -> pure set
                appendRecordSets p (set:|sets)

    handleJoin p (JoinFull info) cond x y = do
        RecordSet cs rs <- handleJoin p (JoinLeft info) cond x y
        RecordSet _ rs' <- do -- get those from the RHS with no matches on LHS
            case y of
                RecordSet _ [] -> eval p cond x y
                RecordSet rcols rrows -> do
                    ~(set:sets) <- forM rrows $ \ rrow -> do
                        let y' = makeRecordSet p rcols [rrow]
                        eval p cond x y' >>= \case
                            RecordSet cols [] -> pure $ makeRecordSet p cols [reverse $ extendWithNulls cols $ reverse rrow]
                            RecordSet cols _ -> pure $ makeRecordSet p cols []
                    appendRecordSets p (set:|sets)
        pure $ makeRecordSet p cs $ rs ++ rs'

    handleJoin _ (JoinSemi _) cond x y = error "semi joins not yet supported" cond x y

    handleStructField expr field = eval (Proxy :: Proxy Concrete) expr >>= \case
        SqlStruct m
            | Just val <- M.lookup (void field) m
            -> pure val
            | otherwise
            -> throwError "missing field in SQL struct"
        _ -> throwError "field access of non-struct value"

    handleTypeCast _ _ _ = throwError "concrete evaluation for type cast expressions not yet supported"

    binop _ op = M.lookup op $ M.fromList
        [ ("+", opAdd)
        , ("=", opEq)
        , ("AND", opAnd)
        ]
      where
        opAdd (SqlInt x) (SqlInt y) = pure $ SqlInt (x + y)
        opAdd (SqlInt _) SqlNull = pure SqlNull
        opAdd SqlNull (SqlInt _) = pure SqlNull
        opAdd _ _ = throwError "unsupported arguments to + operator"

        opEq SqlNull _ = pure SqlNull
        opEq _ SqlNull = pure SqlNull
        opEq x y = pure $ SqlBool $ x == y  -- TODO coerce

        opAnd SqlNull _ = pure SqlNull
        opAnd _ SqlNull = pure SqlNull
        opAnd x y = pure $ SqlBool $ truthy x && truthy y

    unop _ op = M.lookup op $ M.fromList
        [ ("-", neg)
        ]
      where
        neg (SqlInt int) = pure $ SqlInt (-int)
        neg SqlNull = pure SqlNull
        neg _ = throwError "unsupported argument to - operator"


extendWithNulls :: [a] -> [SqlValue] -> [SqlValue]
extendWithNulls (_:xs) (y:ys) = y:extendWithNulls xs ys
extendWithNulls xs [] = map (const SqlNull) xs
extendWithNulls [] _ = error "more values than columns - this should never happen"
