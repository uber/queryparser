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

{-# LANGUAGE ScopedTypeVariables #-}

module Database.Sql.Util.Json.Test where

import Data.Aeson
import Data.Aeson.Types (parse)
import Data.Typeable
import Database.Sql.Type
import Test.QuickCheck hiding (Success)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.Framework.Providers.API (Test)


prop_jsonRoundTrip :: (Eq a, ToJSON a, FromJSON a) => a -> Bool
prop_jsonRoundTrip a = parse parseJSON (toJSON a) == Success a

genTest :: forall a.
    (Eq a, Show a, ToJSON a, FromJSON a, Arbitrary a, Typeable a) =>
    Proxy a -> Test
genTest p@(Proxy :: Proxy a) =
    let testName = ":json round trips for " ++ (show $ typeRep p)
     in testProperty testName (prop_jsonRoundTrip :: a -> Bool)

properties :: [Test]
properties =
    [ -- from Names
      genTest (Proxy :: Proxy (FQSchemaName ()))
    , genTest (Proxy :: Proxy (FunctionName ()))
    , genTest (Proxy :: Proxy (ParamName ()))
      -- from Query
    , genTest (Proxy :: Proxy (Constant ()))
    , genTest (Proxy :: Proxy (Partition RawNames ()))
    , genTest (Proxy :: Proxy (OrderDirection ()))
    , genTest (Proxy :: Proxy (NullPosition ()))
    , genTest (Proxy :: Proxy (FrameType ()))
    , genTest (Proxy :: Proxy (FrameBound ()))
    , genTest (Proxy :: Proxy (Frame ()))
    , genTest (Proxy :: Proxy (OverSubExpr RawNames ()))
    , genTest (Proxy :: Proxy (Expr RawNames ()))
      -- from Type
    , genTest (Proxy :: Proxy (InsertBehavior ()))
    ]
