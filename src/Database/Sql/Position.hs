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

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
module Database.Sql.Position where

import Data.Int (Int64)

import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL

import           Data.Aeson

import Data.Semigroup (Semigroup (..))

import Data.Data (Data)
import GHC.Generics (Generic)

data Position = Position
    { positionLine :: Int64
    , positionColumn :: Int64
    , positionOffset :: Int64
    } deriving (Generic, Data, Show, Eq, Ord)

data Range = Range
    { start :: Position
    , end :: Position
    } deriving (Generic, Data, Show, Eq, Ord)

instance Semigroup Range where
    Range s e <> Range s' e' = Range (min s s') (max e e')

infixr 6 ?<>
(?<>) :: Semigroup a => a -> Maybe a -> a
r ?<> Nothing = r
r ?<> (Just r') = r <> r'

advanceHorizontal :: Int64 -> Position -> Position
advanceHorizontal n p = p
    { positionColumn = positionColumn p + n
    , positionOffset = positionOffset p + n
    }

advanceVertical :: Int64 -> Position -> Position
advanceVertical n p = p
    { positionLine = positionLine p + n
    , positionColumn = if n > 0 then 0 else positionColumn p
    , positionOffset = positionOffset p + n
    }

advance :: Text -> Position -> Position
advance t p =
    let newlines = TL.count "\n" t
     in p
        { positionLine = positionLine p + newlines
        , positionColumn = if newlines == 0
                            then positionColumn p + TL.length t
                            else TL.length $ snd $ TL.breakOnEnd "\n" t

        , positionOffset = positionOffset p + TL.length t
        }

instance ToJSON Position where
    toJSON Position {..} = object
        [ "line" .= positionLine
        , "column" .= positionColumn
        , "offset" .= positionOffset
        ]

instance ToJSON Range where
    toJSON Range {..} = object
        [ "start" .= start
        , "end" .= end
        ]

instance FromJSON Position where
    parseJSON (Object o) = do
        positionLine <- o .: "line" 
        positionColumn <- o .: "column" 
        positionOffset <- o .: "offset" 
        return Position{..}

    parseJSON v = fail $ "don't know how to parse as Position: " ++ show v

instance FromJSON Range where
    parseJSON (Object o) = do
        start <- o .: "start" 
        end <- o .: "end" 
        return Range{..}

    parseJSON v = fail $ "don't know how to parse as Range: " ++ show v
