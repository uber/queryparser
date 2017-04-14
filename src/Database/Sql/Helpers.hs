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

module Database.Sql.Helpers where

import Database.Sql.Position

import           Text.Parsec (ParsecT, Stream, option, optionMaybe)
import qualified Text.Parsec as P

import Data.Semigroup (Semigroup, sconcat, (<>))
import Data.List.NonEmpty (NonEmpty ((:|)))


consumeOrderedOptions :: (Stream s m t, Semigroup a) => a -> [ParsecT s u m a] -> ParsecT s u m a
consumeOrderedOptions defaultVal optionPs = do
    let v = defaultVal
    vs <- sequence $ map (option v) optionPs
    pure $ sconcat $ v :| vs


-- the options may appear in any order, but may only appear once.
consumeUnorderedOptions :: (Stream s m t, Semigroup a) => a -> [ParsecT s u m a] -> ParsecT s u m a
consumeUnorderedOptions defaultVal optionPs = go defaultVal optionPs []
  where
    go v [] _ = return v

    go v untriedPs triedPs = do
        let candidateP:remainingPs = untriedPs
        optionMaybe candidateP >>= \case
            Just v' -> go (v <> v') (remainingPs ++ triedPs) []
            Nothing -> go v remainingPs (candidateP:triedPs)


eof :: (Stream s m t, Show t, Integral u) => ParsecT s u m Range
eof = do
    _ <- P.eof
    p <- position
    return $ Range p p
  where
    position = do
        sourcePos <- P.getPosition
        offset <- P.getState
        let positionLine = fromIntegral $ P.sourceLine sourcePos
            positionColumn = fromIntegral $ P.sourceColumn sourcePos
            positionOffset = fromIntegral $ offset
        return Position{..}
