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

module Database.Sql.Vertica.Parser.IngestionOptions where

import Database.Sql.Info
import Database.Sql.Helpers

import Database.Sql.Vertica.Parser.Internal
import Database.Sql.Position

import qualified Database.Sql.Vertica.Parser.Token as Tok
import Database.Sql.Vertica.Parser.Shared

import           Text.Parsec ( choice
                             , option, optional
                             , sepBy, sepBy1, (<|>))

import Data.Semigroup (Semigroup (..))


ingestionColumnListP :: Parser Range -> Parser Range
ingestionColumnListP exprP = do
    s <- Tok.openP
    _ <- columnSpecP `sepBy1` Tok.commaP
    e <- Tok.closeP
    return $ s <> e
  where
    columnSpecP = do
        (_, s) <- Tok.columnNameP
        e <- consumeOrderedOptions s $
            [ Tok.asP >> exprP
            , delimiterAsP
            , enclosedByP
            , Tok.enforceLengthP
            , escapeFormatP
            , fillerP
            , columnStorageFormatP
            , nullAsP
            , trimByteP
            ]
        return $ s <> e

ingestionColumnOptionP :: Parser Range
ingestionColumnOptionP = do
    s <- Tok.columnP
    _ <- Tok.optionP
    _ <- Tok.openP
    _ <- optionSpecP `sepBy1` Tok.commaP
    e <- Tok.closeP
    return $ s <> e
  where
    optionSpecP = do
        (_, s) <- Tok.columnNameP
        consumeOrderedOptions s $
            [ delimiterAsP
            , enclosedByP
            , Tok.enforceLengthP
            , escapeFormatP
            , columnStorageFormatP
            , nullAsP
            , trimByteP
            ]

fileStorageFormatP :: Parser Range
fileStorageFormatP = choice $
    [ do
         r <- Tok.nativeP
         option r (snd <$> Tok.varCharP)
    , do
         s <- Tok.fixedWidthP
         _ <- Tok.colSizesP
         _ <- Tok.openP
         _ <- Tok.numberP `sepBy1` Tok.commaP
         e <- Tok.closeP
         return $ s <> e
    , Tok.orcP
    , Tok.parquetP
    ]

abortOnErrorP :: Parser Range
abortOnErrorP = Tok.abortP >> Tok.onP >> Tok.errorP

delimiterAsP :: Parser Range
delimiterAsP = Tok.delimiterP >> optional Tok.asP >> snd <$> Tok.stringP

enclosedByP :: Parser Range
enclosedByP = Tok.enclosedP >> optional Tok.byP >> snd <$> Tok.stringP

errorToleranceP :: Parser Range
errorToleranceP = Tok.errorP >> Tok.toleranceP

escapeFormatP :: Parser Range
escapeFormatP = choice $
    [ Tok.escapeP >> Tok.asP >> snd <$> Tok.stringP
    , Tok.noP >> Tok.escapeP
    ]

exceptionsOnNodeP :: Parser Range
exceptionsOnNodeP = do
    s <- Tok.exceptionsP
    e <- snd <$> Tok.stringP
    let onNodeP = Tok.onP >> snd <$> Tok.nodeNameP
    e' <- option e $ last <$> (onNodeP `sepBy1` Tok.commaP)
    return $ s <> e'

fileFilterP :: Parser Range
fileFilterP = do
    s <- Tok.filterP
    _ <- Tok.functionNameP
    _ <- Tok.openP
    let argP = do
            _ <- Tok.paramNameP
            _ <- Tok.equalP
            getInfo <$> constantP
    _ <- argP `sepBy` Tok.commaP
    e <- Tok.closeP
    return $ s <> e

fileParserP :: Parser Range
fileParserP = do
    s <- Tok.parserP
    _ <- Tok.functionNameP
    _ <- Tok.openP
    let argP = do
            _ <- Tok.paramNameP
            _ <- Tok.equalP
            snd <$> Tok.parserNameP
    _ <- argP `sepBy` Tok.commaP
    e <- Tok.closeP
    return $ s <> e

fileSourceP :: Parser Range
fileSourceP = do
    s <- Tok.sourceP
    _ <- Tok.functionNameP
    _ <- Tok.openP
    let argP = do
            _ <- Tok.paramNameP
            _ <- Tok.equalP
            snd <$> Tok.stringP
    _ <- argP `sepBy` Tok.commaP
    e <- Tok.closeP
    return $ s <> e

fillerP :: Parser Range
fillerP = Tok.fillerP >> getInfo <$> dataTypeP

columnStorageFormatP :: Parser Range
columnStorageFormatP = Tok.formatP >> snd <$> Tok.stringP

noCommitP :: Parser Range
noCommitP = Tok.noP >> Tok.commitP

nullAsP :: Parser Range
nullAsP = Tok.nullP >> optional Tok.asP >> snd <$> Tok.stringP

recordTerminatorP :: Parser Range
recordTerminatorP = Tok.recordP >> Tok.terminatorP >> snd <$> Tok.stringP

rejectedDataOnNodeP :: Parser Range
rejectedDataOnNodeP = do
    s <- Tok.rejectedP
    _ <- Tok.dataP
    e <- choice $
        [ do
            e' <- snd <$> Tok.stringP
            let onNodeP = Tok.onP >> snd <$> Tok.nodeNameP
            option e' $ last <$> (onNodeP `sepBy1` Tok.commaP)
        , do
            _ <- Tok.asP
            _ <- Tok.tableP
            getInfo <$> tableNameP
        ]
    return $ s <> e

rejectMaxP :: Parser Range
rejectMaxP = Tok.rejectMaxP >> snd <$> Tok.numberP

skipBytesP :: Parser Range
skipBytesP = Tok.skipP >> Tok.bytesP >> snd <$> Tok.numberP

skipRecordsP :: Parser Range
skipRecordsP = Tok.skipP >> snd <$> Tok.numberP

streamNameP :: Parser Range
streamNameP = Tok.streamP >> Tok.nameP >> snd <$> Tok.stringP

trailingNullColsP :: Parser Range
trailingNullColsP = Tok.trailingP >> Tok.nullColsP

trimByteP :: Parser Range
trimByteP = Tok.trimP >> snd <$> Tok.stringP

compressionP :: Parser Range
compressionP = choice $
    [ Tok.bzipP
    , Tok.gzipP
    , Tok.lzoP
    , Tok.uncompressedP
    ]

loadMethodP :: Parser Range
loadMethodP = Tok.autoP <|> Tok.directP <|> Tok.trickleP
