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

module Database.Sql.Vertica.Parser.Shared where

-- This module exists to prevent cyclic dependencies: parsers that are needed
-- in both Vertica.Parser and Vertica.Parser.IngestionOptions should go in
-- here.

import Database.Sql.Type
import Database.Sql.Info
import Database.Sql.Position

import Database.Sql.Vertica.Type
import Database.Sql.Vertica.Parser.Internal

import qualified Database.Sql.Vertica.Parser.Token as Tok

import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL

import qualified Text.Parsec as P
import           Text.Parsec ( choice
                             , option, optional, optionMaybe
                             , sepBy1, try )

import Control.Arrow (first)
import Data.Semigroup ((<>))


dataTypeP :: Parser (DataType Range)
dataTypeP = choice
    [ try $ do
        r <- Tok.timestampP
        args <- option [] $ do
            arg <- P.between Tok.openP Tok.closeP constantP
            return $ [DataTypeParamConstant arg]
        choice [ do
                     _ <- Tok.withP
                     r' <- Tok.timezoneP
                     return $ PrimitiveDataType (r <> r') "TIMESTAMP WITH TIMEZONE" args
               , do
                     _ <- Tok.withoutP
                     r' <- Tok.timezoneP
                     return $ PrimitiveDataType (r <> r') "TIMESTAMP WITHOUT TIMEZONE" args
               , return $ PrimitiveDataType r "TIMESTAMP" args
               ]
    , try $ do
        r1 <- Tok.longP
        (name, r2) <- Tok.varBinaryP P.<|> Tok.varCharP
        args <- map DataTypeParamConstant <$> argsP
        let r = r1 <> r2
            name' = TL.append "LONG " $ TL.toUpper name
        return $ PrimitiveDataType r name' args
    , do
        r <- Tok.doubleP
        optionMaybe Tok.precisionP >>= \case
            Just r' -> return $ PrimitiveDataType (r <> r') "DOUBLE PRECISION" []
            Nothing -> return $ PrimitiveDataType (r) "DOUBLE" []
    , do
        (name, r) <- Tok.typeNameP
        args <- map DataTypeParamConstant <$> option [] argsP
        return $ PrimitiveDataType r name args
    ]
  where
    argsP = P.between Tok.openP Tok.closeP $ constantP `sepBy1` Tok.commaP


periodP :: Parser (DataType Range)
periodP = do
    (period, r) <- Tok.periodP
    pure $ PrimitiveDataType r period []


constantP :: Parser (Constant Range)
constantP = choice
    [ uncurry (flip StringConstant)
        <$> (try (optional Tok.timestampP) >> Tok.stringP)

    , uncurry (flip NumericConstant) <$> Tok.numberP
    , NullConstant <$> Tok.nullP
    , uncurry (flip BooleanConstant) <$> choice
        [ Tok.trueP >>= \ r -> return (True, r)
        , Tok.falseP >>= \ r -> return (False, r)
        ]

    , try $ do
        dataType <- dataTypeP
        (string, r') <- first TL.decodeUtf8 <$> Tok.stringP
        case dataType of
            PrimitiveDataType _ "interval" [] -> do
                choice
                    [ do
                        period <- periodP
                        pure $ TypedConstant (getInfo dataType <> getInfo period) string period
                    , pure $ TypedConstant (getInfo dataType <> r') string dataType
                    ]
            _ -> pure $ TypedConstant (getInfo dataType <> r') string dataType
    ]


unqualifiedTableNameP :: Parser (UQTableName Range)
unqualifiedTableNameP = do
    (t, r) <- Tok.tableNameP
    return $ QTableName r None t


qualifiedTableNameP :: Parser (Text, Text, Range, Range)
qualifiedTableNameP = do
    (s, r) <- Tok.schemaNameP
    _ <- Tok.dotP
    (t, r') <- Tok.tableNameP

    return (s, t, r, r')


tableNameP :: Parser (TableRef RawNames Range)
tableNameP = choice
    [ try $ do
        (s, t, r, r') <- qualifiedTableNameP
        return $ QTableName r' (Just $ mkNormalSchema s r) t

    , do
        (t, r) <- Tok.tableNameP
        return $ QTableName r Nothing t
    ]


projectionNameP :: Parser (ProjectionName Range)
projectionNameP = choice
    [ try $ do
        (s, r) <- Tok.schemaNameP
        _ <- Tok.dotP
        (p, r') <- Tok.projectionNameP

        return $ ProjectionName (r <> r') (Just $ mkNormalSchema s r) p

    , do
        (p, r) <- Tok.projectionNameP
        return $ ProjectionName r Nothing p
    ]


columnNameP :: Parser (ColumnRef RawNames Range)
columnNameP = choice
    [ try $ do
        t <- tableNameP
        _ <- Tok.dotP
        (c, r) <- choice
            [ Tok.columnNameP
            , first TL.decodeUtf8 <$> Tok.stringP
            ]

        return $ QColumnName r (Just t) c

    , try $ do
        (t, r) <- Tok.tableNameP
        _ <- Tok.dotP
        (c, r') <- Tok.columnNameP

        return $ QColumnName r' (Just $ QTableName r Nothing t) c

    , do
        (c, r) <- Tok.columnNameP

        return $ QColumnName r Nothing c
    ]
