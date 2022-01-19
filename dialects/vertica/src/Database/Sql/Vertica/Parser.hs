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
{-# LANGUAGE TypeFamilies #-}

module Database.Sql.Vertica.Parser where

import Database.Sql.Type
import Database.Sql.Info
import Database.Sql.Helpers
import Database.Sql.Vertica.Type

import Database.Sql.Vertica.Scanner
import Database.Sql.Vertica.Parser.Internal
import Database.Sql.Position

import qualified Database.Sql.Vertica.Parser.Token as Tok
import Database.Sql.Vertica.Parser.IngestionOptions
import Database.Sql.Vertica.Parser.Shared

import           Data.Char (isDigit)
import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL
import qualified Data.List as L

import Data.Maybe (catMaybes, fromMaybe)
import Data.Monoid (Endo (..))
import Data.Semigroup (Option (..))
import qualified Text.Parsec as P
import           Text.Parsec ( chainl1, choice, many, many1
                             , option, optional, optionMaybe
                             , sepBy, sepBy1, try, (<|>), (<?>))

import Control.Arrow (first)
import Control.Monad (void, (>=>), when)


import Data.Semigroup (Semigroup (..), sconcat)
import Data.List.NonEmpty (NonEmpty ((:|)))
import qualified Data.List.NonEmpty as NE (last, fromList)
import Data.Foldable (fold)

statementParser :: Parser (VerticaStatement RawNames Range)
statementParser = do
    maybeStmt <- optionMaybe $ choice
        [ try $ VerticaStandardSqlStatement <$> statementP
        , do
              _ <- try $ P.lookAhead createProjectionPrefixP
              VerticaCreateProjectionStatement <$> createProjectionP
        , try $ VerticaMultipleRenameStatement <$> multipleRenameP
        , try $ VerticaSetSchemaStatement <$> setSchemaP
        , try $ VerticaUnhandledStatement <$> renameProjectionP
        , do
              _ <- try $ P.lookAhead alterResourcePoolPrefixP
              VerticaUnhandledStatement <$> alterResourcePoolP
        , do
              _ <- try $ P.lookAhead createResourcePoolPrefixP
              VerticaUnhandledStatement <$> createResourcePoolP
        , do
              _ <- try $ P.lookAhead dropResourcePoolPrefixP
              VerticaUnhandledStatement <$> dropResourcePoolP
        , do
              _ <- try $ P.lookAhead createFunctionPrefixP
              VerticaUnhandledStatement <$> createFunctionP
        , VerticaUnhandledStatement <$> alterTableAddConstraintP
        , VerticaUnhandledStatement <$> exportToStdoutP
        , do
              _ <- try $ P.lookAhead setSessionPrefixP
              VerticaUnhandledStatement <$> setSessionP
        , VerticaUnhandledStatement <$> setTimeZoneP
        , VerticaUnhandledStatement <$> connectP
        , VerticaUnhandledStatement <$> disconnectP
        , VerticaUnhandledStatement <$> createAccessPolicyP
        , VerticaUnhandledStatement <$> copyFromP
        , VerticaUnhandledStatement <$> showP
        , VerticaMergeStatement <$> mergeP
        ]
    case maybeStmt of
        Just stmt -> terminator >> return stmt
        Nothing -> VerticaStandardSqlStatement <$> emptyStatementP
  where
    terminator = (Tok.semicolonP <|> eof) -- normal statements may be terminated by `;` or eof
    emptyStatementP = EmptyStmt <$> Tok.semicolonP  -- but we don't allow eof here. `;` is the
    -- only way to write the empty statement, i.e. `` (empty string) is not allowed.


-- | parse consumes a statement, or fails
parse :: Text -> Either P.ParseError (VerticaStatement RawNames Range)
parse = P.runParser statementParser 0 "-"  . tokenize

-- | parseAll consumes all input as a single statement, or fails
parseAll :: Text -> Either P.ParseError (VerticaStatement RawNames Range)
parseAll = P.runParser (statementParser <* P.eof) 0 "-"  . tokenize

-- | parseMany consumes multiple statements, or fails
parseMany :: Text -> Either P.ParseError [VerticaStatement RawNames Range]
parseMany = P.runParser (P.many1 statementParser) 0 "-"  . tokenize

-- | parseManyAll consumes all input multiple statements, or fails
parseManyAll :: Text -> Either P.ParseError [VerticaStatement RawNames Range]
parseManyAll text = P.runParser (P.many1 statementParser <* P.eof) 0 "-"  . tokenize $ text

-- | parseManyEithers consumes all input as multiple (statements or failures)
-- it should never fail
parseManyEithers :: Text -> Either P.ParseError [Either (Unparsed Range) (VerticaStatement RawNames Range)]
parseManyEithers text = P.runParser parser 0 "-"  . tokenize $ text
  where
    parser = do
        statements <- P.many1 $ P.setState 0 >> choice
            [ try $ Right <$> statementParser
            , try $ Left <$> do
                ss  <- many Tok.notSemicolonP
                e <- Tok.semicolonP
                pure $ case ss of
                    [] -> Unparsed e
                    s:_ -> Unparsed (s <> e)
            ]

        locs <- many Tok.notSemicolonP
        P.eof
        pure $ case locs of
            [] -> statements
            s:es -> statements ++ [Left $ Unparsed $ sconcat (s:|es)]

optionBool :: Parser a -> Parser Bool
optionBool p = option False $ p >> pure True

statementP :: Parser (Statement Vertica RawNames Range)
statementP = choice
    [ InsertStmt <$> insertP
    , DeleteStmt <$> deleteP
    , QueryStmt <$> queryP
    , explainP
    , TruncateStmt <$> truncateP
    , AlterTableStmt <$> alterTableP
    , do
          _ <- try $ P.lookAhead createSchemaPrefixP
          CreateSchemaStmt <$> createSchemaP
    , do
          _ <- try $ P.lookAhead createExternalTablePrefixP
          CreateTableStmt <$> createExternalTableP
    , do
          _ <- try $ P.lookAhead createViewPrefixP
          CreateViewStmt <$> createViewP
    , CreateTableStmt <$> createTableP
    , do
          _ <- try $ P.lookAhead dropViewPrefixP
          DropViewStmt <$> dropViewP
    , DropTableStmt <$> dropTableP
    , GrantStmt <$> grantP
    , RevokeStmt <$> revokeP
    , BeginStmt <$> beginP
    , CommitStmt <$> commitP
    , RollbackStmt <$> rollbackP
    ]

oqColumnNameP :: Parser (OQColumnName Range)
oqColumnNameP = (\ (c, r') -> QColumnName r' Nothing c) <$> Tok.columnNameP

insertP :: Parser (Insert RawNames Range)
insertP = do
    r <- Tok.insertP
    insertBehavior <- InsertAppend <$> Tok.intoP

    insertTable <- tableNameP

    insertColumns <- optionMaybe $ try $ do
        _ <- Tok.openP
        c:cs <- oqColumnNameP `sepBy1` Tok.commaP
        _ <- Tok.closeP
        pure (c :| cs)

    insertValues <- choice
        [ do
            s <- Tok.defaultP
            e <- Tok.valuesP
            pure $ InsertDefaultValues (s <> e)

        , do
            s <- Tok.valuesP
            _ <- Tok.openP

            x:xs <- defaultExprP `sepBy1` Tok.commaP

            e <- Tok.closeP

            let row = x :| xs
                rows = row :| []  -- there can only be one

            pure $ InsertExprValues (s <> e) rows
        , InsertSelectValues <$> queryP
        ]

    let insertInfo = r <> getInfo insertValues

    pure Insert{..}

defaultExprP :: Parser (DefaultExpr RawNames Range)
defaultExprP = choice
    [ DefaultValue <$> Tok.defaultP
    , ExprValue <$> exprP
    ]


deleteP :: Parser (Delete RawNames Range)
deleteP = do
    r <- Tok.deleteP

    _ <- Tok.fromP
    table <- tableNameP

    maybeExpr <- optionMaybe $ do
        _ <- Tok.whereP
        exprP

    let r' = case maybeExpr of
          Nothing -> getInfo table
          Just expr -> getInfo expr
        info = r <> r'

    pure $ Delete info table maybeExpr


truncateP :: Parser (Truncate RawNames Range)
truncateP = do
    s <- Tok.truncateP
    _ <- Tok.tableP
    table <- tableNameP

    pure $ Truncate (s <> getInfo table) table


querySelectP :: Parser (Query RawNames Range)
querySelectP = do
        select <- selectP
        return $ QuerySelect (selectInfo select) select

queryP :: Parser (Query RawNames Range)
queryP = manyParensP $ do
    with <- option id withP

    query <- ((querySelectP <|> P.between Tok.openP Tok.closeP queryP) `chainl1` (exceptP <|> unionP))
                `chainl1` intersectP

    order <- option id orderP
    limit <- option id limitP
    offset <- option id offsetP

    return $ with $ limit $ offset $ order $ query
  where
    exceptP = do
        r <- Tok.exceptP
        return $ QueryExcept r Unused

    unionP = do
        r <- Tok.unionP
        distinct <- option (Distinct True) distinctP
        return $ QueryUnion r distinct Unused

    intersectP = do
        r <- Tok.intersectP
        return $ QueryIntersect r Unused

    withP = do
        r <- Tok.withP
        withs <- cteP `sepBy1` Tok.commaP

        return $ \ query ->
            let r' = sconcat $ r :| getInfo query : map cteInfo withs
             in QueryWith r' withs query

    cteP = do
        (name, r) <- Tok.tableNameP
        alias <- makeTableAlias r name

        columns <- option []
            $ P.between Tok.openP Tok.closeP $ columnAliasP `sepBy1` Tok.commaP

        _ <- Tok.asP

        (query, r') <- do
            _ <- Tok.openP
            q <- queryP
            r' <- Tok.closeP
            return (q, r')

        return $ CTE (r <> r') alias columns query

    orderP = do
        (r, orders) <- orderTopLevelP
        return $ \ query -> QueryOrder (getInfo query <> r) orders query

    limitP = do
        r <- Tok.limitP
        choice
            [ Tok.numberP >>= \ (v, r') ->
                let limit = Limit (r <> r') v
                 in return $ \ query -> QueryLimit (getInfo query <> r') limit query

            , Tok.nullP >> return id
            ]

    offsetP = do
        r <- Tok.offsetP
        Tok.numberP >>= \ (v, r') ->
            let offset = Offset (r <> r') v
             in return $ \ query -> QueryOffset (getInfo query <> r') offset query


distinctP :: Parser Distinct
distinctP = choice $
    [ Tok.allP >> return (Distinct False)
    , Tok.distinctP >> return (Distinct True)
    ]


explainP :: Parser (Statement Vertica RawNames Range)
explainP = do
    s <- Tok.explainP
    stmt <- choice
        [ InsertStmt <$> insertP
        , DeleteStmt <$> deleteP
        , QueryStmt <$> queryP
        ]

    pure $ ExplainStmt (s <> getInfo stmt) stmt


columnAliasP :: Parser (ColumnAlias Range)
columnAliasP = do
    (name, r) <- Tok.columnNameP
    makeColumnAlias r name


alterTableP :: Parser (AlterTable RawNames Range)
alterTableP = do
    s <- Tok.alterP
    _ <- Tok.tableP
    from <- tableNameP
    _ <- Tok.renameP
    _ <- Tok.toP
    to <- (\ uqtn -> uqtn { tableNameSchema = Nothing }) <$> unqualifiedTableNameP

    pure $ AlterTableRenameTable (s <> getInfo to) from to


createSchemaPrefixP :: Parser Range
createSchemaPrefixP = do
    s <- Tok.createP
    e <- Tok.schemaP
    return $ s <> e

ifNotExistsP :: Parser (Maybe Range)
ifNotExistsP = optionMaybe $ do
    s <- Tok.ifP
    _ <- Tok.notP
    e <- Tok.existsP
    pure $ s <> e

ifExistsP :: Parser Range
ifExistsP = do
    s <- Tok.ifP
    e <- Tok.existsP
    pure $ s <> e

createSchemaP :: Parser (CreateSchema RawNames Range)
createSchemaP = do
    s <- createSchemaPrefixP
    createSchemaIfNotExists <- ifNotExistsP

    (name, r) <- Tok.schemaNameP
    let createSchemaName = mkNormalSchema name r

    e <- option r (Tok.authorizationP >> snd <$> Tok.userNameP)
    let createSchemaInfo = s <> e

    return $ CreateSchema{..}


createTableColumnsP :: Parser (TableDefinition Vertica RawNames Range)
createTableColumnsP = do
    s <- Tok.openP
    c:cs <- columnOrConstraintP `sepBy1` Tok.commaP
    e <- Tok.closeP
    pure $ TableColumns (s <> e) (c:|cs)
  where
    columnOrConstraintP :: Parser (ColumnOrConstraint Vertica RawNames Range)
    columnOrConstraintP = choice
        [ try $ ColumnOrConstraintColumn <$> columnDefinitionP
        , ColumnOrConstraintConstraint <$> constraintDefinitionP
        ]

    columnDefinitionP = do
        (name, s) <- Tok.columnNameP
        columnDefinitionType <- dataTypeP

        updates <- many $ choice [ notNullUpdateP, nullUpdateP, defaultUpdateP ]

        let columnDefinitionInfo = s <> getInfo columnDefinitionType
            columnDefinitionExtra = Nothing  -- TODO
            -- set when applying updates
            columnDefinitionNull = Nothing
            columnDefinitionDefault = Nothing
            columnDefinitionName = QColumnName s None name

        foldr (>=>) pure updates ColumnDefinition{..}

    notNullUpdateP :: Parser (ColumnDefinition d r Range -> Parser (ColumnDefinition d r Range))
    notNullUpdateP = do
        r <- (<>) <$> Tok.notP <*> Tok.nullP

        pure $ \ d -> case columnDefinitionNull d of
            Nothing -> pure $ d { columnDefinitionNull = Just $ NotNull r }
            Just (Nullable _) -> fail "conflicting NULL/NOT NULL specifications on column"
            Just (NotNull _) -> pure d

    nullUpdateP :: Parser (ColumnDefinition d r Range -> Parser (ColumnDefinition d r Range))
    nullUpdateP = do
        r <- Tok.nullP

        pure $ \ d -> case columnDefinitionNull d of
            Nothing -> pure $ d { columnDefinitionNull = Just $ Nullable r }
            Just (NotNull _) -> fail "conflicting NULL/NOT NULL specifications on column"
            Just (Nullable _) -> pure d

    defaultUpdateP :: Parser (ColumnDefinition d RawNames Range -> Parser (ColumnDefinition d RawNames Range))
    defaultUpdateP = do
        _ <- Tok.defaultP
        expr <- exprP

        pure $ \ d -> case columnDefinitionDefault d of
            Nothing -> pure $ d { columnDefinitionDefault = Just expr }
            Just _ -> fail "multiple defaults for column"

    constraintDefinitionP :: Parser (ConstraintDefinition Range)
    constraintDefinitionP = ConstraintDefinition <$> tableConstraintP


createExternalTablePrefixP :: Parser (Range, Externality Range)
createExternalTablePrefixP = do
    s <- Tok.createP
    r <- Tok.externalP
    _ <- Tok.tableP
    return (s, External r)


createExternalTableP :: Parser (CreateTable Vertica RawNames Range)
createExternalTableP = do
    (s, createTableExternality) <- createExternalTablePrefixP
    let createTablePersistence = Persistent
    createTableIfNotExists <- ifNotExistsP
    createTableName <- tableNameP

    createTableDefinition <- createTableColumnsP -- TODO allow for column-name-list syntax

    _ <- optional $ do
            _ <- optional $ Tok.includeP <|> Tok.excludeP
            _ <- Tok.schemaP
            Tok.privilegesP

    _ <- Tok.asP
    e <- Tok.copyP

    e' <- consumeOrderedOptions e $
        [ ingestionColumnListP (getInfo <$> exprP)
        , ingestionColumnOptionP
        , fromP -- you need **either** a FROM or a SOURCE clause, but let's not be fussy
        , fileStorageFormatP
        ]

    e'' <- consumeUnorderedOptions e' $
        [ Tok.withP
        , abortOnErrorP
        , delimiterAsP
        , enclosedByP
        , Tok.enforceLengthP
        , errorToleranceP
        , escapeFormatP
        , exceptionsOnNodeP
        , fileFilterP
        , nullAsP
        , fileParserP
        , recordTerminatorP
        , rejectedDataOnNodeP
        , rejectMaxP
        , skipRecordsP
        , skipBytesP
        , fileSourceP
        , trailingNullColsP
        , trimByteP
        ]

    let createTableInfo = s <> e''
        createTableExtra = Nothing

    pure CreateTable{..}

  where
    stringP :: Parser Range
    stringP = snd <$> Tok.stringP

    fromP :: Parser Range
    fromP = do
        s <- Tok.fromP
        let fileP = do
                r <- stringP
                consumeOrderedOptions r [nodeLocationP, compressionP]
        rs <- fileP `sepBy1` Tok.commaP
        return $ s <> last rs

    nodeLocationP = choice $
        [ Tok.onP >> snd <$> Tok.nodeNameP
        , Tok.onP >> Tok.anyP >> Tok.nodeP
        ]

createViewPrefixP :: Parser (Range, Maybe Range, Persistence Range)
createViewPrefixP = do
    s <- Tok.createP
    ifNotExists <- optionMaybe $ do
        s' <- Tok.orP
        e' <- Tok.replaceP
        pure $ s' <> e'
    persistence <- option Persistent $ Temporary <$> do
        s' <- Tok.localP
        e' <- Tok.temporaryP
        pure $ s' <> e'

    e <- Tok.viewP
    pure (s <> e, ifNotExists, persistence)

schemaPrivilegesP :: Parser Range
schemaPrivilegesP = do
    s <- choice [ Tok.includeP, Tok.excludeP ]
    optional Tok.schemaP
    e <- Tok.privilegesP
    return $ s <> e

createViewP :: Parser (CreateView RawNames Range)
createViewP = do
    (s, createViewIfNotExists, createViewPersistence) <- createViewPrefixP

    createViewName <- tableNameP >>= \case
        QTableName info Nothing view ->
            case createViewPersistence of
                Persistent -> pure $ QTableName info Nothing view
                Temporary _ -> pure $ QTableName info (pure $ QSchemaName info Nothing "<session>" SessionSchema) view
        qualifiedTableName ->
            case createViewPersistence of
                Persistent -> pure $ qualifiedTableName
                Temporary _ -> fail $ "cannot specify schema on a local temporary view"

    createViewColumns <- optionMaybe $ do
        _ <- Tok.openP
        c:cs <- unqualifiedColumnNameP `sepBy1` Tok.commaP
        _ <- Tok.closeP
        return (c:|cs)

    case createViewPersistence of
        Persistent -> optional schemaPrivilegesP
        Temporary _ -> pure ()

    _ <- Tok.asP
    createViewQuery <- queryP

    let createViewInfo = s <> getInfo createViewQuery
    pure CreateView{..}
  where
    unqualifiedColumnNameP = do
        (name, r) <- Tok.columnNameP
        pure $ QColumnName r None name


createTableP :: Parser (CreateTable Vertica RawNames Range)
createTableP = do
    s <- Tok.createP

    (createTablePersistence, isLocal) <- option (Persistent, False) $ do
        isLocal <- option False $ choice
            [ Tok.localP >> pure True
            , Tok.globalP >> pure False
            ]

        createTablePersistence <- Temporary <$> Tok.temporaryP
        pure (createTablePersistence, isLocal)

    let createTableExternality = Internal

    _ <- Tok.tableP

    createTableIfNotExists <- ifNotExistsP

    createTableName <- tableNameP >>= \case
        QTableName info Nothing table ->
            if isLocal
             then pure $ QTableName info (pure $ QSchemaName info Nothing "<session>" SessionSchema) table
             else pure $ QTableName info (pure $ QSchemaName info Nothing "public" NormalSchema) table
        qualifiedTableName ->
            if isLocal
             then fail "cannot specify schema on a local temporary table"
             else pure $ qualifiedTableName

    let onCommitP = case createTablePersistence of
            Persistent -> pure ()
            Temporary _ -> do
                -- TODO (T374141): do something with this
                _ <- Tok.onP
                _ <- Tok.commitP
                _ <- Tok.deleteP <|> Tok.preserveP
                void Tok.rowsP

    createTableDefinition <- choice
        [ createTableColumnsP <* optional onCommitP <* optional schemaPrivilegesP
        , try $ optional onCommitP *> optional schemaPrivilegesP *> createTableAsP
        , optional schemaPrivilegesP *> createTableLikeP
        ]

    createTableExtra <- tableInfoP

    case createTablePersistence of
        Persistent -> pure ()
        Temporary _ -> optional $ do
            _ <- Tok.noP
            void Tok.projectionP

    let e = maybe (getInfo createTableDefinition) getInfo createTableExtra
        createTableInfo = s <> e

    pure CreateTable{..}

  where
    columnListP :: Parser (NonEmpty (UQColumnName Range))
    columnListP = do
        _ <- Tok.openP
        c:cs <- (`sepBy1` Tok.commaP) $ do
            (name, r) <- Tok.columnNameP
            pure $ QColumnName r None name
        _ <- Tok.closeP
        pure (c:|cs)

    createTableLikeP = do
            s <- Tok.likeP
            table <- tableNameP
            e <- option (getInfo table) $ do
                -- TODO - include projection info in createTableExtra
                _ <- Tok.includingP <|> Tok.excludingP
                Tok.projectionsP

            pure $ TableLike (s <> e) table

    createTableAsP = do
            s <- Tok.asP
            columns <- optionMaybe $ try columnListP
            query <- optionalParensP $ queryP
            pure $ TableAs (s <> getInfo query) columns query

    tableInfoP :: Parser (Maybe (TableInfo RawNames Range))
    tableInfoP = do
        mOrdering <- optionMaybe orderTopLevelP
        let tableInfoOrdering = snd <$> mOrdering

        let tableInfoEncoding :: Maybe (TableEncoding RawNames Range)
            tableInfoEncoding = Nothing -- TODO

        tableInfoSegmentation <- optionMaybe $ choice
            [ do
                s <- Tok.unsegmentedP
                choice
                    [ do
                        _ <- Tok.nodeP
                        node <- nodeNameP
                        let e = getInfo node
                        pure $ UnsegmentedOneNode (s <> e) node
                    , do
                        _ <- Tok.allP
                        e <- Tok.nodesP
                        pure $ UnsegmentedAllNodes (s <> e)
                    ]
            , do
                s <- Tok.segmentedP
                _ <- Tok.byP
                expr <- exprP
                list <- nodeListP

                pure $ SegmentedBy (s <> getInfo list) expr list
            ]

        tableInfoKSafety <- optionMaybe $ do
            s <- Tok.ksafeP
            choice
                [ do
                    (n, e) <- integerP
                    pure $ KSafety (s <> e) (Just n)

                , pure $ KSafety s Nothing
                ]

        tableInfoPartitioning <- optionMaybe $ do
            s <- Tok.partitionP
            _ <- Tok.byP
            expr <- exprP
            pure $ Partitioning (s <> getInfo expr) expr

        let infos = [ fst <$> mOrdering
                    , getInfo <$> tableInfoEncoding
                    , getInfo <$> tableInfoSegmentation
                    , getInfo <$> tableInfoKSafety
                    , getInfo <$> tableInfoPartitioning
                    ]

        case getOption $ mconcat $ map Option infos of
            Nothing -> pure Nothing
            Just tableInfoInfo -> pure $ Just TableInfo{..}


dropViewPrefixP :: Parser Range
dropViewPrefixP = do
    s <- Tok.dropP
    e <- Tok.viewP
    pure $ s <> e

dropViewP :: Parser (DropView RawNames Range)
dropViewP = do
    s <- dropViewPrefixP
    dropViewIfExists <- optionMaybe ifExistsP
    dropViewName <- tableNameP

    let dropViewInfo = s <> getInfo dropViewName
    pure DropView{..}

dropTableP :: Parser (DropTable RawNames Range)
dropTableP = do
    s <- Tok.dropP
    _ <- Tok.tableP
    dropTableIfExists <- optionMaybe ifExistsP
    (dropTableName:rest) <- tableNameP `sepBy1` Tok.commaP
    cascade <- optionMaybe Tok.cascadeP

    let dropTableNames = dropTableName :| rest
        dropTableInfo = s <> (fromMaybe (getInfo $ NE.last dropTableNames) cascade)
    pure DropTable{..}


grantP :: Parser (Grant Range)
grantP = do
    s <- Tok.grantP
    e <- many1 Tok.notSemicolonP
    return $ Grant (s <> (last e))

revokeP :: Parser (Revoke Range)
revokeP = do
    s <- Tok.revokeP
    e <- many1 Tok.notSemicolonP
    return $ Revoke (s <> (last e))


beginP :: Parser Range
beginP = do
    s <- choice [ do
                      s <- Tok.beginP
                      e <- option s (Tok.workP <|> Tok.transactionP)
                      return $ s <> e
                , do
                      s <- Tok.startP
                      e <- Tok.transactionP
                      return $ s <> e
                ]
    e <- consumeOrderedOptions s [isolationLevelP, transactionModeP]
    return $ s <> e
  where
    isolationLevelP :: Parser Range
    isolationLevelP = do
        s <- Tok.isolationP
        _ <- Tok.levelP
        e <- choice [ Tok.serializableP
                    , Tok.repeatableP >> Tok.readP
                    , Tok.readP >> (Tok.committedP <|> Tok.uncommittedP)
                    ]
        return $ s <> e

    transactionModeP :: Parser Range
    transactionModeP = do
        s <- Tok.readP
        e <- Tok.onlyP <|> Tok.writeP
        return $ s <> e

commitP :: Parser Range
commitP = do
    s <- Tok.commitP <|> Tok.endP
    e <- option s (Tok.workP <|> Tok.transactionP)
    return $ s <> e

rollbackP :: Parser Range
rollbackP = do
    s <- Tok.rollbackP <|> Tok.abortP
    e <- option s (Tok.workP <|> Tok.transactionP)
    return $ s <> e

nodeListP :: Parser (NodeList Range)
nodeListP = choice
    [ do
        s <- Tok.allP
        e <- Tok.nodesP
        offset <- optionMaybe nodeListOffsetP

        let e' = maybe e getInfo offset

        pure $ AllNodes (s <> e') offset

    , do
        s <- Tok.nodesP
        n:ns <- nodeNameP `sepBy1` Tok.commaP
        let e = getInfo $ last (n:ns)
        pure $ Nodes (s <> e) (n:|ns)
    ]

nodeListOffsetP :: Parser (NodeListOffset Range)
nodeListOffsetP = do
    s <- Tok.offsetP
    (n, e) <- integerP
    pure $ NodeListOffset (s <> e) n

nodeNameP :: Parser (Node Range)
nodeNameP = do
    (node, e) <- Tok.nodeNameP
    pure $ Node e node

integerP :: Parser (Int, Range)
integerP = do
    (n, e) <- Tok.numberP
    case reads $ TL.unpack n of
        [(n', "")] -> pure (n', e)
        _ -> fail $ unwords ["unable to parse", show n, "as integer"]


selectP :: Parser (Select RawNames Range)
selectP = do
    r <- Tok.selectP

    selectDistinct <- option notDistinct distinctP

    selectCols <- do
        selections <- selectionP `sepBy1` Tok.commaP
        let r' = foldl1 (<>) $ map getInfo selections
        return $ SelectColumns r' selections

    selectFrom <- optionMaybe fromP
    selectWhere <- optionMaybe whereP
    selectTimeseries <- optionMaybe timeseriesP
    selectGroup <- optionMaybe groupP
    selectHaving <- optionMaybe havingP
    selectNamedWindow <- optionMaybe namedWindowP

    let (Just selectInfo) = sconcat $ Just r :|
            [ Just $ getInfo selectCols
            , getInfo <$> selectFrom
            , getInfo <$> selectWhere
            , getInfo <$> selectTimeseries
            , getInfo <$> selectGroup
            , getInfo <$> selectHaving
            , getInfo <$> selectNamedWindow
            ]
    return Select{..}

  where
    fromP = do
        r <- Tok.fromP
        tablishes <- tablishP `sepBy1` Tok.commaP

        let r' = foldl (<>) r $ fmap getInfo tablishes
        return $ SelectFrom r' tablishes

    whereP = do
        r <- Tok.whereP
        condition <- exprP
        return $ SelectWhere (r <> getInfo condition) condition

    timeseriesP = do
        s <- Tok.timeseriesP

        selectTimeseriesSliceName <- columnAliasP

        _ <- Tok.asP

        selectTimeseriesInterval <- do
            (c, r) <- Tok.stringP
            pure $ StringConstant r c

        _ <- Tok.overP
        _ <- Tok.openP
        selectTimeseriesPartition <- optionMaybe partitionP
        selectTimeseriesOrder <- do
            _ <- Tok.orderP
            _ <- Tok.byP
            exprP
        e <- Tok.closeP

        let selectTimeseriesInfo = s <> e
        pure $ SelectTimeseries {..}

    toGroupingElement :: PositionOrExpr RawNames Range -> GroupingElement RawNames Range
    toGroupingElement posOrExpr = GroupingElementExpr (getInfo posOrExpr) posOrExpr

    groupP = do
        r <- Tok.groupP
        _ <- Tok.byP
        exprs <- exprP `sepBy1` Tok.commaP

        let selectGroupGroupingElements = map (toGroupingElement . handlePositionalReferences) exprs
            selectGroupInfo = foldl (<>) r $ fmap getInfo selectGroupGroupingElements
        return SelectGroup{..}

    havingP = do
        r <- Tok.havingP
        conditions <- exprP `sepBy1` Tok.commaP

        let r' = foldl (<>) r $ fmap getInfo conditions
        return $ SelectHaving r' conditions

    namedWindowP =
      do
        r <- Tok.windowP
        windows <- (flip sepBy1) Tok.commaP $ do
          name <- windowNameP
          _ <- Tok.asP
          _ <- Tok.openP
          window <- choice
              [ do
                  partition@(Just p) <- Just <$> partitionP
                  order <- option [] orderInWindowClauseP
                  let orderInfos = map getInfo order -- better way?
                      info = L.foldl' (<>) (getInfo p) orderInfos
                  return $ Left $ WindowExpr info partition order Nothing
              , do
                  inherit <- windowNameP
                  order <- option [] orderInWindowClauseP
                  let orderInfo = map getInfo order -- better way?
                      info = L.foldl' (<>) (getInfo inherit) orderInfo
                  return $ Right $ PartialWindowExpr info inherit Nothing order Nothing
              ]
          e <- Tok.closeP
          let info = getInfo name <> e
          return $ case window of
            Left w -> NamedWindowExpr info name w
            Right pw -> NamedPartialWindowExpr info name pw
        let info = L.foldl' (<>) r $ fmap getInfo windows
        return $ SelectNamedWindow info windows

handlePositionalReferences :: Expr RawNames Range -> PositionOrExpr RawNames Range
handlePositionalReferences e = case e of
    ConstantExpr _ (NumericConstant _ n) | TL.all isDigit n -> PositionOrExprPosition (getInfo e) (read $ TL.unpack n) Unused
    _ -> PositionOrExprExpr e


selectStarP :: Parser (Selection RawNames Range)
selectStarP = choice
    [ do
        r <- Tok.starP
        return $ SelectStar r Nothing Unused

    , try $ do
        (t, r) <- Tok.tableNameP
        _ <- Tok.dotP
        r' <- Tok.starP

        return $ SelectStar (r <> r') (Just $ QTableName r Nothing t) Unused

    , try $ do
        (s, t, r, r') <- qualifiedTableNameP
        _ <- Tok.dotP
        r'' <- Tok.starP

        return $ SelectStar (r <> r'')
            (Just $ QTableName r' (Just $ mkNormalSchema s r) t) Unused
    ]


selectionP :: Parser (Selection RawNames Range)
selectionP = try selectStarP <|> do
    expr <- exprP
    alias <- aliasP expr

    return $ SelectExpr (getInfo alias <> getInfo expr) [alias] expr

makeColumnAlias :: Range -> Text -> Parser (ColumnAlias Range)
makeColumnAlias r alias = ColumnAlias r alias . ColumnAliasId <$> getNextCounter

makeTableAlias :: Range -> Text -> Parser (TableAlias Range)
makeTableAlias r alias = TableAlias r alias . TableAliasId <$> getNextCounter

makeDummyAlias :: Range -> Parser (ColumnAlias Range)
makeDummyAlias r = makeColumnAlias r "?column?"


makeExprAlias :: Expr RawNames Range -> Parser (ColumnAlias Range)
makeExprAlias (BinOpExpr info _ _ _) = makeDummyAlias info
makeExprAlias (UnOpExpr info _ _) = makeDummyAlias info
makeExprAlias (LikeExpr info _ _ _ _) = makeDummyAlias info
makeExprAlias (CaseExpr info _ _) = makeDummyAlias info
makeExprAlias (ColumnExpr info (QColumnName _ _ name)) = makeColumnAlias info name
makeExprAlias (ConstantExpr info _) = makeDummyAlias info
makeExprAlias (InListExpr info _ _) = makeDummyAlias info
makeExprAlias (InSubqueryExpr info _ _) = makeDummyAlias info
makeExprAlias (BetweenExpr info _ _ _) = makeDummyAlias info
makeExprAlias (OverlapsExpr info _ _) = makeDummyAlias info
makeExprAlias (AtTimeZoneExpr info _ _) = makeColumnAlias info "timezone" -- because reasons

-- function expressions get the name of the function
makeExprAlias (FunctionExpr info (QFunctionName _ _ name) _ _ _ _ _) = makeColumnAlias info name
makeExprAlias (SubqueryExpr info _) = makeDummyAlias info
makeExprAlias (ArrayExpr info _) = makeDummyAlias info  -- might actually be "array", but I'm not sure how to check
makeExprAlias (ExistsExpr info _) = makeDummyAlias info
makeExprAlias (FieldAccessExpr _ _ _) = fail "Unsupported struct access in Vertica: unused datatype in this dialect"
makeExprAlias (ArrayAccessExpr _ _ _) = fail "Unsupported array access in Vertica: unused datatype in this dialect"
makeExprAlias (TypeCastExpr _ _ expr _) = makeExprAlias expr
makeExprAlias (VariableSubstitutionExpr _) = fail "Unsupported variable substitution in Vertica: unused datatype in this dialect"
makeExprAlias LambdaParamExpr {} = error "Unreachable, vertica does not support lambda"
makeExprAlias LambdaExpr {} = error "Unreachable, vertica does not support lambda"

aliasP :: Expr RawNames Range -> Parser (ColumnAlias Range)
aliasP expr = choice
    [ try $ do
        optional Tok.asP
        (name, r) <- choice
            [ Tok.columnNameP
            , first TL.decodeUtf8 <$> Tok.stringP
            ]
        makeColumnAlias r name

    , do
        _ <- Tok.asP
        _ <- P.between Tok.openP Tok.closeP $ Tok.columnNameP `sepBy1` Tok.commaP
        makeExprAlias expr

    , makeExprAlias expr
    ]


exprP :: Parser (Expr RawNames Range)
exprP = orExprP

parenExprP :: Parser (Expr RawNames Range)
parenExprP = P.between Tok.openP Tok.closeP $ choice
    [ try subqueryExprP
    , exprP
    ]

subqueryExprP :: Parser (Expr RawNames Range)
subqueryExprP = do
    query <- queryP
    return $ SubqueryExpr (getInfo query) query

caseExprP :: Parser (Expr RawNames Range)
caseExprP = do
    r <- Tok.caseP
    whens <- choice
        [ P.many1 $ do
            _ <- Tok.whenP
            condition <- exprP
            _ <- Tok.thenP
            result <- exprP
            return (condition, result)

        , do
            expr <- exprP
            P.many1 $ do
                whenr <- Tok.whenP
                nullseq <- optionMaybe Tok.nullsequalP

                condition <- case nullseq of
                    Nothing -> BinOpExpr whenr "=" expr <$> exprP
                    Just nullseqr -> BinOpExpr (whenr <> nullseqr) "<=>" expr <$> exprP

                _ <- Tok.thenP
                result <- exprP
                return (condition, result)
        ]

    melse <- optionMaybe $ do
        _ <- Tok.elseP
        exprP

    r' <- Tok.endP

    return $ CaseExpr (r <> r') whens melse


fieldTypeP :: Parser (Expr RawNames Range)
fieldTypeP = do
    (ftype, r) <- Tok.fieldTypeP
    return $ ConstantExpr r $ StringConstant r $ TL.encodeUtf8 ftype

functionExprP :: Parser (Expr RawNames Range)
functionExprP = choice
    [ castFuncP
    , dateDiffFuncP
    , extractFuncP
    , try regularFuncP
    , bareFuncP
    ]
  where
    castFuncP = do
        r <- Tok.castP
        _ <- Tok.openP
        e <- exprP
        _ <- Tok.asP
        t <- choice
            [ try $ do
                i <- Tok.intervalP
                (unit, u) <- Tok.datePartP
                pure $ PrimitiveDataType (i <> u) ("INTERVAL " <> TL.toUpper unit) []
            , dataTypeP
            ]
        r' <- Tok.closeP

        return $ TypeCastExpr (r <> r') CastFailureError e t

    dateDiffFuncP = do
        r <- Tok.dateDiffP
        _ <- Tok.openP
        datepart <- choice
            [ do
                _ <- Tok.openP
                expr <- exprP
                _ <- Tok.closeP

                pure expr

            , do
                (string, r') <- Tok.stringP
                pure $ ConstantExpr r' $ StringConstant r' string

            , do
                (string, r') <- Tok.datePartP
                pure $ ConstantExpr r' $ StringConstant r' $ TL.encodeUtf8 string
            ]
        _ <- Tok.commaP
        startExp <- exprP
        _ <- Tok.commaP
        endExp <- exprP
        r' <- Tok.closeP

        return $ FunctionExpr (r <> r') (QFunctionName r Nothing "datediff") notDistinct [datepart, startExp, endExp] [] Nothing Nothing

    extractFuncP = do
        r <- Tok.extractP
        _ <- Tok.openP
        ftype <- fieldTypeP
        _ <- Tok.fromP
        expr <- exprP
        r' <- Tok.closeP

        return $ FunctionExpr (r <> r') (QFunctionName r Nothing "extract") notDistinct [ftype, expr] [] Nothing Nothing

    regularFuncP = do
        name <- choice
            [ try $ do
                (s, r) <- Tok.schemaNameP
                _ <- Tok.dotP
                (f, r') <- Tok.functionNameP
                return $ QFunctionName (r <> r') (Just $ mkNormalSchema s r) f

            , do
                (f, r) <- Tok.functionNameP
                return $ QFunctionName r Nothing f
            ]

        (distinct, arguments, parameters, r') <- do
            _ <- Tok.openP
            (distinct, arguments) <- choice
                [ case name of
                    QFunctionName _ Nothing "count" -> do
                        r' <- Tok.starP
                        return ( notDistinct
                               , [ConstantExpr r' $ NumericConstant r' "1"]
                               )

                    QFunctionName _ Nothing "substring" -> do
                        arg1 <- exprP
                        word <- (const True <$> Tok.fromP)
                            <|> (const False <$> Tok.commaP)
                        arg2 <- exprP
                        arg3 <- optionMaybe $ do
                            _ <- if word then Tok.forP else Tok.commaP
                            exprP
                        return ( notDistinct
                               , arg1 : arg2 : maybe [] pure arg3
                               )

                    _ -> fail "no special case for function"

                , do
                    isDistinct <- distinctP
                    (isDistinct,) . (:[]) <$> exprP

                , (notDistinct,) <$> exprP `sepBy` Tok.commaP
                ]

            parameters <- option [] $ do
                _ <- Tok.usingP
                _ <- Tok.parametersP

                flip sepBy1 Tok.commaP $ do
                    (param, paramr) <- Tok.paramNameP
                    _ <- Tok.equalP
                    expr <- exprP
                    pure (ParamName paramr param, expr)

            optional $ Tok.ignoreP >> Tok.nullsP

            r' <- Tok.closeP
            return (distinct, arguments, parameters, r')

        over <- optionMaybe $ try $ overP

        let r'' = maybe r' getInfo over <> getInfo name

        return $ FunctionExpr r'' name distinct arguments parameters Nothing over

    bareFuncP = do
        (v, r) <- choice
            [ Tok.currentDatabaseP
            , Tok.currentSchemaP
            , Tok.userP
            , Tok.currentUserP
            , Tok.sessionUserP
            , Tok.currentDateP
            , Tok.currentTimeP
            , Tok.currentTimestampP
            , Tok.localTimeP
            , Tok.localTimestampP
            , Tok.sysDateP
            ]

        pure $ FunctionExpr r (QFunctionName r Nothing v) notDistinct [] [] Nothing Nothing

orderTopLevelP :: Parser (Range, [Order RawNames Range])
orderTopLevelP = orderExprP False True

orderInWindowClauseP :: Parser [Order RawNames Range]
orderInWindowClauseP = snd <$> orderExprP True False

orderExprP :: Bool -> Bool -> Parser (Range, [Order RawNames Range])
orderExprP nullsClausePermitted positionalReferencesPermitted = do
    r <- Tok.orderP
    _ <- Tok.byP
    orders <- helperP `sepBy1` Tok.commaP
    let r' = getInfo $ last orders
    return (r <> r', orders)
  where
    helperP :: Parser (Order RawNames Range)
    helperP = do
        expr <- exprP
        let posOrExpr = if positionalReferencesPermitted
                        then handlePositionalReferences expr
                        else PositionOrExprExpr expr
        dir <- directionP
        nulls <- case (nullsClausePermitted, dir) of
            (False, _) -> return $ NullsAuto Nothing
            (True, OrderAsc _) -> option (NullsLast Nothing) nullsP
            (True, OrderDesc _) -> option (NullsFirst Nothing) nullsP
        let info = getInfo expr ?<> getInfo dir <> getInfo nulls
        return $ Order info posOrExpr dir nulls

directionP :: Parser (OrderDirection (Maybe Range))
directionP = option (OrderAsc Nothing) $ choice
    [ OrderAsc . Just <$> Tok.ascP
    , OrderDesc . Just <$> Tok.descP
    ]

nullsP :: Parser (NullPosition (Maybe Range))
nullsP = do
    r <- Tok.nullsP
    choice
        [ Tok.firstP >>= \ r' -> return $ NullsFirst $ Just $ r <> r'
        , Tok.lastP >>= \ r' -> return $ NullsLast $ Just $ r <> r'
        , Tok.autoP >>= \ r' -> return $ NullsAuto $ Just $ r <> r'
        ]

frameP :: Parser (Frame Range)
frameP = do
    ftype <- choice
        [ RowFrame <$> Tok.rowsP
        , RangeFrame <$> Tok.rangeP
        ]

    choice
        [ do
            _ <- Tok.betweenP
            start <- frameBoundP
            _ <- Tok.andP
            end <- frameBoundP

            let r = getInfo ftype <> getInfo end
            return $ Frame r ftype start (Just end)

        , do
            start <- frameBoundP

            let r = getInfo ftype <> getInfo start
            return $ Frame r ftype start Nothing
        ]

frameBoundP :: Parser (FrameBound Range)
frameBoundP = choice
    [ fmap Unbounded $ (<>)
        <$> Tok.unboundedP
        <*> choice [ Tok.precedingP, Tok.followingP ]

    , fmap CurrentRow $ (<>) <$> Tok.currentP <*> Tok.rowP
    , constantP >>= \ expr -> choice
        [ Tok.precedingP >>= \ r ->
            return $ Preceding (getInfo expr <> r) expr

        , Tok.followingP >>= \ r ->
            return $ Following (getInfo expr <> r) expr
        ]
    ]


overP :: Parser (OverSubExpr RawNames Range)
overP = do
    start <- Tok.overP
    subExpr <- choice
        [ Left <$> windowP
        , Right <$> windowNameP
        ]
    return $ case subExpr of
      Left w -> mergeWindowInfo start w
      Right wn -> OverWindowName (start <> getInfo wn) wn
  where
    windowP :: Parser (OverSubExpr RawNames Range)
    windowP = do
      start' <- Tok.openP
      expr <- choice
          [ Left <$> windowExprP start'
          , Right <$> partialWindowExprP start'
          ]
      return $ case expr of
        Left w -> OverWindowExpr (start' <> getInfo w) w
        Right pw -> OverPartialWindowExpr (start' <> getInfo pw) pw

    mergeWindowInfo :: Range -> OverSubExpr RawNames Range -> OverSubExpr RawNames Range
    mergeWindowInfo r = \case
        OverWindowExpr r' WindowExpr{..} ->
            OverWindowExpr (r <> r') $ WindowExpr { windowExprInfo = windowExprInfo <> r , ..}
        OverWindowName r' n -> OverWindowName (r <> r') n
        OverPartialWindowExpr r' PartialWindowExpr{..} ->
            OverPartialWindowExpr (r <> r') $ PartialWindowExpr { partWindowExprInfo = partWindowExprInfo <> r , ..}

windowExprP :: Range -> Parser (WindowExpr RawNames Range)
windowExprP start =
  do
    partition <- optionMaybe partitionP
    order <- option [] orderInWindowClauseP
    frame <- optionMaybe frameP
    end <- Tok.closeP
    let info = start <> end
    return (WindowExpr info partition order frame)

partialWindowExprP :: Range -> Parser (PartialWindowExpr RawNames Range)
partialWindowExprP start =
  do
    inherit <- windowNameP
    order <- option [] orderInWindowClauseP
    frame <- optionMaybe frameP
    end <- Tok.closeP
    let info = start <> end
    return (PartialWindowExpr info inherit Nothing order frame)

windowNameP :: Parser (WindowName Range)
windowNameP =
  do
    (name, r) <- Tok.windowNameP
    return $ WindowName r name

partitionP :: Parser (Partition RawNames Range)
partitionP = do
    r <- Tok.partitionP
    choice
        [ Tok.byP >> (exprP `sepBy1` Tok.commaP) >>= \ exprs ->
                return $ PartitionBy
                            (sconcat $ r :| map getInfo exprs) exprs

        , Tok.bestP >>= \ r' -> return $ PartitionBest (r <> r')
        , Tok.nodesP >>= \ r' -> return $ PartitionNodes (r <> r')
        ]


existsExprP :: Parser (Expr RawNames Range)
existsExprP = do
    r <- Tok.existsP
    _ <- Tok.openP
    query <- queryP
    r' <- Tok.closeP

    return $ ExistsExpr (r <> r') query


arrayExprP :: Parser (Expr RawNames Range)
arrayExprP = do
    s <- Tok.arrayP
    _ <- Tok.openBracketP
    cs <- exprP `sepBy` Tok.commaP
    e <- Tok.closeBracketP
    pure $ ArrayExpr (s <> e) cs


castExprP :: Parser (Expr RawNames Range)
castExprP = foldl (flip ($)) <$> castedP <*> many castP
  where
    castedP :: Parser (Expr RawNames Range)
    castedP = choice
        [ try parenExprP
        , try existsExprP
        , try arrayExprP
        , try functionExprP
        , caseExprP
        , try $ do
            constant <- constantP
            return $ ConstantExpr (getInfo constant) constant

        , do
            name <- columnNameP
            return $ ColumnExpr (getInfo name) name
        ]

    castP :: Parser (Expr RawNames Range -> Expr RawNames Range)
    castP = do
        _ <- Tok.castOpP
        typeName <- dataTypeP

        let r expr = getInfo expr <> getInfo typeName
        return (\ expr -> TypeCastExpr (r expr) CastFailureError expr typeName)


atTimeZoneExprP :: Parser (Expr RawNames Range)
atTimeZoneExprP = foldl (flip ($)) <$> castExprP <*> many atTimeZoneP
  where
    atTimeZoneP :: Parser (Expr RawNames Range -> Expr RawNames Range)
    atTimeZoneP = do
        _ <- Tok.atP
        _ <- Tok.timezoneP
        tz <- castExprP

        return $ \ expr ->
            AtTimeZoneExpr (getInfo expr <> getInfo tz) expr tz


unOpP :: Text -> Parser (Expr RawNames Range -> Expr RawNames Range)
unOpP op = do
    r <- Tok.symbolP op
    return $ \ expr -> UnOpExpr (r <> getInfo expr) (Operator op) expr


negateExprP :: Parser (Expr RawNames Range)
negateExprP = do
    neg <- option id $ choice $ map unOpP [ "+", "-", "@", "~" ]
    expr <- atTimeZoneExprP
    return $ neg expr


binOpP :: Text -> Parser (Expr RawNames Range -> Expr RawNames Range -> Expr RawNames Range)
binOpP op = do
    r <- Tok.symbolP op

    let r' lhs rhs = sconcat $ r :| map getInfo [lhs, rhs]
    return $ \ lhs rhs -> BinOpExpr (r' lhs rhs) (Operator op) lhs rhs


exponentExprP :: Parser (Expr RawNames Range)
exponentExprP = negateExprP `chainl1` binOpP "^"


productExprP :: Parser (Expr RawNames Range)
productExprP = exponentExprP `chainl1` opP
  where
    opP = choice $ map binOpP [ "*", "//", "/", "%" ]


sumExprP :: Parser (Expr RawNames Range)
sumExprP = productExprP `chainl1` opP
  where
    opP = choice $ map binOpP [ "+", "-" ]


bitwiseExprP :: Parser (Expr RawNames Range)
bitwiseExprP = sumExprP `chainl1` opP
  where
    opP = choice $ map binOpP [ "&", "|", "#" ]


bitShiftExprP :: Parser (Expr RawNames Range)
bitShiftExprP = bitwiseExprP `chainl1` opP
  where
    opP = choice $ map binOpP [ "<<", ">>" ]


notP :: Parser (Expr RawNames Range -> Expr RawNames Range)
notP = (\ r -> UnOpExpr r "NOT") <$> Tok.notP

isExprP :: Parser (Expr RawNames Range)
isExprP = do
    expr <- bitShiftExprP
    is <- fmap (foldl (.) id) $ many $ choice
        [ do
            _ <- Tok.isP

            not_ <- option id notP

            (not_ .) <$> choice
                [ Tok.trueP >>= \ r -> return (UnOpExpr r "ISTRUE")
                , Tok.falseP >>= \ r -> return (UnOpExpr r "ISFALSE")
                , Tok.nullP >>= \ r -> return (UnOpExpr r "ISNULL")
                , Tok.unknownP >>= \ r -> return (UnOpExpr r "ISUNKNOWN")
                ]
        , Tok.isnullP >>= \ r -> return (UnOpExpr r "ISNULL")
        , Tok.notnullP >>= \ r -> return (UnOpExpr r "NOT" . UnOpExpr r "ISNULL")
        ]

    return $ is expr


appendExprP :: Parser (Expr RawNames Range)
appendExprP = isExprP `chainl1` binOpP "||"


inExprP :: Parser (Expr RawNames Range)
inExprP = do
    expr <- appendExprP
    not_ <- option id notP
    in_ <- foldl (.) id <$> many inP

    return $ not_ $ in_ expr

  where
    inP = do
        _ <- Tok.inP
        _ <- Tok.openP
        list <- choice
            [ Left <$> queryP
            , Right <$> exprP `sepBy1` Tok.commaP
            ]

        r <- Tok.closeP

        return $ case list of
            Left query ->
                \ expr -> InSubqueryExpr (getInfo expr <> r) query expr

            Right constants ->
                \ expr -> InListExpr (getInfo expr <> r) constants expr

betweenExprP :: Parser (Expr RawNames Range)
betweenExprP = do
    expr <- inExprP
    between <- foldl (.) id <$> many betweenP

    return $ between expr
  where
    betweenP = do
        _ <- Tok.betweenP
        start <- bitShiftExprP
        _ <- Tok.andP
        end <- bitShiftExprP

        let r expr = getInfo expr <> getInfo end
        return $ \ expr -> BetweenExpr (r expr) start end expr


overlapsExprP :: Parser (Expr RawNames Range)
overlapsExprP = try overlapsP <|> betweenExprP
  where
    overlapsP = do
        let pair :: Parser a -> Parser ((a, a), Range)
            pair p = do
                r <- Tok.openP
                s <- p
                _ <- Tok.commaP
                e <- p
                r' <- Tok.closeP
                return ((s, e), r <> r')

        (lhs, r) <- pair exprP
        _ <- Tok.overlapsP
        (rhs, r') <- pair exprP
        return $ OverlapsExpr (r <> r') lhs rhs


likeExprP :: Parser (Expr RawNames Range)
likeExprP = do
    expr <- overlapsExprP
    like <- option id comparisonP
    return $ like expr
  where
    comparisonP :: Parser (Expr RawNames Range -> Expr RawNames Range)
    comparisonP = choice
        [ do
            comparison <- symbolComparisonP
            pattern <- Pattern <$> overlapsExprP
            return $ comparison pattern
        , do
            comparison <- textComparisonP
            pattern <- Pattern <$> overlapsExprP
            escape <- optionMaybe $ do
                _ <- Tok.escapeP
                Escape <$> exprP

            return $ comparison escape pattern
        ]


    symbolComparisonP :: Parser (Pattern RawNames Range -> Expr RawNames Range -> Expr RawNames Range)
    symbolComparisonP = choice $
        let r expr pattern = getInfo expr <> getInfo pattern
         in [ do
                _ <- Tok.likeOpP
                return $ \ pattern expr -> LikeExpr (r pattern expr) "LIKE" Nothing pattern expr

            , do
                _ <- Tok.iLikeOpP
                return $ \ pattern expr -> LikeExpr (r pattern expr) "ILIKE" Nothing pattern expr

            , do
                _ <- Tok.notLikeOpP
                return $ \ pattern expr ->
                    UnOpExpr (r pattern expr) "NOT" $ LikeExpr (r pattern expr) "LIKE" Nothing pattern expr

            , do
                _ <- Tok.notILikeOpP
                return $ \ pattern expr ->
                    UnOpExpr (r pattern expr) "NOT" $ LikeExpr (r pattern expr) "ILIKE" Nothing pattern expr
            , do
                _ <- Tok.regexMatchesP
                return $ \ pattern expr ->
                    BinOpExpr (r pattern expr) "REGEX MATCHES" expr $ patternExpr pattern

            , do
                _ <- Tok.regexIgnoreCaseMatchesP
                return $ \ pattern expr ->
                    BinOpExpr (r pattern expr) "REGEX IGNORE-CASE MATCHES" expr $ patternExpr pattern

            , do
                _ <- Tok.notRegexMatchesP
                return $ \ pattern expr ->
                    UnOpExpr (r pattern expr) "NOT" $
                             BinOpExpr (r pattern expr) "REGEX MATCHES" expr $ patternExpr pattern

            , do
                _ <- Tok.notRegexIgnoreCaseMatchesP
                return $ \ pattern expr ->
                    UnOpExpr (r pattern expr) "NOT" $
                             BinOpExpr (r pattern expr) "REGEX IGNORE-CASE MATCHES" expr $ patternExpr pattern

            ]

    textComparisonP :: Parser (Maybe (Escape RawNames Range) -> Pattern RawNames Range -> Expr RawNames Range -> Expr RawNames Range)
    textComparisonP = do
        not_ <- option id notP

        like <- choice
            [ Tok.likeP >>= \ r -> return $ LikeExpr r "LIKE"
            , Tok.iLikeP >>= \ r -> return $ LikeExpr r "ILIKE"
            , Tok.likeBP >>= \ r -> return $ LikeExpr r "LIKE"
            , Tok.iLikeBP >>= \ r -> return $ LikeExpr r "ILIKE"
            ]

        return $ \ escape pattern expr -> not_ $ like escape pattern expr


mkBinOp :: (Text, a) -> Expr r a -> Expr r a -> Expr r a
mkBinOp (op, r) = BinOpExpr r (Operator op)

inequalityExprP :: Parser (Expr RawNames Range)
inequalityExprP = likeExprP `chainl1` (mkBinOp <$> Tok.inequalityOpP)


equalityExprP :: Parser (Expr RawNames Range)
equalityExprP = inequalityExprP `chainl1` (mkBinOp <$> Tok.equalityOpP)


notExprP :: Parser (Expr RawNames Range)
notExprP = do
    nots <- appEndo . fold . reverse . map Endo <$> many notP
    expr <- equalityExprP
    return $ nots expr


andExprP :: Parser (Expr RawNames Range)
andExprP = notExprP `chainl1`
    (Tok.andP >>= \ r -> return $ BinOpExpr r "AND")


orExprP :: Parser (Expr RawNames Range)
orExprP = andExprP `chainl1` (Tok.orP >>= \ r -> return (BinOpExpr r "OR"))


singleTableP :: Parser (Tablish RawNames Range)
singleTableP = try subqueryP <|> try tableP <|> parenthesizedJoinP
  where
    subqueryP = do
        r <- Tok.openP
        query <- queryP
        _ <- Tok.closeP
        optional Tok.asP
        (name, r') <- Tok.tableNameP

        alias <- makeTableAlias r' name
        return $ TablishSubQuery (r <> r')
                                 (TablishAliasesT alias)
                                 query

    tableP = do
        name <- tableNameP
        maybe_alias <- optionMaybe $ do
            optional Tok.asP
            (alias, r) <- Tok.tableNameP
            makeTableAlias r alias

        let r = case maybe_alias of
                Nothing -> getInfo name
                Just alias -> getInfo alias <> getInfo name
            aliases = maybe TablishAliasesNone TablishAliasesT maybe_alias

        return $ TablishTable r aliases name

    parenthesizedJoinP = do
        tablish <- P.between Tok.openP Tok.closeP $ do
            table <- singleTableP
            joins <- fmap (appEndo . fold . reverse) $ many1 $ Endo <$> joinP
            return $ joins table

        optional $ do
            optional Tok.asP
            void Tok.tableNameP

        pure tablish


optionalParensP :: Parser a -> Parser a
optionalParensP p = try p <|> P.between Tok.openP Tok.closeP p

manyParensP :: Parser a -> Parser a
manyParensP p = try p <|> P.between Tok.openP Tok.closeP (manyParensP p)

tablishP :: Parser (Tablish RawNames Range)
tablishP = do
    table <- singleTableP
    joins <- fmap (appEndo . fold . reverse) $ many $ Endo <$> joinP
    return $ joins table

joinP :: Parser (Tablish RawNames Range -> Tablish RawNames Range)
joinP = regularJoinP <|> naturalJoinP <|> crossJoinP

regularJoinP :: Parser (Tablish RawNames Range -> Tablish RawNames Range)
regularJoinP = do
    maybeJoinType <- optionMaybe $ innerJoinTypeP <|> outerJoinTypeP
    joinType <- Tok.joinP >>= \ r -> return $ case maybeJoinType of
        Nothing -> JoinInner r
        Just joinType -> (<> r) <$> joinType

    rhs <- singleTableP
    condition <- choice
        [ do
            _ <- Tok.onP <?> "condition in join clause"
            JoinOn <$> exprP
        , do
            s <- Tok.usingP <?> "using in join clause"
            _ <- Tok.openP
            names <- flip sepBy1 Tok.commaP $ do
                (name, r) <- Tok.columnNameP
                pure $ QColumnName r None name
            e <- Tok.closeP
            return $ JoinUsing (s <> e) names
        ]

    let r lhs = getInfo lhs <> getInfo rhs <> getInfo condition
    return $ \ lhs ->
        TablishJoin (r lhs) joinType condition lhs rhs

outerJoinTypeP :: Parser (JoinType Range)
outerJoinTypeP = do
    joinType <- choice
        [ Tok.leftP >>= \ r -> return $ JoinLeft r
        , Tok.rightP >>= \ r -> return $ JoinRight r
        , Tok.fullP >>= \ r -> return $ JoinFull r
        ]

    optional Tok.outerP

    return joinType

innerJoinTypeP :: Parser (JoinType Range)
innerJoinTypeP = Tok.innerP >>= \ r -> return $ JoinInner r

naturalJoinP :: Parser (Tablish RawNames Range -> Tablish RawNames Range)
naturalJoinP = do
    r <- Tok.naturalP
    maybeJoinType <- optionMaybe $ innerJoinTypeP <|> outerJoinTypeP
    joinType <- Tok.joinP >>= \ r' -> return $ case maybeJoinType of
        Nothing -> JoinInner r
        Just joinType -> (const $ r <> r') <$> joinType

    rhs <- singleTableP

    let r' lhs = getInfo lhs <> getInfo rhs
    return $ \ lhs -> TablishJoin (r' lhs) joinType (JoinNatural r Unused) lhs rhs

crossJoinP :: Parser (Tablish RawNames Range -> Tablish RawNames Range)
crossJoinP = do
    r <- Tok.crossP
    r'<- Tok.joinP
    rhs <- singleTableP

    let r'' lhs = getInfo lhs <> getInfo rhs
        joinInfo = r <> r'
        true' = JoinOn $ ConstantExpr joinInfo $ BooleanConstant joinInfo True
    return $ \ lhs ->
        TablishJoin (r'' lhs) (JoinInner joinInfo) true' lhs rhs


createProjectionPrefixP :: Parser Range
createProjectionPrefixP = do
    s <- Tok.createP
    e <- Tok.projectionP
    pure $ s <> e


createProjectionP :: Parser (CreateProjection RawNames Range)
createProjectionP = do
    s <- createProjectionPrefixP

    createProjectionIfNotExists <- ifNotExistsP
    createProjectionName <- projectionNameP
    createProjectionColumns <- optionMaybe $ try columnListP
    _ <- Tok.asP
    createProjectionQuery <- queryP

    createProjectionSegmentation <- optionMaybe $ choice
        [ do
            s' <- Tok.unsegmentedP
            choice
                [ do
                    _ <- Tok.nodeP
                    node <- nodeNameP
                    let e' = getInfo node
                    pure $ UnsegmentedOneNode (s' <> e') node
                , do
                    _ <- Tok.allP
                    e' <- Tok.nodesP
                    pure $ UnsegmentedAllNodes (s' <> e')
                ]
        , do
            s' <- Tok.segmentedP
            _ <- Tok.byP
            expr <- exprP
            list <- nodeListP

            pure $ SegmentedBy (s' <> getInfo list) expr list
        ]

    createProjectionKSafety <- optionMaybe $ do
        s' <- Tok.ksafeP
        choice
            [ do
                (n, e') <- integerP
                pure $ KSafety (s' <> e') (Just n)

            , pure $ KSafety s' Nothing
            ]

    let createProjectionInfo =
            sconcat $ s :| catMaybes [ Just $ getInfo createProjectionQuery
                                     , getInfo <$> createProjectionSegmentation
                                     , getInfo <$> createProjectionKSafety
                                     ]

    pure CreateProjection{..}

  where
    columnListP :: Parser (NonEmpty (ProjectionColumn Range))
    columnListP = do
        _ <- Tok.openP
        c:cs <- flip sepBy1 Tok.commaP $ do
            (projectionColumnName, s) <- Tok.columnNameP
            projectionColumnAccessRank <- optionMaybe $ do
                s' <- Tok.accessRankP
                (n, e') <- integerP
                pure $ AccessRank (s' <> e') n

            projectionColumnEncoding <- optionMaybe $ do
                _ <- Tok.encodingP
                Tok.encodingTypeP

            let projectionColumnInfo =
                    sconcat $ s :| catMaybes [ getInfo <$> projectionColumnAccessRank
                                             , getInfo <$> projectionColumnEncoding ]

            pure ProjectionColumn{..}

        _ <- Tok.closeP
        pure (c:|cs)


multipleRenameP :: Parser (MultipleRename RawNames Range)
multipleRenameP = do
    s <- Tok.alterP
    _ <- Tok.tableP

    sources <- tableNameP `sepBy1` Tok.commaP

    _ <- Tok.renameP
    _ <- Tok.toP

    targets <- map (\ uqtn -> uqtn { tableNameSchema = Nothing }) <$> unqualifiedTableNameP `sepBy1` Tok.commaP

    when (length sources /= length targets) $ fail "multi-renames require the same number of sources and targets"

    let e = getInfo $ last targets
        pairs = zip sources targets
        toAlterTableRename = \ (from, to) ->
            AlterTableRenameTable (getInfo from <> getInfo to) from to
        renames = map toAlterTableRename pairs
    pure $ MultipleRename (s <> e) renames


setSchemaP :: Parser (SetSchema RawNames Range)
setSchemaP = do
    s <- Tok.alterP
    _ <- Tok.tableP

    table <- tableNameP

    _ <- Tok.setP
    _ <- Tok.schemaP

    (schema, r) <- Tok.schemaNameP

    e <- option r $ choice [Tok.restrictP, Tok.cascadeP]

    pure $ SetSchema (s <> e) table $ mkNormalSchema schema r


renameProjectionP :: Parser Range
renameProjectionP = do
    s <- Tok.alterP
    _ <- Tok.projectionP

    _ <- projectionNameP

    _ <- Tok.renameP
    _ <- Tok.toP

    to <- projectionNameP

    pure $ s <> getInfo to


alterResourcePoolPrefixP :: Parser Range
alterResourcePoolPrefixP = do
    s <- Tok.alterP
    _ <- Tok.resourceP
    e <- Tok.poolP
    pure $ s <> e

alterResourcePoolP :: Parser Range
alterResourcePoolP = do
    s <- alterResourcePoolPrefixP
    ts <- P.many Tok.notSemicolonP
    pure $ case reverse ts of
        [] -> s
        e:_ -> s <> e


createResourcePoolPrefixP :: Parser Range
createResourcePoolPrefixP = do
    s <- Tok.createP
    _ <- Tok.resourceP
    e <- Tok.poolP
    pure $ s <> e

createResourcePoolP :: Parser Range
createResourcePoolP = do
    s <- createResourcePoolPrefixP
    ts <- P.many Tok.notSemicolonP
    pure $ case reverse ts of
        [] -> s
        e:_ -> s <> e


dropResourcePoolPrefixP :: Parser Range
dropResourcePoolPrefixP = do
    s <- Tok.dropP
    _ <- Tok.resourceP
    e <- Tok.poolP
    pure $ s <> e

dropResourcePoolP :: Parser Range
dropResourcePoolP = do
    s <- dropResourcePoolPrefixP
    e <- Tok.notSemicolonP -- the pool's name
    pure $ s <> e


createFunctionPrefixP :: Parser Range
createFunctionPrefixP = do
    s <- Tok.createP
    _ <- optional $ Tok.orP >> Tok.replaceP
    e <- choice
         [ do
               _ <- optional $ Tok.transformP <|> Tok.analyticP <|> Tok.aggregateP
               Tok.functionP
         , Tok.filterP
         , Tok.parserP
         , Tok.sourceP
         ]
    pure $ s <> e


createFunctionP :: Parser Range
createFunctionP = do
    s <- createFunctionPrefixP
    ts <- P.many Tok.notSemicolonP
    pure $ case reverse ts of
        [] -> s
        e:_ -> s <> e


alterTableAddConstraintP :: Parser Range
alterTableAddConstraintP = do
    s <- Tok.alterP
    _ <- Tok.tableP

    _ <- tableNameP

    _ <- Tok.addP
    e <- tableConstraintP

    pure $ s <> e

tableConstraintP :: Parser Range
tableConstraintP = do
    s <- optionMaybe $ do
        s <- Tok.constraintP
        _ <- Tok.constraintNameP
        return s

    e <- choice
        [ do
            _ <- Tok.primaryP
            _ <- Tok.keyP
            e <- columnListP
            option e (Tok.enabledP <|> Tok.disabledP)
        , do
            _ <- Tok.uniqueP
            e <- columnListP
            option e (Tok.enabledP <|> Tok.disabledP)
        , do
            _ <- Tok.foreignP
            _ <- Tok.keyP
            _ <- columnListP
            _ <- Tok.referencesP
            e <- getInfo <$> tableNameP
            option e columnListP
        , do
            _ <- Tok.checkP
            e <- getInfo <$> exprP
            option e (Tok.enabledP <|> Tok.disabledP)
        ]

    return (maybe e id s <> e)
  where
    columnListP :: Parser Range
    columnListP = do
        s <- Tok.openP
        _ <- Tok.columnNameP `sepBy1` Tok.commaP
        e <- Tok.closeP
        return (s <> e)


exportToStdoutP :: Parser Range
exportToStdoutP = do
    s <- Tok.exportP
    _ <- Tok.toP
    _ <- Tok.stdoutP
    _ <- Tok.fromP
    _ <- tableNameP
    _ <- Tok.openP
    _ <- Tok.columnNameP `sepBy1` Tok.commaP
    e <- Tok.closeP

    pure $ s <> e


setSessionPrefixP :: Parser Range
setSessionPrefixP = do
    s <- Tok.setP
    e <- Tok.sessionP
    return $ s <> e


setSessionP :: Parser Range
setSessionP = do
    s <- setSessionPrefixP
    ts <- P.many Tok.notSemicolonP
    pure $ case reverse ts of
        [] -> s
        e:_ -> s <> e


setTimeZoneP :: Parser Range
setTimeZoneP = do
    s <- Tok.setP
    _ <- Tok.timezoneP
    _ <- Tok.toP
    e <- choice [ Tok.defaultP
                , snd <$> Tok.stringP
                , Tok.intervalP >> snd <$> Tok.stringP
                ]
    return $ s <> e


connectP :: Parser Range
connectP = do
    s <- Tok.connectP
    _ <- Tok.toP
    _ <- Tok.verticaP
    _ <- Tok.databaseNameP
    _ <- Tok.userP
    _ <- Tok.userNameP
    _ <- Tok.passwordP
    e <- snd <$> Tok.stringP <|> snd <$> starsP
    e' <- option e $ do
        _ <- Tok.onP
        _ <- Tok.stringP
        _ <- Tok.commaP
        snd <$> Tok.numberP
    pure $ s <> e'
  where
    starsP = do
        rs <- P.many1 Tok.starP
        let text = TL.take (fromIntegral $ length rs) $ TL.repeat '*'
            r = head rs <> last rs
        pure (text, r)


disconnectP :: Parser Range
disconnectP = do
    s <- Tok.disconnectP
    (_, e) <- Tok.databaseNameP
    pure $ s <> e

createAccessPolicyP :: Parser Range
createAccessPolicyP = do
    s <- Tok.createP
    _ <- Tok.accessP
    _ <- Tok.policyP
    _ <- Tok.onP
    _ <- tableNameP
    _ <- Tok.forP
    _ <- Tok.columnP
    _ <- Tok.columnNameP
    _ <- exprP
    e <- choice [ Tok.enableP, Tok.disableP ]
    pure $ s <> e


copyFromP :: Parser Range
copyFromP = do
    s <- Tok.copyP
    e <- getInfo <$> tableNameP

    e' <- consumeOrderedOptions e $
        [ ingestionColumnListP (getInfo <$> exprP)
        , ingestionColumnOptionP
        , fromP -- you need **either** a FROM or a SOURCE clause, but let's not be fussy
        , fileStorageFormatP
        ]
    e'' <- consumeUnorderedOptions e' $
        [ do
            _ <- optional Tok.withP
            choice [ fileSourceP
                   , fileFilterP
                   , fileParserP
                   ]
        , delimiterAsP
        , trailingNullColsP
        , nullAsP
        , escapeFormatP
        , enclosedByP
        , recordTerminatorP
        , try $ skipRecordsP
        , try $ skipBytesP
        , trimByteP
        , rejectMaxP
        , rejectedDataOnNodeP
        , exceptionsOnNodeP
        , Tok.enforceLengthP
        , errorToleranceP
        , abortOnErrorP
        , optional Tok.storageP >> loadMethodP
        , streamNameP
        , noCommitP
        ]

    return $ s <> e''

  where
    onNodeP :: Range -> Parser Range
    onNodeP r = do
        s <- option r $ choice
                 [ try $ Tok.onP >> snd <$> Tok.nodeNameP
                 , Tok.onP >> Tok.anyP >> Tok.nodeP
                 ]
        e <- option s compressionP
        return $ s <> e

    fromP :: Parser Range
    fromP = do
        outerS <- Tok.fromP
        outerE <- choice $
            [ do
                s <- Tok.stdinP
                e <- option s compressionP
                return $ s <> e
            , do
                (_, s) <- Tok.stringP
                e <- last <$> ((onNodeP s) `sepBy1` Tok.commaP)
                return $ s <> e
            , do
                s <- Tok.localP
                e' <- choice [ do
                                  e <- Tok.stdinP
                                  option e compressionP
                            , let pathToDataP = do
                                      e <- snd <$> Tok.stringP
                                      option e compressionP
                               in last <$> (pathToDataP `sepBy1` Tok.commaP)
                            ]
                return $ s <> e'
            , do
                s <- Tok.verticaP
                _ <- Tok.databaseNameP
                _ <- Tok.dotP
                e <- getInfo <$> tableNameP
                e' <- option e $ do
                    _ <- Tok.openP
                    _ <- Tok.columnNameP `sepBy1` Tok.commaP
                    Tok.closeP
                return $ s <> e'
            ]
        return $ outerS <> outerE


showP :: Parser Range
showP = do
    s <- Tok.showP
    es <- many1 Tok.notSemicolonP
    return $ s <> last es


mergeP :: Parser (Merge RawNames Range)
mergeP = do
    r1 <- Tok.mergeP

    _ <- Tok.intoP
    mergeTargetTable <- tableNameP
    mergeTargetAlias <- optionMaybe tableAliasP

    _ <- Tok.usingP
    mergeSourceTable <- tableNameP
    mergeSourceAlias <- optionMaybe tableAliasP

    _ <- Tok.onP
    mergeCondition <- exprP

    -- lookahead
    mergeUpdateDirective <- optionMaybe $ do
        _ <- try $ P.lookAhead $ Tok.whenP >> Tok.matchedP
        _ <- Tok.whenP
        _ <- Tok.matchedP
        _ <- Tok.thenP
        _ <- Tok.updateP
        _ <- Tok.setP
        NE.fromList <$> colValP `sepBy1` Tok.commaP

    (mergeInsertDirectiveColumns, mergeInsertDirectiveValues, r2) <- option (Nothing, Nothing, Just r1) $ do
        _ <- Tok.whenP
        _ <- Tok.notP
        _ <- Tok.matchedP
        _ <- Tok.thenP
        _ <- Tok.insertP
        cols <- optionMaybe $ NE.fromList <$> P.between Tok.openP Tok.closeP (oqColumnNameP `sepBy1` Tok.commaP)
        _ <- Tok.valuesP
        _ <- Tok.openP
        vals <- NE.fromList <$> defaultExprP `sepBy1` Tok.commaP
        e <- Tok.closeP
        return (cols, Just vals, Just e)

    when ((mergeUpdateDirective, mergeInsertDirectiveValues) == (Nothing, Nothing)) $
        fail "MERGE requires at least one of UPDATE and INSERT"

    let mLastUpdate = fmap (getInfo . snd . NE.last) mergeUpdateDirective
        mLastInsert = r2
        r3 = sconcat $ NE.fromList $ catMaybes [mLastUpdate, mLastInsert]
        mergeInfo = r1 <> r3

    return Merge{..}

  where
    tableAliasP :: Parser (TableAlias Range)
    tableAliasP = do
        (name, r) <- Tok.tableNameP
        makeTableAlias r name

    colValP :: Parser (ColumnRef RawNames Range, DefaultExpr RawNames Range)
    colValP = do
        col <- oqColumnNameP
        _ <- Tok.equalP
        val <- defaultExprP
        return (col { columnNameTable = Nothing }, val)
