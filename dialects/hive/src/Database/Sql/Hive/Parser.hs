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

{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.Sql.Hive.Parser where

import Database.Sql.Type
import Database.Sql.Info
import Database.Sql.Helpers
import Database.Sql.Hive.Type as HT

import Database.Sql.Hive.Scanner
import Database.Sql.Hive.Parser.Internal
import Database.Sql.Position

import qualified Database.Sql.Hive.Parser.Token as Tok

import           Control.Monad (void)
import           Control.Monad.Reader (runReader, local, asks)
import           Data.Char (isDigit)
import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL
import           Data.Set (Set)
import qualified Data.Set as S
import qualified Data.List as L

import Data.Maybe (fromMaybe)
import Data.Monoid (Endo (..))
import qualified Text.Parsec as P
import           Text.Parsec ( chainl1, choice, many
                             , option, optional, optionMaybe
                             , sepBy, sepBy1, try, (<|>), (<?>))


import Data.Semigroup (Semigroup (..), sconcat)
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Foldable (fold)

statementParser :: Parser (HiveStatement RawNames Range)
statementParser = do
    maybeStmt <- optionMaybe $ choice
        [ HiveUseStmt <$> useP
        , HiveAnalyzeStmt <$> analyzeP
        , do
              let options =
                    -- this list is hive-specific statement types that may be
                    -- preceded by an optional `WITH` and an optional inverted
                    -- `FROM`
                    [ (void insertDirectoryPrefixP, fmap HiveInsertDirectoryStmt . insertDirectoryP)
                    ]
                  prefixes = map fst options
                  baseParsers = map snd options
              _ <- try $ P.lookAhead $ optional withP >> invertedFromP >> choice prefixes
              with <- option id withP
              invertedFrom <- invertedFromP
              let parsers = map ($ (with, invertedFrom)) baseParsers
              choice $ parsers
        , try $ HiveTruncatePartitionStmt <$> truncatePartitionStatementP
        , HiveUnhandledStatement <$> describeP
        , HiveUnhandledStatement <$> showP
        , do
              _ <- try $ P.lookAhead createFunctionPrefixP
              HiveUnhandledStatement <$> createFunctionP
        , do
              _ <- try $ P.lookAhead dropFunctionPrefixP
              HiveUnhandledStatement <$> dropFunctionP
        , HiveStandardSqlStatement <$> statementP
        , try $ HiveAlterTableSetLocationStmt <$> alterTableSetLocationP
        , try $ HiveUnhandledStatement <$> alterTableSetTblPropertiesP
        , alterPartitionP
        , HiveSetPropertyStmt <$> setP
        , HiveUnhandledStatement <$> reloadFunctionP
        ]
    case maybeStmt of
        Just stmt -> terminator >> return stmt
        Nothing -> HiveStandardSqlStatement <$> emptyStatementP
  where
    terminator = (Tok.semicolonP <|> eof) -- normal statements may be terminated by `;` or eof
    emptyStatementP = EmptyStmt <$> Tok.semicolonP  -- but we don't allow eof here. `;` is the
    -- only way to write the empty statement, i.e. `` (empty string) is not allowed.


emptyParserScope :: ParserScope
emptyParserScope = ParserScope
    { selectTableAliases = Nothing }

-- | parse consumes a statement, or fails
parse :: Text -> Either P.ParseError (HiveStatement RawNames Range)
parse text = runReader (P.runParserT statementParser 0 "-"  . tokenize $ text) emptyParserScope

-- | parseAll consumes all input as a single statement, or fails
parseAll :: Text -> Either P.ParseError (HiveStatement RawNames Range)
parseAll text = runReader (P.runParserT (statementParser <* P.eof) 0 "-"  . tokenize $ text) emptyParserScope

-- | parseMany consumes multiple statements, or fails
parseMany :: Text -> Either P.ParseError [HiveStatement RawNames Range]
parseMany text = runReader (P.runParserT (P.many1 statementParser) 0 "-"  . tokenize $ text) emptyParserScope

-- | parseManyAll consumes all input multiple statements, or fails
parseManyAll :: Text -> Either P.ParseError [HiveStatement RawNames Range]
parseManyAll text = runReader (P.runParserT (P.many1 statementParser <* P.eof) 0 "-"  . tokenize $ text) emptyParserScope

-- | parseManyEithers consumes all input as multiple (statements or failures)
-- it should never fail
parseManyEithers :: Text -> Either P.ParseError [Either (Unparsed Range) (HiveStatement RawNames Range)]
parseManyEithers text = runReader (P.runParserT parser 0 "-"  . tokenize $ text) emptyParserScope
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

statementP :: Parser (Statement Hive RawNames Range)
statementP = choice
    [ do
          let options =
                -- this list is universal statement types that may be preceded
                -- by an optional `WITH` and an optional inverted `FROM`
                [ (void Tok.insertP, fmap InsertStmt . insertP)
                , (void Tok.selectP, fmap QueryStmt . queryP )
                ]
              prefixes = map fst options
              baseParsers = map snd options
          _ <- try $ P.lookAhead $ optional withP >> invertedFromP >> choice prefixes
          with <- option id withP
          invertedFrom <- invertedFromP
          let parsers = map ($ (with, invertedFrom)) baseParsers
          choice $ parsers
    , InsertStmt <$> loadDataInPathP
    , DeleteStmt <$> deleteP
    , explainP
    , TruncateStmt <$> truncateP
    , do
          _ <- try $ P.lookAhead createSchemaPrefixP
          CreateSchemaStmt <$> createSchemaP
    , do
          _ <- try $ P.lookAhead createViewPrefixP
          CreateViewStmt <$> createViewP
    , CreateTableStmt <$> createTableP
    , DropTableStmt <$> dropTableP
    , do
          _ <- try $ P.lookAhead alterTableRenameTablePrefixP
          AlterTableStmt <$> alterTableRenameTableP
    , do
          _ <- try $ P.lookAhead alterTableRenameColumnPrefixP
          AlterTableStmt <$> alterTableRenameColumnP
    , do
          _ <- try $ P.lookAhead alterTableAddColumnsPrefixP
          AlterTableStmt <$> alterTableAddColumnsP
    , GrantStmt <$> grantP
    , RevokeStmt <$> revokeP
    , CommitStmt <$> Tok.commitP
    , RollbackStmt <$> Tok.rollbackP
    ]

useP :: Parser (Use Range)
useP = do
  r <- Tok.useP

  use <- choice
    [ UseDefault <$> Tok.defaultP
    , UseDatabase . uncurry mkNormalSchema <$> Tok.schemaNameP
    ]

  return $ (r<>) <$> use


analyzeP :: Parser (Analyze RawNames Range)
analyzeP = do
    r <- Tok.analyzeP
    _ <- Tok.tableP
    tn <- tableNameP
    optional $ do
        _ <- Tok.partitionP
        partitionSpecP

    _ <- Tok.computeP
    e <- Tok.statisticsP
    e' <- consumeOrderedOptions e $
            [ do
                  _ <- Tok.forP
                  Tok.columnsP
            , do
                  _ <- Tok.cacheP
                  Tok.metadataP
            , Tok.noScanP
            ]
    return $ Analyze (r<>e') tn


insertDirectoryPrefixP :: Parser (Range, InsertDirectoryLocale Range, Location Range)
insertDirectoryPrefixP = do
    s <- Tok.insertP
    _ <- Tok.overwriteP
    insertDirectoryLocale <- insertDirectoryLocaleP
    insertDirectoryPath <- insertDirectoryPathP

    return (s, insertDirectoryLocale, insertDirectoryPath)

insertDirectoryP :: (QueryPrefix, InvertedFrom) -> Parser (InsertDirectory RawNames Range)
insertDirectoryP (with, farInvertedFrom) = do
    r <- Tok.insertP
    _ <- Tok.overwriteP
    insertDirectoryLocale <- insertDirectoryLocaleP
    insertDirectoryPath <- insertDirectoryPathP

    case farInvertedFrom of
        Just _ -> pure ()
        Nothing -> optional rowFormatP >> optional storedAsP

    insertDirectoryQuery <- case farInvertedFrom of
        Just _ -> querySelectP (with, farInvertedFrom)
        Nothing -> do
                       nearInvertedFrom <- invertedFromP
                       queryP (with, nearInvertedFrom)
    let insertDirectoryInfo = r <> (getInfo insertDirectoryQuery)
    return InsertDirectory{..}
  where
    rowFormatP :: Parser Range
    rowFormatP = do
        s <- Tok.rowP
        _ <- Tok.formatP
        e <- delimitedP
        return $ s <> e

insertDirectoryLocaleP :: Parser (InsertDirectoryLocale Range)
insertDirectoryLocaleP = do
    localToken <- optionMaybe Tok.localP
    let locale = case localToken of
            Just a -> InsertDirectoryLocal a
            Nothing -> InsertDirectoryHDFS
    return locale

insertDirectoryPathP :: Parser (Location Range)
insertDirectoryPathP = do
    r <- Tok.directoryP
    (path, r') <- Tok.stringP
    return $ HDFSPath (r <> r') path

staticPartitionSpecItemP :: Parser (StaticPartitionSpecItem RawNames Range)
staticPartitionSpecItemP = do
    col <- columnNameP
    _ <- Tok.equalP
    val <- constantP
    return $ StaticPartitionSpecItem (getInfo col <> getInfo val) col val

staticPartitionSpecP :: Parser ([StaticPartitionSpecItem RawNames Range], Range)
staticPartitionSpecP = do
    s <- Tok.openP
    items <- staticPartitionSpecItemP `sepBy1` Tok.commaP
    e <- Tok.closeP
    return (items, s <> e)

type PartitionDecider = (Either
                            (StaticPartitionSpecItem RawNames Range)
                            (DynamicPartitionSpecItem RawNames Range))

dynamicPartitionSpecItemP :: Parser (DynamicPartitionSpecItem RawNames Range)
dynamicPartitionSpecItemP = do
    col <- columnNameP
    return $ DynamicPartitionSpecItem (getInfo col) col

partitionSpecDeciderP :: Parser PartitionDecider
partitionSpecDeciderP = do
    item <- choice
        [ do
              sp <- try $ staticPartitionSpecItemP
              return $ Left sp
        , do
              dp <- dynamicPartitionSpecItemP
              return $ Right dp
        ]
    return item

partitionSpecP :: Parser ()
partitionSpecP = do
    -- partitionSpecP currently does not tie down to any specific datatype.
    -- The datatype implementation is being deferred.
    _ <- Tok.openP
    items <- partitionSpecDeciderP `sepBy1` Tok.commaP
    _ <- Tok.closeP
    let dpSpec = L.foldl' specHelper dpSpecBase $ L.reverse items
    case dpSpec of
        Right _ -> return ()
        Left err -> fail err
  where
    specHelper :: (Either String ([StaticPartitionSpecItem RawNames Range],
                                  [DynamicPartitionSpecItem RawNames Range])) ->
                  PartitionDecider ->
                  (Either String ([StaticPartitionSpecItem RawNames Range],
                                  [DynamicPartitionSpecItem RawNames Range]))
    -- Note that specHelper reads partition cols from right to left
    -- This allows for list insertions to output a non-reversed list
    specHelper (Right (spItems, dpItems)) (Left spItem) =
        Right (spItem:spItems, dpItems)

    specHelper (Right (spItems, dpItems)) (Right dpItem) =
        case spItems of
            [] -> Right $ (spItems, dpItem:dpItems)
            _  -> Left  $ "Failed to parse partition \"" ++ show dpItem ++ "\": dynamic partition found preceding static partition"

    specHelper (Left s) _ = Left s

    dpSpecBase :: (Either String ([StaticPartitionSpecItem RawNames Range],
                                  [DynamicPartitionSpecItem RawNames Range]))
    dpSpecBase = Right ([], [])

truncatePartitionStatementP :: Parser (TruncatePartition RawNames Range)
truncatePartitionStatementP = do
    s <- Tok.truncateP
    _ <- Tok.tableP
    table <- tableNameP
    _ <- Tok.partitionP
    (_, e) <- staticPartitionSpecP

    let truncate' = Truncate (s <> getInfo table) table

    return $ TruncatePartition (s <> e) truncate'


describeP :: Parser Range
describeP = do
    s <- Tok.describeP
    e <- P.many1 Tok.notSemicolonP
    return $ s <> last e


showP :: Parser Range
showP = do
    s <- Tok.showP
    e <- P.many1 Tok.notSemicolonP
    return $ s <> last e


createFunctionPrefixP :: Parser Range
createFunctionPrefixP = do
    s <- Tok.createP
    optional Tok.temporaryP
    e <- Tok.functionP
    return $ s <> e

createFunctionP :: Parser Range
createFunctionP = do
   s <- createFunctionPrefixP
   e <- P.many1 Tok.notSemicolonP
   return $ s <> last e


dropFunctionPrefixP :: Parser Range
dropFunctionPrefixP = do
    s <- Tok.dropP
    optional Tok.temporaryP
    e <- Tok.functionP
    return $ s <> e

dropFunctionP :: Parser Range
dropFunctionP = do
   s <- dropFunctionPrefixP
   e <- P.many1 Tok.notSemicolonP
   return $ s <> last e

alterTableSetLocationP :: Parser (AlterTableSetLocation RawNames Range)
alterTableSetLocationP = do
    s <- Tok.alterP
    _ <- Tok.tableP
    table <- tableNameP
    _ <- Tok.setP
    loc <- locationP

    let alterTableSetLocationInfo = s <> getInfo loc
        alterTableSetLocationTable = table
        alterTableSetLocationLocation = loc

    return AlterTableSetLocation{..}

alterTableSetTblPropertiesP :: Parser Range
alterTableSetTblPropertiesP = do
    s <- Tok.alterP
    _ <- Tok.tableP
    _ <- tableNameP
    _ <- Tok.setP
    _ <- Tok.tblPropertiesP
    _ <- Tok.openP
    _ <- (Tok.stringP >> Tok.equalP >> Tok.stringP) `sepBy1` Tok.commaP
    e <- Tok.closeP

    return $ s <> e

alterPartitionP :: Parser (HiveStatement RawNames Range)
alterPartitionP = do
    s <- Tok.alterP
    _ <- Tok.tableP
    tableName <- tableNameP
    choice $
        [ do
            _ <- Tok.partitionP
            (items, _) <- staticPartitionSpecP
            _ <- Tok.setP
            location <- locationP
            pure $ HiveAlterPartitionSetLocationStmt $ AlterPartitionSetLocation (s <> getInfo location) tableName items location
        , HiveUnhandledStatement . (s <>) <$> (addP <|> dropP)
        ]

  where
    addP :: Parser Range
    addP = do
        _ <- Tok.addP
        _ <- ifNotExistsP

        let partitionLocationP = do
              _ <- Tok.partitionP
              (_, e) <- staticPartitionSpecP
              option e (getInfo <$> locationP)
        last <$> P.many1 partitionLocationP

    dropP :: Parser Range
    dropP = do
        _ <- Tok.dropP
        optional $ ifExistsP
        (_, e) <- last <$> P.many1 (Tok.partitionP >> staticPartitionSpecP)
        consumeOrderedOptions e $
            [ Tok.ignoreP >> Tok.protectionP
            , Tok.purgeP
            ]


setP :: Parser (SetProperty Range)
setP = do
    s <- Tok.setP
    option (PrintProperties s "") $ choice $
      [ Tok.minusP >> Tok.keywordP "v" >> pure (PrintProperties s "-v")
      , do
        (name, pe)  <- Tok.propertyNameP
        choice $
          [ do
              _ <- Tok.equalP
              (setConfigValue, e) <- Tok.propertyValuePartP
              let details = SetPropertyDetails (s <> e) name setConfigValue
              pure (SetProperty details)
          , pure (PrintProperties (s <> pe) name)
          ]
      ]

reloadFunctionP :: Parser Range
reloadFunctionP = do
    s <- Tok.reloadP
    e <- Tok.functionP
    return $ s <> e


insertBehaviorHelper :: InsertBehavior Range ->
                        Maybe (TablePartition) ->
                        InsertBehavior Range
insertBehaviorHelper (InsertOverwrite a) (Just partition) = InsertOverwritePartition a partition
insertBehaviorHelper (InsertAppend a) (Just partition) = InsertAppendPartition a partition
insertBehaviorHelper ib _ = ib


insertP :: (QueryPrefix, InvertedFrom) -> Parser (Insert RawNames Range)
insertP (with, farInvertedFrom) = do
    r <- Tok.insertP
    insertBehaviorTok <- choice
        -- T407432 Overhaul Overwrite with Partition support
        [ do
            overwrite <- Tok.overwriteP
            return $ InsertOverwrite overwrite
        , do
            into <- Tok.intoP
            return $ InsertAppend into
        ]

    optional Tok.tableP
    insertTable <- tableNameP

    tablePartition <- optionMaybe $ do
        _ <- Tok.partitionP
        partitionSpecP

    let insertBehavior = insertBehaviorHelper insertBehaviorTok tablePartition

    insertColumns <- optionMaybe $ try $ do
        _ <- Tok.openP
        let oqColumnNameP = (\ (c, r') -> QColumnName r' Nothing c) <$> Tok.columnNameP
        c:cs <- oqColumnNameP `sepBy1` Tok.commaP
        _ <- Tok.closeP
        pure (c :| cs)

    insertValues <- choice
        [ do
            s <- Tok.valuesP
            (e, rows) <- rowsOfValuesP
            pure $ InsertExprValues (s <> e) rows
        , do
            isv <- case farInvertedFrom of
              Just _ -> InsertSelectValues <$> querySelectP (with, farInvertedFrom)
              Nothing -> InsertSelectValues <$> queryP (with, noInversion) -- INSERT .. FROM .. SELECT is not permitted
            pure $ isv
        ]

    let insertInfo = r <> (getInfo insertValues)

    pure Insert{..}
  where
    valueP :: Parser (DefaultExpr RawNames Range)
    valueP = do
        value <- constantP
        let r = getInfo value
        pure $ ExprValue $ ConstantExpr r value

    rowOfValuesP = do
        s <- Tok.openP
        x:xs <- valueP `sepBy1` Tok.commaP
        e <- Tok.closeP
        pure $ (s <> e, x :| xs)

    rowsOfValuesP = do
        rows <- rowOfValuesP `sepBy1` Tok.commaP
        let infos = map fst rows
            r:rs = map snd rows
        pure $ (head infos <> last infos, r :| rs)


loadDataInPathP :: Parser (Insert RawNames Range)
loadDataInPathP = do
    s <- Tok.loadP
    _ <- Tok.dataP
    optional Tok.localP
    _ <- Tok.inPathP
    (path, r) <- Tok.stringP
    maybeOverwrite <- optionMaybe Tok.overwriteP
    into <- Tok.intoP
    _ <- Tok.tableP
    table <- tableNameP
    partitions <- optionMaybe $ do
        _ <- Tok.partitionP
        snd <$> staticPartitionSpecP

    let e = maybe (getInfo table) id partitions
        insertInfo = s <> e
        behaviorTok = case maybeOverwrite of
            Nothing -> InsertAppend into
            Just overwrite -> InsertOverwrite overwrite
        insertBehavior = insertBehaviorHelper behaviorTok (void partitions)
        insertTable = table
        insertColumns = Nothing
        insertValues = InsertDataFromFile r path

    pure Insert{..}

deleteP :: Parser (Delete RawNames Range)
deleteP = do
    r <- Tok.deleteP

    _ <- Tok.fromP
    table <- tableNameP

    maybeExpr <- optionMaybe $ do
        _ <- Tok.whereP
        local (introduceAliases $ tableNameToTableAlias table) exprP

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


type QueryPrefix = Query RawNames Range -> Query RawNames Range

emptyPrefix :: QueryPrefix
emptyPrefix = id

withP :: Parser QueryPrefix
withP = do
    r <- Tok.withP
    withs <- cteP `sepBy1` Tok.commaP

    return $ \ query ->
        let r' = sconcat $ r :| getInfo query : map cteInfo withs
         in QueryWith r' withs query
  where
    cteP = do
        alias <- tableAliasP
        columns <- option []
            $ P.between Tok.openP Tok.closeP $ columnAliasP `sepBy1` Tok.commaP

        _ <- Tok.asP

        (query, r') <- do
            _ <- Tok.openP
            invertedFrom <- invertedFromP
            q <- queryP (emptyPrefix, invertedFrom) -- a query may not have more than 1 WITH clause.
            r' <- Tok.closeP
            return (q, r')

        return $ CTE (getInfo alias <> r') alias columns query

-- parses just a SELECT, consuming no UNIONs
-- i.e. it returns only QuerySelects
querySelectP :: (QueryPrefix, InvertedFrom) -> Parser (Query RawNames Range)
querySelectP (with, invertedFrom) = queryPHelper with invertedFrom False

-- parses SELECTs and UNIONs
-- i.e. it returns QuerySelects and QueryUnions
queryP :: (QueryPrefix, InvertedFrom) -> Parser (Query RawNames Range)
queryP (with, invertedFrom) = queryPHelper with invertedFrom True

queryPHelper :: QueryPrefix -> InvertedFrom -> Bool -> Parser (Query RawNames Range)
queryPHelper with invertedFrom unionsPermitted = do
    -- The invertedFrom, if supplied, will only be applied to the first SELECT.
    -- If invertedFrom is Nothing, then it is assumed the first SELECT has no inversion.
    firstSelect <- onlySelectP invertedFrom

    query <- if unionsPermitted
                 then do
                     maybeUnion <- optionMaybe unionP
                     case maybeUnion of
                         Nothing -> return firstSelect
                         Just union -> do
                             let subsequentSelectP = do
                                     nextInvertedFrom <- invertedFromP
                                     onlySelectP nextInvertedFrom
                             subsequentSelects <- subsequentSelectP `chainl1` unionP
                             return $ union firstSelect subsequentSelects
                 else return firstSelect

    order <- option id orderP
    optional selectClusterP
    limit <- option id limitP

    return $ with $ limit $ order query
  where
    onlySelectP invertedFrom' = do
        select <- selectP invertedFrom'
        return $ QuerySelect (selectInfo select) select

    unionP = do
        r <- Tok.unionP
        distinct <- option (Distinct True) distinctP
        return $ QueryUnion r distinct Unused

    orderP =  do
        (r, orders) <- orderTopLevelP
        return $ \ query -> QueryOrder (getInfo query <> r) orders query

    limitP = do
        r <- Tok.limitP
        Tok.numberP >>= \ (v, r') ->
            let limit = Limit (r <> r') v
             in return $ \ query -> QueryLimit (getInfo query <> r') limit query


distinctP :: Parser Distinct
distinctP = choice $
    [ Tok.allP >> return (Distinct False)
    , Tok.distinctP >> return (Distinct True)
    ]


explainP :: Parser (Statement Hive RawNames Range)
explainP = do
    s <- Tok.explainP
    stmt <- choice
        [ InsertStmt <$> insertP (emptyPrefix, noInversion)
        , DeleteStmt <$> deleteP
        , QueryStmt <$> queryP (emptyPrefix, noInversion)
        ]

    pure $ ExplainStmt (s <> getInfo stmt) stmt


tableAliasP :: Parser (TableAlias Range)
tableAliasP = do
    (name, r) <- Tok.tableNameP
    makeTableAlias r name


columnAliasP :: Parser (ColumnAlias Range)
columnAliasP = do
    (name, r) <- Tok.columnNameP
    makeColumnAlias r name


createSchemaPrefixP :: Parser Range
createSchemaPrefixP = do
    s <- Tok.createP
    e <- Tok.schemaP <|> Tok.databaseP
    return $ s <> e


ifNotExistsP :: Parser (Maybe Range)
ifNotExistsP = optionMaybe $ do
    s' <- Tok.ifP
    _ <- Tok.notP
    e' <- Tok.existsP
    pure $ s' <> e'


commentP :: Parser Range
commentP = do
    s <- Tok.commentP
    (_, e) <- Tok.stringP
    return $ s <> e


locationP :: Parser (Location Range)
locationP = do
    s <- Tok.locationP
    (loc, e) <- Tok.stringP
    return $ HDFSPath (s <> e) loc


createSchemaP :: Parser (CreateSchema RawNames Range)
createSchemaP = do
    s <- createSchemaPrefixP
    createSchemaIfNotExists <- ifNotExistsP

    (name, r) <- Tok.schemaNameP
    let createSchemaName = mkNormalSchema name r

    e <- consumeOrderedOptions r $
            [ commentP
            , getInfo <$> locationP
            , dbPropertiesP
            ]
    let createSchemaInfo = s <> e

    return $ CreateSchema{..}

  where
    dbPropertiesP = do
        s <- Tok.withP
        _ <-Tok.dbPropertiesP
        _ <- Tok.openP
        _ <- propertyP `sepBy1` Tok.commaP
        e <- Tok.closeP
        return $ s <> e


createViewPrefixP :: Parser Range
createViewPrefixP = do
    s <- Tok.createP
    e <- Tok.viewP
    return $ s <> e

createViewP :: Parser (CreateView RawNames Range)
createViewP = do
    s <- createViewPrefixP

    let createViewPersistence = Persistent
    createViewIfNotExists <- ifNotExistsP

    createViewName <- tableNameP

    createViewColumns <- optionMaybe $ do
        _ <- Tok.openP
        c:cs <- flip sepBy1 Tok.commaP $ do
            col <- unqualifiedColumnNameP
            _ <- commentP
            return col
        _ <- Tok.closeP
        return (c:|cs)

    optional commentP
    optional $ do
        _ <- Tok.tblPropertiesP
        _ <- Tok.openP
        _ <- (Tok.stringP >> Tok.equalP >> Tok.stringP) `sepBy1` Tok.commaP
        Tok.closeP

    _ <- Tok.asP
    createViewQuery <- querySelectP (emptyPrefix, noInversion)

    let createViewInfo = s <> getInfo createViewQuery
    pure CreateView{..}


data CreateTablePrefix r a = CreateTablePrefix
    { createTablePrefixInfo :: a
    , createTablePrefixPersistence :: Persistence a
    , createTablePrefixExternality :: Externality a
    , createTablePrefixIfNotExists :: Maybe a
    , createTablePrefixName :: CreateTableName r a
    }

deriving instance ConstrainSNames Eq r a => Eq (CreateTablePrefix r a)
deriving instance ConstrainSNames Show r a => Show (CreateTablePrefix r a)
deriving instance ConstrainSASNames Functor r => Functor (CreateTablePrefix r)
deriving instance ConstrainSASNames Foldable r => Foldable (CreateTablePrefix r)
deriving instance ConstrainSASNames Traversable r => Traversable (CreateTablePrefix r)


createTablePrefixP :: Parser (CreateTablePrefix RawNames Range)
createTablePrefixP = do
    s <- Tok.createP
    createTablePrefixPersistence <- option Persistent $ Temporary <$> Tok.temporaryP
    createTablePrefixExternality <- option Internal (External <$> Tok.externalP)
    _ <- Tok.tableP
    createTablePrefixIfNotExists <- ifNotExistsP
    createTablePrefixName <- tableNameP

    let createTablePrefixInfo = s <> getInfo createTablePrefixName

    return CreateTablePrefix{..}


createTableP :: Parser (CreateTable Hive RawNames Range)
createTableP = choice
    [ do
          _ <- try $ P.lookAhead $ createTablePrefixP >> Tok.likeP
          createTableLikeP
    , createTableStandardP
    ]


createTableLikeP :: Parser (CreateTable Hive RawNames Range)
createTableLikeP = do
    CreateTablePrefix{..} <- createTablePrefixP
    let s = createTablePrefixInfo
        createTablePersistence = createTablePrefixPersistence
        createTableExternality = createTablePrefixExternality
        createTableIfNotExists = createTablePrefixIfNotExists
        createTableName = createTablePrefixName

    _ <- Tok.likeP
    table <- tableNameP

    let e = getInfo table
    e' <- option e $ choice $
        [ getInfo <$> locationP
        , storedAsP
        ]

    let createTableInfo = s <> e'
        createTableDefinition = TableLike (s <> e) table
        createTableExtra = Nothing

    return CreateTable{..}


propertyP :: Parser (HiveMetadataProperty Range)
propertyP = do
    (k, s) <- Tok.stringP
    _ <- Tok.equalP
    (v, e) <- Tok.stringP
    return $ HiveMetadataProperty (s <> e) k v

storedAsP :: Parser Range
storedAsP = do
    s <- Tok.storedP
    _ <- Tok.asP
    e <- choice
        [ Tok.orcP
        , Tok.sequenceFileP
        , Tok.textFileP
        , Tok.rcFileP
        , Tok.parquetP
        , Tok.avroP
        , do
              s' <- Tok.inputFormatP
              _ <- Tok.stringP
              _ <- Tok.outputFormatP
              (_, e') <- Tok.stringP
              return (s' <> e')
        ]
    return $ s <> e


createTableStandardP :: Parser (CreateTable Hive RawNames Range)
createTableStandardP = do
    CreateTablePrefix{..} <- createTablePrefixP
    let s = createTablePrefixInfo
        createTablePersistence = createTablePrefixPersistence
        createTableExternality = createTablePrefixExternality
        createTableIfNotExists = createTablePrefixIfNotExists
        createTableName = createTablePrefixName

    tableDefColumns <- optionMaybe createTableColumnsP
    let e1 = maybe s getInfo tableDefColumns

    e2 <- consumeOrderedOptions e1 $
            [ commentP
            , partitionedByP
            , clusteredByP
            , rowFormatP
            , storedAsP
            , getInfo <$> locationP
            ]

    createTableDefinition <- case tableDefColumns of
        Just definition -> return definition
        Nothing -> choice
            [ createTableAsP
            , createTableNoColumnInfoP e2
            ]

    tblProperties <- option Nothing (Just <$> tblPropertiesP)

    let e3 = getInfo createTableDefinition
        e4 = fromMaybe e2 (hiveMetadataPropertiesInfo <$> tblProperties)
        createTableExtra =
          Just HiveCreateTableExtra
          { hiveCreateTableExtraInfo = e3 <> e4
          , hiveCreateTableExtraTableProperties = tblProperties
          }
        createTableInfo = s <> e1 <> e2 <> e3 <> e4

    pure CreateTable{..}

  where
    columnDefinitionP = do
        (name, s) <- Tok.columnNameP
        columnDefinitionType <- dataTypeP
        optional commentP

        let columnDefinitionInfo = s <> getInfo columnDefinitionType
            columnDefinitionExtra = Nothing  -- TODO
            columnDefinitionNull = Nothing
            columnDefinitionDefault = Nothing
            columnDefinitionName = QColumnName s None name

        pure ColumnDefinition{..}

    partitionedByP = do
        _ <- Tok.partitionedP
        _ <- Tok.byP
        _ <- Tok.openP
        _ <- columnDefinitionP `sepBy1` Tok.commaP
        Tok.closeP

    clusteredByP = do
        _ <- Tok.clusteredP
        _ <- Tok.byP
        _ <- Tok.openP
        _ <- Tok.columnNameP `sepBy1` Tok.commaP
        _ <- Tok.closeP
        optional $ do
            _ <- Tok.sortedP
            _ <- Tok.byP
            _ <- Tok.openP
            _ <- (Tok.columnNameP >> optional directionP) `sepBy1` Tok.commaP
            Tok.closeP
        _ <- Tok.intoP
        _ <- Tok.numberP
        Tok.bucketsP

    serdeP = do
        _ <- Tok.serdeP
        e <- snd <$> Tok.stringP
        option e $ do
            _ <- Tok.withP
            _ <- Tok.serdePropertiesP
            _ <- Tok.openP
            _ <- propertyP `sepBy1` Tok.commaP
            Tok.closeP

    rowFormatP = do
        _ <- Tok.rowP
        _ <- Tok.formatP
        delimitedP <|> serdeP

    createTableColumnsP = do
        s <- Tok.openP
        c:cs <- (ColumnOrConstraintColumn <$> columnDefinitionP) `sepBy1` Tok.commaP
        e <- Tok.closeP
        pure $ TableColumns (s <> e) (c:|cs)

    createTableAsP = do
        s <- Tok.asP
        with <- option id withP
        query <- queryP (with, noInversion)  -- WITH *is* permitted in a CTAS. Inverted FROM is *not*.
        pure $ TableAs (s <> getInfo query) Nothing query

    createTableNoColumnInfoP r =
        -- r represents 'the point at which we decided there was no column info'
        pure $ TableNoColumnInfo r


tblPropertiesP :: Parser (HiveMetadataProperties Range)
tblPropertiesP = do
        s <- Tok.tblPropertiesP
        _ <- Tok.openP
        l <- propertyP `sepBy1` Tok.commaP
        e <- Tok.closeP
        let hiveMetadataPropertiesInfo = s <> e
            hiveMetadataPropertiesProperties = l
        pure $ HiveMetadataProperties{..}


delimitedP :: Parser Range
delimitedP = do
    s <- Tok.delimitedP
    e <- consumeOrderedOptions s $
        [ do
              _ <- Tok.fieldsP
              e' <- terminatedByCharP
              option e' $ do
                  _ <- Tok.escapedP
                  _ <- Tok.byP
                  snd <$> Tok.stringP
        , do
              _ <- Tok.collectionP
              _ <- Tok.itemsP
              terminatedByCharP
        , do
              _ <- Tok.mapP
              _ <- Tok.keysP
              terminatedByCharP
        , do
              _ <- Tok.linesP
              terminatedByCharP
        , do
              _ <- Tok.nullP
              _ <- Tok.definedP
              _ <- Tok.asP
              snd <$> Tok.stringP
          ]
    return $ s <> e
  where
    terminatedByCharP = do
        _ <- Tok.terminatedP
        _ <- Tok.byP
        snd <$> Tok.stringP



ifExistsP :: Parser Range
ifExistsP = do
    s <- Tok.ifP
    e <- Tok.existsP
    pure $ s <> e


dropTableP :: Parser (DropTable RawNames Range)
dropTableP = do
    s <- Tok.dropP
    _ <- Tok.tableP
    dropTableIfExists <- optionMaybe ifExistsP
    dropTableName <- tableNameP
    purge <- optionMaybe Tok.purgeP

    let dropTableInfo = s <> (fromMaybe (getInfo dropTableName) purge)
        dropTableNames = dropTableName :| []
    pure DropTable{..}


alterTableRenameTablePrefixP :: Parser (Range, TableName RawNames Range)
alterTableRenameTablePrefixP = do
    s <- Tok.alterP
    _ <- Tok.tableP
    from <- tableNameP
    _ <- Tok.renameP
    pure $ (s, from)


alterTableRenameTableP :: Parser (AlterTable RawNames Range)
alterTableRenameTableP = do
    (s, from) <- alterTableRenameTablePrefixP
    _ <- Tok.toP
    to <- tableNameP

    pure $ AlterTableRenameTable (s <> getInfo to) from to


alterTableRenameColumnPrefixP :: Parser (Range, TableName RawNames Range)
alterTableRenameColumnPrefixP = do
    s <- Tok.alterP
    _ <- Tok.tableP
    table <- tableNameP
    optional $ Tok.partitionP >> staticPartitionSpecP
    _ <- Tok.changeP
    pure (s, table)

unqualifiedColumnNameP :: Parser (UQColumnName Range)
unqualifiedColumnNameP = do
    (col, r) <- Tok.columnNameP
    pure $ QColumnName r None col

alterTableRenameColumnP :: Parser (AlterTable RawNames Range)
alterTableRenameColumnP = do
    (s, table) <- alterTableRenameColumnPrefixP
    optional Tok.columnP
    from <- unqualifiedColumnNameP
    to <- unqualifiedColumnNameP
    e <- getInfo <$> dataTypeP
    e' <- consumeOrderedOptions e $
              [ commentP
              , choice [ Tok.firstP
                       , Tok.afterP >> snd <$> Tok.columnNameP
                       ]
              , Tok.cascadeP <|> Tok.restrictP
              ]
    pure $ AlterTableRenameColumn (s <> e') table from to

alterTableAddColumnsPrefixP :: Parser (Range, TableName RawNames Range)
alterTableAddColumnsPrefixP = do
    s <- Tok.alterP
    _ <- Tok.tableP
    table <- tableNameP
    -- Note that Hive has ALTER TABLE ADD COLUMN for single partitions
    -- (!). Current behavior is to treat it the same as a top level ALTER TABLE
    -- ADD COLUMN. That seems fine initially, but hard to say if it makes sense
    -- in the long term.
    optional $ Tok.partitionP >> partitionSpecP
    _ <- Tok.addP
    e <- Tok.columnsP
    pure (s <> e, table)

alterTableAddColumnsP :: Parser (AlterTable RawNames Range)
alterTableAddColumnsP = do
    (s, table) <- alterTableAddColumnsPrefixP
    _ <- Tok.openP
    c:cs <- (colP `sepBy1` Tok.commaP)
    e <- Tok.closeP
    e' <- option e (Tok.cascadeP <|> Tok.restrictP)
    pure $ AlterTableAddColumns (s <> e') table (c:|cs)
  where
    colP :: Parser (UQColumnName Range)
    colP = do
        col <- unqualifiedColumnNameP
        _ <- dataTypeP
        _ <- optional commentP
        return col

grantP :: Parser (Grant Range)
grantP = do
    s <- Tok.grantP
    e <- P.many1 Tok.notSemicolonP
    return $ Grant (s <> (last e))

revokeP :: Parser (Revoke Range)
revokeP = do
    s <- Tok.revokeP
    e <- P.many1 Tok.notSemicolonP
    return $ Revoke (s <> (last e))

integerP :: Parser (Int, Range)
integerP = do
    (n, e) <- Tok.numberP
    case reads $ TL.unpack n of
        [(n', "")] -> pure (n', e)
        _ -> fail $ unwords ["unable to parse", show n, "as integer"]


countingSepBy1 :: (Integer -> Parser b) -> Parser c -> Parser [b]
countingSepBy1 f sep = do
    x <- f 0
    xs <- rest 1
    pure (x:xs)
  where
    rest n = choice
        [ do
            _ <- sep
            x <- f n
            xs <- rest (n + 1)
            pure (x:xs)
        , pure []
        ]


introduceAliases :: Set Text -> ParserScope -> ParserScope
introduceAliases aliases = \ scope ->
    let unioned = case selectTableAliases scope of
            Nothing -> aliases
            Just existing -> S.union existing aliases
    in scope { selectTableAliases = Just unioned }

tablishToTableAlias :: Tablish RawNames Range -> Set Text
tablishToTableAlias (TablishTable _ aliases tableName) = case aliases of
    TablishAliasesNone -> tableNameToTableAlias tableName
    TablishAliasesT (TableAlias _ name _) -> S.singleton name
    TablishAliasesTC _ _ -> error "shouldn't happen in hive"
tablishToTableAlias (TablishSubQuery _ aliases _) = case aliases of
    TablishAliasesNone -> error "shouldn't happen in hive"
    TablishAliasesT (TableAlias _ name _) -> S.singleton name
    TablishAliasesTC _ _ -> error "shouldn't happen in hive"
tablishToTableAlias (TablishLateralView _ LateralView{..} _) = case lateralViewAliases of
    TablishAliasesNone -> error "shouldn't happen in hive"
    TablishAliasesT (TableAlias _ name _) -> S.singleton name
    TablishAliasesTC (TableAlias _ name _) _ -> S.singleton name
tablishToTableAlias (TablishJoin _ (JoinSemi _) _ lTablish _) =
    tablishToTableAlias lTablish
tablishToTableAlias (TablishJoin _ _ _ lTablish rTablish) =
    tablishToTableAlias lTablish `S.union` tablishToTableAlias rTablish

tableNameToTableAlias :: OQTableName Range -> Set Text
tableNameToTableAlias (QTableName _ _ name) = S.singleton name


fromP :: Parser (SelectFrom RawNames Range)
fromP = do
    r <- Tok.fromP
    tablishes <- tablishP `sepBy1` Tok.commaP

    let r' = foldl (<>) r $ fmap getInfo tablishes
    return $ SelectFrom r' tablishes

type InvertedFrom = Maybe (SelectFrom RawNames Range)

noInversion :: InvertedFrom
noInversion = Nothing

invertedFromP :: Parser InvertedFrom
invertedFromP = optionMaybe fromP


selectP :: InvertedFrom -> Parser (Select RawNames Range)
selectP invertedFrom = do
    r <- Tok.selectP

    selectDistinct <- option notDistinct distinctP

    aliases <- try $ selectScopeLookAhead invertedFrom

    selectCols <- do
        selections <- local (introduceAliases aliases) $ selectionP `countingSepBy1` Tok.commaP
        let r' = foldl1 (<>) $ map getInfo selections
        return $ SelectColumns r' selections

    selectFrom <- maybe (optionMaybe fromP) (return . Just) invertedFrom
    selectWhere <- optionMaybe $ local (introduceAliases aliases) whereP
    let selectTimeseries = Nothing
    selectGroup <- optionMaybe selectGroupP
    selectHaving <- optionMaybe havingP
    selectNamedWindow <- optionMaybe namedWindowP

    let (Just selectInfo) = sconcat $ Just r :|
            [ Just $ getInfo selectCols
            , getInfo <$> selectFrom
            , getInfo <$> selectWhere
            , getInfo <$> selectGroup
            , getInfo <$> selectHaving
            , getInfo <$> selectNamedWindow
            ]
    return Select{..}

  where
    selectScopeLookAhead :: InvertedFrom -> Parser (Set Text)
    selectScopeLookAhead invertedFrom' = P.lookAhead $ do
        _ <- selectionP (-1) `sepBy1` Tok.commaP
        from <- maybe (optionMaybe fromP) (return . Just) invertedFrom'

        let tablishes = case from of
                Just (SelectFrom _ ts) -> ts
                Nothing -> []
            aliases = L.foldl' S.union S.empty $ map tablishToTableAlias tablishes
        return aliases

    whereP = do
        r <- Tok.whereP
        condition <- exprP
        return $ SelectWhere (r <> getInfo condition) condition

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
          s <- Tok.openP
          window <- choice
              [ do
                  partition <- optionMaybe partitionP
                  order <- option [] orderInWindowClauseP
                  frame <- optionMaybe frameP
                  e <- Tok.closeP
                  let info = s <> e
                  return $ Left $ WindowExpr info partition order frame
              , do
                  inherit <- windowNameP
                  partition <- optionMaybe partitionP
                  order <- option [] orderInWindowClauseP
                  frame <- optionMaybe frameP
                  e <- Tok.closeP
                  let info = s <> e
                  return $ Right $ PartialWindowExpr info inherit partition order frame
              ]

          let infof = (getInfo name <>)
          return $ case window of
            Left w -> NamedWindowExpr (infof $ getInfo w) name w
            Right pw -> NamedPartialWindowExpr (infof $ getInfo pw) name pw
        let info = L.foldl' (<>) r $ fmap getInfo windows
        return $ SelectNamedWindow info windows


handlePositionalReferences :: Expr RawNames Range -> PositionOrExpr RawNames Range
handlePositionalReferences e = case e of
    ConstantExpr _ (NumericConstant _ n) | TL.all isDigit n -> PositionOrExprPosition (getInfo e) (read $ TL.unpack n) Unused
    _ -> PositionOrExprExpr e

selectGroupP :: Parser (SelectGroup RawNames Range)
selectGroupP = do
    r <- Tok.groupP
    _ <- Tok.byP

    rawExprs <- exprP `sepBy1` Tok.commaP
    let exprs = map (toGroupingElement . handlePositionalReferences) rawExprs

    sets <- option [] $ choice
        [ groupingSetsP
        , do
            _ <- try $ P.lookAhead $ Tok.withP >> Tok.cubeP
            cubeP rawExprs
        , do
            _ <- try $ P.lookAhead $ Tok.withP >> Tok.rollupP
            rollupP rawExprs
        ]

    let selectGroupGroupingElements = exprs ++ sets
        selectGroupInfo = foldl (<>) r $ fmap getInfo selectGroupGroupingElements

    return SelectGroup{..}
  where
    toGroupingElement :: PositionOrExpr RawNames Range -> GroupingElement RawNames Range
    toGroupingElement posOrExpr = GroupingElementExpr (getInfo posOrExpr) posOrExpr

    groupingSetP :: Parser (GroupingElement RawNames Range)
    groupingSetP = choice $
        [ do
              s <- Tok.openP
              sets <- exprP `sepBy` Tok.commaP
              e <- Tok.closeP
              return $ GroupingElementSet (s <> e) sets
        , do
              -- if no parens, it will be the singleton list.
              expr <- exprP
              return $ GroupingElementSet (getInfo expr) [expr]
        ]

    groupingSetsP :: Parser [GroupingElement RawNames Range]
    groupingSetsP = do
        _ <- Tok.groupingP
        _ <- Tok.setsP
        _ <- Tok.openP
        sets <- groupingSetP `sepBy1` Tok.commaP
        _ <- Tok.closeP
        return sets

    toGroupingSet :: Range -> [Expr RawNames Range] -> GroupingElement RawNames Range
    toGroupingSet r [] = GroupingElementSet r []
    toGroupingSet _ exprs =
        let s = getInfo $ head exprs
            e = getInfo $ last exprs
         in GroupingElementSet (s <> e) exprs

    cubeP :: [Expr RawNames Range] -> Parser [GroupingElement RawNames Range]
    cubeP exprs = do
        _ <- Tok.withP
        _ <- Tok.cubeP
        let dimensions = L.subsequences exprs
            defaultRange = (getInfo $ head exprs) <> (getInfo $ last exprs)
        return $ map (toGroupingSet defaultRange) dimensions

    rollupP :: [Expr RawNames Range] -> Parser [GroupingElement RawNames Range]
    rollupP exprs = do
        _ <- Tok.withP
        _ <- Tok.rollupP
        let dimensions = L.reverse $ L.inits exprs
            defaultRange = (getInfo $ head exprs) <> (getInfo $ last exprs)
        return $ map (toGroupingSet defaultRange) dimensions


-- | 'selectClusterP' parses for either clusterby or distributeby/sortby
-- T478023 - implement clusterby in select datatype
selectClusterP :: Parser ()
selectClusterP = choice
    [ clusterP
    , distributeSortP
    ]

  where
    clusterP :: Parser ()
    clusterP =
      do
        _ <- Tok.clusterP
        _ <- Tok.byP
        _ <- sepBy1 exprP Tok.commaP
        return ()

    distributeSortP :: Parser ()
    distributeSortP =
      do
        optional distributeP
        optional sortP

    distributeP :: Parser ()
    distributeP =
      do
        _ <- Tok.distributeP
        _ <- Tok.byP
        _ <- sepBy1 exprP Tok.commaP
        return ()

    sortP :: Parser ()
    sortP =
      do
        _ <- Tok.sortP
        _ <- Tok.byP
        _ <- flip sepBy1 Tok.commaP $ do
          expr <- exprP
          direction <- option (OrderAsc Nothing) $ choice
              [ OrderAsc . Just <$> Tok.ascP
              , OrderDesc . Just <$> Tok.descP
              ]
          return (expr, direction)
        return ()


qualifiedTableNameP :: Parser (Text, Text, Range, Range)
qualifiedTableNameP = do
    (s, r) <- Tok.schemaNameP
    _ <- Tok.dotP
    (t, r') <- Tok.tableNameP

    return (s, t, r, r')


-- | The columnName parser has been overhauled with checks for table names. If
-- a scope is present (i.e. in a select statement), the table name must be
-- a member of the tableAlias list for the parser to succeed. Otherwise,
-- the table parser fails and execution tries the next parser choice.
--
-- Should the scope not be set, e.g. when selectP is performing lookahead
-- to build scope, this check is skipped.
checkTableNameInScopeP :: Text -> Parser ()
checkTableNameInScopeP name = do
    maybeScope <- asks selectTableAliases
    case maybeScope of
        Just scope ->
            case L.find (==name) scope of
                Just _  -> return ()
                Nothing -> fail $ "Table " ++ (show name) ++
                    " doesn't exist in table scope " ++ show maybeScope
        Nothing -> return ()


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


tableNameP :: Parser (OQTableName Range)
tableNameP = choice
    [ try $ do
        (s, t, r, r') <- qualifiedTableNameP
        return $ QTableName r' (Just $ mkNormalSchema s r) t

    , do
        (t, r) <- Tok.tableNameP
        return $ QTableName r Nothing t
    ]


arrayAccessP :: Parser (Expr RawNames Range -> Expr RawNames Range)
arrayAccessP = do
    _ <- Tok.openBracketP
    index <- exprP
    e <- Tok.closeBracketP
    return $ \ expr ->
        let exprR = getInfo expr <> e
        in ArrayAccessExpr exprR expr index


structFieldNameP :: Parser (StructFieldName Range)
structFieldNameP = do
    (t, r) <- Tok.structFieldNameP
    return $ StructFieldName r t


structAccessP :: Parser (Expr RawNames Range -> Expr RawNames Range)
structAccessP = do
    _ <- Tok.dotP
    field <- structFieldNameP
    return $ \ struct ->
        let r = getInfo struct <> getInfo field
         in FieldAccessExpr r struct field


columnNameP :: Parser (OQColumnName Range)
columnNameP = choice
    -- Note that in hive, column names cannot lead with schema qualifiers
    [ try $ do
        (t, r) <- Tok.tableNameP
        _ <- Tok.dotP
        (c, r') <- Tok.columnNameP

        _ <- checkTableNameInScopeP t

        return $ QColumnName r' (Just $ QTableName r Nothing t) c

    , do
        (c, r) <- Tok.columnNameP

        return $ QColumnName r Nothing c
    ]


selectionP :: Integer -> Parser (Selection RawNames Range)
selectionP idx = try selectStarP <|> do
    expr <- exprP
    aliases <- aliasesP expr idx
    let info = foldr (<>) (getInfo expr) (map getInfo aliases)

    return $ SelectExpr info aliases expr
  where
    aliasesP :: Expr RawNames Range -> Integer -> Parser [ColumnAlias Range]
    aliasesP expr idx' = choice
        [ try $ do
            optional Tok.asP
            (name, r) <- Tok.columnNameP -- SELECT 1 as foo FROM bar;
            pure <$> makeColumnAlias r name

        , try $ do
            _ <- Tok.asP
            P.between Tok.openP Tok.closeP $ flip sepBy1 Tok.commaP $ do
                (name, r) <- Tok.columnNameP -- SELECT <expr> as (foo1, foo2) FROM bar;
                makeColumnAlias r name

        , do
            r <- Tok.asP
            pure <$> makeColumnAlias r "as" -- SELECT 1 as FROM bar;  i.e. `as` is the alias!

        , pure <$> makeExprAlias expr idx'
        ]


makeColumnAlias :: Range -> Text -> Parser (ColumnAlias Range)
makeColumnAlias r alias = ColumnAlias r alias . ColumnAliasId <$> getNextCounter

makeTableAlias :: Range -> Text -> Parser (TableAlias Range)
makeTableAlias r alias = TableAlias r alias . TableAliasId <$> getNextCounter

makeDummyAlias :: Range -> Integer -> Parser (ColumnAlias Range)
makeDummyAlias r idx = makeColumnAlias r $ TL.pack $ "_c" ++ show idx

makeExprAlias :: Expr RawNames Range -> Integer -> Parser (ColumnAlias Range)
makeExprAlias (ColumnExpr info (QColumnName _ _ name)) _ = makeColumnAlias info name
makeExprAlias expr idx = makeDummyAlias (getInfo expr) idx


exprP :: Parser (Expr RawNames Range)
exprP = orExprP

parenExprP :: Parser (Expr RawNames Range)
parenExprP = P.between Tok.openP Tok.closeP exprP

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
                condition <- BinOpExpr whenr "=" expr <$> exprP
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
        t <- dataTypeP
        r' <- Tok.closeP

        return $ TypeCastExpr (r <> r') CastFailureError e t

    dateDiffFuncP = do
        r <- Tok.dateDiffP
        _ <- Tok.openP

        date1 <- exprP
        _ <- Tok.commaP
        date2 <- exprP
        r' <- Tok.closeP

        return $ FunctionExpr (r <> r') (QFunctionName r Nothing "datediff") notDistinct [date1, date2] [] Nothing Nothing

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

                    _ -> fail "not count, can't star"

                , do
                    isDistinct <- distinctP
                    (isDistinct,) . (:[]) <$> exprP

                , (notDistinct,) <$> exprP `sepBy` Tok.commaP
                ]

            optional $ Tok.ignoreP >> Tok.nullsP

            r' <- Tok.closeP
            return (distinct, arguments, [], r')

        over <- optionMaybe $ try $ overP

        let r'' = maybe r' getInfo over <> getInfo name

        return $ FunctionExpr r'' name distinct arguments parameters Nothing over

    bareFuncP = do
        (v, r) <- choice
            [ Tok.currentDatabaseP
            , Tok.currentSchemaP
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
        let info = (getInfo expr) ?<> (getInfo dir) <> (getInfo nulls)
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
    partition <- optionMaybe partitionP
    order <- option [] orderInWindowClauseP
    frame <- optionMaybe frameP
    end <- Tok.closeP
    let info = start <> end
    return (PartialWindowExpr info inherit partition order frame)

windowNameP :: Parser (WindowName Range)
windowNameP =
  do
    (name, r) <- Tok.windowNameP
    return $ WindowName r name

partitionP :: Parser (Partition RawNames Range)
partitionP = do
    r <- Tok.partitionP
    choice
        [ do
              _ <- Tok.byP
              exprs <- optionalParensP $ exprP `sepBy1` Tok.commaP
              return $ PartitionBy (sconcat $ r :| map getInfo exprs) exprs

        , Tok.bestP >>= \ r' -> return $ PartitionBest (r <> r')
        , Tok.nodesP >>= \ r' -> return $ PartitionNodes (r <> r')
        ]


dataTypeP :: Parser (DataType Range)
dataTypeP = choice
    [ arrayTypeP
    , mapTypeP
    , structTypeP
    , unionTypeP
    , primitiveTypeP
    ]
  where
    primitiveTypeP = do
        (name, r) <- Tok.typeNameP
        args <- option [] $ P.between Tok.openP Tok.closeP $ constantP `sepBy1` Tok.commaP
        return $ PrimitiveDataType r name $ map DataTypeParamConstant args

    arrayTypeP = do
        s <- Tok.arrayP
        _ <- Tok.openAngleP
        itemType <- dataTypeP
        e <- Tok.closeAngleP
        return $ ArrayDataType (s <> e) itemType

    mapTypeP = do
        s <- Tok.mapP
        _ <- Tok.openAngleP
        keyType <- primitiveTypeP
        _ <- Tok.commaP
        valueType <- dataTypeP
        e <- Tok.closeAngleP
        return $ MapDataType (s <> e) keyType valueType

    structTypeP = do
        s <- Tok.structP
        _ <- Tok.openAngleP
        let fieldP = do
              (name, _) <- Tok.structFieldNameP
              _ <- Tok.colonP
              type_ <- dataTypeP
              optional commentP
              return (name, type_)
        fields <- fieldP `sepBy1` Tok.commaP
        e <- Tok.closeAngleP
        return $ StructDataType (s <> e) fields

    unionTypeP = do
        s <- Tok.uniontypeP
        _ <- Tok.openAngleP
        types <- dataTypeP `sepBy1` Tok.commaP
        e <- Tok.closeAngleP
        return $ UnionDataType (s <> e) types


existsExprP :: Parser (Expr RawNames Range)
existsExprP = do
    r <- Tok.existsP
    _ <- Tok.openP
    query <- querySelectP (emptyPrefix, noInversion)
    r' <- Tok.closeP

    return $ ExistsExpr (r <> r') query


columnExprP :: Parser (Expr RawNames Range)
columnExprP = do
    col <- columnNameP
    return $ ColumnExpr (getInfo col) col


variableSubstitutionP :: Parser (Expr RawNames Range)
variableSubstitutionP = do
    info <- Tok.variableSubstitutionP
    return $ VariableSubstitutionExpr info


exprWithArrayOrStructAccessP :: Parser (Expr RawNames Range)
exprWithArrayOrStructAccessP = foldl (flip ($)) <$> baseP <*> many (structAccessP <|> arrayAccessP)
  where
    baseP :: Parser (Expr RawNames Range)
    baseP = choice
        [ try parenExprP
        , try existsExprP
        , try functionExprP
        , caseExprP
        , try $ do
            constant <- constantP
            return $ ConstantExpr (getInfo constant) constant

        , columnExprP
        , variableSubstitutionP
        ]


unOpP :: Text -> Parser (Expr RawNames Range -> Expr RawNames Range)
unOpP op = do
    r <- Tok.symbolP op
    return $ \ expr -> UnOpExpr (r <> getInfo expr) (Operator op) expr


unaryPrefixExprP :: Parser (Expr RawNames Range)
unaryPrefixExprP = do
    prefix <- option id $ choice $ map unOpP [ "+", "-", "~" ]
    expr <- exprWithArrayOrStructAccessP
    return $ prefix expr


notOperatorP :: Parser (Expr RawNames Range -> Expr RawNames Range)
notOperatorP = (\ r -> UnOpExpr r "NOT") <$> Tok.notOperatorP

unarySuffixExprP :: Parser (Expr RawNames Range)
unarySuffixExprP = do
    expr <- unaryPrefixExprP
    is <- option id $ do
            _ <- Tok.isP
            not_ <- option id notOperatorP
            (not_ .) <$> (Tok.nullP >>= \ r -> return (UnOpExpr r "ISNULL"))

    return $ is expr


binOpP :: Text -> Parser (Expr RawNames Range -> Expr RawNames Range -> Expr RawNames Range)
binOpP op = do
    r <- Tok.symbolP op

    let r' lhs rhs = sconcat $ r :| map getInfo [lhs, rhs]
    return $ \ lhs rhs -> BinOpExpr (r' lhs rhs) (Operator op) lhs rhs


bitwiseXorExprP :: Parser (Expr RawNames Range)
bitwiseXorExprP = unarySuffixExprP `chainl1` binOpP "^"


productExprP :: Parser (Expr RawNames Range)
productExprP = bitwiseXorExprP `chainl1` opP
  where
    opP = choice $ map binOpP [ "*", "/", "%" ]


sumExprP :: Parser (Expr RawNames Range)
sumExprP = productExprP `chainl1` opP
  where
    opP = choice $ map binOpP [ "+", "-" ]


stringExprP :: Parser (Expr RawNames Range)
stringExprP = sumExprP `chainl1` opP
  where
    opP = choice $ map binOpP [ "||" ]


bitwiseAndExprP :: Parser (Expr RawNames Range)
bitwiseAndExprP = stringExprP `chainl1` binOpP "&"


bitwiseOrExprP :: Parser (Expr RawNames Range)
bitwiseOrExprP = bitwiseAndExprP `chainl1` binOpP "|"


inExprP :: Parser (Expr RawNames Range)
inExprP = do
    expr <- bitwiseOrExprP
    not_ <- option id notOperatorP
    in_ <- foldl (.) id <$> many inP

    return $ not_ $ in_ expr

  where
    inP = do
        _ <- Tok.inP
        _ <- Tok.openP
        list <- choice
            [ Left <$> queryP (emptyPrefix, noInversion)
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
        start <- sumExprP
        _ <- Tok.andP
        end <- sumExprP

        let r expr = getInfo expr <> getInfo end
        return $ \ expr -> BetweenExpr (r expr) start end expr


likeExprP :: Parser (Expr RawNames Range)
likeExprP = do
    expr <- betweenExprP
    like <- option id comparisonP
    return $ like expr
  where
    comparisonP :: Parser (Expr RawNames Range -> Expr RawNames Range)
    comparisonP = do
      comparison <- textComparisonP
      pattern <- Pattern <$> betweenExprP
      return $ comparison Nothing pattern

    textComparisonP :: Parser (Maybe (Escape RawNames Range) -> Pattern RawNames Range -> Expr RawNames Range -> Expr RawNames Range)
    textComparisonP = do
        not_ <- option id notOperatorP

        like <- choice
            [ Tok.likeP   >>= \ r -> return $ LikeExpr r "LIKE"
            , Tok.rlikeP  >>= \ r -> return $ LikeExpr r "RLIKE"
            , Tok.regexpP >>= \ r -> return $ LikeExpr r "REGEXP"
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
    not_ <- option id notOperatorP
    expr <- equalityExprP
    return $ not_ expr


andExprP :: Parser (Expr RawNames Range)
andExprP = notExprP `chainl1`
    (Tok.andP >>= \ r -> return $ BinOpExpr r "AND")


orExprP :: Parser (Expr RawNames Range)
orExprP = andExprP `chainl1` (Tok.orP >>= \ r -> return (BinOpExpr r "OR"))


singleTableP :: Parser (Tablish RawNames Range)
singleTableP = try subqueryP <|> try tableP
  where
    subqueryP = do
        r <- Tok.openP
        invertedFrom <- invertedFromP
        query <- queryP (emptyPrefix, invertedFrom)
        _ <- Tok.closeP
        maybe_alias <- aliasP
        case maybe_alias of
            Nothing -> fail $ "in hive, tablish subquery must have alias"
            Just alias -> return $ TablishSubQuery (r <> getInfo alias) (TablishAliasesT alias) query

    tableP = do
        name <- tableNameP
        _ <- optional tableSampleP
        maybe_alias <- aliasP
        let r = case maybe_alias of
                Nothing -> getInfo name
                Just alias -> getInfo alias <> getInfo name
            aliases = maybe TablishAliasesNone TablishAliasesT maybe_alias

        return $ TablishTable r aliases name

    -- This is slightly complicated because `full` is permitted as a table
    -- alias, but `full` is also a reserved word that may follow a table alias.
    -- Strategy is lookahead... if the next tokens are `full` and `outer` and
    -- `join`, we definitely don't have an alias so return Nothing. Otherwise,
    -- there may or may not be an alias.
    aliasP :: Parser (Maybe (TableAlias Range))
    aliasP = choice
        [ do
              _ <- try $ P.lookAhead $ Tok.fullP >> optional Tok.outerP >> Tok.joinP
              return Nothing
        , optionMaybe $ (optional Tok.asP) >> tableAliasP
        ]

    tableSampleP :: Parser Range
    tableSampleP = do
        s <- Tok.tableSampleP
        _ <- Tok.openP
        _ <- choice $
            [ do
                s' <- Tok.bucketP
                _ <- Tok.numberP
                _ <- Tok.outP
                _ <- Tok.ofP
                _ <- Tok.numberP
                option s' $ do
                    _ <- Tok.onP
                    choice $ [ try $ Tok.randP >> Tok.openP >> Tok.closeP
                             , snd <$> Tok.columnNameP
                             ]
            , Tok.numberP >> (Tok.percentP <|> Tok.rowsP)
            , snd <$> Tok.byteAmountP
            ]
        e <- Tok.closeP
        return $ s <> e

singleTableWithViewsP :: Parser (Tablish RawNames Range)
singleTableWithViewsP = do
    table <- singleTableP
    views <- fmap (appEndo . fold . reverse) $ many $ Endo <$> lateralViewP
    return $ views table


-- see https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView
lateralViewP :: Parser (Tablish RawNames Range -> Tablish RawNames Range)
lateralViewP = do
    s <- Tok.lateralP
    _ <- Tok.viewP
    lateralViewOuter <- optionMaybe Tok.outerP
    lateralViewExprs <- (:[]) <$> functionExprP
    let lateralViewWithOrdinality = False -- it's not a thing in Hive

    tAlias <- tableAliasP
    cAliases <- optionMaybe $ do
        _ <- Tok.asP
        columnAliasP `sepBy1` Tok.commaP
    let lateralViewAliases = case cAliases of
            Nothing -> TablishAliasesT tAlias
            Just cAliases' -> TablishAliasesTC tAlias cAliases'

        e = getInfo tAlias
        es = maybe [] (map getInfo) cAliases
        lateralViewInfo = s <> sconcat (e:|es)

    pure $ \ lhs -> TablishLateralView (getInfo lhs <> lateralViewInfo) LateralView{..} (Just lhs)


optionalParensP :: Parser a -> Parser a
optionalParensP p = try p <|> P.between Tok.openP Tok.closeP p

manyParensP :: Parser a -> Parser a
manyParensP p = try p <|> P.between Tok.openP Tok.closeP (manyParensP p)

tablishP :: Parser (Tablish RawNames Range)
tablishP = do
    table <- singleTableWithViewsP
    joins <- fmap (appEndo . fold . reverse) $ many $ Endo <$> joinP
    return $ joins table

joinP :: Parser (Tablish RawNames Range -> Tablish RawNames Range)
joinP = do
    maybeJoinType <- optionMaybe $ innerJoinTypeP <|> crossJoinTypeP <|> try semiJoinTypeP <|> outerJoinTypeP
    joinType <- Tok.joinP >>= \ r -> return $ case maybeJoinType of
        Nothing -> JoinInner r
        Just joinType -> (<> r) <$> joinType

    rhs <- singleTableWithViewsP
    maybeCondition <- optionMaybe $ do
        _ <- Tok.onP <?> "condition in join clause"
        JoinOn <$> exprP

    let condition = case maybeCondition of
            Nothing -> let info = getInfo joinType <> getInfo rhs
                        in JoinOn $ ConstantExpr info $ BooleanConstant info True
            Just c -> c
        joinType' = case (joinType, maybeCondition) of
            -- omitting the ON clause turns a LEFT SEMI JOIN into an INNER JOIN
            (JoinSemi r, Nothing) -> JoinInner r
            _ -> joinType

    let r lhs = getInfo lhs <> getInfo rhs <> getInfo condition
    return $ \ lhs ->
        TablishJoin (r lhs) joinType' condition lhs rhs

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

crossJoinTypeP :: Parser (JoinType Range)
crossJoinTypeP = Tok.crossP >>= \ r -> return $ JoinInner r

semiJoinTypeP :: Parser (JoinType Range)
semiJoinTypeP = do
  r <- Tok.leftP
  r' <- Tok.semiP
  return $ JoinSemi (r <> r')

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
    ]
