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

module Database.Sql.Presto.Parser where

import Database.Sql.Type
import Database.Sql.Position
import Database.Sql.Helpers
import Database.Sql.Info
import Database.Sql.Presto.Type
import Database.Sql.Presto.Scanner
import Database.Sql.Presto.Parser.Internal

import qualified Database.Sql.Presto.Parser.Token as Tok

import qualified Text.Parsec as P
import           Text.Parsec ( chainl1, choice, many
                             , option, optional, optionMaybe
                             , sepBy, sepBy1, try, (<|>), (<?>))

import           Data.Char (isDigit)
import           Data.Foldable (fold)
import qualified Data.List as L
import           Data.List.NonEmpty (NonEmpty ((:|)))
import           Data.Maybe (catMaybes, fromMaybe)
import           Data.Monoid (Endo (..))
import           Data.Semigroup (Semigroup (..), sconcat)
import           Data.Set (Set)
import qualified Data.Set as S
import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL

import Control.Arrow (first)
import Control.Monad (when)
import Control.Monad.Reader (runReader, local, asks)


statementParser :: Parser (PrestoStatement RawNames Range)
statementParser = do
    maybeStmt <- optionMaybe $ choice
        [ try $ PrestoStandardSqlStatement <$> statementP
        , PrestoUnhandledStatement <$> explainP
        , PrestoUnhandledStatement <$> showP
        , PrestoUnhandledStatement <$> callP
        , PrestoUnhandledStatement <$> describeP
        , PrestoUnhandledStatement <$> setP
        ]
    case maybeStmt of
        Just stmt -> terminator >> return stmt
        Nothing -> PrestoStandardSqlStatement <$> emptyStatementP
  where
    terminator = (Tok.semicolonP <|> eof) -- normal statements may be terminated by `;` or eof
    emptyStatementP = EmptyStmt <$> Tok.semicolonP  -- but we don't allow eof here. `;` is the
    -- only way to write the empty statement, i.e. `` (empty string) is not allowed.

emptyParserScope :: ParserScope
emptyParserScope = ParserScope
    { selectTableAliases = Nothing }

-- | parse consumes a statement, or fails
parse :: Text -> Either P.ParseError (PrestoStatement RawNames Range)
parse text = runReader (P.runParserT statementParser 0 "-"  . tokenize $ text) emptyParserScope

-- | parseAll consumes all input as a single statement, or fails
parseAll :: Text -> Either P.ParseError (PrestoStatement RawNames Range)
parseAll text = runReader (P.runParserT (statementParser <* P.eof) 0 "-"  . tokenize $ text) emptyParserScope

-- | parseMany consumes multiple statements, or fails
parseMany :: Text -> Either P.ParseError [PrestoStatement RawNames Range]
parseMany text = runReader (P.runParserT (P.many1 statementParser) 0 "-"  . tokenize $ text) emptyParserScope

-- | parseManyAll consumes all input multiple statements, or fails
parseManyAll :: Text -> Either P.ParseError [PrestoStatement RawNames Range]
parseManyAll text = runReader (P.runParserT (P.many1 statementParser <* P.eof) 0 "-"  . tokenize $ text) emptyParserScope

-- | parseManyEithers consumes all input as multiple (statements or failures)
-- it should never fail
parseManyEithers :: Text -> Either P.ParseError [Either (Unparsed Range) (PrestoStatement RawNames Range)]
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


statementP :: Parser (Statement Presto RawNames Range)
statementP = choice
    [ QueryStmt <$> queryP
    , DeleteStmt <$> deleteP
    , do
          _ <- try $ P.lookAhead dropViewPrefixP
          DropViewStmt <$> dropViewP
    , DropTableStmt <$> dropTableP
    , GrantStmt <$> grantP
    , RevokeStmt <$> revokeP
    , InsertStmt <$> insertP
    , CreateTableStmt <$> createTableP
    ]

queryP :: Parser (Query RawNames Range)
queryP = do
    with <- option id withP
    queryNoWith <- queryNoWithP
    return $ with queryNoWith
  where
    withP :: Parser (Query RawNames Range -> Query RawNames Range)
    withP = do
        r <- Tok.withP
        withs <- cteP `sepBy1` Tok.commaP

        return $ \ query ->
            let r' = sconcat $ r :| getInfo query : map cteInfo withs
             in QueryWith r' withs query

    cteP :: Parser (CTE RawNames Range)
    cteP = do
        alias <- tableAliasP
        columns <- option [] $ P.between Tok.openP Tok.closeP $ columnAliasP `sepBy1` Tok.commaP
        _ <- Tok.asP
        _ <- Tok.openP
        query <- queryP
        r' <- Tok.closeP

        return $ CTE (getInfo alias <> r') alias columns query

queryNoWithP :: Parser (Query RawNames Range)
queryNoWithP = do
    queryTerm <- queryTermP
    order <- option id (orderWrapperP queryTerm)
    limit <- option id limitP
    return $ limit $ order queryTerm
  where
    queryTermP = (queryPrimaryP `chainl1` (exceptP <|> unionP))
                `chainl1` intersectP

    queryPrimaryP = choice
        [ querySelectP
        -- TODO table
        -- TODO values
        , P.between Tok.openP Tok.closeP queryNoWithP
        ]

    exceptP = do
        r <- Tok.exceptP
        optional Tok.distinctP
        return $ QueryExcept r Unused

    unionP = do
        r <- Tok.unionP
        distinct <- option (Distinct True) distinctP
        return $ QueryUnion r distinct Unused

    intersectP = do
        r <- Tok.intersectP
        optional Tok.distinctP
        return $ QueryIntersect r Unused


    orderWrapperP query = do
        let aliases = aliasesForOrders query
        (r, orders) <- local (introduceAliases aliases) orderTopLevelP
        return $ \ query' -> QueryOrder (getInfo query' <> r) orders query'

    aliasesForOrders query = case query of
        QuerySelect _ s -> tableAliases $ selectFrom s
        _ -> S.empty

    limitP = do
        r <- Tok.limitP
        choice
            [ do
                  (NumericConstant r' num) <- numberConstantP
                  let limit = Limit (r <> r') num
                  return $ \ query -> QueryLimit (getInfo query <> r') limit query

            , Tok.allP >> return id
            ]

querySelectP :: Parser (Query RawNames Range)
querySelectP = do
    select <- selectP
    return $ QuerySelect (selectInfo select) select

selectP :: Parser (Select RawNames Range)
selectP = do
    r <- Tok.selectP

    selectDistinct <- option notDistinct distinctP

    aliases <- try selectScopeLookAhead

    selectCols <- do
        selections <- local (introduceAliases aliases) $ selectionP `countingSepBy1` Tok.commaP
        let r' = foldl1 (<>) $ map getInfo selections
        return $ SelectColumns r' selections

    selectFrom <- optionMaybe fromP
    selectWhere <- optionMaybe $ local (introduceAliases aliases) whereP
    let selectTimeseries = Nothing
    selectGroup <- optionMaybe $ local (introduceAliases aliases) groupP
    selectHaving <- optionMaybe $ local (introduceAliases aliases) havingP
    selectNamedWindow <- optionMaybe $ local (introduceAliases aliases) namedWindowP
    let Just selectInfo = sconcat $ Just r :|
            [ Just $ getInfo selectCols
            , getInfo <$> selectFrom
            , getInfo <$> selectWhere
            , getInfo <$> selectGroup
            , getInfo <$> selectHaving
            , getInfo <$> selectNamedWindow
            ]

    pure Select{..}

  where
    selectScopeLookAhead :: Parser (Set Text)
    selectScopeLookAhead = P.lookAhead $ do
        _ <- selectionP (-1) `sepBy1` Tok.commaP
        from <- optionMaybe fromP
        return $ tableAliases from

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


distinctP :: Parser Distinct
distinctP = choice $
    [ Tok.allP >> return (Distinct False)
    , Tok.distinctP >> return (Distinct True)
    ]


tableAliases :: Maybe (SelectFrom RawNames Range) -> Set Text
tableAliases from =
    let tablishes = case from of
            Just (SelectFrom _ ts) -> ts
            Nothing -> []
      in L.foldl' S.union S.empty $ map tablishToTableAlias tablishes
  where
    tablishToTableAlias :: Tablish RawNames Range -> Set Text
    tablishToTableAlias (TablishTable _ aliases tableName) = case aliases of
        TablishAliasesNone -> tableNameToTableAlias tableName
        TablishAliasesT (TableAlias _ name _) -> S.singleton name
        TablishAliasesTC (TableAlias _ name _) _ -> S.singleton name
    tablishToTableAlias (TablishSubQuery _ aliases _) = case aliases of
        TablishAliasesNone -> S.empty
        TablishAliasesT (TableAlias _ name _) -> S.singleton name
        TablishAliasesTC (TableAlias _ name _) _ -> S.singleton name
    tablishToTableAlias (TablishParenthesizedRelation _ aliases _) = case aliases of
        TablishAliasesNone -> S.empty
        TablishAliasesT (TableAlias _ name _) -> S.singleton name
        TablishAliasesTC (TableAlias _ name _) _ -> S.singleton name
    tablishToTableAlias (TablishLateralView _ LateralView{..} _) = case lateralViewAliases of
        TablishAliasesNone -> S.empty
        TablishAliasesT (TableAlias _ name _) -> S.singleton name
        TablishAliasesTC (TableAlias _ name _) _ -> S.singleton name
    tablishToTableAlias (TablishJoin _ (JoinSemi _) _ _ _) =
        error "this shouldn't happen: no SEMI JOIN in Presto"
    tablishToTableAlias (TablishJoin _ _ _ lTablish rTablish) =
        tablishToTableAlias lTablish `S.union` tablishToTableAlias rTablish

tableNameToTableAlias :: OQTableName Range -> Set Text
tableNameToTableAlias (QTableName _ Nothing tname) = S.singleton tname
tableNameToTableAlias (QTableName _ (Just (QSchemaName _ _ _ SessionSchema)) _) =
    error "this shouldn't happen: no SessionSchema in Presto"
tableNameToTableAlias (QTableName _ (Just (QSchemaName _ Nothing sname _)) tname) =
    S.fromList [ tname
               , sname <> "." <> tname
               ]
tableNameToTableAlias (QTableName _ (Just (QSchemaName _ (Just (DatabaseName _ dname)) sname _)) tname) =
    S.fromList [ tname
               , sname <> "." <> tname
               , dname <> "." <> sname <> "." <> tname
               ]

introduceAliases :: Set Text -> ParserScope -> ParserScope
introduceAliases aliases = \ scope ->
    let unioned = case selectTableAliases scope of
            Nothing -> aliases
            Just existing -> S.union existing aliases
    in scope { selectTableAliases = Just unioned }


fromP :: Parser (SelectFrom RawNames Range)
fromP = do
    r <- Tok.fromP
    relations <- relationP `sepBy1` Tok.commaP

    let r' = foldl (<>) r $ fmap getInfo relations
    return $ SelectFrom r' relations

relationP :: Parser (Tablish RawNames Range)
relationP = do
    table <- sampledRelationP
    joins <- fmap (appEndo . fold . reverse) $ many $ Endo <$> joinP
    return $ joins table

sampledRelationP :: Parser (Tablish RawNames Range)
sampledRelationP = do
    table <- aliasedRelationP
    _ <- optional tableSampleP
    return table
  where
    tableSampleP :: Parser Range
    tableSampleP = do
        s <- Tok.tableSampleP
        _ <- sampleTypeP
        _ <- Tok.openP
        _ <- numberExprP -- T655130
        e <- Tok.closeP
        return $ s <> e

    sampleTypeP :: Parser Range
    sampleTypeP = choice [ Tok.bernoulliP, Tok.systemP, Tok.poissonizedP ]

    aliasedRelationP :: Parser (Tablish RawNames Range)
    aliasedRelationP = do
        let placeholder = error "placeholder aliases never got replaced!"
        t <- choice $
            [ do
                  name <- tableNameP
                  return $ TablishTable (getInfo name) placeholder name

            , do
                  r1 <- Tok.unnestP
                  _ <- Tok.openP
                  let lateralViewOuter = Nothing -- not an option in Presto
                  args <- exprP `sepBy1` Tok.commaP
                  r2 <- Tok.closeP
                  let lateralViewExprs = [FunctionExpr (r1 <> r2) (QFunctionName r1 Nothing "unnest") notDistinct args [] Nothing Nothing]
                  (lateralViewWithOrdinality, r3) <- option (False, r2) $ do
                      _ <- Tok.withP
                      (True, ) <$> Tok.ordinalityP

                  let lateralViewInfo = r1 <> r3
                      lateralViewAliases = placeholder
                  return $ TablishLateralView lateralViewInfo LateralView{..} Nothing

            , P.between Tok.openP Tok.closeP $ choice
                [ try $ do
                      r <- relationP
                      return $ TablishParenthesizedRelation (getInfo r) placeholder r
                , do
                      q <- queryP
                      return $ TablishSubQuery (getInfo q) placeholder q
                ]
            ]
        as <- tablishAliasesP
        let withAliases = case t of
                TablishTable info _ tableRef -> TablishTable info as tableRef
                TablishSubQuery info _ query -> TablishSubQuery info as query
                TablishJoin _ _ _ _ _ -> error "shouldn't happen"
                TablishLateralView info LateralView{..} lhs -> TablishLateralView info LateralView{lateralViewAliases = as, ..} lhs
                TablishParenthesizedRelation info _ relation -> TablishParenthesizedRelation info as relation
        return withAliases

    tablishAliasesP :: Parser (TablishAliases Range)
    tablishAliasesP = do
        option TablishAliasesNone $ try $ do
            -- the try is because TABLESAMPLE may either be a table alias OR
            -- the start of a tablesample clause
            _ <- optional Tok.asP
            tAlias@(TableAlias _ name _) <- tableAliasP
            when (TL.toLower name == "tablesample") $ P.lookAhead $ P.notFollowedBy sampleTypeP

            option (TablishAliasesT tAlias) $ do
                cAliases <- P.between Tok.openP Tok.closeP $ columnAliasP `sepBy1` Tok.commaP
                return $ TablishAliasesTC tAlias cAliases


tableAliasP :: Parser (TableAlias Range)
tableAliasP = do
    (name, r) <- Tok.tableNameP
    makeTableAlias r name

columnAliasP :: Parser (ColumnAlias Range)
columnAliasP = do
    (name, r) <- Tok.columnNameP
    makeColumnAlias r name

lambdaParamP :: Parser (LambdaParam Range)
lambdaParamP = do
    (name, r) <- Tok.lambdaParamP
    makeLambdaParam r name

joinP :: Parser (Tablish RawNames Range -> Tablish RawNames Range)
joinP = crossJoinP <|> regularJoinP <|> naturalJoinP
  where
    crossJoinP :: Parser (Tablish RawNames Range -> Tablish RawNames Range)
    crossJoinP = do
        r <- Tok.crossP
        r'<- Tok.joinP
        rhs <- sampledRelationP
        let info = r <> r'
            joinType = JoinInner info
            condition = JoinOn $ ConstantExpr info $ BooleanConstant info True
        return $ \ lhs ->
            TablishJoin (getInfo lhs <> getInfo rhs) joinType condition lhs rhs

    regularJoinP :: Parser (Tablish RawNames Range -> Tablish RawNames Range)
    regularJoinP = do
        joinType <- joinTypeP
        rhs <- relationP
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
        return $ \ lhs ->
            TablishJoin (getInfo rhs <> getInfo lhs) joinType condition lhs rhs

    naturalJoinP :: Parser (Tablish RawNames Range -> Tablish RawNames Range)
    naturalJoinP = do
        r <- Tok.naturalP
        joinType <- joinTypeP
        rhs <- sampledRelationP
        let condition = JoinNatural r Unused
        return $ \ lhs ->
            TablishJoin (getInfo rhs <> getInfo lhs) joinType condition lhs rhs

    joinTypeP :: Parser (JoinType Range)
    joinTypeP = do
        maybeJoinType <- optionMaybe $ innerJoinTypeP <|> outerJoinTypeP
        Tok.joinP >>= \ r -> return $ case maybeJoinType of
            Nothing -> JoinInner r
            Just joinType -> (<> r) <$> joinType

    innerJoinTypeP :: Parser (JoinType Range)
    innerJoinTypeP = JoinInner <$> Tok.innerP

    outerJoinTypeP :: Parser (JoinType Range)
    outerJoinTypeP = do
        joinType <- choice
            [ JoinLeft <$> Tok.leftP
            , JoinRight <$> Tok.rightP
            , JoinFull <$> Tok.fullP
            ]
        optional Tok.outerP
        return joinType

databaseNameP :: Parser (DatabaseName Range)
databaseNameP = do
    (db, r) <- Tok.databaseNameP
    return $ DatabaseName r db

unqualifiedSchemaNameP :: Parser (UQSchemaName Range)
unqualifiedSchemaNameP = uncurry mkNormalSchema <$> Tok.schemaNameP

-- schemaNameP is greedy: it will consume the largest chunk of text that looks
-- like a schema name.
schemaNameP :: Parser (SchemaName RawNames Range)
schemaNameP = choice
    [ try qualifiedSchemaNameP
    , do
        uqsn <- unqualifiedSchemaNameP
        pure uqsn { schemaNameDatabase = Nothing }
    ]
  where
    qualifiedSchemaNameP :: Parser (SchemaName RawNames Range)
    qualifiedSchemaNameP = do
        db <- databaseNameP
        _ <- Tok.dotP
        s <- unqualifiedSchemaNameP
        return s { schemaNameDatabase = Just db }

unqualifiedTableNameP :: Parser (UQTableName Range)
unqualifiedTableNameP = do
    (t, r) <- Tok.tableNameP
    return $ QTableName r None t

-- tableNameP is also greedy
tableNameP :: Parser (TableRef RawNames Range)
tableNameP = choice
    [ try qualifiedTableNameP
    , do
        uqtn <- unqualifiedTableNameP
        pure uqtn { tableNameSchema = Nothing }
    ]
  where
    qualifiedTableNameP :: Parser (TableRef RawNames Range)
    qualifiedTableNameP = choice
        [ try $ do
              d <- databaseNameP
              _ <- Tok.dotP
              s <- unqualifiedSchemaNameP
              _ <- Tok.dotP
              t <- unqualifiedTableNameP
              return t { tableNameSchema = Just s { schemaNameDatabase = Just d } }
        , do
              s <- unqualifiedSchemaNameP
              _ <- Tok.dotP
              t <- unqualifiedTableNameP
              return $ t { tableNameSchema = Just s { schemaNameDatabase = Nothing } }
        ]

unqualifiedColumnNameP :: Parser (UQColumnName Range)
unqualifiedColumnNameP = do
    (c, r) <- Tok.columnNameP
    return $ QColumnName r None c

-- | parsing of qualified columnNames respects the following rules:
--
-- 1) you need to know what tablishes are in scope when parsing a column ref
-- 2) column refs may only be as qualified as the table that introduced them
-- 3) column refs are greedy w.r.t. dots (if a qualified table name and a CTE
--    have the same prefix, a qualified column ref prefers the table)
--
-- If a scope is present (i.e. while parsing selections), the table name must be
-- a member of the tableAlias list for the parser to succeed. Otherwise,
-- the column parser fails and execution tries the next parser choice.
--
-- Should the scope not be set, e.g. when selectP is performing lookahead
-- to build scope, this check is skipped.
columnNameP :: Parser (ColumnRef RawNames Range)
columnNameP = choice
    [ try qualifiedColumnNameP
    , do
        uqcn <- unqualifiedColumnNameP
        pure uqcn { columnNameTable = Nothing }
    ]
  where
    qualifiedColumnNameP :: Parser (ColumnRef RawNames Range)
    qualifiedColumnNameP = choice
        [ try $ do
              d@(DatabaseName _ dName) <- databaseNameP
              _ <- Tok.dotP
              s@(QSchemaName _ _ sName _) <- unqualifiedSchemaNameP
              _ <- Tok.dotP
              t@(QTableName _ _ tName) <- unqualifiedTableNameP

              checkTableNameInScopeP $ dName <> "." <> sName <> "." <> tName

              _ <- Tok.dotP
              c <- unqualifiedColumnNameP
              return c { columnNameTable = Just t { tableNameSchema = Just s { schemaNameDatabase = Just d } } }

        , try $ do
              s@(QSchemaName _ _ sName _) <- unqualifiedSchemaNameP
              _ <- Tok.dotP
              t@(QTableName _ _ tName) <- unqualifiedTableNameP

              checkTableNameInScopeP $ sName <> "." <> tName

              _ <- Tok.dotP
              c <- unqualifiedColumnNameP
              return c { columnNameTable = Just t { tableNameSchema = Just s { schemaNameDatabase = Nothing } } }

        , do
              t@(QTableName _ _ tName) <- unqualifiedTableNameP

              checkTableNameInScopeP tName

              _ <- Tok.dotP
              c <- unqualifiedColumnNameP
              return c { columnNameTable = Just t { tableNameSchema = Nothing } }
        ]

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


-- functionNameP is also greedy
functionNameP :: Parser (FunctionName Range)
functionNameP = choice
    [ try $ do
          d <- databaseNameP
          _ <- Tok.dotP
          s <- unqualifiedSchemaNameP
          _ <- Tok.dotP
          (f, r) <- Tok.functionNameP
          return $ QFunctionName (getInfo d <> r) (Just s { schemaNameDatabase = Just d }) f
    , try $ do
          s <- unqualifiedSchemaNameP
          _ <- Tok.dotP
          (f, r) <- Tok.functionNameP
          return $ QFunctionName (getInfo s <> r) (Just s { schemaNameDatabase = Nothing }) f
    , do
          (f, r) <- Tok.functionNameP
          return $ QFunctionName r Nothing f
    ]

-- selectStarP is also greedy
selectStarP :: Parser (Selection RawNames Range)
selectStarP = choice
    [ do
        r <- Tok.starP
        return $ SelectStar r Nothing Unused

    , try $ do
        t <- unqualifiedTableNameP
        _ <- Tok.dotP
        r' <- Tok.starP
        return $ SelectStar (getInfo t <> r') (Just t { tableNameSchema = Nothing }) Unused

    , try $ do
        s <- unqualifiedSchemaNameP
        _ <- Tok.dotP
        t <- unqualifiedTableNameP
        _ <- Tok.dotP
        r'' <- Tok.starP
        return $ SelectStar (getInfo s <> r'') (Just t { tableNameSchema = Just s { schemaNameDatabase = Nothing }}) Unused

    , try $ do
        d <- databaseNameP
        _ <- Tok.dotP
        s <- unqualifiedSchemaNameP
        _ <- Tok.dotP
        t <- unqualifiedTableNameP
        _ <- Tok.dotP
        r'' <- Tok.starP
        return $ SelectStar (getInfo d <> r'') (Just t { tableNameSchema = Just s { schemaNameDatabase = Just d }}) Unused
    ]


selectionP :: Integer -> Parser (Selection RawNames Range)
selectionP idx = try selectStarP <|> do
    expr <- exprP
    alias <- aliasP expr idx
    return $ SelectExpr (getInfo alias <> getInfo expr) [alias] expr
  where
    aliasP :: Expr RawNames Range -> Integer -> Parser (ColumnAlias Range)
    aliasP expr idx' = choice
        [ do
            optional Tok.asP
            (name, r) <- Tok.columnNameP
            makeColumnAlias r name

        , case expr of
            LambdaExpr {} -> fail "Lambda expression should always be used inside a function"
            _ -> makeExprAlias expr idx'
        ]

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

makeTableAlias :: Range -> Text -> Parser (TableAlias Range)
makeTableAlias r alias = TableAlias r alias . TableAliasId <$> getNextCounter

makeColumnAlias :: Range -> Text -> Parser (ColumnAlias Range)
makeColumnAlias r alias = ColumnAlias r alias . ColumnAliasId <$> getNextCounter

makeLambdaParam :: Range -> Text -> Parser (LambdaParam Range)
makeLambdaParam r name = LambdaParam r name . LambdaParamId <$> getNextCounter

makeDummyAlias :: Range -> Integer -> Parser (ColumnAlias Range)
makeDummyAlias r idx = makeColumnAlias r $ TL.pack $ "_col" ++ show idx

makeExprAlias :: Expr RawNames Range -> Integer -> Parser (ColumnAlias Range)
makeExprAlias (BinOpExpr info _ _ _) idx = makeDummyAlias info idx
makeExprAlias (UnOpExpr info _ _) idx = makeDummyAlias info idx
makeExprAlias (LikeExpr info _ _ _ _) idx = makeDummyAlias info idx
makeExprAlias (CaseExpr info _ _) idx = makeDummyAlias info idx
makeExprAlias (ColumnExpr info (QColumnName _ _ name)) _ = makeColumnAlias info name
makeExprAlias (ConstantExpr info _) idx = makeDummyAlias info idx
makeExprAlias (InListExpr info _ _) idx = makeDummyAlias info idx
makeExprAlias (InSubqueryExpr info _ _) idx = makeDummyAlias info idx
makeExprAlias (BetweenExpr info _ _ _) idx = makeDummyAlias info idx
makeExprAlias (OverlapsExpr _ _ _) _ = fail "Unsupported overlaps expr in Presto: unused expr-type in this dialect"
makeExprAlias (AtTimeZoneExpr info _ _) idx = makeDummyAlias info idx
makeExprAlias (FunctionExpr info _ _ _ _ _ _) idx = makeDummyAlias info idx
makeExprAlias (SubqueryExpr info _) idx = makeDummyAlias info idx
makeExprAlias (ArrayExpr info _) idx = makeDummyAlias info idx
makeExprAlias (ExistsExpr info _) idx = makeDummyAlias info idx
makeExprAlias (FieldAccessExpr info _ _) idx = makeDummyAlias info idx
makeExprAlias (ArrayAccessExpr info _ _) idx = makeDummyAlias info idx
makeExprAlias (TypeCastExpr _ _ expr _) idx = makeExprAlias expr idx
makeExprAlias (VariableSubstitutionExpr _) _ = fail "Unsupported variable substitution in Presto: unused expr-type in this dialect"
makeExprAlias LambdaParamExpr {} _ = error "Unreachable, unresolved expr can not be lambda param"
makeExprAlias LambdaExpr {} _ = error "Unreachable, selection parser should reject lambda expression"


unOpP :: Text -> Parser (Expr RawNames Range -> Expr RawNames Range)
unOpP op = do
    r <- Tok.symbolP op
    return $ \ expr -> UnOpExpr (r <> getInfo expr) (Operator op) expr

binOpP :: Text -> Parser (Expr RawNames Range -> Expr RawNames Range -> Expr RawNames Range)
binOpP op = do
    r <- Tok.symbolP op
    let r' lhs rhs = sconcat $ r :| map getInfo [lhs, rhs]
    return $ \ lhs rhs -> BinOpExpr (r' lhs rhs) (Operator op) lhs rhs


exprP :: Parser (Expr RawNames Range)
exprP = orExprP

orExprP :: Parser (Expr RawNames Range)
orExprP = andExprP `chainl1` (Tok.orP >>= \ r -> return (BinOpExpr r "OR"))

andExprP :: Parser (Expr RawNames Range)
andExprP = notExprP `chainl1`
    (Tok.andP >>= \ r -> return $ BinOpExpr r "AND")

notP :: Parser (Expr RawNames Range -> Expr RawNames Range)
notP = (\ r -> UnOpExpr r "NOT") <$> Tok.notP

notExprP :: Parser (Expr RawNames Range)
notExprP = do
    nots <- appEndo . fold . reverse . map Endo <$> many notP
    expr <- predicatedExprP
    return $ nots expr


predicatedExprP :: Parser (Expr RawNames Range)
predicatedExprP = do
    value <- valueExprP
    predicate <- option id predicateP
    return $ predicate value

valueExprP :: Parser (Expr RawNames Range)
valueExprP = concatExprP

concatExprP :: Parser (Expr RawNames Range)
concatExprP = sumExprP `chainl1` binOpP "||"

sumExprP :: Parser (Expr RawNames Range)
sumExprP = productExprP `chainl1` opP
  where
    opP = choice $ map binOpP ["+", "-"]

productExprP :: Parser (Expr RawNames Range)
productExprP = negateExprP `chainl1` opP
  where
    opP = choice $ map binOpP ["*", "/", "%"]

negateExprP :: Parser (Expr RawNames Range)
negateExprP = do
    neg <- option id $ choice $ map unOpP ["+", "-"]
    expr <- atTimeZoneExprP
    return $ neg expr

intervalP :: Parser (Expr RawNames Range)
intervalP = do
    r <- Tok.intervalP
    sign <- option [] $ pure <$> signP
    str <- stringConstantP
    from <- intervalFieldP
    maybeTo <- optionMaybe $ Tok.toP >> intervalFieldP

    let (info, attrs) =
            case maybeTo of
                Nothing -> (r <> getInfo from, sign ++ [from])
                Just to -> (r <> getInfo to, sign ++ [from, to])

    return $ TypeCastExpr info CastFailureError str $ PrimitiveDataType info "interval" attrs
  where
    signP :: Parser (DataTypeParam Range)
    signP = do
        (sign, r) <- choice [ ("+",) <$> Tok.symbolP "+"
                            , ("-",) <$> Tok.symbolP "-"
                            ]
        pure $ DataTypeParamConstant $ StringConstant r sign

    intervalFieldP :: Parser (DataTypeParam Range)
    intervalFieldP = do
        (field, r) <- Tok.intervalFieldP
        pure $ DataTypeParamConstant $ StringConstant r $ TL.encodeUtf8 field

atTimeZoneExprP :: Parser (Expr RawNames Range)
atTimeZoneExprP = foldl (flip ($)) <$> primaryExprP <*> many atTimeZoneP
  where
    atTimeZoneP :: Parser (Expr RawNames Range -> Expr RawNames Range)
    atTimeZoneP = do
        _ <- Tok.atP
        _ <- Tok.timezoneP
        tz <- choice [stringConstantP, intervalP]

        return $ \ expr ->
            AtTimeZoneExpr (getInfo expr <> getInfo tz) expr tz

stringConstantP :: Parser (Expr RawNames Range)
stringConstantP = do
    (str, r) <- Tok.stringP
    return $ ConstantExpr r (StringConstant r str)


primaryExprP :: Parser (Expr RawNames Range)
primaryExprP = foldl (flip ($)) <$> baseP <*> many (arrayAccessP <|> structAccessP)
  where
    baseP = choice
        [ extractPrimaryExprP
        , normalizePrimaryExprP
        , try lambdaP
        , try substringPrimaryExprP -- try is because `substring` is both a special-form function and a regular function
        , try positionPrimaryExprP -- try is because `position` could be a column name / UDF function name
        , bareFuncPrimaryExprP
        , arrayPrimaryExprP
        , castPrimaryExprP
        , casePrimaryExprP
        , existsPrimaryExprP
        , try subqueryPrimaryExprP -- try is for the parens (for implicitRow)
        , implicitRowPrimaryExprP
        , try rowPrimaryExprP -- try is for the identifier
        , try functionCallPrimaryExprP -- try is for the identifier (for columnRef)
        , parameterPrimaryExprP
        , binaryLiteralPrimaryExprP
        , intervalP
        , try $ constantPrimaryExprP
        , columnRefPrimaryExprP
        ]

extractPrimaryExprP :: Parser (Expr RawNames Range)
extractPrimaryExprP = do
    -- https://prestodb.io/docs/current/functions/datetime.html
    r <- Tok.extractP
    _ <- Tok.openP
    unit <- unitP
    _ <- Tok.fromP
    expr <- valueExprP
    r' <- Tok.closeP
    return $ FunctionExpr (r <> r') (QFunctionName r Nothing "extract") notDistinct [unit, expr] [] Nothing Nothing
  where
    unitP = do
        (unit, r) <- Tok.extractUnitP
        return $ ConstantExpr r $ StringConstant r $ TL.encodeUtf8 unit

normalizePrimaryExprP :: Parser (Expr RawNames Range)
normalizePrimaryExprP = do
    -- https://prestodb.io/docs/current/functions/string.html
    r <- Tok.normalizeP
    _ <- Tok.openP
    strExpr <- valueExprP
    (form, formR) <- option ("nfc", getInfo strExpr) $ Tok.commaP >> Tok.normalFormP
    r' <- Tok.closeP

    let formExpr = ConstantExpr formR $ StringConstant formR $ TL.encodeUtf8 form
    return $ FunctionExpr (r <> r') (QFunctionName r Nothing "normalize") notDistinct [strExpr, formExpr] [] Nothing Nothing

substringPrimaryExprP :: Parser (Expr RawNames Range)
substringPrimaryExprP = do
    r <- Tok.substringP
    _ <- Tok.openP
    str <- valueExprP
    _ <- Tok.fromP
    start <- valueExprP
    maybeLen <- optionMaybe $ Tok.forP >> valueExprP
    r' <- Tok.closeP

    let args = catMaybes [Just str, Just start, maybeLen]
    return $ FunctionExpr (r <> r') (QFunctionName r Nothing "substring") notDistinct args [] Nothing Nothing

bareFuncPrimaryExprP :: Parser (Expr RawNames Range)
bareFuncPrimaryExprP = do
    (func, r, alwaysBare) <- Tok.possiblyBareFuncP
    let name = QFunctionName r Nothing func
    (args, r') <- if alwaysBare
                  then pure ([], r)
                  else option ([], r) precisionP
    return $ FunctionExpr (r <> r') name notDistinct args [] Nothing Nothing

  where
    precisionP = do
        s <- Tok.openP
        numExpr <- numberExprP
        e <- Tok.closeP
        return ([numExpr], s <> e)

arrayPrimaryExprP :: Parser (Expr RawNames Range)
arrayPrimaryExprP = do
    r <- Tok.arrayP
    _ <- Tok.openBracketP
    exprs <- exprP `sepBy` Tok.commaP
    r' <- Tok.closeBracketP
    return $ ArrayExpr (r <> r') exprs

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

windowNameP :: Parser (WindowName Range)
windowNameP =
  do
    (name, r) <- Tok.windowNameP
    return $ WindowName r name

partitionP :: Parser (Partition RawNames Range)
partitionP = do
    r <- Tok.partitionP
    _ <- Tok.byP
    exprs <- exprP `sepBy1` Tok.commaP
    return $ PartitionBy (sconcat $ r :| map getInfo exprs) exprs

dataTypeP :: Parser (DataType Range)
dataTypeP = foldl (flip ($)) <$> typeP <*> many arraySuffixP
  where
    typeP :: Parser (DataType Range)
    typeP = choice [ arrayTypeP
                   , mapTypeP
                   , rowTypeP
                   , baseTypeP
                   ]

    arraySuffixP :: Parser (DataType Range -> DataType Range)
    arraySuffixP = ArrayDataType <$> Tok.arrayP

    baseTypeP = do
        (name, r) <- Tok.typeNameP
        let typeParameterP = choice
                [ DataTypeParamConstant <$> numberConstantP
                , DataTypeParamType <$> dataTypeP
                ]
        args <- option [] $ P.between Tok.openP Tok.closeP $ typeParameterP `sepBy1` Tok.commaP
        return $ PrimitiveDataType r name args

    arrayTypeP = do
        s <- Tok.arrayP
        _ <- Tok.openAngleP
        itemType <- dataTypeP
        e <- Tok.closeAngleP
        return $ ArrayDataType (s <> e) itemType

    mapTypeP = do
        s <- Tok.mapP
        _ <- Tok.openAngleP
        keyType <- dataTypeP
        _ <- Tok.commaP
        valueType <- dataTypeP
        e <- Tok.closeAngleP
        return $ MapDataType (s <> e) keyType valueType

    rowTypeP = do
        s <- Tok.rowP
        _ <- Tok.openP
        let fieldP = do
              (name, _) <- Tok.structFieldNameP
              type_ <- dataTypeP
              return (name, type_)
        fields <- fieldP `sepBy1` Tok.commaP
        e <- Tok.closeP
        return $ StructDataType (s <> e) fields


castPrimaryExprP :: Parser (Expr RawNames Range)
castPrimaryExprP = do
    (onFail, r) <- Tok.castFuncP
    _ <- Tok.openP
    e <- exprP
    _ <- Tok.asP
    t <- dataTypeP
    r' <- Tok.closeP

    return $ TypeCastExpr (r <> r') onFail e t

casePrimaryExprP :: Parser (Expr RawNames Range)
casePrimaryExprP = do
    r <- Tok.caseP
    whens <- choice
        [ P.many1 $ do
            _ <- Tok.whenP
            condition <- exprP
            _ <- Tok.thenP
            result <- exprP
            return (condition, result)

        , do
            value <- valueExprP
            P.many1 $ do
                whenr <- Tok.whenP
                condition <- BinOpExpr whenr "=" value <$> exprP
                _ <- Tok.thenP
                result <- exprP
                return (condition, result)
        ]

    melse <- optionMaybe $ do
        _ <- Tok.elseP
        exprP

    r' <- Tok.endP

    return $ CaseExpr (r <> r') whens melse

existsPrimaryExprP :: Parser (Expr RawNames Range)
existsPrimaryExprP = do
    r <- Tok.existsP
    _ <- Tok.openP
    query <- queryP
    r' <- Tok.closeP
    return $ ExistsExpr (r <> r') query

subqueryPrimaryExprP :: Parser (Expr RawNames Range)
subqueryPrimaryExprP = P.between Tok.openP Tok.closeP $ do
    query <- queryP
    return $ SubqueryExpr (getInfo query) query

functionCallPrimaryExprP :: Parser (Expr RawNames Range)
functionCallPrimaryExprP = do
    name@(QFunctionName r _ _) <- functionNameP
    (distinct, args) <- P.between Tok.openP Tok.closeP $ choice
        [ do
              r' <- Tok.starP
              return (notDistinct, [ConstantExpr r' $ NumericConstant r' "1"])
        , do
              isDistinct <- option notDistinct distinctP
              exprs <- exprP `sepBy` Tok.commaP
              return (isDistinct, exprs)
        ]
    let params = []
    filter' <- optionMaybe filterP
    over <- optionMaybe overP

    let info :: Range
        info = sconcat $ r :| concat [ maybe r getInfo filter' : []
                                     , maybe r getInfo over : []
                                     , map getInfo args
                                     ]
    return $ FunctionExpr info name distinct args params filter' over
  where
    filterP = do
        r <- Tok.filterP
        _ <- Tok.openP
        _ <- Tok.whereP
        expr <- exprP
        r' <- Tok.closeP
        return $ Filter (r <> r') expr

lambdaP :: Parser (Expr RawNames Range) 
lambdaP = do
    (params, start) <- choice
        [ do
            s <- Tok.openP
            params <- lambdaParamP `sepBy` Tok.commaP
            _ <- Tok.closeP
            return (params, s)
        , do
            p <- lambdaParamP  
            return ([p], getInfo p)
        ]
    _ <- Tok.symbolP "->"
    body <- exprP
    return $ LambdaExpr (start <> getInfo body) params body

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
            ]

rowPrimaryExprP :: Parser (Expr RawNames Range)
rowPrimaryExprP = do
    r <- Tok.rowP
    _ <- Tok.openP
    exprs <- exprP `sepBy1` Tok.commaP
    r' <- Tok.closeP

    let name = QFunctionName r Nothing "row"
    return $ FunctionExpr (r <> r') name notDistinct exprs [] Nothing Nothing

implicitRowPrimaryExprP :: Parser (Expr RawNames Range)
implicitRowPrimaryExprP = do
    r <- Tok.openP
    exprs <- exprP `sepBy1` Tok.commaP
    r' <- Tok.closeP

    case exprs of
        [] -> error "this shouldn't happen with sepBy1"
        [e] -> return e
        es -> let name = QFunctionName r Nothing "implicit row"
               in return $ FunctionExpr (r <> r') name notDistinct es [] Nothing Nothing

positionPrimaryExprP :: Parser (Expr RawNames Range)
positionPrimaryExprP = do
    r <- Tok.positionP
    _ <- Tok.openP
    substring <- valueExprP
    _ <- Tok.inP
    string <- valueExprP
    r' <- Tok.closeP
    return $ FunctionExpr (r <> r') (QFunctionName r Nothing "position") notDistinct [substring, string] [] Nothing Nothing

parameterPrimaryExprP :: Parser (Expr RawNames Range)
parameterPrimaryExprP = do
    r <- Tok.questionMarkP
    return $ ConstantExpr r $ ParameterConstant r

binaryLiteralPrimaryExprP :: Parser (Expr RawNames Range)
binaryLiteralPrimaryExprP = do
    (bytes, r) <- Tok.binaryLiteralP
    return $ ConstantExpr r $ StringConstant r bytes

constantPrimaryExprP :: Parser (Expr RawNames Range)
constantPrimaryExprP = do
  val <- choice [ stringP
                , booleanP
                , numberConstantP
                , typedConstantP
                , nullP
                ]
  return $ ConstantExpr (getInfo val) val
  where
    stringP = uncurry (flip StringConstant) <$> Tok.stringP

    booleanP = uncurry (flip BooleanConstant) <$> choice
        [ Tok.trueP >>= \ r -> return (True, r)
        , Tok.falseP >>= \ r -> return (False, r)
        ]

    typedConstantP = do
        (typeStr, r) <- Tok.typedConstantTypeP
        let dataType = PrimitiveDataType r typeStr []
        (string, r') <- first TL.decodeUtf8 <$> Tok.stringP
        pure $ TypedConstant (r <> r') string dataType

    nullP = NullConstant <$> Tok.nullP

numberExprP :: Parser (Expr RawNames Range)
numberExprP = do
    num <- numberConstantP
    return $ ConstantExpr (getInfo num) num

numberConstantP :: Parser (Constant Range)
numberConstantP = uncurry (flip NumericConstant) <$> Tok.numberP

columnRefPrimaryExprP :: Parser (Expr RawNames Range)
columnRefPrimaryExprP = do
    name <- columnNameP
    return $ ColumnExpr (getInfo name) name

structAccessP :: Parser (Expr RawNames Range -> Expr RawNames Range)
structAccessP = do
    _ <- Tok.dotP
    field <- structFieldNameP
    return $ \ struct ->
        let r = getInfo struct <> getInfo field
         in FieldAccessExpr r struct field
  where
    structFieldNameP :: Parser (StructFieldName Range)
    structFieldNameP = uncurry (flip StructFieldName) <$> Tok.structFieldNameP

arrayAccessP :: Parser (Expr RawNames Range -> Expr RawNames Range)
arrayAccessP = do
    _ <- Tok.openBracketP
    index <- valueExprP
    e <- Tok.closeBracketP
    return $ \ expr ->
        let exprR = getInfo expr <> e
        in ArrayAccessExpr exprR expr index


predicateP :: Parser (Expr RawNames Range -> Expr RawNames Range)
predicateP = choice
    [ isPredicateP
    , try likePredicateP  -- the try is for the optional NOT
    , try inPredicateP  -- the try is for the optional NOT
    , betweenPredicateP
    , try quantifiedComparisonPredicateP -- the try is for the comparison operator
    , unquantifiedComparisonPredicateP
    ]

optionalNotWrapper :: Parser (Expr RawNames Range -> Expr RawNames Range)
optionalNotWrapper = do
    maybeNot <- optionMaybe Tok.notP
    return $ maybe id (\ r -> UnOpExpr r "NOT") maybeNot

isPredicateP :: Parser (Expr RawNames Range -> Expr RawNames Range)
isPredicateP = do
    r <- Tok.isP

    notWrapper <- optionalNotWrapper
    predicate <- choice [ Left <$> (Tok.distinctP >> Tok.fromP >> valueExprP)
                        , Right <$> Tok.nullP
                        ]
    return $ case predicate of
        Left expr ->
            \ expr' -> notWrapper $ BinOpExpr (r <> getInfo expr) "IS DISTINCT FROM" expr expr'
        Right nullKeyword ->
            \ expr' -> notWrapper $ UnOpExpr (r <> nullKeyword) "ISNULL" expr'

likePredicateP :: Parser (Expr RawNames Range -> Expr RawNames Range)
likePredicateP = do
    notWrapper <- optionalNotWrapper
    r <- Tok.likeP
    pattern <- Pattern <$> valueExprP
    escape <- optionMaybe $ do
        _ <- Tok.escapeP
        Escape <$> valueExprP
    return $ \ expr -> notWrapper $ LikeExpr r "LIKE" escape pattern expr

inPredicateP :: Parser (Expr RawNames Range -> Expr RawNames Range)
inPredicateP = do
    notWrapper <- optionalNotWrapper
    r <- Tok.inP
    _ <- Tok.openP
    predicate <- choice
        [ Left <$> queryP
        , Right <$> exprP `sepBy1` Tok.commaP
        ]
    r' <- Tok.closeP

    return $ case predicate of
        Left query ->
            \ expr -> notWrapper $ InSubqueryExpr (r <> r') query expr
        Right exprs ->
            \ expr -> notWrapper $ InListExpr (r <> r') exprs expr

betweenPredicateP :: Parser (Expr RawNames Range -> Expr RawNames Range)
betweenPredicateP = do
    notWrapper <- optionalNotWrapper
    _ <- Tok.betweenP
    lower <- valueExprP
    _ <- Tok.andP
    upper <- valueExprP
    return $ \ expr -> notWrapper $ BetweenExpr (getInfo expr <> getInfo upper) lower upper expr


quantifiedComparisonPredicateP :: Parser (Expr RawNames Range -> Expr RawNames Range)
quantifiedComparisonPredicateP = do
    (op, _) <- Tok.comparisonOperatorP
    (quantifier, _) <- Tok.comparisonQuantifierP
    _ <- Tok.openP
    query <- queryP
    r <- Tok.closeP

    let op' = Operator $ TL.unwords [op, quantifier]
        subquery = SubqueryExpr (getInfo query) query
    return $ \ expr -> BinOpExpr (getInfo expr <> r) op' expr subquery

unquantifiedComparisonPredicateP :: Parser (Expr RawNames Range -> Expr RawNames Range)
unquantifiedComparisonPredicateP = do
    (op, _) <- Tok.comparisonOperatorP
    rhs <- valueExprP
    return $ \ lhs -> BinOpExpr (getInfo lhs <> getInfo rhs) (Operator op) lhs rhs


whereP :: Parser (SelectWhere RawNames Range)
whereP = do
    r <- Tok.whereP
    condition <- exprP
    return $ SelectWhere (r <> getInfo condition) condition


handlePositionalReferences :: Expr RawNames Range -> PositionOrExpr RawNames Range
handlePositionalReferences e = case e of
    ConstantExpr _ (NumericConstant _ n) | TL.all isDigit n -> PositionOrExprPosition (getInfo e) (read $ TL.unpack n) Unused
    _ -> PositionOrExprExpr e

groupP :: Parser (SelectGroup RawNames Range)
groupP = do
    r <- Tok.groupP
    _ <- Tok.byP
    optional distinctP

    selectGroupGroupingElements <- concat <$> groupingElementP `sepBy1` Tok.commaP
    let selectGroupInfo = foldl (<>) r $ fmap getInfo selectGroupGroupingElements

    return SelectGroup{..}
  where
    toGroupingElement :: PositionOrExpr RawNames Range -> GroupingElement RawNames Range
    toGroupingElement posOrExpr = GroupingElementExpr (getInfo posOrExpr) posOrExpr

    groupingElementP :: Parser [GroupingElement RawNames Range]
    groupingElementP = choice
        [ singleExprP
        , parenExprsP
        , rollupP
        , cubeP
        , groupingSetsP
        ]

    singleExprP = do
        e <- exprP
        return [toGroupingElement $ handlePositionalReferences e]

    parenExprsP = do
        _ <- Tok.openP
        es <- exprP `sepBy` Tok.commaP
        _ <- Tok.closeP
        return $ map (toGroupingElement . handlePositionalReferences) es

    toGroupingSet :: Range -> [Expr RawNames Range] -> GroupingElement RawNames Range
    toGroupingSet r [] = GroupingElementSet r []
    toGroupingSet _ exprs =
        let s = getInfo $ head exprs
            e = getInfo $ last exprs
         in GroupingElementSet (s <> e) exprs

    rollupP = do
        _ <- Tok.rollupP
        _ <- Tok.openP
        cols <- columnRefPrimaryExprP `sepBy1` Tok.commaP
        _ <- Tok.closeP
        let dimensions = L.reverse $ L.inits cols
            defaultRange = (getInfo $ head cols) <> (getInfo $ last cols)
        return $ map (toGroupingSet defaultRange) dimensions

    cubeP = do
        _ <- Tok.cubeP
        _ <- Tok.openP
        cols <- columnRefPrimaryExprP `sepBy1` Tok.commaP
        _ <- Tok.closeP
        let dimensions = L.subsequences cols
            defaultRange = (getInfo $ head cols) <> (getInfo $ last cols)
        return $ map (toGroupingSet defaultRange) dimensions

    groupingSetP = choice $
        [ do
              s <- Tok.openP
              sets <- columnRefPrimaryExprP `sepBy1` Tok.commaP
              e <- Tok.closeP
              return $ GroupingElementSet (s <> e) sets
        , do
              -- if no parens, it will be the singleton list.
              col <- columnRefPrimaryExprP
              return $ GroupingElementSet (getInfo col) [col]
        ]

    groupingSetsP = do
        _ <- Tok.groupingP
        _ <- Tok.setsP
        _ <- Tok.openP
        sets <- groupingSetP `sepBy1` Tok.commaP
        _ <- Tok.closeP
        return sets


havingP :: Parser (SelectHaving RawNames Range)
havingP = do
    r <- Tok.havingP
    condition <- exprP
    return $ SelectHaving (r <> getInfo condition) [condition]


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
    table <- tableNameP

    let dropTableInfo = s <> getInfo table
        dropTableNames = table :| []
    pure DropTable{..}

ifExistsP :: Parser Range
ifExistsP = do
    s <- Tok.ifP
    e <- Tok.existsP
    pure $ s <> e


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


insertP :: Parser (Insert RawNames Range)
insertP = do
    r <- Tok.insertP
    insertBehavior <- InsertAppend <$> Tok.intoP

    insertTable <- tableNameP

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
        , InsertSelectValues <$> queryP
        ]

    let insertInfo = r <> getInfo insertValues

    pure Insert{..}
  where
    valueP :: Parser (DefaultExpr RawNames Range)
    valueP = ExprValue <$> exprP

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


explainP :: Parser Range
explainP = do
    s <- Tok.explainP
    e <- getInfo <$> queryP
    return $ s <> e


showP :: Parser Range
showP = do
    s <- Tok.showP
    e <- P.many1 Tok.notSemicolonP
    return $ s <> (last e)


callP :: Parser Range
callP = do
    s <- Tok.callP
    e <- P.many1 Tok.notSemicolonP
    return $ s <> (last e)


describeP :: Parser Range
describeP = do
    s <- Tok.describeP
    e <- P.many1 Tok.notSemicolonP
    return $ s <> last e

setP :: Parser Range
setP = do
    s <- Tok.setP
    _ <- choice [Tok.roleP, Tok.sessionP, Tok.timezoneP]
    ts <- P.many Tok.notSemicolonP
    pure $ case reverse ts of
        [] -> s
        e:_ -> s <> e

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

-- TODO: support create table with column definitions
createTableP :: Parser (CreateTable Presto RawNames Range)
createTableP = do
    s <- Tok.createP
    _ <- Tok.tableP

    let createTablePersistence = Persistent
        createTableExternality = Internal
        createTableExtra = Nothing

    createTableIfNotExists <- ifNotExistsP

    createTableName <- tableNameP 
    columns <- optionMaybe columnListP
    _ <- optional commentP 
    _ <- optional propertiesP
    createTableDefinition <- choice [createTableAsP columns]

    let e = getInfo createTableDefinition
        createTableInfo = s <> e
    pure CreateTable{..}

  where
    createTableAsP columns = do
        s <- Tok.asP
        (query, qInfo) <- choice 
            [ do
                s' <- Tok.openP
                q <- queryP
                e <- Tok.closeP
                return (q, s' <> e)
            , do
                q <- queryP
                return (q, getInfo q)
            ]
        withData <- optionMaybe withDataP
        let e = fromMaybe qInfo withData
        return $ TableAs (s <> e) columns query

    columnListP :: Parser (NonEmpty (UQColumnName Range))
    columnListP = do
        _ <- Tok.openP
        c:cs <- (`sepBy1` Tok.commaP) $ do
            (name, r) <- Tok.columnNameP
            pure $ QColumnName r None name
        _ <- Tok.closeP
        pure (c:|cs)
    

commentP :: Parser Range
commentP = do
    s <- Tok.commentP
    (_, e) <- Tok.stringP
    return $ s <> e

propertiesP :: Parser Range
propertiesP = do
    s <- Tok.withP
    _ <- Tok.openP
    _ <- propertyP `sepBy1` Tok.commaP
    e <- Tok.closeP
    return $ s <> e

propertyP :: Parser Range
propertyP = do
    (_, s) <- Tok.propertyNameP
    _ <- Tok.equalP
    e <- exprP
    return $ s <> getInfo e
    
withDataP :: Parser Range
withDataP = do
    s <- Tok.withP
    _ <- optionMaybe Tok.noP
    e <- Tok.dataP
    return $ s <> e

ifNotExistsP :: Parser (Maybe Range)
ifNotExistsP = optionMaybe $ do
    s <- Tok.ifP
    _ <- Tok.notP
    e <- Tok.existsP
    pure $ s <> e
