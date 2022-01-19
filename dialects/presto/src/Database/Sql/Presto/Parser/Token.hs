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

module Database.Sql.Presto.Parser.Token where

import Database.Sql.Type.Query (CastFailureAction(..))
import Database.Sql.Presto.Token
import Database.Sql.Presto.Parser.Internal

import Database.Sql.Position

import qualified Text.Parsec as P
import qualified Text.Parsec.Pos as P

import           Data.ByteString.Lazy (ByteString)
import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL

import qualified Data.Map as M


-- helpers

showTok :: (Token, Position, Position) -> String
showTok (t, _, _) = show t

posFromTok :: P.SourcePos ->
              (Token, Position, Position) ->
              [(Token, Position, Position)] ->
              P.SourcePos
posFromTok _ (_, pos, _) _ = flip P.setSourceLine (fromEnum $ positionLine pos)
                           $ flip P.setSourceColumn (fromEnum $ positionColumn pos)
                           $ P.initialPos "-"

tokEqualsP :: Token -> Parser Range
tokEqualsP tok = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok', s, e) =
        if tok == tok'
         then Just $ Range s e
         else Nothing

tokNotEqualsP :: Token -> Parser Range
tokNotEqualsP tok = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok', s, e) =
        if tok /= tok'
         then Just $ Range s e
         else Nothing

testNameTok :: (Token, Position, Position) -> Maybe (Text, Range)
testNameTok (tok, s, e) =
  case tok of
    TokWord _ name -> Just (name, Range s e)
    _ -> Nothing

symbolP :: Text -> Parser Range
symbolP op = tokEqualsP $ TokSymbol op

keywordP :: Text -> Parser Range
keywordP keyword = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord False name
            | name == keyword -> Just (Range s e)

        _ -> Nothing

-- onto the actual parsers

dotP :: Parser Range
dotP = symbolP "."

starP :: Parser Range
starP = symbolP "*"

plusP :: Parser Range
plusP = symbolP "+"

minusP :: Parser Range
minusP = symbolP "-"

commaP :: Parser Range
commaP = symbolP ","

openP :: Parser Range
openP = symbolP "("

closeP :: Parser Range
closeP = symbolP ")"

openBracketP :: Parser Range
openBracketP = symbolP "["

closeBracketP :: Parser Range
closeBracketP = symbolP "]"

openAngleP :: Parser Range
openAngleP = symbolP "<"

closeAngleP :: Parser Range
closeAngleP = symbolP ">"

questionMarkP :: Parser Range
questionMarkP = symbolP "?"

equalP :: Parser Range
equalP = symbolP "="

stringP :: Parser (ByteString, Range)
stringP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokString string -> Just (string, Range s e)
        _ -> Nothing

binaryLiteralP :: Parser (ByteString, Range)
binaryLiteralP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokBinary bytes -> Just (bytes, Range s e)
        _ -> Nothing

numberP :: Parser (Text, Range)
numberP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokNumber number -> Just (number, Range s e)
        _ -> Nothing

typeNameP :: Parser (Text, Range)
typeNameP = P.choice
    [ P.try $ multiWordTypeP -- the try is for plain old `double`, `time`, or `timestamp`
    , singleWordTypeP
    ]
  where
    multiWordTypeP = P.choice
        [ do
              s <- doubleP
              e <- precisionP
              return ("double precision", s <> e)
        , do
              r <- Left <$> timeP P.<|> Right <$> timestampP
              _ <- withP
              e <- timezoneP
              case r of
                  Left s -> return ("time with time zone", s <> e)
                  Right s -> return ("timestamp with time zone", s <> e)
        ]

    singleWordTypeP = P.tokenPrim showTok posFromTok testNameTok

intervalFieldP :: Parser (Text, Range)
intervalFieldP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord _ name | TL.toLower name `elem` fields -> Just (TL.toLower name, Range s e)
        _ -> Nothing

    fields = ["year", "month", "day", "hour", "minute", "second"]

comparisonOperatorP :: Parser (Text, Range)
comparisonOperatorP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (TokSymbol op, s, e)
        | op `elem` ["=", "<>", "!=", "<", "<=", ">", ">="] = Just (op, Range s e)

    testTok _ = Nothing

comparisonQuantifierP :: Parser (Text, Range)
comparisonQuantifierP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord _ name | TL.toLower name `elem` quantifiers -> Just (TL.toLower name, Range s e)
        _ -> Nothing

    quantifiers = ["all", "some", "any"]

extractUnitP :: Parser (Text, Range)
extractUnitP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord _ name | TL.toLower name `elem` units -> Just (TL.toLower name, Range s e)
        _ -> Nothing

    units = [ "year", "quarter", "month", "week"
            , "day", "day_of_month", "day_of_week", "dow", "day_of_year", "doy"
            , "year_of_week", "yow"
            , "hour", "minute", "second"
            , "timezone_hour", "timezone_minute"
            ]

typedConstantTypeP :: Parser (Text, Range)
typedConstantTypeP = P.choice
    [ P.try $ multiWordTypeP -- the try is for plain old `double`
    , singleWordTypeP
    ]
  where
    multiWordTypeP = do
        s <- doubleP
        e <- precisionP
        return ("double precision", s <> e)

    singleWordTypeP = P.tokenPrim showTok posFromTok testTok

    testTok (tok, s, e) = case tok of
        TokWord _ name | TL.toLower name `elem` types -> Just (TL.toLower name, Range s e)
        _ -> Nothing

    -- generated with some trial and error (and guesswork) based on
    -- https://github.com/prestodb/presto/blob/master/presto-main/src/main/java/com/facebook/presto/type/TypeRegistry.java
    types = [ "boolean", "bigint", "integer", "smallint", "tinyint"
            , "double", "real", "varbinary", "date", "time", "timestamp"
            , "hyperloglog", "p4hyperloglog", "joniregexp", "re2jregexp"
            , "likepattern", "jsonpath", "color", "json", "codepoints"
            , "varchar", "char", "decimal"
            ]

normalFormP :: Parser (Text, Range)
normalFormP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord _ name | TL.toLower name `elem` forms -> Just (TL.toLower name, Range s e)
        _ -> Nothing

    forms = [ "nfc", "nfd", "nfkc", "nfkd" ]

possiblyBareFuncP :: Parser (Text, Range, Bool)
possiblyBareFuncP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord _ name | TL.toLower name `M.member` funcs ->
                           let name' = TL.toLower name
                               r = Range s e
                               Just alwaysBare = M.lookup name' funcs
                            in Just (name', r, alwaysBare)
        _ -> Nothing

    funcs = M.fromList [ ("current_date",      True)
                       , ("current_time",      False)
                       , ("current_timestamp", False)
                       , ("localtime",         False)
                       , ("localtimestamp",    False)
                       ]

castFuncP :: Parser (CastFailureAction, Range)
castFuncP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord _ name | TL.toLower name == "cast" -> Just (CastFailureError, Range s e)
        TokWord _ name | TL.toLower name == "try_cast" -> Just (CastFailureToNull, Range s e)
        _ -> Nothing


-- In presto, databases are called "catalogs".
-- Notionally they are "one coarser than schema"
databaseNameP :: Parser (Text, Range)
databaseNameP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeSchemaName (wordInfo name) -> Just (name, Range s e)

        _ -> Nothing

schemaNameP :: Parser (Text, Range)
schemaNameP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeSchemaName (wordInfo name) -> Just (name, Range s e)

        _ -> Nothing

tableNameP :: Parser (Text, Range)
tableNameP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeTableName (wordInfo name) -> Just (name, Range s e)

        _ -> Nothing

columnNameP :: Parser (Text, Range)
columnNameP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeColumnName (wordInfo name) -> Just (name, Range s e)

        _ -> Nothing

lambdaParamP :: Parser (Text, Range)
lambdaParamP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeColumnName (wordInfo name) -> Just (name, Range s e)

        _ -> Nothing

propertyNameP :: Parser (Text, Range)
propertyNameP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeColumnName (wordInfo name) -> Just (name, Range s e)

        _ -> Nothing

structFieldNameP :: Parser (Text, Range)
structFieldNameP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeColumnName (wordInfo name) -> Just (name, Range s e)

        _ -> Nothing

paramNameP :: Parser (Text, Range)
paramNameP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeColumnName (wordInfo name) -> Just (name, Range s e)

        _ -> Nothing

functionNameP :: Parser (Text, Range)
functionNameP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeFunctionName (wordInfo name) ->
                Just (name, Range s e)

        _ -> Nothing

allP :: Parser Range
allP = keywordP "all"

andP :: Parser Range
andP = keywordP "and"

arrayP :: Parser Range
arrayP = keywordP "array"

asP :: Parser Range
asP = keywordP "as"

ascP :: Parser Range
ascP = keywordP "asc"

atP :: Parser Range
atP = keywordP "at"

bernoulliP :: Parser Range
bernoulliP = keywordP "bernoulli"

betweenP :: Parser Range
betweenP = keywordP "between"

byP :: Parser Range
byP = keywordP "by"

callP :: Parser Range
callP = keywordP "call"

caseP :: Parser Range
caseP = keywordP "case"

crossP :: Parser Range
crossP = keywordP "cross"

cubeP :: Parser Range
cubeP = keywordP "cube"

currentP :: Parser Range
currentP = keywordP "current"

deleteP :: Parser Range
deleteP = keywordP "delete"

descP :: Parser Range
descP = keywordP "desc"

describeP :: Parser Range
describeP = keywordP "describe" P.<|> keywordP "desc"

distinctP :: Parser Range
distinctP = keywordP "distinct"

doubleP :: Parser Range
doubleP = keywordP "double"

dropP :: Parser Range
dropP = keywordP "drop"

elseP :: Parser Range
elseP = keywordP "else"

endP :: Parser Range
endP = keywordP "end"

escapeP :: Parser Range
escapeP = keywordP "escape"

exceptP :: Parser Range
exceptP = keywordP "except"

existsP :: Parser Range
existsP = keywordP "exists"

explainP :: Parser Range
explainP = keywordP "explain"

extractP :: Parser Range
extractP = keywordP "extract"

falseP :: Parser Range
falseP = keywordP "false"

filterP :: Parser Range
filterP = keywordP "filter"

firstP :: Parser Range
firstP = keywordP "first"

followingP :: Parser Range
followingP = keywordP "following"

forP :: Parser Range
forP = keywordP "for"

fromP :: Parser Range
fromP = keywordP "from"

fullP :: Parser Range
fullP = keywordP "full"

grantP :: Parser Range
grantP = keywordP "grant"

groupP :: Parser Range
groupP = keywordP "group"

groupingP :: Parser Range
groupingP = keywordP "grouping"

havingP :: Parser Range
havingP = keywordP "having"

ifP :: Parser Range
ifP = keywordP "if"

intervalP :: Parser Range
intervalP = keywordP "interval"

inP :: Parser Range
inP = keywordP "in"

innerP :: Parser Range
innerP = keywordP "inner"

insertP :: Parser Range
insertP = keywordP "insert"

intersectP :: Parser Range
intersectP = keywordP "intersect"

intoP :: Parser Range
intoP = keywordP "into"

isP :: Parser Range
isP = keywordP "is"

joinP :: Parser Range
joinP = keywordP "join"

lastP :: Parser Range
lastP = keywordP "last"

leftP :: Parser Range
leftP = keywordP "left"

likeP :: Parser Range
likeP = keywordP "like"

limitP :: Parser Range
limitP = keywordP "limit"

mapP :: Parser Range
mapP = keywordP "map"

naturalP :: Parser Range
naturalP = keywordP "natural"

normalizeP :: Parser Range
normalizeP = keywordP "normalize"

notP :: Parser Range
notP = keywordP "not"

nullP :: Parser Range
nullP = keywordP "null"

nullsP :: Parser Range
nullsP = keywordP "nulls"

onP :: Parser Range
onP = keywordP "on"

orP :: Parser Range
orP = keywordP "or"

orderP :: Parser Range
orderP = keywordP "order"

ordinalityP :: Parser Range
ordinalityP = keywordP "ordinality"

outerP :: Parser Range
outerP = keywordP "outer"

overP :: Parser Range
overP = keywordP "over"

partitionP :: Parser Range
partitionP = keywordP "partition"

poissonizedP :: Parser Range
poissonizedP = keywordP "poissonized"

positionP :: Parser Range
positionP = keywordP "position"

precedingP :: Parser Range
precedingP = keywordP "preceding"

precisionP :: Parser Range
precisionP = keywordP "precision"

rangeP :: Parser Range
rangeP = keywordP "range"

revokeP :: Parser Range
revokeP = keywordP "revoke"

rightP :: Parser Range
rightP = keywordP "right"

rollupP :: Parser Range
rollupP = keywordP "rollup"

rowP :: Parser Range
rowP = keywordP "row"

rowsP :: Parser Range
rowsP = keywordP "rows"

selectP :: Parser Range
selectP = keywordP "select"

semicolonP :: Parser Range
semicolonP = symbolP ";"

notSemicolonP :: Parser Range
notSemicolonP = tokNotEqualsP $ TokSymbol ";"

setsP :: Parser Range
setsP = keywordP "sets"

showP :: Parser Range
showP = keywordP "show"

substringP :: Parser Range
substringP = keywordP "substring"

systemP :: Parser Range
systemP = keywordP "system"

tableP :: Parser Range
tableP = keywordP "table"

tableSampleP :: Parser Range
tableSampleP = keywordP "tablesample"

thenP :: Parser Range
thenP = keywordP "then"

timeP :: Parser Range
timeP = keywordP "time"

timestampP :: Parser Range
timestampP = keywordP "timestamp"

timezoneP :: Parser Range
timezoneP = do
    s <- keywordP "time"
    e <- keywordP "zone"
    pure $ s <> e

toP :: Parser Range
toP = keywordP "to"

trueP :: Parser Range
trueP = keywordP "true"

unboundedP :: Parser Range
unboundedP = keywordP "unbounded"

unionP :: Parser Range
unionP = keywordP "union"

unnestP :: Parser Range
unnestP = keywordP "unnest"

usingP :: Parser Range
usingP = keywordP "using"

valuesP :: Parser Range
valuesP = keywordP "values"

viewP :: Parser Range
viewP = keywordP "view"

whenP :: Parser Range
whenP = keywordP "when"

whereP :: Parser Range
whereP = keywordP "where"

withP :: Parser Range
withP = keywordP "with"

setP :: Parser Range
setP = keywordP "set"

roleP :: Parser Range
roleP = keywordP "role"

sessionP :: Parser Range
sessionP = keywordP "session"

windowP :: Parser Range
windowP = keywordP "window"

windowNameP :: Parser (Text, Range)
windowNameP = P.tokenPrim showTok posFromTok testNameTok

createP :: Parser Range
createP = keywordP "create"

commentP :: Parser Range
commentP = keywordP "comment"

noP :: Parser Range
noP = keywordP "no"

dataP :: Parser Range
dataP = keywordP "data"
