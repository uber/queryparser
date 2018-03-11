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

module Database.Sql.Hive.Parser.Token where


import Database.Sql.Hive.Token
import Database.Sql.Hive.Parser.Internal

import Database.Sql.Position

import qualified Text.Parsec as P
import qualified Text.Parsec.Pos as P

import           Data.Char (isDigit)
import           Data.String
import           Data.Text.Lazy hiding (foldl1, map, head, last, all, null, init)
import qualified Data.Text.Lazy.Encoding as TL
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BL
import           Data.Semigroup ((<>))


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

variableSubstitutionP :: Parser Range
variableSubstitutionP = P.tokenPrim showTok posFromTok testVariableTok
  where
    testVariableTok (tok, s, e) =
        case tok of
            TokVariable _ _ -> Just (Range s e)
            _ -> Nothing

typeNameP :: Parser (Text, Range)
typeNameP = P.tokenPrim showTok posFromTok testNameTok

nodeNameP :: Parser (Text, Range)
nodeNameP = P.tokenPrim showTok posFromTok testNameTok

structFieldNameP :: Parser (Text, Range)
structFieldNameP = P.tokenPrim showTok posFromTok testNameTok

windowNameP :: Parser (Text, Range)
windowNameP = P.tokenPrim showTok posFromTok testNameTok

datePartP :: Parser (Text, Range)
datePartP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord _ name | toLower name `elem` parts -> Just (toLower name, Range s e)
        _ -> Nothing

    parts = [ "year", "yy", "yyyy"
            , "quarter", "qq", "q"
            , "month", "mm", "m"
            , "day", "dd", "d", "dy", "dayofyear", "y"
            , "week", "wk", "ww"
            , "hour", "hh"
            , "minute", "mi", "n"
            , "second", "ss", "s"
            , "millisecond", "ms"
            , "microsecond", "mcs", "us"
            ]


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


projectionNameP :: Parser (Text, Range)
projectionNameP = P.tokenPrim showTok posFromTok testTok
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


functionNameP :: Parser (Text, Range)
functionNameP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeFunctionName (wordInfo name) ->
                Just (name, Range s e)

        _ -> Nothing

propertyValuePartP :: Parser (Text, Range)
propertyValuePartP = textUntilP [";"]

-- Hive supports property names and values contianing
--  equal signs and spaces. Here we stop on the first equal sign.
propertyNameP :: Parser (Text, Range)
propertyNameP = textUntilP ["=", ";"]

-- Parses all tokens until a token equal to a given string is found
--  returns parsed text.
textUntilP :: [Text] -> Parser (Text, Range)
textUntilP x = do
    res <- P.many anyTokenExceptX
    let name = Data.Text.Lazy.concat $ fst <$> res
        s = snd $ head res
        e = snd $ last res
    pure (name, s <> e)
    where
      anyTokenExceptX :: Parser (Text, Range)
      anyTokenExceptX = P.tokenPrim showTok posFromTok $
        \ (tok, s, e) -> case tok of
          TokSymbol t | not (t `elem` x) ->
            Just (t, Range s e)
          TokWord _ t | not (t `elem` x) ->
            Just (t, Range s e)
          TokNumber t | not (t `elem` x) ->
            Just (t, Range s e)
          _ -> Nothing


keywordP :: Text -> Parser Range
keywordP keyword = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord False name
            | name == keyword -> Just (Range s e)

        _ -> Nothing

fieldTypeP :: Parser (Text, Range)
fieldTypeP = P.tokenPrim showTok posFromTok testTok
  where
    isField :: (Eq s, IsString s) => s -> Bool
    isField = flip elem
        [ "century", "day", "decade", "doq", "dow", "doy", "epoch"
        , "hour", "isodow", "isoweek", "isoyear", "microseconds"
        , "millennium", "milliseconds", "minute", "month", "quarter"
        , "second", "time zone", "timezone_hour", "timezone_minute"
        , "week", "year"
        ]

    testTok (tok, s, e) = case tok of
        TokWord _ field | isField field -> Just (field, Range s e)
        TokString field | isField field -> Just (TL.decodeUtf8 field, Range s e)
        _ -> Nothing

periodP :: Parser (Text, Range)
periodP = P.tokenPrim showTok posFromTok testTok
  where
    isPeriod :: (Eq s, IsString s) => s -> Bool
    isPeriod = flip elem [ "day", "hour", "minute", "month", "second", "year" ]

    testTok (tok, s, e) = case tok of
        TokWord _ period | isPeriod period -> Just (period, Range s e)
        TokString period | isPeriod period -> Just (TL.decodeUtf8 period, Range s e)
        _ -> Nothing

byteAmountP :: Parser (Text, Range)
byteAmountP = P.tokenPrim showTok posFromTok testTok
  where
    isByteAmount :: Text -> Bool
    isByteAmount text =
      let str = unpack $ toLower text
       in and [ not $ null str
              , last str `elem` ['b', 'k', 'm', 'g']
              , all isDigit $ init str
              ]

    testTok (tok, s, e) = case tok of
        TokWord False byteAmount | isByteAmount byteAmount -> Just (byteAmount, Range s e)
        _ -> Nothing

stringP :: Parser (ByteString, Range)
stringP = do
    tokens <- P.many1 singleStringTokenP
    let s = foldl1 BL.append $ map fst tokens
        r = foldl1 (<>) $ map snd tokens

    pure $ (s, r)
  where
    singleStringTokenP :: Parser (ByteString, Range)
    singleStringTokenP = P.tokenPrim showTok posFromTok testTok
      where
        testTok (tok, s, e) = case tok of
            TokString string -> Just (string, Range s e)
            _ -> Nothing

numberP :: Parser (Text, Range)
numberP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokNumber number -> Just (number, Range s e)
        _ -> Nothing

dotP :: Parser Range
dotP = symbolP "."

equalP :: Parser Range
equalP = symbolP "="

colonP :: Parser Range
colonP = symbolP ":"

symbolP :: Text -> Parser Range
symbolP op = tokEqualsP $ TokSymbol op

starP :: Parser Range
starP = symbolP "*"

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

castP :: Parser Range
castP = keywordP "cast"

castOpP :: Parser Range
castOpP = symbolP "::"

minusP :: Parser Range
minusP = symbolP "-"

accessRankP :: Parser Range
accessRankP = keywordP "accessrank"

addP :: Parser Range
addP = keywordP "add"

afterP :: Parser Range
afterP = keywordP "after"

allP :: Parser Range
allP = keywordP "all"

alterP :: Parser Range
alterP = keywordP "alter"

analyzeP :: Parser Range
analyzeP = keywordP "analyze"

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

autoP :: Parser Range
autoP = keywordP "auto"

avroP :: Parser Range
avroP = keywordP "avro"

bestP :: Parser Range
bestP = keywordP "best"

betweenP :: Parser Range
betweenP = keywordP "between"

bucketP :: Parser Range
bucketP = keywordP "bucket"

bucketsP :: Parser Range
bucketsP = keywordP "buckets"

byP :: Parser Range
byP = keywordP "by"

cacheP :: Parser Range
cacheP = keywordP "cache"

cascadeP :: Parser Range
cascadeP = keywordP "cascade"

caseP :: Parser Range
caseP = keywordP "case"

changeP :: Parser Range
changeP = keywordP "change"

clusterP :: Parser Range
clusterP = keywordP "cluster"

clusteredP :: Parser Range
clusteredP = keywordP "clustered"

collectionP :: Parser Range
collectionP = keywordP "collection"

columnP :: Parser Range
columnP = keywordP "column"

columnsP :: Parser Range
columnsP = keywordP "columns"

commaP :: Parser Range
commaP = symbolP ","

commentP :: Parser Range
commentP = keywordP "comment"

commitP :: Parser Range
commitP = keywordP "commit"

computeP :: Parser Range
computeP = keywordP "compute"

createP :: Parser Range
createP = keywordP "create"

crossP :: Parser Range
crossP = keywordP "cross"

cubeP :: Parser Range
cubeP = keywordP "cube"

currentP :: Parser Range
currentP = keywordP "current"

currentDatabaseP :: Parser (Text, Range)
currentDatabaseP = ("current_database",) <$> keywordP "current_database"

currentDateP :: Parser (Text, Range)
currentDateP = ("current_date",) <$> keywordP "current_date"

currentSchemaP :: Parser (Text, Range)
currentSchemaP = ("current_schema",) <$> keywordP "current_schema"

currentTimeP :: Parser (Text, Range)
currentTimeP = ("current_time",) <$> keywordP "current_time"

currentTimestampP :: Parser (Text, Range)
currentTimestampP = ("current_timestamp",) <$> keywordP "current_timestamp"

currentUserP :: Parser (Text, Range)
currentUserP = ("current_user",) <$> keywordP "current_user"

dataP :: Parser Range
dataP = keywordP "data"

databaseP :: Parser Range
databaseP = keywordP "database"

dateDiffP :: Parser Range
dateDiffP = keywordP "datediff"

dbPropertiesP :: Parser Range
dbPropertiesP = keywordP "dbproperties"

defaultP :: Parser Range
defaultP = keywordP "default"

definedP :: Parser Range
definedP = keywordP "defined"

deleteP :: Parser Range
deleteP = keywordP "delete"

delimitedP :: Parser Range
delimitedP = keywordP "delimited"

descP :: Parser Range
descP = keywordP "desc"

describeP :: Parser Range
describeP = keywordP "describe" P.<|> keywordP "desc"

directoryP :: Parser Range
directoryP = keywordP "directory"

distinctP :: Parser Range
distinctP = keywordP "distinct"

distributeP :: Parser Range
distributeP = keywordP "distribute"

dropP :: Parser Range
dropP = keywordP "drop"

elseP :: Parser Range
elseP = keywordP "else"

encodingP :: Parser Range
encodingP = keywordP "encoding"

endP :: Parser Range
endP = keywordP "end"

escapeP :: Parser Range
escapeP = keywordP "escape"

escapedP :: Parser Range
escapedP = keywordP "escaped"

excludingP :: Parser Range
excludingP = keywordP "excluding"

existsP :: Parser Range
existsP = keywordP "exists"

explainP :: Parser Range
explainP = keywordP "explain"

externalP :: Parser Range
externalP = keywordP "external"

extractP :: Parser Range
extractP = keywordP "extract"

falseP :: Parser Range
falseP = keywordP "false"

fieldsP :: Parser Range
fieldsP = keywordP "fields"

firstP :: Parser Range
firstP = keywordP "first"

followingP :: Parser Range
followingP = keywordP "following"

forP :: Parser Range
forP = keywordP "for"

formatP :: Parser Range
formatP = keywordP "format"

fromP :: Parser Range
fromP = keywordP "from"

functionP :: Parser Range
functionP = keywordP "function"

fullP :: Parser Range
fullP = keywordP "full"

globalP :: Parser Range
globalP =  keywordP "global"

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

ignoreP :: Parser Range
ignoreP = keywordP "ignore"

inP :: Parser Range
inP = keywordP "in"

includingP :: Parser Range
includingP = keywordP "including"

inPathP :: Parser Range
inPathP = keywordP "inpath"

innerP :: Parser Range
innerP = keywordP "inner"

inputFormatP :: Parser Range
inputFormatP = keywordP "inputformat"

insertP :: Parser Range
insertP = keywordP "insert"

intervalP :: Parser Range
intervalP = keywordP "interval"

intoP :: Parser Range
intoP = keywordP "into"

isP :: Parser Range
isP = keywordP "is"

itemsP :: Parser Range
itemsP = keywordP "items"

joinP :: Parser Range
joinP = keywordP "join"

keysP :: Parser Range
keysP = keywordP "keys"

ksafeP :: Parser Range
ksafeP = keywordP "ksafe"

lastP :: Parser Range
lastP = keywordP "last"

lateralP :: Parser Range
lateralP = keywordP "lateral"

leftP :: Parser Range
leftP = keywordP "left"

likeP :: Parser Range
likeP = keywordP "like"

limitP :: Parser Range
limitP = keywordP "limit"

linesP :: Parser Range
linesP = keywordP "lines"

loadP :: Parser Range
loadP =  keywordP "load"

localP :: Parser Range
localP =  keywordP "local"

localTimeP :: Parser (Text, Range)
localTimeP = ("localtime",) <$> keywordP "localtime"

localTimestampP :: Parser (Text, Range)
localTimestampP = ("localtimestamp",) <$> keywordP "localtimestamp"

locationP :: Parser Range
locationP = keywordP "location"

mapP :: Parser Range
mapP = keywordP "map"

metadataP :: Parser Range
metadataP = keywordP "metadata"

noP :: Parser Range
noP = keywordP "no"

nodeP :: Parser Range
nodeP = keywordP "node"

nodesP :: Parser Range
nodesP = keywordP "nodes"

noScanP :: Parser Range
noScanP = keywordP "noscan"

notP :: Parser Range
notP = keywordP "not"

notOperatorP :: Parser Range
notOperatorP = keywordP "not" P.<|> symbolP "!"

nullP :: Parser Range
nullP = keywordP "null"

nullsP :: Parser Range
nullsP = keywordP "nulls"

nullsequalP :: Parser Range
nullsequalP = keywordP "nullsequal"

ofP :: Parser Range
ofP = keywordP "of"

offsetP :: Parser Range
offsetP = keywordP "offset"

onP :: Parser Range
onP = keywordP "on"

orP :: Parser Range
orP = keywordP "or"

orcP :: Parser Range
orcP = keywordP "orc"

orderP :: Parser Range
orderP = keywordP "order"

overlapsP :: Parser Range
overlapsP = keywordP "overlaps"

overwriteP :: Parser Range
overwriteP = keywordP "overwrite"

outP :: Parser Range
outP = keywordP "out"

outerP :: Parser Range
outerP = keywordP "outer"

outputFormatP :: Parser Range
outputFormatP = keywordP "outputformat"

overP :: Parser Range
overP = keywordP "over"

parametersP :: Parser Range
parametersP = keywordP "parameters"

parquetP :: Parser Range
parquetP = keywordP "parquet"

partitionP :: Parser Range
partitionP = keywordP "partition"

partitionedP :: Parser Range
partitionedP = keywordP "partitioned"

percentP :: Parser Range
percentP = keywordP "percent"

precedingP :: Parser Range
precedingP = keywordP "preceding"

preserveP :: Parser Range
preserveP = keywordP "preserve"

projectionP :: Parser Range
projectionP = keywordP "projection"

projectionsP :: Parser Range
projectionsP = keywordP "projections"

protectionP :: Parser Range
protectionP = keywordP "protection"

purgeP :: Parser Range
purgeP = keywordP "purge"

randP :: Parser Range
randP = keywordP "rand"

rangeP :: Parser Range
rangeP = keywordP "range"

rcFileP :: Parser Range
rcFileP = keywordP "rcfile"

regexpP :: Parser Range
regexpP = keywordP "regexp"

reloadP :: Parser Range
reloadP = keywordP "reload"

renameP :: Parser Range
renameP = keywordP "rename"

restrictP :: Parser Range
restrictP = keywordP "restrict"

revokeP :: Parser Range
revokeP = keywordP "revoke"

rlikeP :: Parser Range
rlikeP = keywordP "rlike"

rightP :: Parser Range
rightP = keywordP "right"

rollbackP :: Parser Range
rollbackP = keywordP "rollback"

rollupP :: Parser Range
rollupP = keywordP "rollup"

rowP :: Parser Range
rowP = keywordP "row"

rowsP :: Parser Range
rowsP = keywordP "rows"

schemaP :: Parser Range
schemaP = keywordP "schema"

segmentedP :: Parser Range
segmentedP = keywordP "segmented"

selectP :: Parser Range
selectP = keywordP "select"

semicolonP :: Parser Range
semicolonP = symbolP ";"

notSemicolonP :: Parser Range
notSemicolonP = tokNotEqualsP $ TokSymbol ";"

semiP :: Parser Range
semiP = keywordP "semi"

sessionUserP :: Parser (Text, Range)
sessionUserP = ("session_user",) <$> keywordP "session_user"

sequenceFileP :: Parser Range
sequenceFileP = keywordP "sequencefile"

serdeP :: Parser Range
serdeP = keywordP "serde"

serdePropertiesP :: Parser Range
serdePropertiesP = keywordP "serdeproperties"

setP :: Parser Range
setP = keywordP "set"

setsP :: Parser Range
setsP = keywordP "sets"

showP :: Parser Range
showP = keywordP "show"

sortP :: Parser Range
sortP = keywordP "sort"

sortedP :: Parser Range
sortedP = keywordP "sorted"

statisticsP :: Parser Range
statisticsP = keywordP "statistics"

storedP :: Parser Range
storedP = keywordP "stored"

structP :: Parser Range
structP = keywordP "struct"

sysDateP :: Parser (Text, Range)
sysDateP = ("sysdate",) <$> keywordP "sysdate"

tableP :: Parser Range
tableP = keywordP "table"

tableSampleP :: Parser Range
tableSampleP = keywordP "tablesample"

tblPropertiesP :: Parser Range
tblPropertiesP = keywordP "tblproperties"

temporaryP :: Parser Range
temporaryP = keywordP "temporary" P.<|> keywordP "temp"

terminatedP :: Parser Range
terminatedP = keywordP "terminated"

textFileP :: Parser Range
textFileP = keywordP "textfile"

thenP :: Parser Range
thenP = keywordP "then"

timeseriesP :: Parser Range
timeseriesP = keywordP "timeseries"

timestampP :: Parser Range
timestampP = keywordP "timestamp"

toP :: Parser Range
toP = keywordP "to"

trueP :: Parser Range
trueP = keywordP "true"

truncateP :: Parser Range
truncateP = keywordP "truncate"

unboundedP :: Parser Range
unboundedP = keywordP "unbounded"

unionP :: Parser Range
unionP = keywordP "union"

uniontypeP :: Parser Range
uniontypeP = keywordP "uniontype"

unknownP :: Parser Range
unknownP = keywordP "unknown"

unsegmentedP :: Parser Range
unsegmentedP = keywordP "unsegmented"

useP :: Parser Range
useP = keywordP "use"

userP :: Parser (Text, Range)
userP = ("user",) <$> keywordP "user"

valuesP :: Parser Range
valuesP = keywordP "values"

viewP :: Parser Range
viewP = keywordP "view"

whenP :: Parser Range
whenP = keywordP "when"

whereP :: Parser Range
whereP = keywordP "where"

windowP :: Parser Range
windowP = keywordP "window"

withP :: Parser Range
withP = keywordP "with"

inequalityOpP :: Parser (Text, Range)
inequalityOpP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (TokSymbol op, s, e)
        | op `elem` ["<", ">", "<=", ">="] = Just (op, Range s e)

    testTok _ = Nothing

equalityOpP :: Parser (Text, Range)
equalityOpP = P.tokenPrim showTok posFromTok testTok
  where
    testTok (TokSymbol op, s, e)
        | op `elem` ["=", "==", "<=>", "<>", "!="] = Just (op, Range s e)

    testTok _ = Nothing
