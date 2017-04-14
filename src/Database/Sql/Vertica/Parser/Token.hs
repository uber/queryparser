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

module Database.Sql.Vertica.Parser.Token where


import Database.Sql.Vertica.Token
import Database.Sql.Vertica.Type
import Database.Sql.Vertica.Parser.Internal

import Database.Sql.Position

import qualified Text.Parsec as P
import qualified Text.Parsec.Pos as P

import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL
import           Data.ByteString.Lazy (ByteString)

import Data.String

import Data.Semigroup ((<>))


showTok :: (Token, Position, Position) -> String
showTok (t, _, _) = show t

posFromTok :: (Token, Position, Position) -> P.SourcePos
posFromTok (_, pos, _) = flip P.setSourceLine (fromEnum $ positionLine pos)
                       $ flip P.setSourceColumn (fromEnum $ positionColumn pos)
                       $ P.initialPos "-"

tokEqualsP :: Token -> Parser Range
tokEqualsP tok = P.token showTok posFromTok testTok
  where
    testTok (tok', s, e) =
        if tok == tok'
         then Just $ Range s e
         else Nothing

tokNotEqualsP :: Token -> Parser Range
tokNotEqualsP tok = P.token showTok posFromTok testTok
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

typeNameP :: Parser (Text, Range)
typeNameP = P.token showTok posFromTok testNameTok

nodeNameP :: Parser (Text, Range)
nodeNameP = P.token showTok posFromTok testNameTok

databaseNameP :: Parser (Text, Range)
databaseNameP = P.token showTok posFromTok testNameTok

userNameP :: Parser (Text, Range)
userNameP = P.token showTok posFromTok testNameTok

paramNameP :: Parser (Text, Range)
paramNameP = P.token showTok posFromTok testNameTok

windowNameP :: Parser (Text, Range)
windowNameP = P.token showTok posFromTok testNameTok

parserNameP :: Parser (Text, Range)
parserNameP = P.token showTok posFromTok testNameTok

datePartP :: Parser (Text, Range)
datePartP = P.token showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord _ name | TL.toLower name `elem` parts -> Just (TL.toLower name, Range s e)
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
schemaNameP = P.token showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeSchemaName (wordInfo name) -> Just (name, Range s e)

        _ -> Nothing


tableNameP :: Parser (Text, Range)
tableNameP = P.token showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeTableName (wordInfo name) -> Just (name, Range s e)

        _ -> Nothing


projectionNameP :: Parser (Text, Range)
projectionNameP = P.token showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeTableName (wordInfo name) -> Just (name, Range s e)

        _ -> Nothing


columnNameP :: Parser (Text, Range)
columnNameP = P.token showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeColumnName (wordInfo name) -> Just (name, Range s e)

        _ -> Nothing


functionNameP :: Parser (Text, Range)
functionNameP = P.token showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord True name -> Just (name, Range s e)
        TokWord False name
            | wordCanBeFunctionName (wordInfo name) ->
                Just (name, Range s e)

        _ -> Nothing

constraintNameP :: Parser (Text, Range)
constraintNameP = P.token showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        -- constraint names MAY be quoted.
        TokWord _ name -> Just (name, Range s e)
        _ -> Nothing

keywordP :: Text -> Parser Range
keywordP keyword = P.token showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokWord False name
            | name == keyword -> Just (Range s e)

        _ -> Nothing

fieldTypeP :: Parser (Text, Range)
fieldTypeP = P.token showTok posFromTok testTok
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
periodP = P.token showTok posFromTok testTok
  where
    isPeriod :: (Eq s, IsString s) => s -> Bool
    isPeriod = flip elem [ "day", "hour", "minute", "month", "second", "year" ]

    testTok (tok, s, e) = case tok of
        TokWord _ period | isPeriod period -> Just (period, Range s e)
        TokString period | isPeriod period -> Just (TL.decodeUtf8 period, Range s e)
        _ -> Nothing

stringP :: Parser (ByteString, Range)
stringP = P.token showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokString string -> Just (string, Range s e)
        _ -> Nothing

numberP :: Parser (Text, Range)
numberP = P.token showTok posFromTok testTok
  where
    testTok (tok, s, e) = case tok of
        TokNumber number -> Just (number, Range s e)
        _ -> Nothing

encodingTypeP :: Parser (Encoding Range)
encodingTypeP =  P.token showTok posFromTok testTok
  where
    testTok (tok, s, e) =
        let r = Range s e
         in case tok of
                TokWord False "auto" -> Just (EncodingAuto r)
                TokWord False "block_dict" -> Just (EncodingBlockDict r)
                TokWord False "blockdict_comp" -> Just (EncodingBlockDictComp r)
                TokWord False "bzip_comp" -> Just (EncodingBZipComp r)
                TokWord False "commondelta_comp" -> Just (EncodingCommonDeltaComp r)
                TokWord False "deltarange_comp" -> Just (EncodingDeltaRangeComp r)
                TokWord False "deltaval" -> Just (EncodingDeltaVal r)
                TokWord False "gcddelta" -> Just (EncodingGCDDelta r)
                TokWord False "gzip_comp" -> Just (EncodingGZipComp r)
                TokWord False "rle" -> Just (EncodingRLE r)
                TokWord False "none" -> Just (EncodingNone r)

                _ -> Nothing

dotP :: Parser Range
dotP = symbolP "."

equalP :: Parser Range
equalP = symbolP "="

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

castP :: Parser Range
castP = keywordP "cast"

castOpP :: Parser Range
castOpP = symbolP "::"

minusP :: Parser Range
minusP = symbolP "-"

abortP :: Parser Range
abortP = keywordP "abort"

accessP :: Parser Range
accessP = keywordP "access"

accessRankP :: Parser Range
accessRankP = keywordP "accessrank"

addP :: Parser Range
addP = keywordP "add"

aggregateP :: Parser Range
aggregateP = keywordP "aggregate"

allP :: Parser Range
allP = keywordP "all"

alterP :: Parser Range
alterP = keywordP "alter"

analyticP :: Parser Range
analyticP = keywordP "analytic"

andP :: Parser Range
andP = keywordP "and"

anyP :: Parser Range
anyP = keywordP "any"

arrayP :: Parser Range
arrayP = keywordP "array"

asP :: Parser Range
asP = keywordP "as"

ascP :: Parser Range
ascP = keywordP "asc"

atP :: Parser Range
atP = keywordP "at"

authorizationP :: Parser Range
authorizationP = keywordP "authorization"

autoP :: Parser Range
autoP = keywordP "auto"

beginP :: Parser Range
beginP = keywordP "begin"

bestP :: Parser Range
bestP = keywordP "best"

betweenP :: Parser Range
betweenP = keywordP "between"

byP :: Parser Range
byP = keywordP "by"

bytesP :: Parser Range
bytesP = keywordP "bytes"

bzipP :: Parser Range
bzipP = keywordP "bzip"

cascadeP :: Parser Range
cascadeP = keywordP "cascade"

caseP :: Parser Range
caseP = keywordP "case"

checkP :: Parser Range
checkP = keywordP "check"

colSizesP :: Parser Range
colSizesP = keywordP "colsizes"

columnP :: Parser Range
columnP = keywordP "column"

commaP :: Parser Range
commaP = symbolP ","

commitP :: Parser Range
commitP = keywordP "commit"

committedP :: Parser Range
committedP = keywordP "committed"

connectP :: Parser Range
connectP = keywordP "connect"

constraintP :: Parser Range
constraintP = keywordP "constraint"

copyP :: Parser Range
copyP = keywordP "copy"

createP :: Parser Range
createP = keywordP "create"

crossP :: Parser Range
crossP = keywordP "cross"

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

dateDiffP :: Parser Range
dateDiffP = keywordP "datediff"

defaultP :: Parser Range
defaultP = keywordP "default"

deleteP :: Parser Range
deleteP = keywordP "delete"

delimiterP :: Parser Range
delimiterP = keywordP "delimiter"

descP :: Parser Range
descP = keywordP "desc"

directP :: Parser Range
directP = keywordP "direct"

disableP :: Parser Range
disableP = keywordP "disable"

disabledP :: Parser Range
disabledP = keywordP "disabled"

disconnectP :: Parser Range
disconnectP = keywordP "disconnect"

distinctP :: Parser Range
distinctP = keywordP "distinct"

doubleP :: Parser Range
doubleP = keywordP "double"

dropP :: Parser Range
dropP = keywordP "drop"

elseP :: Parser Range
elseP = keywordP "else"

enableP :: Parser Range
enableP = keywordP "enable"

enabledP :: Parser Range
enabledP = keywordP "enabled"

enclosedP :: Parser Range
enclosedP = keywordP "enclosed"

encodingP :: Parser Range
encodingP = keywordP "encoding"

endP :: Parser Range
endP = keywordP "end"

enforceLengthP :: Parser Range
enforceLengthP = keywordP "enforcelength"

errorP :: Parser Range
errorP = keywordP "error"

escapeP :: Parser Range
escapeP = keywordP "escape"

exceptP :: Parser Range
exceptP = keywordP "except"

exceptionsP :: Parser Range
exceptionsP = keywordP "exceptions"

excludeP :: Parser Range
excludeP = keywordP "exclude"

excludingP :: Parser Range
excludingP = keywordP "excluding"

existsP :: Parser Range
existsP = keywordP "exists"

explainP :: Parser Range
explainP = keywordP "explain"

exportP :: Parser Range
exportP = keywordP "export"

externalP :: Parser Range
externalP = keywordP "external"

extractP :: Parser Range
extractP = keywordP "extract"

falseP :: Parser Range
falseP = keywordP "false"

fillerP :: Parser Range
fillerP = keywordP "filler"

filterP :: Parser Range
filterP = keywordP "filter"

firstP :: Parser Range
firstP = keywordP "first"

fixedWidthP :: Parser Range
fixedWidthP = keywordP "fixedwidth"

followingP :: Parser Range
followingP = keywordP "following"

forP :: Parser Range
forP = keywordP "for"

foreignP :: Parser Range
foreignP = keywordP "foreign"

formatP :: Parser Range
formatP = keywordP "format"

fromP :: Parser Range
fromP = keywordP "from"

fullP :: Parser Range
fullP = keywordP "full"

functionP :: Parser Range
functionP = keywordP "function"

globalP :: Parser Range
globalP =  keywordP "global"

grantP :: Parser Range
grantP = keywordP "grant"

groupP :: Parser Range
groupP = keywordP "group"

gzipP :: Parser Range
gzipP = keywordP "gzip"

havingP :: Parser Range
havingP = keywordP "having"

ifP :: Parser Range
ifP = keywordP "if"

ignoreP :: Parser Range
ignoreP = keywordP "ignore"

iLikeP :: Parser Range
iLikeP = keywordP "ilike"

iLikeBP :: Parser Range
iLikeBP = keywordP "ilikeb"

inP :: Parser Range
inP = keywordP "in"

includeP :: Parser Range
includeP = keywordP "include"

includingP :: Parser Range
includingP = keywordP "including"

innerP :: Parser Range
innerP = keywordP "inner"

insertP :: Parser Range
insertP = keywordP "insert"

intersectP :: Parser Range
intersectP = keywordP "intersect"

intervalP :: Parser Range
intervalP = keywordP "interval"

intoP :: Parser Range
intoP = keywordP "into"

isP :: Parser Range
isP = keywordP "is"

isnullP :: Parser Range
isnullP = keywordP "isnull"

isolationP :: Parser Range
isolationP = keywordP "isolation"

joinP :: Parser Range
joinP = keywordP "join"

keyP :: Parser Range
keyP = keywordP "key"

ksafeP :: Parser Range
ksafeP = keywordP "ksafe"

lastP :: Parser Range
lastP = keywordP "last"

leftP :: Parser Range
leftP = keywordP "left"

levelP :: Parser Range
levelP = keywordP "level"

likeP :: Parser Range
likeP = keywordP "like"

likeBP :: Parser Range
likeBP = keywordP "likeB"

limitP :: Parser Range
limitP = keywordP "limit"

localP :: Parser Range
localP =  keywordP "local"

localTimeP :: Parser (Text, Range)
localTimeP = ("localtime",) <$> keywordP "localtime"

localTimestampP :: Parser (Text, Range)
localTimestampP = ("localtimestamp",) <$> keywordP "localtimestamp"

longP :: Parser Range
longP = keywordP "long"

lzoP :: Parser Range
lzoP = keywordP "lzo"

matchedP :: Parser Range
matchedP = keywordP "matched"

mergeP :: Parser Range
mergeP = keywordP "merge"

nameP :: Parser Range
nameP = keywordP "name"

nativeP :: Parser Range
nativeP = keywordP "native"

naturalP :: Parser Range
naturalP = keywordP "natural"

noP :: Parser Range
noP = keywordP "no"

nodeP :: Parser Range
nodeP = keywordP "node"

nodesP :: Parser Range
nodesP = keywordP "nodes"

notP :: Parser Range
notP = keywordP "not"

notnullP :: Parser Range
notnullP = keywordP "notnull"

nullsP :: Parser Range
nullsP = keywordP "nulls"

nullsequalP :: Parser Range
nullsequalP = keywordP "nullsequal"

nullP :: Parser Range
nullP = keywordP "null"

nullColsP :: Parser Range
nullColsP = keywordP "nullcols"

offsetP :: Parser Range
offsetP = keywordP "offset"

onP :: Parser Range
onP = keywordP "on"

onlyP :: Parser Range
onlyP = keywordP "only"

optionP :: Parser Range
optionP = keywordP "option"

orP :: Parser Range
orP = keywordP "or"

orcP :: Parser Range
orcP = keywordP "orc"

orderP :: Parser Range
orderP = keywordP "order"

overlapsP :: Parser Range
overlapsP = keywordP "overlaps"

outerP :: Parser Range
outerP = keywordP "outer"

overP :: Parser Range
overP = keywordP "over"

parametersP :: Parser Range
parametersP = keywordP "parameters"

parquetP :: Parser Range
parquetP = keywordP "parquet"

parserP :: Parser Range
parserP = keywordP "parser"

partitionP :: Parser Range
partitionP = keywordP "partition"

passwordP :: Parser Range
passwordP = keywordP "password"

poolP :: Parser Range
poolP = keywordP "pool"

policyP :: Parser Range
policyP = keywordP "policy"

precedingP :: Parser Range
precedingP = keywordP "preceding"

precisionP :: Parser Range
precisionP = keywordP "precision"

preserveP :: Parser Range
preserveP = keywordP "preserve"

primaryP :: Parser Range
primaryP = keywordP "primary"

privilegesP :: Parser Range
privilegesP = keywordP "privileges"

projectionP :: Parser Range
projectionP = keywordP "projection"

projectionsP :: Parser Range
projectionsP = keywordP "projections"

rangeP :: Parser Range
rangeP = keywordP "range"

readP :: Parser Range
readP = keywordP "read"

recordP :: Parser Range
recordP = keywordP "record"

referencesP :: Parser Range
referencesP = keywordP "references"

rejectedP :: Parser Range
rejectedP = keywordP "rejected"

rejectMaxP :: Parser Range
rejectMaxP = keywordP "rejectmax"

renameP :: Parser Range
renameP = keywordP "rename"

repeatableP :: Parser Range
repeatableP = keywordP "repeatable"

replaceP :: Parser Range
replaceP = keywordP "replace"

resourceP :: Parser Range
resourceP = keywordP "resource"

restrictP :: Parser Range
restrictP = keywordP "restrict"

revokeP :: Parser Range
revokeP = keywordP "revoke"

rightP :: Parser Range
rightP = keywordP "right"

rollbackP :: Parser Range
rollbackP = keywordP "rollback"

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

setP :: Parser Range
setP = keywordP "set"

serializableP :: Parser Range
serializableP = keywordP "serializable"

sessionP :: Parser Range
sessionP = keywordP "session"

sessionUserP :: Parser (Text, Range)
sessionUserP = ("session_user",) <$> keywordP "session_user"

showP :: Parser Range
showP = keywordP "show"

skipP :: Parser Range
skipP = keywordP "skip"

sourceP :: Parser Range
sourceP = keywordP "source"

startP :: Parser Range
startP = keywordP "start"

stdinP :: Parser Range
stdinP = keywordP "stdin"

stdoutP :: Parser Range
stdoutP = keywordP "stdout"

storageP :: Parser Range
storageP = keywordP "storage"

streamP :: Parser Range
streamP = keywordP "stream"

sysDateP :: Parser (Text, Range)
sysDateP = ("sysdate",) <$> keywordP "sysdate"

tableP :: Parser Range
tableP = keywordP "table"

temporaryP :: Parser Range
temporaryP = keywordP "temporary" P.<|> keywordP "temp"

terminatorP :: Parser Range
terminatorP = keywordP "terminator"

thenP :: Parser Range
thenP = keywordP "then"

timeseriesP :: Parser Range
timeseriesP = keywordP "timeseries"

timestampP :: Parser Range
timestampP = keywordP "timestamp" P.<|> do
    r <- keywordP "time"
    r' <- keywordP "stamp"
    return $ r <> r'

timezoneP :: Parser Range
timezoneP = keywordP "timezone" P.<|> do
    r <- keywordP "time"
    r' <- keywordP "zone"
    return $ r <> r'

toleranceP :: Parser Range
toleranceP = keywordP "tolerance"

trailingP :: Parser Range
trailingP = keywordP "trailing"

transactionP :: Parser Range
transactionP = keywordP "transaction"

transformP :: Parser Range
transformP = keywordP "transform"

trickleP :: Parser Range
trickleP = keywordP "trickle"

trimP :: Parser Range
trimP = keywordP "trim"

trueP :: Parser Range
trueP = keywordP "true"

truncateP :: Parser Range
truncateP = keywordP "truncate"

toP :: Parser Range
toP = keywordP "to"

unboundedP :: Parser Range
unboundedP = keywordP "unbounded"

uncommittedP :: Parser Range
uncommittedP = keywordP "uncommitted"

uncompressedP :: Parser Range
uncompressedP = keywordP "uncompressed"

unionP :: Parser Range
unionP = keywordP "union"

uniqueP :: Parser Range
uniqueP = keywordP "unique"

unknownP :: Parser Range
unknownP = keywordP "unknown"

unsegmentedP :: Parser Range
unsegmentedP = keywordP "unsegmented"

updateP :: Parser Range
updateP = keywordP "update"

userP :: Parser (Text, Range)
userP = ("user",) <$> keywordP "user"

usingP :: Parser Range
usingP = keywordP "using"

valuesP :: Parser Range
valuesP = keywordP "values"

varBinaryP :: Parser (Text, Range)
varBinaryP = ("varbinary", ) <$> keywordP "varbinary"

varCharP :: Parser (Text, Range)
varCharP = ("varchar", ) <$> keywordP "varchar"

verticaP :: Parser Range
verticaP = keywordP "vertica"

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

withoutP :: Parser Range
withoutP = keywordP "without"

workP :: Parser Range
workP = keywordP "work"

writeP :: Parser Range
writeP = keywordP "write"

likeOpP :: Parser Range
likeOpP = symbolP "~~"

iLikeOpP :: Parser Range
iLikeOpP = symbolP "~~*"

notLikeOpP :: Parser Range
notLikeOpP = symbolP "!~~"

notILikeOpP :: Parser Range
notILikeOpP = symbolP "!~~*"

regexMatchesP :: Parser Range
regexMatchesP = symbolP "~"

notRegexMatchesP :: Parser Range
notRegexMatchesP = symbolP "!~"

regexIgnoreCaseMatchesP :: Parser Range
regexIgnoreCaseMatchesP = symbolP "~*"

notRegexIgnoreCaseMatchesP :: Parser Range
notRegexIgnoreCaseMatchesP = symbolP "!~*"

inequalityOpP :: Parser (Text, Range)
inequalityOpP = P.token showTok posFromTok testTok
  where
    -- <=> is actually an equality, but it seems to parse here
    testTok (TokSymbol op, s, e)
        | op `elem` ["<", ">", "<>", "<=>", "!="] = Just (op, Range s e)

    testTok _ = Nothing

equalityOpP :: Parser (Text, Range)
equalityOpP = P.token showTok posFromTok testTok
  where
    testTok (TokSymbol op, s, e)
        | op `elem` ["<=", ">=", "="] = Just (op, Range s e)

    testTok _ = Nothing
