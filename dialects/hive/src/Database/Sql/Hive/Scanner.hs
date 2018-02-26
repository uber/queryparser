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

module Database.Sql.Hive.Scanner where

import Prelude hiding ((&&), (||), not)

import Data.Int (Int64)
import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL
import           Data.ByteString.Lazy (ByteString)

import Data.List (sortBy)
import Data.Foldable (asum)
import Data.Char (isAlphaNum, isAlpha, isSpace, isDigit)

import Control.Monad.State

import Database.Sql.Position
import Database.Sql.Hive.Token

import Data.Predicate.Class


isWordBody :: Char -> Bool
isWordBody = isAlphaNum || (== '_') || (== '$')

isHSpace :: Char -> Bool
isHSpace = isSpace && not (== '\n')

operators :: [Text]
operators = sortBy (flip compare)
    [ "+", "-", "*", "/", "%"
    , "||"
    , "&", "|", "^", "~"
    , "!"
    , ":"
    , "!=", "<>", ">", "<", ">=", "<=", "<=>", "=", "=="
    , "(", ")", "[", "]", ",", ";"
    ]

isOperator :: Char -> Bool
isOperator c = elem c $ map TL.head operators

isPlusOrMinus :: Text -> Bool
isPlusOrMinus s = elem s ["+", "-"]

parseNumber :: Text -> ((Token, Int64), Text)
parseNumber = runState $ do
    ipart <- state $ TL.span isDigit
    gets (TL.take 1) >>= \case
        "" -> pure (TokNumber ipart, TL.length ipart)
        "." -> do
            modify $ TL.drop 1
            fpart <- state $ TL.span isDigit
            gets (TL.take 1) >>= \case
                "e" -> do
                    modify $ TL.drop 1
                    sign <- gets (TL.take 1) >>= \case
                        s | isPlusOrMinus s -> modify (TL.drop 1) >> pure s
                        _ -> pure ""
                    state (TL.span isDigit) >>= \ epart ->
                        let number = TL.concat [ipart, ".", fpart, "e", sign, epart]
                         in pure ( if TL.null epart
                                    then TokError "..."
                                    else TokNumber number
                                 , TL.length number
                                 )
                _ ->
                    let number = TL.concat [ipart, ".", fpart]
                     in pure ( TokNumber number, TL.length number )
        "e" -> do
            modify $ TL.drop 1
            sign <- gets (TL.take 1) >>= \case
                s | isPlusOrMinus s -> modify (TL.drop 1) >> pure s
                _ -> pure ""
            epart <- state $ TL.span isDigit
            gets (TL.take 1) >>= \case
                c | (not . TL.null && isWordBody . TL.head) c || TL.null epart -> do
                    rest <- state $ TL.span isWordBody
                    let word = TL.concat [ipart, "e", sign, epart, rest]
                    pure (TokWord False word, TL.length word)
                  | otherwise ->
                    let number = TL.concat [ipart, "e", sign, epart]
                     in pure (TokNumber number, TL.length number)

        c | (isAlpha || (== '_')) (TL.head c) -> do
            rest <- state $ TL.span (isAlpha || (== '_'))
            let word = TL.concat [ipart, rest]
            pure (TokWord False word, TL.length word)

        _ -> pure (TokNumber ipart, TL.length ipart)

tokenize :: Text -> [(Token, Position, Position)]
tokenize = go (Position 1 0 0)
  where
    go :: Position -> Text -> [(Token, Position, Position)]
    go _ "" = []
    go p t = case TL.head t of
        c | isAlpha c ->
            case tokUnquotedWord p t of
                (name, rest, p') -> (TokWord False name, p, p') : go p' rest

        c | isDigit c ->
            let ((token, len), rest) = parseNumber t
                p' = advanceHorizontal len p
             in (token, p, p') : go p' rest

        '$' | "${" `TL.isPrefixOf` t ->
            let ((token, len), rest) = parseVariable t
                p' = advanceHorizontal len p
             in (token, p, p') : go p' rest

        '`' ->
            case tokQuotedWord p t of
                Left p' -> [(TokError "end of input inside name", p, p')]
                Right (name, rest, p') -> (TokWord True name, p, p') : go p' rest

        c | (== '\n') c -> let (newlines, rest) = TL.span (== '\n') t
                               p' = advanceVertical (TL.length newlines) p
                            in go p' rest

        c | isHSpace c -> let (spaces, rest) = TL.span isHSpace t
                              p' = advanceHorizontal (TL.length spaces) p
                           in go p' rest

        -- comments
        '-' | "--" `TL.isPrefixOf` t ->
            let (comment, rest) = TL.span (/= '\n') t
                p' = advanceVertical 1
                        (advanceHorizontal (TL.length comment) p)

             in go p' (TL.drop 1 rest)

        -- Join hints
        -- T399601: restrict to context aware join hints, which should follow a select
        '/' | "/*+" `TL.isPrefixOf` t ->
            case TL.breakOn "*/" t of
                (comment, "") ->
                    let p' = advance comment p
                     in [(TokError "unterminated join hint", p, p')]
                (comment, rest) ->
                    let p' = advance (TL.append comment "*/") p
                     in go p' $ TL.drop 2 rest

        c | c == '\'' || c == '"' ->
            case tokString c p t of
                Left (tok, p') -> [(tok, p, p')]
                Right (string, rest, p') -> (TokString string, p, p') : go p' rest

        '.' ->
            let p' = advanceHorizontal 1 p
             in (TokSymbol ".", p, p') : go p' (TL.tail t)


        c | isOperator c -> case readOperator t of

            Just (sym, rest) -> let p' = advanceHorizontal (TL.length sym) p
                                 in (TokSymbol sym, p, p') : go p' rest

            Nothing ->
                let opchars = TL.take 5 t
                    p' = advance opchars p
                    message = unwords
                        [ "unrecognized operator starting with"
                        , show opchars
                        ]
                 in [(TokError message, p, p')]

        c ->
            let message = unwords
                    [ "unmatched character ('" ++ show c ++ "') at position"
                    , show p
                    ]

             in [(TokError message, p, advanceHorizontal 1 p)]

    readOperator t = asum
        $ map (\ op -> (op,) <$> TL.stripPrefix op t) operators

tokUnquotedWord :: Position -> Text -> (Text, Text, Position)
tokUnquotedWord pos input =
    case TL.span (isAlphaNum || (== '_')) input of
        (word, rest) -> (TL.toLower word, rest, advanceHorizontal (TL.length word) pos)

tokQuotedWord :: Position -> Text -> Either Position (Text, Text, Position)
tokQuotedWord pos = go (advanceHorizontal 1 pos) [] . TL.tail
  where
    go p _ "" = Left p
    go p ts input = case TL.head input of
         c | c == '`' ->
            let (quotes, rest) = TL.span (== '`') input
                len = TL.length quotes
             in if len `mod` 2 == 0
                 then go (advanceHorizontal len p)
                         (TL.take (len `div` 2) quotes :ts)
                         rest

                 else Right ( TL.concat $ reverse (TL.take (len `div` 2) quotes : ts)
                            , rest
                            , advanceHorizontal len p
                            )

         _ -> let (t, rest) = TL.span (/= '`') input
               in go (advance t p) (t:ts) rest

tokString :: Char -> Position -> Text -> Either (Token, Position) (ByteString, Text, Position)
tokString quote pos = go (advanceHorizontal 1 pos) [] . TL.drop 1
  where
    boring = not . (`elem` [quote, '\\'])
    halve txt = TL.take (TL.length txt `div` 2) txt
    go p ts input = case TL.span boring input of
        (cs, "") -> Left (TokError "end of input inside string", advance cs p)
        ("", rest) -> handleSlashes p ts rest
        (cs, rest) -> handleSlashes (advance cs p) (cs:ts) rest

    handleSlashes p ts input = case TL.span (== '\\') input of
        (cs, "") -> Left (TokError "end of input inside string", advance cs p)
        ("", _) -> handleQuote p ts input
        (slashes, rest) ->
            let len = TL.length slashes
             in if len `mod` 2 == 0
                 then go (advanceHorizontal len p) (halve slashes:ts) rest
                 else case TL.splitAt 1 rest of
                    (c, rest')
                        | c == "a" -> go (advanceHorizontal (len + 1) p) ("\a":halve slashes:ts) rest'
                        | c == "b" -> go (advanceHorizontal (len + 1) p) ("\BS":halve slashes:ts) rest'
                        | c == "f" -> go (advanceHorizontal (len + 1) p) ("\FF":halve slashes:ts) rest'
                        | c == "n" -> go (advanceHorizontal (len + 1) p) ("\n":halve slashes:ts) rest'
                        | c == "r" -> go (advanceHorizontal (len + 1) p) ("\r":halve slashes:ts) rest'
                        | c == "t" -> go (advanceHorizontal (len + 1) p) ("\t":halve slashes:ts) rest'
                        | c == "v" -> go (advanceHorizontal (len + 1) p) ("\v":halve slashes:ts) rest'
                        | c == "'" -> go (advanceHorizontal (len + 1) p) ("'":halve slashes:ts) rest'
                        | c == "\"" -> go (advanceHorizontal (len + 1) p) ("\"":halve slashes:ts) rest'
                        | otherwise -> go (advanceHorizontal (len + 1) p) (c:"\\":halve slashes:ts) rest'

    handleQuote p ts input = case TL.splitAt 1 input of
        (c, rest) | c == TL.singleton quote ->
            Right ( TL.encodeUtf8 $ TL.concat $ reverse ts
                  , rest
                  , advanceHorizontal 1 p
                  )
        x -> error $ "this shouldn't happen: handleQuote splitInput got " ++ show x

parseVariable :: Text -> ((Token, Int64), Text)
parseVariable = runState $ do
    let endOfInput = "end of input inside variable substitution"
        missingNamespaceOrName = "variable substitutions must have a namespace and a name"
        colon = (== ':')
        closeCurly = (== '}')

    modify $ TL.drop 2  -- consume the initial ${
    namespace <- state (TL.break colon)

    gets (TL.take 1) >>= \case
        "" -> -- there was no : in the rest of the text
            let varLen = 2 + TL.length namespace
             in if (not $ TL.null namespace) && (TL.last namespace == '}')
                then pure (TokError missingNamespaceOrName, varLen)
                else pure (TokError endOfInput, varLen)

        _ -> do
            modify $ TL.drop 1  -- consume the :
            gets (TL.take 2) >>= \case
                "${" -> do -- RECURSE!
                    ((subName, subLen), rest) <- gets parseVariable
                    _ <- put rest
                    gets (TL.take 1) >>= \case
                        "}" -> do
                            modify $ TL.drop 1  -- consume the }
                            let varLen = 2 + TL.length namespace + 1 + subLen + 1
                                varName = TokVariable namespace $ DynamicName subName
                            pure $ liftInnerErrors $ enforceNamespaces $ (varName, varLen)
                        _ -> do
                            let varLen = 2 + TL.length namespace + 1 + subLen
                            pure (TokError endOfInput, varLen)

                _ -> do
                    name <- state (TL.break closeCurly)
                    gets (TL.take 1) >>= \case
                        "" -> -- there was no } in the rest of the text
                            let varLen = 2 + TL.length namespace + 1 + TL.length name
                             in pure (TokError endOfInput, varLen)
                        _ -> do
                            modify $ TL.drop 1  -- consume the }
                            let varLen = 2 + TL.length namespace + 1 + TL.length name + 1
                            if TL.null name
                            then pure (TokError missingNamespaceOrName, varLen)
                            else pure $ enforceNamespaces $ (TokVariable namespace $ StaticName name, varLen)
  where
    enforceNamespaces tok@(TokVariable ns _, len) =
        let allowedNamespaces = ["hiveconf", "system", "env", "define", "hivevar"]
            permitted = (`elem` allowedNamespaces)
         in if permitted ns
            then tok
            else (TokError $ "bad namespace in variable substitution: " ++ show ns, len)
    enforceNamespaces x = x

    liftInnerErrors (TokVariable _ (DynamicName (TokError msg)), len) = (TokError msg, len)
    liftInnerErrors x = x
