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

module Database.Sql.Presto.Scanner where

import Control.Monad.State

import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Builder as BB

import Data.List (sortBy)
import Data.Foldable (asum)
import Data.Char (isAlphaNum, isAlpha, isSpace, isDigit, isHexDigit)
import Data.Int (Int64)

import Database.Sql.Position
import Database.Sql.Presto.Token

import Control.Applicative (liftA2)

import Numeric (readHex)


isWordHead :: Char -> Bool
isWordHead = liftA2 (||) isAlpha (== '_')

isWordBody :: Char -> Bool
isWordBody = liftA2 (||) isAlphaNum (== '_')

isHSpace :: Char -> Bool
isHSpace = liftA2 (&&) isSpace (/= '\n')

operators :: [Text]
operators = sortBy (flip compare)
    [ "+", "-", "*", "/", "%"
    , "!=", "<>", ">", "<", ">=", "<=", "="
    , "||"
    , "(", ")", "[", "]", ",", ";"
    , "?"
    , "->"
    ]

isOperator :: Char -> Bool
isOperator c = elem c $ map TL.head operators

tokenize :: Text -> [(Token, Position, Position)]
tokenize = go (Position 1 0 0)
  where
    go :: Position -> Text -> [(Token, Position, Position)]
    go _ "" = []
    go p t = case TL.head t of
        c | c `elem` ['x', 'X'] && TL.length t >= 2 && TL.index t 1 == '\'' ->
            case tokBinaryLiteral p $ TL.drop 2 t of
                Left (err, p') -> [(TokError err, p, p')]
                Right (bytes, rest, p') -> (TokBinary bytes, p, p') : go p' rest


        c | isWordHead c || c == '"' ->
            case tokName p t of
                Left token -> [token]
                Right (name, quoted, rest, p') -> (TokWord quoted name, p, p') : go p' rest

        c | (== '\n') c -> let (newlines, rest) = TL.span (== '\n') t
                               p' = advanceVertical (TL.length newlines) p
                            in go p' rest

        c | isHSpace c -> let (spaces, rest) = TL.span isHSpace t
                              p' = advanceHorizontal (TL.length spaces) p
                           in go p' rest

        '-' | "--" `TL.isPrefixOf` t ->
            let (comment, rest) = TL.span (/= '\n') t
                p' = advanceVertical 1
                        (advanceHorizontal (TL.length comment) p)

             in go p' (TL.drop 1 rest)

        '/' | "/*" `TL.isPrefixOf` t ->
            case TL.breakOn "*/" t of
                (comment, "") ->
                    let p' = advance comment p
                     in [(TokError "unterminated comment", p, p')]
                (comment, rest) ->
                    let p' = advance (TL.append comment "*/") p
                     in go p' $ TL.drop 2 rest

        '\'' ->
            case tokString p '\'' t of
                Left p' -> [(TokError "end of input inside string", p, p')]
                Right (string, rest, p') -> (TokString $ TL.encodeUtf8 string, p, p') : go p' rest

        '.' ->
            case TL.span isDigit (TL.tail t) of
                ("", _) ->
                    let p' = advanceHorizontal 1 p
                     in (TokSymbol ".", p, p') : go p' (TL.tail t)

                _ ->
                    let ((tok, len), _) = tokNumber t
                        p' = advanceHorizontal len p
                        rest = TL.drop len t
                     in (tok, p, p') : go p' rest

        c | isDigit c ->
            let ((tok, len), _) = tokNumber t
                p' = advanceHorizontal len p
                rest = TL.drop len t
             in (tok, p, p') : go p' rest

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
                    [ "unmatched character (" ++ show c ++ ") at position"
                    , show p
                    ]

             in [(TokError message, p, advanceHorizontal 1 p)]

    readOperator t = asum
        $ map (\ op -> (op,) <$> TL.stripPrefix op t) operators


tokString :: Position -> Char -> Text -> Either Position (Text, Text, Position)
tokString pos d = go (advanceHorizontal 1 pos) [] . TL.tail
  where
    go p _ "" = Left p
    go p ts input = case TL.head input of
         c | c == d ->
            let (quotes, rest) = TL.span (== d) input
                len = TL.length quotes
                t = TL.take (len `div` 2) quotes
            in if len `mod` 2 == 0
                then go (advanceHorizontal len p) (t:ts) rest

                else let str = TL.concat $ reverse $ t:ts
                         p' = advanceHorizontal len p
                      in Right (str, rest, p')

         _ -> let (t, rest) = TL.span (/= d) input
               in go (advance t p) (t:ts) rest


tokBinaryLiteral :: Position -> Text -> Either (String, Position) (ByteString, Text, Position)
tokBinaryLiteral p "" = Left ("end of input inside binary literal", p)
tokBinaryLiteral p t =
    let (body, rest) = TL.span (/= '\'')t
        collapsed = TL.filter (not . isSpace) body
        bad_digits = TL.filter (not . isHexDigit) collapsed
        good_digits = TL.filter isHexDigit collapsed
        has_even_num_digits = (TL.length good_digits) `mod` 2 == 0
        p' = advanceHorizontal 2 $ advance body $ advanceHorizontal 1 p
     in case () of
          _ | TL.null rest -> Left ("end of input inside binary literal", p)
          _ | not $ TL.null bad_digits -> Left ("binary literal must only have hex-digits", p')
          _ | not has_even_num_digits -> Left ("binary literal must contain an even number of hex-digits", p')
          _ -> Right (toByteString good_digits, TL.drop 1 rest, p')
  where
    toByteString :: Text -> ByteString
    toByteString "" = ""
    toByteString t' = let [(num, _)] = (readHex $ TL.unpack t') :: [(Int, String)]
                      in BB.toLazyByteString $ BB.word8 $ fromIntegral num

tokName :: Position -> Text -> Either (Token, Position, Position) (Text, Bool, Text, Position)
tokName pos = go pos
  where
    go :: Position -> Text -> Either (Token, Position, Position) (Text, Bool, Text, Position)
    go p ""  = error $ "unexpected call to tokName (at EOF) at " ++ show p
    go p input = case TL.head input of
      c | isWordHead c ->
        let (body, rest) = TL.span isWordBody $ TL.tail input
            word = TL.toLower $ TL.cons c body
            p' = advanceHorizontal (TL.length word) p
         in Right (word, False, rest, p')

      c | c == '"' ->
        case tokString p '"' input of
            Left p' -> Left (TokError "end of input inside quoted name", p, p')
            Right (quoted, rest, p') -> Right (quoted, True, rest, p')

      _ -> error $ "unexpected call to tokName (not at word head) at " ++ show p

tokNumber :: Text -> ((Token, Int64), Text)
tokNumber = runState $ do
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
                        let number = case True of
                                       _ | TL.null fpart -> TL.concat [ipart, ".", fpart]
                                       _ | TL.null epart -> TL.concat [ipart, ".", fpart]
                                       _ -> TL.concat [ipart, ".", fpart, "e", sign, epart]
                         in pure ( TokNumber number, TL.length number )

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
                c | (liftA2 (&&) (not . TL.null) (isWordBody . TL.head)) c || TL.null epart -> do
                    rest <- state $ TL.span isWordBody
                    let word = TL.concat [ipart, "e", sign, epart, rest]
                    pure (TokError "identifiers must not start with a digit", TL.length word)
                  | otherwise ->
                    let number = TL.concat [ipart, "e", sign, epart]
                     in pure (TokNumber number, TL.length number)

        c | (liftA2 (||) isAlpha (== '_')) (TL.head c) -> do
            rest <- state $ TL.span (liftA2 (||) isAlpha (== '_'))
            let word = TL.concat [ipart, rest]
            pure (TokError "identifiers must not start with a digit", TL.length word)

        _ -> pure (TokNumber ipart, TL.length ipart)

  where
    isPlusOrMinus :: Text -> Bool
    isPlusOrMinus s = elem s ["+", "-"]
