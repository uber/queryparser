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

module Database.Sql.Vertica.Scanner where

import Prelude hiding ((&&), (||), not)

import           Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TL
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Builder as BB

import Data.List (sortBy)
import Data.Foldable (asum)
import Data.Char (isAlphaNum, isAlpha, isSpace, isDigit)

import Database.Sql.Position
import Database.Sql.Vertica.Token

import Data.Predicate.Class

import Numeric (readHex, readOct)


isWordBody :: Char -> Bool
isWordBody = isAlphaNum || (== '_') || (== '$')

isHSpace :: Char -> Bool
isHSpace = isSpace && not (== '\n')

operators :: [Text]
operators = sortBy (flip compare)
    [ "+", "-", "*", "^", "//", "/", "|/", "||/", "||", "@", "%"
    , "~~", "~~*", "!~~", "!~~*", "!"
    , "~", "~*", "!~", "!~*"
    , "!=", "<>", ">", "<", ">=", "<=", "<=>", "="
    , "::", "(", ")", "[", "]", ",", ";"
    ]

isOperator :: Char -> Bool
isOperator c = elem c $ map TL.head operators

tokenize :: Text -> [(Token, Position, Position)]
tokenize = go (Position 1 0 0)
  where
    go :: Position -> Text -> [(Token, Position, Position)]
    go _ "" = []
    go p t = case TL.head t of
        c | c `elem` ['e', 'E'] && TL.length t > 2 && TL.index t 1 == '\'' ->
            case tokExtString p t of
                Left (err, p') -> [(err, p, p')]
                Right (string, rest, p') -> (TokString string, p, p') : go p' rest

        c | isAlpha c || c == '_' || c == '"' ->
            case tokName p t of
                Left token -> [token]
                Right (name, quoted, rest, p') -> (TokWord quoted name, p, p') : go p' rest

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

                (digits, rest) ->
                    let p' = advanceHorizontal (1 + TL.length digits) p
                     in (TokNumber $ TL.cons '.' digits, p, p') : go p' rest

        c | isDigit c -> let (num, rest) = TL.span (isDigit || (=='.')) t
                             p' = advanceHorizontal (TL.length num) p
                          in (TokNumber num, p, p') : go p' rest

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


-- | tokString returns Text, not ByteString, because there is no way to put arbitrary byte sequences in this kind of Vertica string (... for now?)
tokString :: Position -> Char -> Text -> Either Position (Text, Text, Position)
tokString pos d = go (advanceHorizontal 1 pos) [] . TL.tail
  where
    go p _ "" = Left p
    go p ts input = case TL.head input of
         c | c == d ->
            let (quotes, rest) = TL.span (== d) input
                len = TL.length quotes
            in if len `mod` 2 == 0
                then go (advanceHorizontal len p)
                        (TL.take (len `div` 2) quotes :ts)
                        rest

                else co (advanceHorizontal len p)
                        (TL.take (len `div` 2) quotes :ts)
                        rest

         _ -> let (t, rest) = TL.span (/= d) input
               in go (advance t p) (t:ts) rest

    co p ts input =
        case TL.span isSpace input of
            (space, rest)
                | TL.any (== '\n') space && TL.take 1 rest == TL.singleton d -> go (advanceHorizontal 1 $ advance space p) ts (TL.drop 1 rest)
                | otherwise -> Right (TL.concat $ reverse ts, input, p)


tokExtString :: Position -> Text -> Either (Token, Position) (ByteString, Text, Position)
tokExtString pos = go (advanceHorizontal 2 pos) [] . TL.drop 2
  where
    boring = not . (`elem` ['\'', '\\'])
    halve bs = BL.take (BL.length bs `div` 2) bs
    go p bss input = case TL.span boring input of
        (cs, "") -> Left (TokError "end of input inside string", advance cs p)
        ("", rest) -> handleSlashesOrQuote p bss rest
        (cs, rest) ->
            let bs = TL.encodeUtf8 cs
             in handleSlashesOrQuote (advance cs p) (bs:bss) rest

    handleSlashesOrQuote p bss input = case TL.span (== '\\') input of
        (cs, "") -> Left (TokError "end of input inside string", advance cs p) -- `input` matches `^(\\)+$`
        ("", _) -> handleQuote p bss input -- `input` matches `^'`
        (slashes, rest) -> -- `input` matches `^(\\)+.+`
            let slashes' = TL.encodeUtf8 slashes
                len = BL.length slashes'
             in if len `mod` 2 == 0
                 then go (advanceHorizontal len p) (halve slashes':bss) rest
                 else case TL.splitAt 1 rest of
                    (c, rest')
                        | c == "b" -> go (advanceHorizontal (len + 1) p) ("\BS":halve slashes':bss) rest'
                        | c == "f" -> go (advanceHorizontal (len + 1) p) ("\FF":halve slashes':bss) rest'
                        | c == "n" -> go (advanceHorizontal (len + 1) p) ("\n":halve slashes':bss) rest'
                        | c == "r" -> go (advanceHorizontal (len + 1) p) ("\r":halve slashes':bss) rest'
                        | c == "t" -> go (advanceHorizontal (len + 1) p) ("\t":halve slashes':bss) rest'
                        | c == "'" -> go (advanceHorizontal (len + 1) p) ("'":halve slashes':bss) rest'
                        | c == "x" -> consumeHex rest' len p slashes' bss -- consume one or two hex
                          -- digits, translating into the corresponding utf8 value. If no hex digits
                          -- follow the x, it's a literal x as if there were no backslash.
                        | hasOctalPrefix rest -> consumeOctal rest len p slashes' bss -- consume one or two
                          -- or three octal digits, translating into the corresponding utf8 value. If
                          -- it's > o277, take the 8 LSB.
                        | otherwise -> go (advanceHorizontal (len + 1) p) (TL.encodeUtf8 c:halve slashes':bss) rest'
                          -- the backslash has no effect, ie it's the same as if there were no backslash.

    handleQuote p bss input = case TL.splitAt 1 input of
        ("'", rest) -> Right ( BL.concat $ reverse bss
                             , rest
                             , advanceHorizontal 1 p
                             )
        _ -> error "this shouldn't happen"

    consumeHex rest' len p slashes' bss =
        let charLimit = 2
            isHex = flip elem (['0'..'9'] ++ ['a'..'f'] ++ ['A'..'F'])
            (hex, _) = TL.span isHex (TL.take charLimit rest')

            toByteString :: TL.Text -> BL.ByteString
            toByteString "" = "x"
            toByteString t =
                let [(num, _)] = (readHex $ TL.unpack t) :: [(Int, String)]
                 in BB.toLazyByteString $ BB.word8 $ fromIntegral num

            rest'' = TL.drop (fromIntegral $ TL.length hex) rest'
         in go (advanceHorizontal (len + 1 + TL.length hex) p) (toByteString hex:halve slashes':bss) rest''

    isOctal = flip elem ['0'..'7']

    hasOctalPrefix text = isOctal $ TL.head text

    consumeOctal rest len p slashes' bss =
        let charLimit = 3
            (octal, _) = TL.span isOctal (TL.take charLimit rest)

            toByteString :: TL.Text -> BL.ByteString
            toByteString "" = error "this shouldn't happen"
            toByteString t =
                let [(num, _)] = (readOct $ TL.unpack t) :: [(Int, String)]
                 in BB.toLazyByteString $ BB.word8 $ fromIntegral num

            rest' = TL.drop (fromIntegral $ TL.length octal) rest
         in go (advanceHorizontal (len + TL.length octal) p) (toByteString octal:halve slashes':bss) rest'


tokName :: Position -> Text -> Either (Token, Position, Position) (Text, Bool, Text, Position)
tokName pos = go pos [] False
  where
    go :: Position -> [Text] -> Bool -> Text -> Either (Token, Position, Position) (Text, Bool, Text, Position)
    go p [] _ ""  = error $ "parse error at " ++ show p
    go p ts seen_quotes "" = Right (TL.concat $ reverse ts, seen_quotes, "", p)
    go p ts seen_quotes input = case TL.head input of
-- Case where outside double quote
      c | isWordBody c ->
        let (word, rest) = TL.span isWordBody input
            p' = advanceHorizontal (TL.length word) p
         in go p' (TL.toLower word:ts) seen_quotes rest

      c | c == '"' ->
        case tokString p '"' input of
            Left p' -> Left (TokError "end of input inside string", p, p')
            Right (quoted, rest, p') -> go p' (quoted:ts) True rest

      _ -> case ts of
         [] -> error "empty token"
         _ -> Right (TL.concat $ reverse ts, seen_quotes, input, p)

