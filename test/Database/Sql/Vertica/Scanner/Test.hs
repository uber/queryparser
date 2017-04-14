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

module Database.Sql.Vertica.Scanner.Test where

import Test.HUnit
import Database.Sql.Position
import Database.Sql.Vertica.Scanner
import Database.Sql.Vertica.Token

initPos :: Position
initPos = Position 1 0 0

testTokenizer :: Test
testTokenizer =
  test
  [ "Test names"
    ~: test[
      "Names are lowercased"
      ~: test[ tokenize "potato" ~?= [( TokWord False "potato"
                                      , Position 1 0 0
                                      , Position 1 6 6 )]

             , tokenize "Potato" ~?= [( TokWord False "potato"
                                      , Position 1 0 0
                                      , Position 1 6 6 )]

             , tokenize "POTATO" ~?= [( TokWord False "potato"
                                      , Position 1 0 0
                                      , Position 1 6 6 )]
             ],

      "Accept leading underscore"
      ~: test[ tokenize "_potato" ~?= [( TokWord False "_potato"
                                       , Position 1 0 0
                                       , Position 1 7 7 )]

             , tokenize "_Potato" ~?= [( TokWord False "_potato"
                                       , Position 1 0 0
                                       , Position 1 7 7 )]

             , tokenize "_POTATO" ~?= [( TokWord False "_potato"
                                       , Position 1 0 0
                                       , Position 1 7 7 )]
             ],

      "Double quotes are untouched"
      ~: test[ tokenize "\"potato\"" ~?= [( TokWord True "potato"
                                          , Position 1 0 0
                                          , Position 1 8 8 )]

             , tokenize "\"Potato\"" ~?= [( TokWord True "Potato"
                                          , Position 1 0 0
                                          , Position 1 8 8 )]

             , tokenize "\"POTATO\"" ~?= [( TokWord True "POTATO"
                                          , Position 1 0 0
                                          , Position 1 8 8 )]
             ],

      "Single quoted single quote"
      ~: test [ tokenize "E'\\\\\\\\ \t foo \\\\\\''" ~?= [( TokString "\\\\ \t foo \\'"
                                       , Position 1 0 0
                                       , Position 1 18 18
                                       )]],

      "Special backslash rules in extended strings"
      ~: test [ tokenize "E'\\b'" ~?= [( TokString "\BS"
                                       , Position 1 0 0
                                       , Position 1 5 5
                                       )]
              , tokenize "E'\\\\'" ~?= [( TokString "\\"
                                        , Position 1 0 0
                                        , Position 1 5 5
                                        )]
              , tokenize "E'\\036'" ~?= [( TokString "\o36"
                                         , Position 1 0 0
                                         , Position 1 7 7
                                         )]
              , tokenize "E'\\03'" ~?= [( TokString "\o3"
                                        , Position 1 0 0
                                        , Position 1 6 6
                                        )]
              , tokenize "E'\\1'" ~?= [( TokString "\o1"
                                       , Position 1 0 0
                                       , Position 1 5 5
                                       )]
              , tokenize "E'\\a'" ~?= [( TokString "a"
                                       , Position 1 0 0
                                       , Position 1 5 5
                                       )]
              , tokenize "E'\\9'" ~?= [( TokString "9"
                                       , Position 1 0 0
                                       , Position 1 5 5
                                       )]
              , tokenize "E'\\8'" ~?= [( TokString "8"
                                       , Position 1 0 0
                                       , Position 1 5 5
                                       )]
              , tokenize "E'\\7'" ~?= [( TokString "\o7"
                                       , Position 1 0 0
                                       , Position 1 5 5
                                       )]
              , tokenize "E'\\x'" ~?= [( TokString "x"
                                       , Position 1 0 0
                                       , Position 1 5 5
                                       )]
              , tokenize "E'\\x7f3'" ~?= [( TokString "\DEL3"
                                          , Position 1 0 0
                                          , Position 1 8 8
                                          )]
              , tokenize "E'\\464'" ~?= [( TokString "4"
                                         , Position 1 0 0
                                         , Position 1 7 7
                                         )]
              , tokenize "E'\\xfc1'" ~?= [( TokString "\252\&1"
                                          , Position 1 0 0
                                          , Position 1 8 8
                                          )]
              ],

      "Whitespace splits names, unless inside double quotes"
      ~: test[ tokenize "foo bar"
               ~?= [ ( TokWord False "foo"
                     , initPos
                     , Position 1 3 3
                     )
                   , ( TokWord False "bar"
                     , Position 1 4 4
                     , Position 1 7 7
                     )
                   ]

             , tokenize "fo\"o b\"ar"
               ~?= [( TokWord True "foo bar", initPos, Position 1 9 9 )]
             ],

      unwords
        [ "Special characters mean nothing in double quotes"
        , "(except escaped double quotes)"
        ] ~: test
            [ tokenize "\" \\(>,>)/ #--.;\"\"\""
                ~?= [( TokWord True " \\(>,>)/ #--.;\""
                     , initPos
                     , Position 1 18 18 )]
            ],

      "Double quotes can be arbitrarily combined"
      ~: test[ tokenize "Ab\"CdEf\"Gh\"Ij\"K"
               ~?= [( TokWord True "abCdEfghIjk", initPos, Position 1 15 15 )]
             ],

      "Quotes get continued if there is a newline"
      ~: test[ tokenize "'foo'\n 'bar' 'baz'"
               ~?= [( TokString "foobar", initPos, Position 2 6 12 )
                   ,( TokString "baz", Position 2 7 13, Position 2 12 18 )]
             , tokenize "'foo'\n 'bar'\n'baz'"
               ~?= [( TokString "foobarbaz", initPos, Position 3 5 18 )]
             ]
      ],

    "Test keywords"
    ~: test[
      "All"
      ~: test [ tokenize "all" ~?= [( TokWord False "all"
                                    , initPos
                                    , Position 1 3 3 )]

              , tokenize "All" ~?= [( TokWord False "all"
                                    , initPos
                                    , Position 1 3 3 )]

              , tokenize "ALL" ~?= [( TokWord False "all"
                                    , initPos
                                    , Position 1 3 3 )]
              ],


      "And"
      ~: test [ tokenize "and" ~?= [( TokWord False "and"
                                    , initPos
                                    , Position 1 3 3 )]

              , tokenize "And" ~?= [( TokWord False "and"
                                    , initPos
                                    , Position 1 3 3 )]

              , tokenize "AND" ~?= [( TokWord False "and"
                                    , initPos
                                    , Position 1 3 3 )]
              ],


      "As"
      ~: test [ tokenize "as" ~?= [( TokWord False "as"
                                   , initPos
                                   , Position 1 2 2 )]

              , tokenize "As" ~?= [( TokWord False "as"
                                   , initPos
                                   , Position 1 2 2 )]

              , tokenize "AS" ~?= [( TokWord False "as"
                                   , initPos
                                   , Position 1 2 2 )]
              ]
      ]
  ]

tests :: Test
tests = test [ testTokenizer ]
