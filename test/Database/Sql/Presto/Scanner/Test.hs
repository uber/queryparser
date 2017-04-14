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

module Database.Sql.Presto.Scanner.Test where

import Test.HUnit
import Database.Sql.Position
import Database.Sql.Presto.Scanner
import Database.Sql.Presto.Token

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
      ~: test [ tokenize "'foo'''" ~?= [( TokString "foo'"
                                        , Position 1 0 0
                                        , Position 1 7 7
                                        )]
              , tokenize "'fo''o'" ~?= [( TokString "fo'o"
                                        , Position 1 0 0
                                        , Position 1 7 7
                                        )]
              , tokenize "'''foo'" ~?= [( TokString "'foo"
                                        , Position 1 0 0
                                        , Position 1 7 7
                                        )]],

      "Empty string is allowed"
      ~: test [ tokenize "\"\"" ~?= [( TokWord True ""
                                        , Position 1 0 0
                                        , Position 1 2 2
                                        )]
              ],

      "Double quoted double quote"
      ~: test [ tokenize "\"\"\"\"" ~?= [( TokWord True "\""
                                        , Position 1 0 0
                                        , Position 1 4 4
                                        )]
              ],

      "Unterminated double quote"
      ~: test [ tokenize "\"asdf" ~?= [( TokError "end of input inside quoted name"
                                       , Position 1 0 0
                                       , Position 1 5 5
                                       )]
              ],

      "Mid-word quote splits names"
      ~: test [ tokenize "\"foo\"bar" ~?= [( TokWord True "foo", Position 1 0 0, Position 1 5 5 )
                                          ,( TokWord False "bar", Position 1 5 5, Position 1 8 8 )]
              ],

      "Names may not start with a number"
      ~: test [ tokenize "123foo" ~?= [( TokError "identifiers must not start with a digit"
                                        , Position 1 0 0
                                        , Position 1 6 6
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

             , tokenize "\"foo bar\""
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
            ]

      ],

    "Test comments"
    ~: test[
      "Input may end with a comment"
      ~: let expected = [( TokWord False "select", initPos, Position 1 6 6 )
                        ,( TokNumber "1", Position 1 7 7, Position 1 8 8 )
                        ]
          in test [ tokenize "select 1 /*asdf*/" ~?= expected
                  , tokenize "select 1/*asdf*/" ~?= expected
                  , tokenize "select 1 --asdf" ~?= expected
                  , tokenize "select 1--asdf" ~?= expected
                  ]
      ],

    "Test numbers"
    ~: test[
      "Exponents are allowed"
      ~: test [ tokenize "1e1" ~?= [( TokNumber "1e1", initPos, Position 1 3 3 )]
              , tokenize "1.1" ~?= [( TokNumber "1.1", initPos, Position 1 3 3 )]
              , tokenize "1.1e1" ~?= [( TokNumber "1.1e1", initPos, Position 1 5 5 )]
              , tokenize "1.1e+1" ~?= [( TokNumber "1.1e+1", initPos, Position 1 6 6 )]
              , tokenize "1.1e-1" ~?= [( TokNumber "1.1e-1", initPos, Position 1 6 6 )]
              , tokenize ".1" ~?= [( TokNumber ".1", initPos, Position 1 2 2 )]
              , tokenize ".1e1" ~?= [( TokNumber ".1e1", initPos, Position 1 4 4 )]
              , tokenize ".1e+1" ~?= [( TokNumber ".1e+1", initPos, Position 1 5 5 )]
              , tokenize ".1e-1" ~?= [( TokNumber ".1e-1", initPos, Position 1 5 5 )]
              -- `e` (or other identifiers) may not follow integers
              , tokenize "1e" ~?= [ ( TokError "identifiers must not start with a digit"
                                    , initPos
                                    , Position 1 2 2 )]
              , tokenize "1e1f" ~?= [ ( TokError "identifiers must not start with a digit"
                                      , initPos
                                      , Position 1 4 4 )]
              , tokenize "1f" ~?= [ ( TokError "identifiers must not start with a digit"
                                    , initPos
                                    , Position 1 2 2 )]
              -- `e` (or other identifiers) may follow floating point literals (even funky looking ones)
              , tokenize ".e" ~?= [ ( TokSymbol ".", initPos, Position 1 1 1 )
                                  , ( TokWord False "e", Position 1 1 1, Position 1 2 2 )]
              , tokenize "1.1e" ~?= [ ( TokNumber "1.1", initPos, Position 1 3 3 )
                                    , ( TokWord False "e", Position 1 3 3, Position 1 4 4 )]
              , tokenize "1.e" ~?= [ ( TokNumber "1.", initPos, Position 1 2 2 )
                                   , ( TokWord False "e", Position 1 2 2, Position 1 3 3 )]
              , tokenize ".1e" ~?= [ ( TokNumber ".1", initPos, Position 1 2 2 )
                                   , ( TokWord False "e", Position 1 2 2, Position 1 3 3 )]
              , tokenize ".f" ~?= [ ( TokSymbol ".", initPos, Position 1 1 1 )
                                  , ( TokWord False "f", Position 1 1 1, Position 1 2 2 )]
              , tokenize "1.1f" ~?= [ ( TokNumber "1.1", initPos, Position 1 3 3 )
                                    , ( TokWord False "f", Position 1 3 3, Position 1 4 4 )]
              , tokenize "1.f" ~?= [ ( TokNumber "1.", initPos, Position 1 2 2 )
                                   , ( TokWord False "f", Position 1 2 2, Position 1 3 3 )]
              , tokenize ".1f" ~?= [ ( TokNumber ".1", initPos, Position 1 2 2 )
                                   , ( TokWord False "f", Position 1 2 2, Position 1 3 3 )]
              ]
      ],

    "Test binary literals"
    ~: test
      [ "basic happy path" ~: test
            [ tokenize "x'00'" ~?= [( TokBinary "\NUL", initPos, Position 1 5 5)]
            ]

      , "empty string" ~: test
            [ tokenize "x''" ~?= [( TokBinary "", initPos, Position 1 3 3)]
            ]

      , "the x may be upper or lower" ~: test
            [ tokenize "x''" ~?= [( TokBinary "", initPos, Position 1 3 3)]
            , tokenize "X''" ~?= [( TokBinary "", initPos, Position 1 3 3)]
            ]

      , "spaces allowed anywhere inside the single quotes, and are collapsed" ~: test
            [ tokenize "x' 0   0 '" ~?= [( TokBinary "\NUL", initPos, Position 1 10 10)]
            ]

      , "consecutive binary literals are not collapsed" ~: test
            [ tokenize "x'00' x'11'" ~?= [( TokBinary "\NUL", initPos, Position 1 5 5)
                                         ,( TokBinary "\x11", Position 1 6 6, Position 1 11 11)
                                         ]
            , tokenize "x'00' '11'" ~?= [( TokBinary "\NUL", initPos, Position 1 5 5)
                                        ,( TokString "11", Position 1 6 6, Position 1 10 10)
                                        ]
            ]

      , "only hex digits allowed" ~: test
            [ tokenize "x'z'" ~?= [( TokError "binary literal must only have hex-digits"
                                   , initPos
                                   , Position 1 4 4)]
            ]

      , "require an even number of digits" ~: test
            [ tokenize "x'000'" ~?= [( TokError "binary literal must contain an even number of hex-digits"
                                     , initPos
                                     , Position 1 6 6)]
            ]

      , "useful error message if unterminated" ~: test
            [ tokenize "x'" ~?= [( TokError "end of input inside binary literal", initPos, initPos)]
            , tokenize "x'00" ~?= [( TokError "end of input inside binary literal", initPos, initPos)]
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
      ],

    "Test misc"
    ~: test[
      "unmatched characters raise error"
      ~: test [ tokenize "@" ~?= [( TokError "unmatched character ('@') at position Position {positionLine = 1, positionColumn = 0, positionOffset = 0}"
                                  , initPos
                                  , Position 1 1 1)]
        ],

      "unmatched operators raise error"
      ~: test [ tokenize "|!" ~?= [( TokError "unrecognized operator starting with \"|!\""
                                  , initPos
                                  , Position 1 2 2)]
        ]
      ]
  ]

tests :: Test
tests = test [ testTokenizer ]
