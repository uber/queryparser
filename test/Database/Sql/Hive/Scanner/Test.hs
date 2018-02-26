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

module Database.Sql.Hive.Scanner.Test where

import Test.HUnit
import Database.Sql.Position
import Database.Sql.Hive.Scanner
import Database.Sql.Hive.Token

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

      "Back quotes are untouched"
      ~: test[ tokenize "`potato`" ~?= [( TokWord True "potato"
                                          , Position 1 0 0
                                          , Position 1 8 8 )]

             , tokenize "`Potato`" ~?= [( TokWord True "Potato"
                                          , Position 1 0 0
                                          , Position 1 8 8 )]

             , tokenize "`POTATO`" ~?= [( TokWord True "POTATO"
                                          , Position 1 0 0
                                          , Position 1 8 8 )]
             , tokenize "`my_table`" ~?= [( TokWord True "my_table"
                                            , Position 1 0 0
                                            , Position 1 10 10 )]
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

             , tokenize "`foo bar`"
               ~?= [( TokWord True "foo bar", initPos, Position 1 9 9 )]
             ],

      "Quotes break tokens"
      ~: test[ tokenize "Ab\"CdEf\"Gh\'Ij\'K`Lm`No"
               ~?= [ ( TokWord False "ab", initPos, Position 1 2 2)
                   , ( TokString "CdEf", Position 1 2 2, Position 1 8 8)
                   , ( TokWord False "gh", Position 1 8 8, Position 1 10 10)
                   , ( TokString "Ij", Position 1 10 10, Position 1 14 14)
                   , ( TokWord False "k", Position 1 14 14, Position 1 15 15 )
                   , ( TokWord True "Lm", Position 1 15 15, Position 1 19 19 )
                   , ( TokWord False "no", Position 1 19 19, Position 1 21 21 )
                   ]
             ],

      "Names can start with a number, but a number is not a name"
      ~: test[ tokenize "1foo"
               ~?= [ ( TokWord False "1foo"
                     , initPos
                     , Position 1 4 4
                     )
                   ]

             , tokenize "1efoo"
               ~?= [ ( TokWord False "1efoo"
                     , initPos
                     , Position 1 5 5
                     )
                   ]

             , tokenize "1e"
               ~?= [ ( TokWord False "1e"
                     , initPos
                     , Position 1 2 2
                     )
                   ]

             , tokenize "1_"
               ~?= [ ( TokWord False "1_"
                     , initPos
                     , Position 1 2 2
                     )
                   ]

             , tokenize "50"
               ~?= [( TokNumber "50", initPos, Position 1 2 2 )]

             , tokenize "1.1"
               ~?= [( TokNumber "1.1", initPos, Position 1 3 3 )]

             , tokenize "1.1a"
               ~?= [( TokNumber "1.1", initPos, Position 1 3 3 ), ( TokWord False "a", Position 1 3 3, Position 1 4 4 )]

             , tokenize "1.1e1"
               ~?= [( TokNumber "1.1e1", initPos, Position 1 5 5 )]

             , tokenize "1.1e+1"
               ~?= [( TokNumber "1.1e+1", initPos, Position 1 6 6 )]

             , tokenize "1.1e-1"
               ~?= [( TokNumber "1.1e-1", initPos, Position 1 6 6 )]

             , tokenize "1.1e"
               ~?= [( TokError "...", initPos, Position 1 4 4 )]
             ]
      ],

    "Test operators"
    ~: test[
      "Relational"
      ~: test [ tokenize "!=" ~?= [( TokSymbol "!="
                                    , initPos
                                    , Position 1 2 2 )]
              , tokenize "<>" ~?= [( TokSymbol "<>"
                                    , initPos
                                    , Position 1 2 2 )]
              , tokenize ">" ~?= [( TokSymbol ">"
                                    , initPos
                                    , Position 1 1 1 )]
              , tokenize "<" ~?= [( TokSymbol "<"
                                    , initPos
                                    , Position 1 1 1 )]
              , tokenize ">=" ~?= [( TokSymbol ">="
                                    , initPos
                                    , Position 1 2 2 )]
              , tokenize "<=" ~?= [( TokSymbol "<="
                                    , initPos
                                    , Position 1 2 2 )]
              , tokenize "<=>" ~?= [( TokSymbol "<=>"
                                    , initPos
                                    , Position 1 3 3 )]
              , tokenize "=" ~?= [( TokSymbol "="
                                    , initPos
                                    , Position 1 1 1 )]
              , tokenize "==" ~?= [( TokSymbol "=="
                                    , initPos
                                    , Position 1 2 2 )]
              ],

      "Bits"
      ~: test [ tokenize "&" ~?= [( TokSymbol "&"
                                    , initPos
                                    , Position 1 1 1 )]
              , tokenize "|" ~?= [( TokSymbol "|"
                                    , initPos
                                    , Position 1 1 1 )]
              , tokenize "^" ~?= [( TokSymbol "^"
                                    , initPos
                                    , Position 1 1 1 )]
              , tokenize "~" ~?= [( TokSymbol "~"
                                    , initPos
                                    , Position 1 1 1 )]
              , tokenize "!" ~?= [( TokSymbol "!"
                                    , initPos
                                    , Position 1 1 1 )]
              ],

      "Arithmetic"
      ~: test [ tokenize "+" ~?= [( TokSymbol "+"
                                    , initPos
                                    , Position 1 1 1 )]
              , tokenize "-" ~?= [( TokSymbol "-"
                                    , initPos
                                    , Position 1 1 1 )]
              , tokenize "*" ~?= [( TokSymbol "*"
                                    , initPos
                                    , Position 1 1 1 )]
              , tokenize "/" ~?= [( TokSymbol "/"
                                    , initPos
                                    , Position 1 1 1 )]
              , tokenize "%" ~?= [( TokSymbol "%"
                                    , initPos
                                    , Position 1 1 1 )]
              ],

      "Strings"
      ~: test [ tokenize "||" ~?= [( TokSymbol "||"
                                    , initPos
                                    , Position 1 2 2 )]
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
    "Single Quotes"
    ~: test[
        "Backslash escape on unknown character preserves backslash"
        ~: test [ tokenize "'\\_'" ~?= [(TokString "\\_", initPos, Position 1 4 4)]
                ]
      ],

    "Variable Substitution"
    -- see https://cwiki.apache.org/confluence/display/Hive/LanguageManual+VariableSubstitution
    ~: test[
        "Test basic variable substitution"
        ~: test [ tokenize "${hiveconf:foo}" ~?= [( TokVariable "hiveconf" (StaticName "foo")
                                                  , initPos
                                                  , Position 1 15 15
                                                  )
                                                 ]
                ],
        "Test nested variable substitution (sigh)"
        ~: test [ tokenize "${hiveconf:${hiveconf:b}}"
                  ~?= [(TokVariable "hiveconf" (DynamicName
                                                (TokVariable "hiveconf" (StaticName "b")))
                       , initPos
                       , Position 1 25 25)]
                ],
        "Test that all the allowed namespaces parse"
        ~: test [ tokenize "${hiveconf:foo}" ~?= [( TokVariable "hiveconf" (StaticName "foo")
                                                  , initPos
                                                  , Position 1 15 15
                                                  )
                                                 ],
                  tokenize "${system:foo}" ~?= [( TokVariable "system" (StaticName "foo")
                                                , initPos
                                                , Position 1 13 13
                                                )
                                               ],
                  tokenize "${env:foo}" ~?= [( TokVariable "env" (StaticName "foo")
                                             , initPos
                                             , Position 1 10 10
                                             )
                                            ],
                  tokenize "${define:foo}" ~?= [( TokVariable "define" (StaticName "foo")
                                                , initPos
                                                , Position 1 13 13
                                                )
                                               ],
                  tokenize "${hivevar:foo}" ~?= [( TokVariable "hivevar" (StaticName "foo")
                                                 , initPos
                                                 , Position 1 14 14
                                                 )
                                                ]
                  ],
        "Test things that shouldn't parse"
        ~: test [ tokenize "${"
                  ~?= [( TokError "end of input inside variable substitution"
                       , initPos, Position 1 2 2 )]
                , tokenize "${}"
                  ~?= [( TokError "variable substitutions must have a namespace and a name", initPos
                       , Position 1 3 3 )]
                , tokenize "${:}"
                  ~?= [( TokError "variable substitutions must have a namespace and a name", initPos
                       , Position 1 4 4 )]
                , tokenize "${hiveconf}"
                  ~?= [( TokError "variable substitutions must have a namespace and a name", initPos
                       , Position 1 11 11 )]
                , tokenize "${hiveconf"
                  ~?= [( TokError "end of input inside variable substitution"
                       , initPos, Position 1 10 10 )]
                , tokenize "${hiveconf:"
                  ~?= [( TokError "end of input inside variable substitution"
                       , initPos, Position 1 11 11 )]
                , tokenize "${hiveconf:}"
                  ~?= [( TokError "variable substitutions must have a namespace and a name", initPos
                       , Position 1 12 12 )]
                , tokenize "${hiveconf:b"
                  ~?= [( TokError "end of input inside variable substitution"
                       , initPos, Position 1 12 12 )]

                , tokenize "${hiveconf:${}"
                  ~?= [( TokError "end of input inside variable substitution"
                       , initPos, Position 1 14 14 )]

                , tokenize "${badNamespace:foo}"
                  ~?= [( TokError "bad namespace in variable substitution: \"badNamespace\""
                       , initPos, Position 1 19 19 )]
                , tokenize "${badNamespace1:${hiveconf:foo}}"
                  ~?= [( TokError "bad namespace in variable substitution: \"badNamespace1\""
                       , initPos, Position 1 32 32 )]
                , tokenize "${hiveconf:${badNamespace2:foo}}"
                  ~?= [( TokError "bad namespace in variable substitution: \"badNamespace2\""
                       , initPos, Position 1 32 32 )]
                ]
      ]
  ]

tests :: Test
tests = test [ testTokenizer ]
