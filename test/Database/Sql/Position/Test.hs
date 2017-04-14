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

module Database.Sql.Position.Test where

import Test.HUnit
import Database.Sql.Position
import Data.Int (Int64)

-- test1 :: Test
-- test1 = TestCase (assertEqual "Advancing horizontal" 1 foo)

-- tests :: Test
-- tests = TestList [TestLabel "test1" test1]

testPos :: (Int64, Int64, Int64) -> Position
testPos (l, c, o) =
    Position { positionLine = l
             , positionColumn = c
             , positionOffset = o
             }

tests :: Test
tests = test
        [ "Advance horizontal zero"
          ~: "(advanceHorizontal 0 testPos)"
          ~: test [advanceHorizontal 0
                        (testPos (0,0,0)) ~?= testPos (0,0,0),
                   advanceHorizontal 0
                        (testPos (1,1,1)) ~?= testPos (1,1,1)],
          "Advance horizontal positive"
          ~: "(advanceHorizontal 1 testPos)"
          ~: test [advanceHorizontal 1
                        (testPos (0,0,0)) ~?= testPos (0,1,1),
                   advanceHorizontal 1
                        (testPos (1,3,6)) ~?= testPos (1,4,7)],
          "Advance vertical zero"
          ~: "(advanceVertical 0 testPos)"
          ~: test [advanceVertical 0
                        (testPos (0,0,0)) ~?= testPos (0,0,0),
                   advanceVertical 0
                        (testPos (2,2,2)) ~?= testPos (2,2,2)],
          "Advance vertical positive"
          ~: "(advanceVertical 1 testPos)"
          ~: test [advanceVertical 1
                        (testPos (0,0,0)) ~?= testPos (1,0,1),
                   advanceVertical 1
                        (testPos (5,2,6)) ~?= testPos (6,0,7),
                   advanceVertical 8
                        (testPos (2,7,3)) ~?= testPos (10,0,11)],
          "Advance"
          ~: "(advance \"potato\" testPos)"
          ~: test [advance "potato"
                        (testPos (0,0,0)) ~?= testPos (0,6,6),
                   advance "potato"
                        (testPos (6,0,8)) ~?= testPos (6,6,14),
                   advance "potato"
                        (testPos (6,3,36)) ~?= testPos (6,9,42)]
        ]
