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

{-# LANGUAGE ScopedTypeVariables #-}

module Test.HUnit.Ticket (ticket, ticket') where

import Test.HUnit
import Trace.Hpc.Reflect
import Control.Exception (catch, SomeException)
import System.IO.Unsafe (unsafePerformIO)
import System.Environment (lookupEnv)
import Control.Monad (when)


-- | This lets us annotate tests with `ticket "T#####"`
--
-- If the environment variable TICKET is set and matches the ticket number
-- specified, then we require that the tests *pass*.  Otherwise, we require
-- that the tests *fail*.

ticket :: String -> [Assertion] -> Test
ticket t goal = case unsafePerformIO (lookupEnv "TICKET") of
    Just t' | t == t' -> ("Verifying ticket " ++ t) ~: map TestCase goal
    _ -> ("Awaiting ticket " ++ t) ~: map (TestCase . checkFails) goal

checkFails :: IO () -> IO ()
checkFails check = do
    tix <- examineTix

    passed <- (check >> pure True) `catch` (\ (_ :: SomeException) -> pure False)

    clearTix
    updateTix tix

    when passed $ assertFailure "check for ticket behaviour unexpectedly passed"


-- | ticket' is a more rigorous version of ticket
--
-- In addition to testing for the correct behavior, it has an inverted
-- set of tests demonstrating the current behavior.

ticket' :: String -> [Assertion] -> [Assertion] -> Test
ticket' t goal current = case unsafePerformIO (lookupEnv "TICKET") of
    Just t' | t == t' -> ("Verifying ticket " ++ t) ~: map TestCase goal
    _ -> ("Awaiting ticket " ++ t) ~: map (TestCase . checkFails) goal ++ map TestCase current
