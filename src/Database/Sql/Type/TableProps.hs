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

{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}

module Database.Sql.Type.TableProps where

import Data.Aeson

import Test.QuickCheck

import Data.Data (Data)
import GHC.Generics (Generic)


-- nb (Vertica):
--    both global and local temporary tables have type TemporaryTable
--    as data flow respects session boundaries; they are distinguished
--    by the fact that local temporary tables live in v_temp_schema

data Persistence a
    = Temporary a
    | Persistent
    deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data Externality a
    = External a
    | Internal
    deriving (Generic, Data, Eq, Ord, Show, Functor, Foldable, Traversable)

data Existence
    = Exists
    | DoesNotExist
      deriving (Generic, Data, Eq, Ord, Show)

data TableType
    = Table
    | View
    deriving (Generic, Data, Eq, Ord, Show)


instance ToJSON a => ToJSON (Persistence a) where
    toJSON Persistent = object
        [ "tag" .= String "Persistent"
        ]

    toJSON (Temporary info) = object
        [ "tag" .= String "Temporary"
        , "info" .= info
        ]

instance ToJSON a => ToJSON (Externality a) where
    toJSON (External a) = object
        [ "tag" .= String "External"
        , "info" .= a
        ]

    toJSON Internal = object ["tag" .= String "Internal"]

instance ToJSON Existence where
    toJSON Exists = object ["tag" .= String "Exists"]
    toJSON DoesNotExist = object ["tag" .= String "DoesNotExist"]

instance ToJSON TableType where
    toJSON Table = object ["tag" .= String "Table"]
    toJSON View = object ["tag" .= String "View"]


instance FromJSON a => FromJSON (Persistence a) where
    parseJSON (Object o) = o .: "tag" >>= \case
            String "Persistent" -> pure Persistent
            String "Temporary" -> Temporary <$> o .: "info"
            _ -> fail "unrecognized tag on Persistence object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Persistence:"
        , show v
        ]

instance FromJSON a => FromJSON (Externality a) where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "External" -> External <$> o .: "info"
        String "Internal" -> pure Internal
        _ -> fail "unrecognized tag on Externality object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Externality:"
        , show v
        ]

instance FromJSON Existence where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "Exists" -> pure Exists
        String "DoesNotExist" -> pure DoesNotExist
        _ -> fail "unrecognized tag on Existence object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as Existence:"
        , show v
        ]

instance FromJSON TableType where
    parseJSON (Object o) = o .: "tag" >>= \case
        String "Table" -> pure Table
        String "View" -> pure View
        _ -> fail "unrecognized tag on TableType object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as TableType:"
        , show v
        ]


instance Arbitrary a => Arbitrary (Persistence a) where
    arbitrary = oneof [ Temporary <$> arbitrary
                      , pure Persistent
                      ]
    shrink (Temporary info) =
        [ Persistent ] ++
        [ Temporary info' | info' <- shrink info ]
    shrink Persistent = []

instance Arbitrary TableType where
    arbitrary = oneof [ pure Table
                      , pure View
                      ]
    shrink Table = []
    shrink View = []
