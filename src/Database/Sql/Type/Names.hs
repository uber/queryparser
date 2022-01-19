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
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DeriveGeneric #-}

module Database.Sql.Type.Names where

import Data.Hashable
import Data.Text.Lazy (Text, pack)
import Data.Aeson
import Data.String
import Data.Functor.Identity
import Data.Data (Data, Typeable)

import qualified Data.Map as M
import           Data.Map (Map)

import GHC.Exts (Constraint)
import GHC.Generics
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)
import Data.Proxy

import Control.Applicative (Alternative (..))
import Control.Monad (void)

import Test.QuickCheck


type ConstrainSNames (c :: * -> Constraint) r a =
    ( c a
    , c (TableRef r a)
    , c (TableName r a)
    , c (CreateTableName r a)
    , c (DropTableName r a)
    , c (SchemaName r a)
    , c (CreateSchemaName r a)
    , c (ColumnRef r a)
    , c (NaturalColumns r a)
    , c (UsingColumn r a)
    , c (StarReferents r a)
    , c (PositionExpr r a)
    , c (ComposedQueryColumns r a)
    )

type ConstrainSASNames (c :: (* -> *) -> Constraint) r =
    ( c (TableRef r)
    , c (TableName r)
    , c (CreateTableName r)
    , c (DropTableName r)
    , c (SchemaName r)
    , c (CreateSchemaName r)
    , c (ColumnRef r)
    , c (NaturalColumns r)
    , c (UsingColumn r)
    , c (StarReferents r)
    , c (PositionExpr r)
    , c (ComposedQueryColumns r)
    )


class Resolution r where
    -- | TableRef refers to either a table in the catalog, or an alias
    type TableRef r :: * -> *

    -- | TableName refers to a table in the catalog
    type TableName r :: * -> *

    -- | CreateTableName refers to a table that might be in the catalog
    --
    -- Used for CREATE TABLE, special rules for resolution
    type CreateTableName r :: * -> *

    -- | DropTableName refers to a table that might be in the catalog
    --
    -- Used for DROP TABLE, special rules for resolution
    type DropTableName r :: * -> *

    -- | SchemaName refers to a schema in the catalog
    type SchemaName r :: * -> *

    -- | CreateSchemaName refers to a table that might be in the catalog
    --
    -- Used for CREATE SCHEMA, special rules for resolution
    type CreateSchemaName r :: * -> *

    -- | ColumnRef refers to either a column in the catalog, or an alias
    type ColumnRef r :: * -> *

    -- | NaturalColumns refers to columns compared in a natural join
    type NaturalColumns r :: * -> *

    -- | UsingColumn refers to columns that appear in USING (...)
    type UsingColumn r :: * -> *

    type StarReferents r :: * -> *

    type PositionExpr r :: * -> *

    type ComposedQueryColumns r :: * -> *


type FQCN = FullyQualifiedColumnName
data FullyQualifiedColumnName = FullyQualifiedColumnName
    { fqcnDatabaseName :: Text
    , fqcnSchemaName :: Text
    , fqcnTableName :: Text
    , fqcnColumnName :: Text
    } deriving (Data, Generic, Ord, Eq, Show)

type FQTN = FullyQualifiedTableName
data FullyQualifiedTableName = FullyQualifiedTableName
    { fqtnDatabaseName :: Text
    , fqtnSchemaName :: Text
    , fqtnTableName :: Text
    } deriving (Data, Generic, Eq, Ord, Show)

qualifyColumnName :: FQTableName a -> UQColumnName b -> FQColumnName ()
qualifyColumnName fqtn uqcn = uqcn{columnNameInfo = (), columnNameTable = pure $ void fqtn}

fqcnToFQCN :: FQColumnName a -> FullyQualifiedColumnName
fqcnToFQCN (QColumnName _ (Identity (QTableName _ (Identity (QSchemaName _ (Identity (DatabaseName _ database)) schema _)) table)) column) =
    FullyQualifiedColumnName database schema table column

fqtnToFQTN :: FQTableName a -> FullyQualifiedTableName
fqtnToFQTN (QTableName _ (Identity (QSchemaName _ (Identity (DatabaseName _ database)) schema _)) table) =
    FullyQualifiedTableName database schema table

data DatabaseName a = DatabaseName a Text
  deriving (Data, Generic, Read, Show, Eq, Ord, Functor, Foldable, Traversable)

data SchemaType = NormalSchema | SessionSchema
  deriving (Data, Generic, Read, Show, Eq, Ord)

data QSchemaName f a = QSchemaName
    { schemaNameInfo :: a
    , schemaNameDatabase :: f (DatabaseName a)
    , schemaNameName :: Text -- for a SessionSchema, this is the session id
    , schemaNameType :: SchemaType
    } deriving (Generic, Functor, Foldable, Traversable)

deriving instance (Data (f (DatabaseName a)), Data a, Typeable f, Typeable a) => Data (QSchemaName f a)
deriving instance (Eq a, Eq (f (DatabaseName a))) => Eq (QSchemaName f a)
deriving instance (Ord a, Ord (f (DatabaseName a))) => Ord (QSchemaName f a)
deriving instance (Read a, Read (f (DatabaseName a))) => Read (QSchemaName f a)
deriving instance (Show a, Show (f (DatabaseName a))) => Show (QSchemaName f a)

type UQSchemaName = QSchemaName No
type OQSchemaName = QSchemaName Maybe
type FQSchemaName = QSchemaName Identity

mkNormalSchema :: Alternative f => Text -> a -> QSchemaName f a
mkNormalSchema name info = QSchemaName info empty name NormalSchema

instance Hashable a => Hashable (DatabaseName a)

instance Arbitrary a => Arbitrary (DatabaseName a) where
    arbitrary = do
        Identifier name :: Identifier '["fooDatabase", "barDatabase"] <- arbitrary
        DatabaseName <$> arbitrary <*> pure name
    shrink (DatabaseName info name) =
        [DatabaseName info name' | Identifier name' <- shrink (Identifier name :: Identifier '["fooDatabase", "barDatabase"])]

instance Hashable SchemaType

instance (Hashable (f (DatabaseName a)), Hashable a) => Hashable (QSchemaName f a)

instance (Arbitrary (f (DatabaseName a)), Arbitrary a) => Arbitrary (QSchemaName f a) where
    arbitrary = oneof
         [ do
               Identifier name :: Identifier '["public", "fooSchema"] <- arbitrary
               QSchemaName <$> arbitrary <*> arbitrary <*> pure name <*> pure NormalSchema
         , do
               Identifier name :: Identifier '["session-asdf", "session-hjkl"] <- arbitrary
               QSchemaName <$> arbitrary <*> arbitrary <*> pure name <*> pure SessionSchema
         ]
    shrink (QSchemaName info database name _) =
         [QSchemaName info database' name' NormalSchema | (database', Identifier name') <- shrink (database, Identifier name :: Identifier '["public", "fooSchema"])]


arbitraryUnquotedIdentifier :: Gen Text
arbitraryUnquotedIdentifier = do
    -- in Vertica: character a-zA-Z_, then a-zA-Z_ or $ or "unicode letter"
    c <- elements openingChars
    tailLength <- growingElements [1..31]  -- identifiers are up to 128 bytes: limit to 32 chars
    cs <- vectorOf tailLength $ elements subsequentChars
    pure $ pack $ c:cs
      where
        openingChars = ['a'..'z'] ++ ['A'..'Z'] ++ ['_']
        subsequentChars = openingChars ++ ['$', 'ñ', 'á']

arbitraryQuotedIdentifier :: Gen Text
arbitraryQuotedIdentifier = do
    -- in Vertica: anything as long as it's enclosed by double-quotes;
    -- double-quotes may appear in the string. When rendering, the escape for a
    -- " is a " again, e.g. "enclosed""doublequotes" has a single " in it.
    length' <- growingElements [1..32]
    pack <$> vectorOf length' arbitrary

arbitraryIdentifier :: Gen Text
arbitraryIdentifier = frequency
    [(3, arbitraryUnquotedIdentifier),
     (1, arbitraryQuotedIdentifier)]


-- | Identifiers picked first from (and shrunk to) symbols in type list.  Used for testing.
data Identifier (ids :: [Symbol]) = Identifier Text deriving Eq

class KnownSymbols (xs :: [Symbol]) where
    symbolVals :: proxy xs -> [String]

instance KnownSymbols '[] where
    symbolVals _ = []

instance (KnownSymbol x, KnownSymbols xs) => KnownSymbols (x ': xs) where
    symbolVals _ = symbolVal (Proxy :: Proxy x) : symbolVals (Proxy :: Proxy xs)

instance KnownSymbols ids => Arbitrary (Identifier ids) where
    arbitrary = do
        arb <- Identifier <$> arbitraryIdentifier
        growingElements $ ids ++ [arb]
      where
        ids = Identifier . pack <$> symbolVals (Proxy :: Proxy ids)

    shrink i = takeWhile (/= i) ids
      where
        ids = Identifier . pack <$> symbolVals (Proxy :: Proxy ids)

data QTableName f a = QTableName
    { tableNameInfo :: a
    , tableNameSchema :: f (QSchemaName f a)
    , tableNameName :: Text
    } deriving (Generic, Functor, Foldable, Traversable)

deriving instance (Data a, Data (f (QSchemaName f a)), Typeable f, Typeable a) => Data (QTableName f a)
deriving instance (Eq a, Eq (f (QSchemaName f a))) => Eq (QTableName f a)
deriving instance (Ord a, Ord (f (QSchemaName f a))) => Ord (QTableName f a)
deriving instance (Read a, Read (f (QSchemaName f a))) => Read (QTableName f a)
deriving instance (Show a, Show (f (QSchemaName f a))) => Show (QTableName f a)

data No a = None deriving (Data, Generic, Eq, Show, Read, Ord, Functor, Foldable, Traversable)

instance Applicative No where
    pure = const None
    None <*> None = None

instance Arbitrary (No a) where
    arbitrary = pure None

instance Hashable (No a) where
    hashWithSalt salt _ = hashWithSalt salt ()

instance ToJSON (No a) where
    toJSON _ = Null

instance FromJSON (No a) where
    parseJSON _ = pure None

instance Alternative No where
    empty = None
    None <|> None = None

type UQTableName = QTableName No
type OQTableName = QTableName Maybe
type FQTableName = QTableName Identity

newtype TableAliasId
    = TableAliasId Integer
      deriving (Data, Generic, Read, Show, Eq, Ord)

data TableAlias a
    = TableAlias a Text TableAliasId
      deriving ( Data, Generic
               , Read, Show, Eq, Ord
               , Functor, Foldable, Traversable)

tableAliasName :: TableAlias a -> UQTableName a
tableAliasName (TableAlias info name _) = QTableName info None name


data RNaturalColumns a = RNaturalColumns [RUsingColumn a]
      deriving ( Data, Generic
               , Read, Show, Eq, Ord
               , Functor, Foldable, Traversable)

data RUsingColumn a = RUsingColumn (RColumnRef a) (RColumnRef a)
      deriving ( Data, Generic
               , Read, Show, Eq, Ord
               , Functor, Foldable, Traversable)

instance (Hashable (f (QSchemaName f a)), Hashable a) => Hashable (QTableName f a)

instance (Arbitrary (f (QSchemaName f a)), Arbitrary a) => Arbitrary (QTableName f a) where
    arbitrary = do
        Identifier name :: Identifier '["fooTable", "barTable"] <- arbitrary
        QTableName <$> arbitrary <*> arbitrary <*> pure name
    shrink (QTableName info schema name) =
        [QTableName info schema' name' | (schema', Identifier name') <- shrink (schema, Identifier name :: Identifier '["fooTable", "barTable"])]

data QFunctionName f a = QFunctionName
    { functionNameInfo :: a
    , functionNameSchema :: f (QSchemaName f a)
    , functionNameName :: Text
    } deriving (Generic, Functor, Foldable, Traversable)

deriving instance (Data a, Data (f (QSchemaName f a)), Typeable f, Typeable a) => Data (QFunctionName f a)
deriving instance (Eq a, Eq (f (QSchemaName f a))) => Eq (QFunctionName f a)
deriving instance (Ord a, Ord (f (QSchemaName f a))) => Ord (QFunctionName f a)
deriving instance (Read a, Read (f (QSchemaName f a))) => Read (QFunctionName f a)
deriving instance (Show a, Show (f (QSchemaName f a))) => Show (QFunctionName f a)

type FunctionName = QFunctionName Maybe


instance ( Arbitrary (f (QSchemaName f a))
         , Arbitrary a
         , Eq (f SchemaType)
         , Applicative f
         ) => Arbitrary (QFunctionName f a) where
    arbitrary = do
        Identifier name :: Identifier '["fooFunc", "barFunc"] <- arbitrary
        QFunctionName <$> arbitrary <*> arbitraryNormalSchema <*> pure name
      where
        isSessionSchema :: f (QSchemaName f a) -> Bool
        isSessionSchema schema = fmap schemaNameType schema == pure SessionSchema
        arbitraryNormalSchema = arbitrary `suchThat` (not . isSessionSchema)
    shrink (QFunctionName info schema name) =
        [QFunctionName info schema' name' | (schema', Identifier name') <- shrink (schema, Identifier name :: Identifier '["fooName", "barName"])]

data QColumnName f a = QColumnName
    { columnNameInfo :: a
    , columnNameTable :: f (QTableName f a)
    , columnNameName :: Text
    } deriving (Generic, Functor, Foldable, Traversable)

deriving instance (Data (f (QTableName f a)), Data a, Typeable f, Typeable a) => Data (QColumnName f a)
deriving instance (Eq (f (QTableName f a)), Eq a) => Eq (QColumnName f a)
deriving instance (Ord (f (QTableName f a)), Ord a) => Ord (QColumnName f a)
deriving instance (Read (f (QTableName f a)), Read a) => Read (QColumnName f a)
deriving instance (Show (f (QTableName f a)), Show a) => Show (QColumnName f a)

instance (Hashable (f (QTableName f a)), Hashable a) => Hashable (QColumnName f a)


type UQColumnName = QColumnName No
type OQColumnName = QColumnName Maybe
type FQColumnName = QColumnName Identity

instance IsString (UQColumnName ()) where
    fromString s = QColumnName{..}
      where
        columnNameTable = None
        columnNameName = fromString s
        columnNameInfo = ()

newtype ColumnAliasId
    = ColumnAliasId Integer
      deriving (Data, Generic, Read, Show, Eq, Ord)

newtype LambdaParamId
    = LambdaParamId Integer
      deriving (Data, Generic, Read, Show, Eq, Ord)

instance (Arbitrary (f (QTableName f a)), Arbitrary a) => Arbitrary (QColumnName f a) where
    arbitrary = do
        Identifier name :: Identifier '["fooColumn", "barColumn"] <- arbitrary
        QColumnName <$> arbitrary <*> arbitrary <*> pure name
    shrink (QColumnName info table name) =
        [QColumnName info table' name' | (table', Identifier name') <- shrink (table, Identifier name :: Identifier '["fooColumn", "barColumn"])]

data ColumnAlias a
    = ColumnAlias a Text ColumnAliasId
      deriving ( Data, Generic
               , Read, Show, Eq, Ord
               , Functor, Foldable, Traversable)

data LambdaParam a
    = LambdaParam a Text LambdaParamId
      deriving ( Data, Generic
               , Read, Show, Eq, Ord
               , Functor, Foldable, Traversable)

columnAliasName :: ColumnAlias a -> UQColumnName a
columnAliasName (ColumnAlias info name _) = QColumnName info None name

data RColumnRef a
    = RColumnRef (FQColumnName a)
    | RColumnAlias (ColumnAlias a)
      deriving ( Data, Generic
               , Read, Show, Eq, Ord
               , Functor, Foldable, Traversable)


data StructFieldName a = StructFieldName a Text
    deriving (Data, Generic, Eq, Ord, Show, Functor, Foldable, Traversable)

newtype FieldChain = FieldChain (Map (StructFieldName ()) FieldChain)
    deriving (Eq, Ord, Show)

instance Semigroup FieldChain where
    FieldChain m <> FieldChain n
        | M.null m || M.null n = FieldChain M.empty
        | otherwise = FieldChain $ M.unionWith (<>) m n


data ParamName a
    = ParamName a Text
      deriving (Data, Generic, Eq, Ord, Read, Show, Functor, Foldable, Traversable)

instance Arbitrary a => Arbitrary (ParamName a) where
    arbitrary = ParamName <$> arbitrary <*> arbitraryUnquotedIdentifier
    shrink (ParamName info name) = [ ParamName info name' | Identifier name' <- shrink (Identifier name :: Identifier '["my_param_name"]) ]


instance ToJSON a => ToJSON (DatabaseName a) where
    toJSON (DatabaseName info database) = object
        [ "tag" .= String "DatabaseName"
        , "info" .= info
        , "database" .= database
        ]

instance ToJSON SchemaType

instance (ToJSON (f (DatabaseName a)), ToJSON a) => ToJSON (QSchemaName f a) where
    toJSON (QSchemaName info database schema schemaType) = object
        [ "tag" .= String "QSchemaName"
        , "info" .= info
        , "database" .= database
        , "schema" .= schema
        , "schemaType" .= schemaType
        ]


instance (ToJSON (f (QSchemaName f a)), ToJSON a) => ToJSON (QTableName f a) where
    toJSON (QTableName info schema table) = object
        [ "tag" .= String "QTableName"
        , "info" .= info
        , "schema" .= schema
        , "table" .= table
        ]

instance ToJSON a => ToJSON (TableAlias a) where
    toJSON (TableAlias info name (TableAliasId ident)) = object
        [ "tag" .= String "TableAlias"
        , "info" .= info
        , "name" .= name
        , "ident" .= ident
        ]

instance ToJSON a => ToJSON (RNaturalColumns a) where
    toJSON (RNaturalColumns cols) = object
        [ "tag" .= String "RNaturalColumns"
        , "cols" .= cols
        ]

instance ToJSON a => ToJSON (RUsingColumn a) where
    toJSON (RUsingColumn left right) = object
        [ "tag" .= String "RUsingColumn"
        , "left" .= left
        , "right" .= right
        ]

instance (ToJSON (f (QSchemaName f a)), ToJSON a) => ToJSON (QFunctionName f a) where
    toJSON (QFunctionName info schema fn) = object
        [ "tag" .= String "QFunctionName"
        , "info" .= info
        , "schema" .= schema
        , "function" .= fn
        ]



instance (ToJSON (f (QTableName f a)), ToJSON a) => ToJSON (QColumnName f a) where
    toJSON (QColumnName info table column) = object
        [ "tag" .= String "QColumnName"
        , "info" .= info
        , "table" .= table
        , "column" .= column
        ]


instance ToJSON a => ToJSON (RColumnRef a) where
    toJSON (RColumnRef column) = object
        [ "tag" .= String "RColumnRef"
        , "column" .= column
        ]

    toJSON (RColumnAlias alias) = object
        [ "tag" .= String "RColumnAlias"
        , "alias" .= alias
        ]

instance ToJSON a => ToJSON (ColumnAlias a) where
    toJSON (ColumnAlias info name (ColumnAliasId ident)) = object
        [ "tag" .= String "ColumnAlias"
        , "info" .= info
        , "name" .= name
        , "ident" .= ident
        ]

instance ToJSON a => ToJSON (LambdaParam a) where
    toJSON (LambdaParam info name (LambdaParamId ident)) = object
        [ "tag" .= String "LambdaParam"
        , "info" .= info
        , "name" .= name
        , "ident" .= ident
        ]


instance ToJSON a => ToJSON (StructFieldName a) where
    toJSON (StructFieldName info name) = object
        [ "tag" .= String "StructFieldName"
        , "info" .= info
        , "name" .= name
        ]


instance ToJSON a => ToJSON (ParamName a) where
    toJSON (ParamName info param) = object
        [ "tag" .= String "ParamName"
        , "info" .= info
        , "param" .= param
        ]


instance FromJSON a => FromJSON (DatabaseName a) where
    parseJSON (Object o) = do
        String "DatabaseName" <- o .: "tag"
        DatabaseName <$> o .: "info" <*> o .: "database"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as DatabaseName:"
        , show v
        ]

instance FromJSON SchemaType

instance (FromJSON (f (DatabaseName a)), FromJSON a) => FromJSON (QSchemaName f a) where
    parseJSON (Object o) = do
        String "QSchemaName" <- o .: "tag"
        QSchemaName <$> o .: "info" <*> o .: "database" <*> o .: "schema" <*> o .: "schemaType"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as QSchemaName:"
        , show v
        ]

instance (FromJSON (f (QSchemaName f a)), FromJSON a) => FromJSON (QTableName f a) where
    parseJSON (Object o) = do
        String "QTableName" <- o .: "tag"
        QTableName <$> o .: "info" <*> o .: "schema" <*> o .: "table"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as QTableName:"
        , show v
        ]

instance FromJSON a => FromJSON (TableAlias a) where
    parseJSON (Object o) = do
        String "TableAlias" <- o .: "tag"
        TableAlias <$> o .: "info" <*> o .: "name" <*> (TableAliasId <$> o .: "ident")

    parseJSON v = fail $ unwords
        [ "don't know how to parse as TableAlias:"
        , show v
        ]


instance (FromJSON (f (QSchemaName f a)), FromJSON a) => FromJSON (QFunctionName f a) where
    parseJSON (Object o) = do
        String "QFunctionName" <- o .: "tag"
        QFunctionName <$> o .: "info" <*> o .: "schema" <*> o .: "function"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as QFunctionName:"
        , show v
        ]


instance (FromJSON (f (QTableName f a)), FromJSON a) => FromJSON (QColumnName f a) where
    parseJSON (Object o) = do
        String "QColumnName" <- o .: "tag"
        QColumnName <$> o .: "info" <*> o .: "table" <*> o .: "column"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as QColumnName:"
        , show v
        ]


instance FromJSON a => FromJSON (RColumnRef a) where
    parseJSON (Object o) = do
        o .: "tag" >>= \case
            String "RColumnRef" -> RColumnRef <$> o .: "table"
            String "RColumnAlias" -> RColumnAlias <$> o .: "alias"
            String tag -> fail $ "unrecognized tag for RColumnRef object: " ++ show tag
            _ -> fail $ "unexpected value type for tag on RColumnRef object"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as RColumnRef:"
        , show v
        ]

instance FromJSON a => FromJSON (ColumnAlias a) where
    parseJSON (Object o) = do
        String "ColumnAlias" <- o .: "tag"
        ColumnAlias <$> o .: "info" <*> o .: "name" <*> (ColumnAliasId <$> o .: "ident")

    parseJSON v = fail $ unwords
        [ "don't know how to parse as ColumnAlias:"
        , show v
        ]

instance FromJSON a => FromJSON (LambdaParam a) where
    parseJSON (Object o) = do
        String "LambdaParam" <- o .: "tag"
        LambdaParam <$> o .: "info" <*> o .: "name" <*> (LambdaParamId <$> o .: "ident")

    parseJSON v = fail $ unwords
        [ "don't know how to parse as LambdaParam:"
        , show v
        ]

instance FromJSON a => FromJSON (StructFieldName a) where
    parseJSON (Object o) = do
        String "StructFieldName" <- o .: "tag"
        StructFieldName
            <$> o .: "info"
            <*> o .: "name"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as StructFieldName:"
        , show v
        ]


instance FromJSON a => FromJSON (ParamName a) where
    parseJSON (Object o) = do
        String "ParamName" <- o .: "tag"
        ParamName <$> o .: "info" <*> o .: "param"

    parseJSON v = fail $ unwords
        [ "don't know how to parse as ParamName:"
        , show v
        ]
