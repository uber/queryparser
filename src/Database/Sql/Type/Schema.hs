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

{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Sql.Type.Schema where

import Prelude hiding ((&&), (||), not)

import Database.Sql.Type.Names
import Database.Sql.Type.TableProps
import Database.Sql.Type.Scope

import Control.Arrow (first)
import Control.Monad.Except
import Control.Monad.Writer
import Data.Functor.Identity

import qualified Data.HashMap.Strict as HMS

import Data.Maybe (mapMaybe, maybeToList)
import Data.Predicate.Class


overWithColumns :: (r a -> s a) -> WithColumns r a -> WithColumns s a
overWithColumns f (WithColumns r cs) = WithColumns (f r) cs


resolvedColumnHasName :: QColumnName f a -> RColumnRef a -> Bool
resolvedColumnHasName (QColumnName _ _ name) (RColumnAlias (ColumnAlias _ name' _)) = name' == name
resolvedColumnHasName (QColumnName _ _ name) (RColumnRef (QColumnName _ _ name')) = name' == name

makeCatalog :: CatalogMap -> Path -> CurrentDatabase -> Catalog
makeCatalog catalog path currentDb = Catalog{..}
  where
    catalogResolveTableNameHelper oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db@(DatabaseName _ _)) schemaName schemaType)) tableName) = do
        let fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            fqtn = QTableName tInfo (pure fqsn) tableName
            default' = RTableName fqtn (persistentTable [])
            missingD = Left $ MissingDatabase db
            missingS = Left $ MissingSchema oqsn
            missingT = Left $ MissingTable oqtn
            tableNameResolved = Right $ TableNameResolved oqtn default'
        case HMS.lookup (void db) catalog of
            Nothing -> tell [missingD, missingS, missingT, tableNameResolved] >> pure default'
            Just database ->
                case HMS.lookup (QSchemaName () None schemaName schemaType) database of
                    Nothing -> tell [missingS, missingT, tableNameResolved] >> pure default'
                    Just schema -> do
                        case HMS.lookup (QTableName () None tableName) schema of
                            Nothing -> tell [missingT, tableNameResolved] >> pure default'
                            Just table -> do
                                let rtn = RTableName fqtn table
                                tell [Right $ TableNameResolved oqtn rtn]
                                pure rtn

    catalogResolveTableNameHelper _ = error "only call catalogResolveTableNameHelper with fully qualified table name"

    catalogResolveTableName oqtn@(QTableName _ (Just (QSchemaName _ (Just (DatabaseName _ _)) _ _)) _) =
        catalogResolveTableNameHelper oqtn

    catalogResolveTableName (QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName) =
        catalogResolveTableNameHelper $ QTableName tInfo (Just $ inCurrentDb oqsn) tableName

    catalogResolveTableName oqtn@(QTableName tInfo Nothing tableName) = do
        let getTableFromSchema uqsn@(QSchemaName _ None schemaName schemaType) = do
                db <- HMS.lookup currentDb catalog
                schema <- HMS.lookup uqsn db
                table <- HMS.lookup (QTableName () None tableName) schema
                let db' = fmap (const tInfo) currentDb
                    fqsn = QSchemaName tInfo (pure db') schemaName schemaType
                    fqtn = QTableName tInfo (pure fqsn) tableName
                pure $ RTableName fqtn table

        case mapMaybe getTableFromSchema path of
            rtn:_ -> do
                tell [Right $ TableNameResolved oqtn rtn]
                pure rtn
            [] -> throwError $ MissingTable oqtn

    -- TODO session schemas should have the name set to the session ID
    catalogResolveSchemaName :: forall a . OQSchemaName a -> CatalogObjectResolver a (FQSchemaName a)
    catalogResolveSchemaName (QSchemaName sInfo (Just db) schemaName schemaType) =
        pure $ QSchemaName sInfo (pure db) schemaName schemaType
    catalogResolveSchemaName oqsn@(QSchemaName _ Nothing _ _) =
        pure $ inCurrentDb oqsn

    catalogHasDatabase databaseName =
        case HMS.member (void databaseName) catalog of
            False -> DoesNotExist
            True -> Exists

    catalogHasSchema schemaName =
        case HMS.lookup currentDb catalog of
            Just db -> case HMS.member (void schemaName) db of
                False -> DoesNotExist
                True -> Exists
            Nothing -> DoesNotExist

    catalogResolveTableRefHelper oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db@(DatabaseName _ _)) schemaName schemaType)) tableName) = do
        let fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            fqtn = QTableName tInfo (pure fqsn) tableName

        case HMS.lookup (void db) catalog of
            Nothing -> throwError $ MissingDatabase db
            Just database -> case HMS.lookup (QSchemaName () None schemaName schemaType) database of
                    Nothing -> throwError $ MissingSchema oqsn
                    Just tables -> do
                        case HMS.lookup (QTableName () None tableName) tables of
                            Nothing -> throwError $ MissingTable oqtn
                            Just table@SchemaMember{..} -> do
                                let makeRColumnRef (QColumnName () None name) = RColumnRef $ QColumnName tInfo (pure fqtn) name
                                    tableRef = RTableRef fqtn table
                                tell [Right $ TableRefResolved oqtn tableRef]
                                pure $ WithColumns tableRef [(Just tableRef, map makeRColumnRef columnsList)]

    catalogResolveTableRefHelper _ = error "only call catalogResolveTableRefHelper with fully qualified table name"

    catalogResolveTableRef _ oqtn@(QTableName _ (Just (QSchemaName _ (Just (DatabaseName _ _)) _ _)) _) =
        catalogResolveTableRefHelper oqtn

    catalogResolveTableRef _ (QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName) =
        catalogResolveTableRefHelper $ QTableName tInfo (Just $ inCurrentDb oqsn) tableName

    catalogResolveTableRef boundCTEs oqtn@(QTableName tInfo Nothing tableName) = do
        case filter (resolvedTableHasName oqtn . fst) $ map (first RTableAlias) boundCTEs of
            [(t, cs)] -> do
                tell [Right $ TableRefResolved oqtn t]
                pure $ WithColumns t [(Just t, cs)]
            _:_ -> throwError $ AmbiguousTable oqtn
            [] -> do
                let getTableFromSchema uqsn@(QSchemaName _ None schemaName schemaType) = do
                        db <- HMS.lookup currentDb catalog
                        schema <- HMS.lookup uqsn db
                        table@SchemaMember{..} <- HMS.lookup (QTableName () None tableName) schema
                        let db' = fmap (const tInfo) currentDb
                            fqsn = QSchemaName tInfo (pure db') schemaName schemaType
                            fqtn = QTableName tInfo (pure fqsn) tableName
                            makeRColumnRef (QColumnName () None name) = RColumnRef $ QColumnName tInfo (pure fqtn) name
                            tableRef = RTableRef fqtn table
                        pure $ WithColumns tableRef [(Just tableRef, map makeRColumnRef columnsList)]

                case mapMaybe getTableFromSchema path of
                    table@(WithColumns tableRef _):_ -> do
                        tell [Right $ TableRefResolved oqtn tableRef]
                        pure table
                    [] -> throwError $ MissingTable oqtn

    catalogResolveCreateSchemaName oqsn = do
        fqsn@(QSchemaName _ (Identity db) schemaName schemaType) <- case schemaNameType oqsn of
            NormalSchema -> catalogResolveSchemaName oqsn
            SessionSchema -> error "can't create the session schema"
        existence <- case HMS.lookup (void db) catalog of
            Nothing -> tell [Left $ MissingDatabase db] >> pure DoesNotExist
            Just database -> if HMS.member (QSchemaName () None schemaName schemaType) database
                then pure Exists
                else pure DoesNotExist
        let rcsn = RCreateSchemaName fqsn existence
        tell [Right $ CreateSchemaNameResolved oqsn rcsn]
        pure rcsn

    catalogResolveCreateTableName name = do
        oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db) schemaName schemaType)) tableName) <-
                case name of
                    oqtn@(QTableName _ Nothing _) -> pure $ inHeadOfPath oqtn
                    QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName -> pure $ QTableName tInfo (pure $ inCurrentDb oqsn) tableName
                    _ -> pure name

        let missingD = Left $ MissingDatabase db
            missingS = Left $ MissingSchema oqsn
        existence <- case HMS.lookup (void db) catalog of
            Nothing -> tell [missingD, missingS] >> pure DoesNotExist
            Just database -> case HMS.lookup (QSchemaName () None schemaName schemaType) database of
                Nothing -> tell [missingS] >> pure DoesNotExist
                Just schema -> if HMS.member (QTableName () None tableName) schema
                     then pure Exists
                     else pure DoesNotExist

        let fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            rctn = RCreateTableName (QTableName tInfo (pure fqsn) tableName) existence
        tell [Right $ CreateTableNameResolved oqtn rctn]

        pure rctn

    inCurrentDb :: Applicative g => QSchemaName f a -> QSchemaName g a
    inCurrentDb (QSchemaName sInfo _ schemaName schemaType) =
        let db = fmap (const sInfo) currentDb
         in QSchemaName sInfo (pure db) schemaName schemaType

    inHeadOfPath :: Applicative g => QTableName f a -> QTableName g a
    inHeadOfPath (QTableName tInfo _ tableName) =
        let db = fmap (const tInfo) currentDb
            QSchemaName _ None schemaName schemaType = head path
            qsn = QSchemaName tInfo (pure db) schemaName schemaType
         in QTableName tInfo (pure qsn) tableName

    catalogResolveColumnName :: forall a . [(Maybe (RTableRef a), [RColumnRef a])] -> OQColumnName a -> CatalogObjectResolver a (RColumnRef a)
    catalogResolveColumnName boundColumns oqcn@(QColumnName cInfo (Just oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db) schema schemaType)) table)) column) = do
        case filter (maybe False (resolvedTableHasDatabase db && resolvedTableHasSchema oqsn && resolvedTableHasName oqtn) . fst) boundColumns of
            [] -> throwError $ UnintroducedTable oqtn
            _:_:_ -> throwError $ AmbiguousTable oqtn
            [(_, columns)] ->
                case filter (resolvedColumnHasName oqcn) columns of
                    [] -> do
                        let c = RColumnRef $ QColumnName cInfo (pure $ QTableName tInfo (pure $ QSchemaName sInfo (pure db) schema schemaType) table) column
                        tell [ Left $ MissingColumn oqcn
                             , Right $ ColumnRefResolved oqcn c
                             ]
                        pure c
                    [c] -> do
                        let c' = fmap (const cInfo) c
                        tell [Right $ ColumnRefResolved oqcn c']
                        pure c'
                    _ -> throwError $ AmbiguousColumn oqcn

    catalogResolveColumnName boundColumns oqcn@(QColumnName cInfo (Just oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo Nothing schema schemaType)) table)) column) = do
        case filter (maybe False (resolvedTableHasSchema oqsn && resolvedTableHasName oqtn) . fst) boundColumns of
            [] -> throwError $ UnintroducedTable oqtn
            _:_:_ -> throwError $ AmbiguousTable oqtn
            [(table', columns)] ->
                case filter (resolvedColumnHasName oqcn) columns of
                    [] -> do
                        let Just (RTableRef (QTableName _ (Identity (QSchemaName _ (Identity (DatabaseName _ db)) _ _)) _) _) = table' -- this pattern match shouldn't fail:
                            -- the `maybe False` prevents Nothings, and the `resolvedTableHasSchema` prevents RTableAliases
                            c = RColumnRef $ QColumnName cInfo (pure $ QTableName tInfo (pure $ QSchemaName sInfo (pure $ DatabaseName sInfo db) schema schemaType) table) column
                        tell [ Left $ MissingColumn oqcn
                             , Right $ ColumnRefResolved oqcn c
                             ]
                        pure c
                    [c] -> do
                        let c' = fmap (const cInfo) c
                        tell [Right $ ColumnRefResolved oqcn c']
                        pure c'
                    _ -> throwError $ AmbiguousColumn oqcn

    catalogResolveColumnName boundColumns oqcn@(QColumnName cInfo (Just oqtn@(QTableName tInfo Nothing table)) column) = do
        let setInfo :: Functor f => f a -> f a
            setInfo = fmap (const cInfo)

        case [ (t, cs) | (mt, cs) <- boundColumns, t <- maybeToList mt, resolvedTableHasName oqtn t ] of
            [] -> throwError $ UnintroducedTable oqtn
            [(table', columns)] -> do
                case filter (resolvedColumnHasName oqcn) columns of
                    [] -> case table' of
                        RTableAlias _ -> throwError $ MissingColumn oqcn
                        RTableRef fqtn@(QTableName _ (Identity (QSchemaName _ (Identity (DatabaseName _ db)) schema schemaType)) _) _ -> do
                            let c = RColumnRef $ QColumnName cInfo (pure $ setInfo fqtn) column
                            tell [ Left $ MissingColumn $ QColumnName cInfo (Just $ QTableName tInfo (Just $ QSchemaName cInfo (Just $ DatabaseName cInfo db) schema schemaType) table) column
                                 , Right $ ColumnRefResolved oqcn c]
                            pure c
                    [c] -> do
                        let c' = setInfo c
                        tell [Right $ ColumnRefResolved oqcn c']
                        pure c'
                    _ -> throwError $ AmbiguousColumn oqcn
            tables -> do
                tell [Left $ AmbiguousTable oqtn]
                case filter (resolvedColumnHasName oqcn) $ snd =<< tables of
                    [] -> throwError $ MissingColumn oqcn
                    [c] -> do
                        let c' = setInfo c
                        tell [Right $ ColumnRefResolved oqcn c']
                        pure c'
                    _ -> throwError $ AmbiguousColumn oqcn

    catalogResolveColumnName boundColumns oqcn@(QColumnName cInfo Nothing _) = do
        let columns = snd =<< boundColumns
        case filter (resolvedColumnHasName oqcn) columns of
            [] -> throwError $ MissingColumn oqcn
            [c] -> do
                let c' = fmap (const cInfo) c
                tell [Right $ ColumnRefResolved oqcn c']
                pure c'
            _ -> throwError $ AmbiguousColumn oqcn

    catalogHasTable tableName =
        let getTableFromSchema uqsn = do
                database <- HMS.lookup currentDb catalog
                schema <- HMS.lookup uqsn database
                pure $ HMS.member tableName schema
         in case any id $ mapMaybe getTableFromSchema path of
            False -> DoesNotExist
            True -> Exists

    overCatalogMap f =
        let (cm, extra) = f catalog
         in seq cm $ (makeCatalog cm path currentDb, extra)

    catalogMap = catalog

    catalogWithPath newPath = makeCatalog catalog newPath currentDb

    catalogWithDatabase = makeCatalog catalog path

defaultSchemaMember :: SchemaMember
defaultSchemaMember = SchemaMember{..}
  where
    tableType = Table
    persistence = Persistent
    columnsList = []
    viewQuery = Nothing

unknownDatabase :: a -> DatabaseName a
unknownDatabase info = DatabaseName info "<unknown>"

unknownSchema :: a -> FQSchemaName a
unknownSchema info = QSchemaName info (pure $ unknownDatabase info) "<unknown>" NormalSchema

unknownTable :: a -> FQTableName a
unknownTable info = QTableName info (pure $ unknownSchema info) "<unknown>"

makeDefaultingCatalog :: CatalogMap -> Path -> CurrentDatabase -> Catalog
makeDefaultingCatalog catalog path currentDb = Catalog{..}
  where
    catalogResolveTableNameHelper oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db@(DatabaseName _ _)) schemaName schemaType)) tableName) = do
        let fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            fqtn = QTableName tInfo (pure fqsn) tableName
            default' = RTableName fqtn (persistentTable [])
            missingD = Left $ MissingDatabase db
            missingS = Left $ MissingSchema oqsn
            missingT = Left $ MissingTable oqtn
            tableNameResolved = Right $ TableNameResolved oqtn default'
        case HMS.lookup (void db) catalog of
            Nothing -> tell [missingD, missingS, missingT, tableNameResolved] >> pure default'
            Just database ->
                case HMS.lookup (QSchemaName () None schemaName schemaType) database of
                    Nothing -> tell [missingS, missingT, tableNameResolved] >> pure default'
                    Just schema -> do
                        case HMS.lookup (QTableName () None tableName) schema of
                            Nothing -> tell [missingT, tableNameResolved] >> pure default'
                            Just table -> do
                                let rtn = RTableName fqtn table
                                tell [Right $ TableNameResolved oqtn rtn]
                                pure rtn

    catalogResolveTableNameHelper _ = error "only call catalogResolveTableNameHelper with fully qualified table name"

    catalogResolveTableName oqtn@(QTableName _ (Just (QSchemaName _ (Just (DatabaseName _ _)) _ _)) _) =
        catalogResolveTableNameHelper oqtn

    catalogResolveTableName (QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName) =
        catalogResolveTableNameHelper $ QTableName tInfo (Just $ inCurrentDb oqsn) tableName

    catalogResolveTableName oqtn@(QTableName tInfo Nothing tableName) = do
        let getTableFromSchema uqsn@(QSchemaName _ None schemaName schemaType) = do
                db <- HMS.lookup currentDb catalog
                schema <- HMS.lookup uqsn db
                table <- HMS.lookup (QTableName () None tableName) schema
                let db' = fmap (const tInfo) currentDb
                    fqsn = QSchemaName tInfo (pure db') schemaName schemaType
                    fqtn = QTableName tInfo (pure fqsn) tableName
                pure $ RTableName fqtn table

        case mapMaybe getTableFromSchema path of
            rtn:_ -> do
                tell [Right $ TableNameResolved oqtn rtn]
                pure rtn
            [] -> do
                let rtn = RTableName (inHeadOfPath oqtn) $ persistentTable []
                tell [ Left $ MissingTable oqtn
                     , Right $ TableNameDefaulted oqtn rtn
                     ]
                pure rtn

    inCurrentDb :: Applicative g => QSchemaName f a -> QSchemaName g a
    inCurrentDb (QSchemaName sInfo _ schemaName schemaType) =
        let db = fmap (const sInfo) currentDb
         in QSchemaName sInfo (pure db) schemaName schemaType

    inHeadOfPath :: Applicative g => QTableName f a -> QTableName g a
    inHeadOfPath (QTableName tInfo _ tableName) =
        let db = fmap (const tInfo) currentDb
            QSchemaName _ None schemaName schemaType = head path
            fqsn = QSchemaName tInfo (pure db) schemaName schemaType
         in QTableName tInfo (pure fqsn) tableName

    -- TODO session schemas should have the name set to the session ID
    catalogResolveSchemaName :: forall a . OQSchemaName a -> CatalogObjectResolver a (FQSchemaName a)
    catalogResolveSchemaName (QSchemaName sInfo (Just db) schemaName schemaType) =
        pure $ QSchemaName sInfo (pure db) schemaName schemaType
    catalogResolveSchemaName oqsn@(QSchemaName _ Nothing _ _) =
        pure $ inCurrentDb oqsn

    catalogHasDatabase databaseName =
        case HMS.member (void databaseName) catalog of
            False -> DoesNotExist
            True -> Exists

    catalogHasSchema schemaName =
        case HMS.lookup currentDb catalog of
            Just db -> case HMS.member (void schemaName) db of
                False -> DoesNotExist
                True -> Exists
            Nothing -> DoesNotExist

    catalogResolveTableRefHelper oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db@(DatabaseName _ _)) schemaName schemaType)) tableName) = do
        let fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            fqtn = QTableName tInfo (pure fqsn) tableName
            defaultTableRef = RTableRef fqtn defaultSchemaMember
            missingD = Left $ MissingDatabase db
            missingS = Left $ MissingSchema oqsn
            missingT = Left $ MissingTable oqtn
            tableRefResolved = Right $ TableRefResolved oqtn defaultTableRef
            default' = WithColumns defaultTableRef [(Just defaultTableRef, [])]

        case HMS.lookup (void db) catalog of
            Nothing -> tell [missingD, missingS, missingT, tableRefResolved] >> pure default'
            Just database -> case HMS.lookup (QSchemaName () None schemaName schemaType) database of
                Nothing -> tell [missingS, missingT, tableRefResolved] >> pure default'
                Just schema -> do
                    case HMS.lookup (QTableName () None tableName) schema of
                        Nothing -> tell [missingT, tableRefResolved] >> pure default'
                        Just table@SchemaMember{..} -> do
                            let makeRColumnRef (QColumnName () None name) = RColumnRef $ QColumnName tInfo (pure fqtn) name
                                tableRef = RTableRef fqtn table
                            tell [Right $ TableRefResolved oqtn tableRef]
                            pure $ WithColumns tableRef [(Just tableRef, map makeRColumnRef columnsList)]

    catalogResolveTableRefHelper _ = error "only call catalogResolveTableRefHelper with fully qualified table name"

    catalogResolveTableRef _ oqtn@(QTableName _ (Just (QSchemaName _ (Just (DatabaseName _ _)) _ _)) _) =
        catalogResolveTableRefHelper oqtn

    catalogResolveTableRef _ (QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName) =
        catalogResolveTableRefHelper $ QTableName tInfo (Just $ inCurrentDb oqsn) tableName

    catalogResolveTableRef boundCTEs oqtn@(QTableName tInfo Nothing tableName) = do
        case filter (resolvedTableHasName oqtn . fst) $ map (first RTableAlias) boundCTEs of
            ts@((t, _):rest) -> do
                if (null rest)
                   then tell [ Right $ TableRefResolved oqtn t ]
                   else tell [ Left $ AmbiguousTable oqtn
                             , Right $ TableRefDefaulted oqtn t
                             ]
                let ts' = map (first Just) ts
                pure $ WithColumns t ts'
            [] -> do
                let getTableFromSchema uqsn@(QSchemaName _ None schemaName schemaType) = do
                        db <- HMS.lookup currentDb catalog
                        schema <- HMS.lookup uqsn db
                        table@SchemaMember{..} <- HMS.lookup (QTableName () None tableName) schema
                        let db' = fmap (const tInfo) currentDb
                            fqsn = QSchemaName tInfo (pure db') schemaName schemaType
                            fqtn = QTableName tInfo (pure fqsn) tableName
                            makeRColumnRef (QColumnName () None name) = RColumnRef $ QColumnName tInfo (pure fqtn) name
                            tableRef = RTableRef fqtn table
                        pure $ WithColumns tableRef [(Just tableRef, map makeRColumnRef columnsList)]

                case mapMaybe getTableFromSchema path of
                    table@(WithColumns tableRef _):_ -> do
                        tell [Right $ TableRefResolved oqtn tableRef]
                        pure table
                    [] -> do
                        let tableRef = RTableRef (inHeadOfPath oqtn) defaultSchemaMember
                        tell [ Left $ MissingTable oqtn
                             , Right $ TableRefDefaulted oqtn tableRef
                             ]
                        -- TODO: deal with columns
                        pure $ WithColumns tableRef [(Just tableRef, [])]

    catalogResolveCreateSchemaName oqsn = do
        fqsn@(QSchemaName _ (Identity db) schemaName schemaType) <- case schemaNameType oqsn of
            NormalSchema -> catalogResolveSchemaName oqsn
            SessionSchema -> error "can't create the session schema"
        existence <- case HMS.lookup (void db) catalog of
            Nothing -> tell [Left $ MissingDatabase db] >> pure DoesNotExist
            Just database -> if HMS.member (QSchemaName () None schemaName schemaType) database
                then pure Exists
                else pure DoesNotExist
        let rcsn = RCreateSchemaName fqsn existence
        tell [Right $ CreateSchemaNameResolved oqsn rcsn]
        pure rcsn

    catalogResolveCreateTableName name = do
        oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db) schemaName schemaType)) tableName) <-
                case name of
                    oqtn@(QTableName _ Nothing _) -> pure $ inHeadOfPath oqtn
                    (QTableName tInfo (Just oqsn@(QSchemaName _ Nothing _ _)) tableName) -> pure $ QTableName tInfo (pure $ inCurrentDb oqsn) tableName
                    _ -> pure name

        let missingD = Left $ MissingDatabase db
            missingS = Left $ MissingSchema oqsn
        existence <- case HMS.lookup (void db) catalog of
            Nothing -> tell [missingD, missingS] >> pure DoesNotExist
            Just database -> case HMS.lookup (QSchemaName () None schemaName schemaType) database of
                Nothing -> tell [missingS] >> pure DoesNotExist
                Just schema -> if HMS.member (QTableName () None tableName) schema
                     then pure Exists
                     else pure DoesNotExist

        let fqsn = QSchemaName sInfo (pure db) schemaName schemaType
            rctn = RCreateTableName (QTableName tInfo (pure fqsn) tableName) existence
        tell [Right $ CreateTableNameResolved oqtn rctn]

        pure rctn

    catalogResolveColumnName :: forall a . [(Maybe (RTableRef a), [RColumnRef a])] -> OQColumnName a -> CatalogObjectResolver a (RColumnRef a)
    catalogResolveColumnName boundColumns oqcn@(QColumnName cInfo (Just oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo (Just db) schema schemaType)) table)) column) = do
        case filter (maybe False (resolvedTableHasDatabase db && resolvedTableHasSchema oqsn && resolvedTableHasName oqtn) . fst) boundColumns of
            [] -> tell [Left $ UnintroducedTable oqtn]
            _:_:_ -> tell [Left $ AmbiguousTable oqtn]
            [(_, columns)] ->
                case filter (resolvedColumnHasName oqcn) columns of
                    [] -> tell [Left $ MissingColumn oqcn]
                    [_] -> pure ()
                    _ -> tell [Left $ AmbiguousColumn oqcn]
        let columnRef = RColumnRef $ QColumnName cInfo (pure $ QTableName tInfo (pure $ QSchemaName sInfo (pure db) schema schemaType) table) column
        tell [Right $ ColumnRefResolved oqcn columnRef]
        pure columnRef

    catalogResolveColumnName boundColumns oqcn@(QColumnName cInfo (Just oqtn@(QTableName tInfo (Just oqsn@(QSchemaName sInfo Nothing schema schemaType)) table)) column) = do
        let filtered = filter (maybe False (resolvedTableHasSchema oqsn && resolvedTableHasName oqtn) . fst) boundColumns
            fqtnDefault = QTableName tInfo (Identity $ inCurrentDb oqsn) table
        fqtn <- case filtered of
            [] -> tell [Left $ UnintroducedTable oqtn] >> pure fqtnDefault
            _:_:_ -> tell [Left $ AmbiguousTable oqtn] >> pure fqtnDefault
            [(table', columns)] -> do
                let Just (RTableRef (QTableName _ (Identity (QSchemaName _ (Identity (DatabaseName _ db)) _ _)) _) _) = table' -- this pattern match shouldn't fail:
                     -- the `maybe False` prevents Nothings, and the `resolvedTableHasSchema` prevents RTableAliases
                    oqcnKnownDb = QColumnName cInfo (Just $ QTableName tInfo (Just $ QSchemaName sInfo (Just $ DatabaseName cInfo db) schema schemaType) table) column
                    fqtnKnownDb = QTableName tInfo (Identity $ QSchemaName sInfo (Identity $ DatabaseName cInfo db) schema schemaType) table
                case filter (resolvedColumnHasName oqcn) columns of
                    [] -> tell [Left $ MissingColumn oqcnKnownDb] >> pure fqtnKnownDb
                    [_] -> pure fqtnKnownDb
                    _ -> tell [Left $ AmbiguousColumn oqcnKnownDb] >> pure fqtnKnownDb

        let columnRef = RColumnRef $ QColumnName cInfo (pure fqtn) column
        tell [Right $ ColumnRefResolved oqcn columnRef]
        pure columnRef

    catalogResolveColumnName boundColumns oqcn@(QColumnName cInfo (Just oqtn@(QTableName tInfo Nothing table)) column) = do
        let setInfo :: Functor f => f a -> f a
            setInfo = fmap (const cInfo)

        case [ (t, cs) | (mt, cs) <- boundColumns, t <- maybeToList mt, resolvedTableHasName oqtn t ] of
            [] -> do
                let c = RColumnRef $ QColumnName cInfo (pure $ QTableName tInfo (pure $ unknownSchema tInfo) table) column
                tell [ Left $ UnintroducedTable oqtn
                     , Right $ ColumnRefDefaulted oqcn c
                     ]
                pure c
            [(table', columns)] -> do
                case filter (resolvedColumnHasName oqcn) columns of
                    [] -> case table' of
                        RTableAlias _ -> do
                            let c = RColumnRef $ QColumnName cInfo (pure $ QTableName tInfo (pure $ unknownSchema tInfo) table) column
                            tell [ Left $ MissingColumn oqcn
                                 , Right $ ColumnRefDefaulted oqcn c
                                 ]
                            pure c
                        RTableRef fqtn@(QTableName _ (Identity (QSchemaName _ (Identity (DatabaseName _ db)) schema schemaType)) _) _ -> do
                            let c = RColumnRef $ QColumnName cInfo (pure $ setInfo fqtn) column
                            tell [ Left $ MissingColumn $ QColumnName cInfo (Just $ QTableName tInfo (Just $ QSchemaName cInfo (Just $ DatabaseName cInfo db) schema schemaType) table) column
                                 , Right $ ColumnRefResolved oqcn c]
                            pure c
                    c:rest -> do
                        let c' = setInfo c
                        if (null rest)
                            then tell [Right $ ColumnRefResolved oqcn c']
                            else tell [ Left $ AmbiguousColumn oqcn
                                      , Right $ ColumnRefDefaulted oqcn c'
                                      ]
                        pure c'
            tables -> do
                tell [Left $ AmbiguousTable oqtn]
                case filter (resolvedColumnHasName oqcn) $ snd =<< tables of
                    [] -> do
                        let c = RColumnRef $ QColumnName cInfo (pure $ QTableName tInfo (pure $ unknownSchema tInfo) table) column
                        tell [ Left $ MissingColumn oqcn
                             , Right $ ColumnRefDefaulted oqcn c
                             ]
                        pure c
                    c:rest -> do
                        let c' = setInfo c
                        if (null rest)
                            then tell [Right $ ColumnRefResolved oqcn c']
                            else tell [ Left $ AmbiguousColumn oqcn
                                      , Right $ ColumnRefDefaulted oqcn c'
                                      ]
                        pure c'

    catalogResolveColumnName boundColumns oqcn@(QColumnName cInfo Nothing column) = do
        let columns = snd =<< boundColumns
        case filter (resolvedColumnHasName oqcn) columns of
            [] -> do
                let table =
                        case boundColumns of
                            [(Just (RTableRef t _), _)] -> t
                            _ -> unknownTable cInfo
                    c = RColumnRef $ QColumnName cInfo (pure table) column
                tell [ Left $ MissingColumn oqcn
                     , Right $ ColumnRefDefaulted oqcn c
                     ]
                pure c
            c:rest -> do
                let c' = fmap (const cInfo) c
                if (null rest)
                    then tell [ Right $ ColumnRefResolved oqcn c' ]
                    else tell [ Left $ AmbiguousColumn oqcn
                              , Right $ ColumnRefDefaulted oqcn c'
                              ]
                pure c'

    catalogHasTable tableName =
        let getTableFromSchema uqsn = do
                database <- HMS.lookup currentDb catalog
                schema <- HMS.lookup uqsn database
                pure $ HMS.member tableName schema
         in case any id $ mapMaybe getTableFromSchema path of
            False -> DoesNotExist
            True -> Exists

    overCatalogMap f =
        let (cm, extra) = f catalog
         in seq cm $ (makeDefaultingCatalog cm path currentDb, extra)

    catalogMap = catalog

    catalogWithPath newPath = makeDefaultingCatalog catalog newPath currentDb

    catalogWithDatabase = makeDefaultingCatalog catalog path
