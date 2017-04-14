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

module Database.Sql.Util.Schema where

import qualified Data.HashMap.Strict as HMS

import qualified Data.List as L
import Data.List.NonEmpty (NonEmpty (..))

import Data.Maybe (mapMaybe, maybeToList)

import Database.Sql.Type.Names
import Database.Sql.Type.TableProps
import Database.Sql.Type.Scope

import qualified Database.Sql.Util.Scope as Scope
import qualified Database.Sql.Type as AST

import qualified Data.Foldable as F

import Control.Monad.Reader
import Data.Functor.Identity


data SchemaChange
    = AddColumn (FQColumnName ())
    | DropColumn (FQColumnName ())
    | CreateTable (FQTableName ()) SchemaMember
    | DropTable (FQTableName ())
    | CreateView (FQTableName ()) SchemaMember
    | DropView (FQTableName ())
    | CreateSchema (FQSchemaName ()) SchemaMap
    | DropSchema (FQSchemaName ())
    | CreateDatabase (DatabaseName ()) DatabaseMap
    | UsePath [UQSchemaName ()]


data SchemaChangeError
    = DatabaseMissing (DatabaseName ())
    | SchemaMissing (FQSchemaName ())
    | TableMissing (FQTableName ())
    | ColumnMissing (FQColumnName ())
    | DatabaseCollision (DatabaseName ())
    | SchemaCollision (FQSchemaName ())
    | TableCollision (FQTableName ())
    | ColumnCollision (FQColumnName ())
    | UnsupportedColumnChange (FQTableName ())
    deriving (Eq, Show)


applySchemaChange :: SchemaChange -> Catalog -> (Catalog, [SchemaChangeError])
applySchemaChange (AddColumn fqcn@(QColumnName _ (Identity fqtn@(QTableName _ (Identity fqsn@(QSchemaName _ (Identity db) _ _)) _)) _)) Catalog{..} =
    overCatalogMap $ \ catalog -> do
        let voidDb = void db
            uqsn = fqsn { schemaNameDatabase = None }
            uqtn = fqtn { tableNameSchema = None }
            uqcn = fqcn { columnNameTable = None }
        case HMS.lookup voidDb catalog of
            Nothing ->
                let schema = HMS.singleton uqtn (persistentTable [uqcn])
                    database = HMS.singleton uqsn schema
                 in (HMS.insert voidDb database catalog, [DatabaseMissing voidDb])
            Just database ->
                case HMS.lookup uqsn database of
                    Nothing ->
                        let schema = HMS.singleton uqtn (persistentTable [uqcn])
                         in (HMS.adjust (HMS.insert uqsn schema) voidDb catalog, [SchemaMissing fqsn])
                    Just schema ->
                        case HMS.lookup uqtn schema of
                            Nothing ->
                                let schema' = HMS.insert uqtn (persistentTable [uqcn]) schema
                                 in (HMS.adjust (HMS.insert uqsn schema') voidDb catalog, [TableMissing fqtn])
                            Just SchemaMember{..}
                                | tableType /= Table -> (catalog, [UnsupportedColumnChange fqtn])
                                | L.elem uqcn columnsList -> (catalog, [ColumnCollision fqcn])
                                | otherwise ->
                                    let appendColumn sm = sm { columnsList = columnsList ++ [uqcn] }
                                     in (HMS.adjust (HMS.adjust (HMS.adjust appendColumn uqtn) uqsn) voidDb catalog, [])

applySchemaChange (DropColumn fqcn@(QColumnName _ (Identity fqtn@(QTableName _ (Identity fqsn@(QSchemaName _ (Identity db) _ _)) _)) _)) Catalog{..} =
    overCatalogMap $ \ catalog -> do
        let voidDb = void db
            uqsn = fqsn { schemaNameDatabase = None }
            uqtn = fqtn { tableNameSchema = None }
            uqcn = fqcn { columnNameTable = None }
        case HMS.lookup voidDb catalog of
            Nothing ->
                let schema = HMS.singleton uqtn (persistentTable [])
                    database = HMS.singleton uqsn schema
                 in (HMS.insert voidDb database catalog, [DatabaseMissing voidDb])
            Just database ->
                case HMS.lookup uqsn database of
                    Nothing ->
                        let schema = HMS.singleton uqtn (persistentTable [])
                         in (HMS.adjust (HMS.insert uqsn schema) voidDb catalog, [SchemaMissing fqsn])
                    Just schema ->
                        case HMS.lookup uqtn schema of
                            Nothing ->
                                let schema' = HMS.insert uqtn (persistentTable []) schema
                                 in (HMS.adjust (HMS.insert uqsn schema') voidDb catalog, [TableMissing fqtn])
                            Just SchemaMember{..}
                                | tableType /= Table -> (catalog, [UnsupportedColumnChange fqtn])
                                | L.elem uqcn columnsList ->
                                    let removeColumn sm = sm { columnsList = L.delete uqcn columnsList }
                                     in (HMS.adjust (HMS.adjust (HMS.adjust removeColumn uqtn) uqsn) voidDb catalog, [])
                                | otherwise -> (catalog, [ColumnMissing fqcn])

applySchemaChange (CreateTable fqtn@(QTableName _ (Identity fqsn@(QSchemaName _ (Identity db) _ _)) _) table) Catalog{..} =
    overCatalogMap $ \ catalog -> do
        let voidDb = void db
            uqsn = fqsn { schemaNameDatabase = None }
            uqtn = fqtn { tableNameSchema = None }
        case HMS.lookup voidDb catalog of
            Nothing ->
                let schema = HMS.singleton uqtn table
                    database = HMS.singleton uqsn schema
                 in (HMS.insert voidDb database catalog, [DatabaseMissing voidDb])
            Just database ->
                case HMS.lookup uqsn database of
                    Nothing ->
                        let schema = HMS.singleton uqtn table
                         in (HMS.adjust (HMS.insert uqsn schema) voidDb catalog, [SchemaMissing fqsn])
                    Just schema ->
                        ( HMS.adjust (HMS.adjust (HMS.insert uqtn table) uqsn) voidDb catalog
                        , [TableCollision fqtn | HMS.member uqtn schema]
                        )

applySchemaChange (DropTable fqtn@(QTableName _ (Identity fqsn@(QSchemaName _ (Identity db) _ _)) _)) Catalog{..} =
    overCatalogMap $ \ catalog -> do
        let voidDb = void db
            uqsn = fqsn { schemaNameDatabase = None }
            uqtn = fqtn { tableNameSchema = None }
        case HMS.lookup voidDb catalog of
            Nothing ->
                let database = HMS.singleton uqsn HMS.empty
                 in (HMS.insert voidDb database catalog, [DatabaseMissing voidDb])
            Just database ->
                case HMS.lookup uqsn database of
                    Nothing -> (HMS.adjust (HMS.insert uqsn HMS.empty) voidDb catalog, [SchemaMissing fqsn])
                    Just schema ->
                        ( HMS.adjust (HMS.adjust (HMS.delete uqtn) uqsn) voidDb catalog
                        , [TableMissing fqtn | not $ HMS.member uqtn schema]
                        )

applySchemaChange (CreateView fqvn@(QTableName _ (Identity fqsn@(QSchemaName _ (Identity db) _ _)) _) view) Catalog{..} =
    overCatalogMap $ \ catalog -> do
        let voidDb = void db
            uqsn = fqsn { schemaNameDatabase = None }
            uqvn = fqvn { tableNameSchema = None }
        case HMS.lookup voidDb catalog of
            Nothing ->
                let schema = HMS.singleton uqvn view
                    database = HMS.singleton uqsn schema
                 in (HMS.insert voidDb database catalog, [DatabaseMissing voidDb])
            Just database ->
                case HMS.lookup uqsn database of
                    Nothing ->
                        let schema = HMS.singleton uqvn view
                         in (HMS.adjust (HMS.insert uqsn schema) voidDb catalog, [SchemaMissing fqsn])
                    Just schema ->
                        ( HMS.adjust (HMS.adjust (HMS.insert uqvn view) uqsn) voidDb catalog
                        , [TableCollision fqvn | HMS.member uqvn schema]
                        )

applySchemaChange (DropView fqvn@(QTableName _ (Identity fqsn@(QSchemaName _ (Identity db) _ _)) _)) Catalog{..} =
    overCatalogMap $ \ catalog -> do
        let voidDb = void db
            uqsn = fqsn { schemaNameDatabase = None }
            uqvn = fqvn { tableNameSchema = None }
        case HMS.lookup voidDb catalog of
            Nothing ->
                let database = HMS.singleton uqsn HMS.empty
                 in (HMS.insert voidDb database catalog, [DatabaseMissing voidDb])
            Just database ->
                case HMS.lookup uqsn database of
                    Nothing -> (HMS.adjust (HMS.insert uqsn HMS.empty) voidDb catalog, [SchemaMissing fqsn])
                    Just schema ->
                        ( HMS.adjust (HMS.adjust (HMS.delete uqvn) uqsn) voidDb catalog
                        , [TableMissing fqvn | not $ HMS.member uqvn schema]
                        )

applySchemaChange (CreateSchema fqsn@(QSchemaName _ (Identity db) _ _) schema) Catalog{..} = overCatalogMap $ \ catalog ->
    let voidDb = void db
        uqsn = fqsn { schemaNameDatabase = None }
     in case HMS.lookup voidDb catalog of
            Nothing ->
                let database = HMS.singleton uqsn schema
                 in (HMS.insert voidDb database catalog, [DatabaseMissing voidDb])
            Just database ->
                ( HMS.adjust (HMS.insert uqsn schema) voidDb catalog
                , [SchemaCollision fqsn | HMS.member uqsn database]
                )

applySchemaChange (DropSchema fqsn@(QSchemaName _ (Identity db) _ _)) Catalog{..} = overCatalogMap $ \ catalog ->
    let voidDb = void db
        uqsn = fqsn { schemaNameDatabase = None }
     in case HMS.lookup voidDb catalog of
            Nothing ->
                let database = HMS.singleton uqsn HMS.empty
                 in (HMS.insert voidDb database catalog, [DatabaseMissing voidDb])
            Just database ->
                ( HMS.adjust (HMS.delete uqsn) voidDb catalog
                , [SchemaMissing fqsn | not $ HMS.member uqsn database]
                )

applySchemaChange (CreateDatabase db database) Catalog{..} = overCatalogMap $ \ catalog ->
    ( HMS.insert (void db) database catalog
    , [DatabaseCollision $ void db]
    )

applySchemaChange (UsePath path) Catalog{..} = (catalogWithPath path, [])


class HasSchemaChange q where
    getSchemaChange :: q -> [SchemaChange]


instance HasSchemaChange (AST.Statement d AST.ResolvedNames a) where
    getSchemaChange (AST.QueryStmt _) = []
    getSchemaChange (AST.InsertStmt _) = []
    getSchemaChange (AST.UpdateStmt _) = []
    getSchemaChange (AST.CreateTableStmt (AST.CreateTable{createTableName = AST.RCreateTableName _ AST.Exists, createTableIfNotExists = Just _})) = []
    getSchemaChange (AST.CreateTableStmt (AST.CreateTable{createTableName = AST.RCreateTableName tableName _, ..})) =
        let tableType = Table
            persistence = (void createTablePersistence)
            columnsList = getColumnsForTableDefinition createTableDefinition
            viewQuery = Nothing
         in [CreateTable (void tableName) SchemaMember{..}]

      where
        getColumnsForTableDefinition :: AST.TableDefinition d AST.ResolvedNames a -> [UQColumnName ()]
        getColumnsForTableDefinition (AST.TableColumns _ (c:|cs)) =
            let toColumnName (AST.ColumnOrConstraintColumn (AST.ColumnDefinition{..})) = Just $ void columnDefinitionName
                toColumnName (AST.ColumnOrConstraintConstraint _) = Nothing
             in mapMaybe toColumnName (c:cs)

        getColumnsForTableDefinition (AST.TableLike _ (AST.RTableName _ SchemaMember{..})) =
            case tableType of
                Table -> columnsList
                View -> fail "this shouldn't happen"

        getColumnsForTableDefinition (AST.TableAs _ (Just (c:|cs)) _) = map void (c:cs)
        getColumnsForTableDefinition (AST.TableAs _ Nothing query) = map toUQCN $ Scope.queryColumnNames query

        getColumnsForTableDefinition (AST.TableNoColumnInfo _) = []

    getSchemaChange (AST.AlterTableStmt stmt) = getSchemaChange stmt


    getSchemaChange (AST.DeleteStmt _) = []
    getSchemaChange (AST.TruncateStmt _) = []
    getSchemaChange (AST.DropTableStmt AST.DropTable{dropTableNames = tables}) =
      F.foldMap (\case
                   AST.RDropExistingTableName fqtn _ -> [DropTable $ void fqtn]
                   AST.RDropMissingTableName _ -> []
               ) tables
    getSchemaChange (AST.CreateViewStmt (AST.CreateView{createViewName = AST.RCreateTableName _ AST.Exists, createViewIfNotExists = Just _})) = []
    getSchemaChange (AST.CreateViewStmt (AST.CreateView{createViewName = AST.RCreateTableName viewName _, ..})) =
        let tableType = View
            persistence = (void createViewPersistence)
            columnsList = case createViewColumns of
                Just (c:|cs) -> map void $ c:cs
                Nothing -> map toUQCN $ Scope.queryColumnNames createViewQuery
            viewQuery =  Just (void $ createViewQuery)
         in [CreateView (void viewName) SchemaMember{..}]
    getSchemaChange (AST.DropViewStmt AST.DropView{dropViewName = AST.RDropExistingTableName fqvn _}) = [DropView $ void fqvn]
    getSchemaChange (AST.DropViewStmt AST.DropView{dropViewName = AST.RDropMissingTableName _}) = []
    getSchemaChange (AST.CreateSchemaStmt (AST.CreateSchema{createSchemaName = AST.RCreateSchemaName _ AST.Exists, createSchemaIfNotExists = Just _})) = []
    getSchemaChange (AST.CreateSchemaStmt (AST.CreateSchema{createSchemaName = AST.RCreateSchemaName schemaName _})) = [CreateSchema (void schemaName) HMS.empty]
    getSchemaChange (AST.GrantStmt _) = []
    getSchemaChange (AST.RevokeStmt _) = []
    getSchemaChange (AST.BeginStmt _) = []
    getSchemaChange (AST.CommitStmt _) = []
    getSchemaChange (AST.RollbackStmt _) = []
    getSchemaChange (AST.ExplainStmt _ _) = []
    getSchemaChange (AST.EmptyStmt _) = []


instance HasSchemaChange (AST.AlterTable AST.ResolvedNames a) where
    getSchemaChange (AST.AlterTableRenameTable _ (AST.RTableName from _) (AST.RTableName to table)) =
        [ DropTable (void from)
        , CreateTable (void to) table
        ]
    getSchemaChange (AST.AlterTableRenameColumn _ (AST.RTableName table _) from to) =
        let sameCol :: AST.UQColumnName a -> AST.UQColumnName a -> Bool
            sameCol (AST.QColumnName _ _ fromName) (AST.QColumnName _ _ toName) = fromName == toName
         in if sameCol from to
            then []
            else [ DropColumn $ void $ from { columnNameTable = Identity table }
                 , AddColumn $ void $ to { columnNameTable = Identity table }
                 ]
    getSchemaChange (AST.AlterTableAddColumns _ (AST.RTableName table _) (c:|cs)) =
        let toAddColumn uqcn = AddColumn $ void $ uqcn { columnNameTable = Identity table }
         in map toAddColumn (c:cs)

toUQCN :: AST.RColumnRef a -> UQColumnName ()
toUQCN (AST.RColumnRef (QColumnName _ _ column)) = QColumnName () None column
toUQCN (AST.RColumnAlias (ColumnAlias _ column _)) = QColumnName () None column

instance HasSchemaChange (AST.ResolutionError a) where
    getSchemaChange (AST.MissingDatabase db) = [CreateDatabase (void db) HMS.empty]

    getSchemaChange (AST.MissingSchema oqsn) = maybeToList $ do
        case schemaNameType oqsn of
            NormalSchema -> pure ()
            SessionSchema -> error "missing session schema?"
        db <- AST.schemaNameDatabase oqsn
        pure $ CreateSchema (void oqsn { schemaNameDatabase = pure db } ) HMS.empty

    getSchemaChange (AST.MissingTable oqtn) = maybeToList $ do
        oqsn <- AST.tableNameSchema oqtn
        db <- AST.schemaNameDatabase oqsn
        pure $ CreateTable (void oqtn { tableNameSchema = pure oqsn { schemaNameDatabase = pure db } } ) (persistentTable [])

    getSchemaChange (AST.AmbiguousTable _) = []

    getSchemaChange (AST.MissingColumn oqcn) = maybeToList $ do
            oqtn <- AST.columnNameTable oqcn
            oqsn <- AST.tableNameSchema oqtn
            db <- AST.schemaNameDatabase oqsn
            pure $ AddColumn $ void oqcn { columnNameTable = pure oqtn { tableNameSchema = pure oqsn { schemaNameDatabase = pure db } } }

    getSchemaChange (AST.AmbiguousColumn _) = []
    getSchemaChange (AST.UnintroducedTable _) = []
    getSchemaChange (AST.UnexpectedTable table) = [DropTable (void table)]
    getSchemaChange (AST.UnexpectedSchema table) = [DropSchema (void table)]
    getSchemaChange (AST.BadPositionalReference _ _) = []

instance HasSchemaChange (ResolutionSuccess a) where
    getSchemaChange (ColumnRefDefaulted _ (RColumnRef name)) = [AddColumn $ void name]
    getSchemaChange (TableNameDefaulted _ (RTableName name table)) = [CreateTable (void name) table]
    getSchemaChange (TableRefDefaulted _ (RTableRef name table)) = [CreateTable (void name) table]

    -- I don't think we can infer anything about the schema from aliases
    getSchemaChange (ColumnRefDefaulted _ (RColumnAlias _)) = []
    getSchemaChange (TableRefDefaulted _ (RTableAlias _)) = []

    -- resolving means we have it right, no changes
    getSchemaChange (TableNameResolved _ _) = []
    getSchemaChange (CreateTableNameResolved _ _) = []
    getSchemaChange (CreateSchemaNameResolved _ _) = []
    getSchemaChange (TableRefResolved _ _) = []
    getSchemaChange (ColumnRefResolved _ _) = []
