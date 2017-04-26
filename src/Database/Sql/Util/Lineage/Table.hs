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

module Database.Sql.Util.Lineage.Table where

import           Database.Sql.Type
import           Database.Sql.Util.Tables

import qualified Data.Set as S
import           Data.Set (Set)

import qualified Data.Map as M
import           Data.Map (Map)

import qualified Data.Foldable as F

import Data.Functor.Identity


-- | TableLineage is a set of descendants, each with an associated set of ancestors.
-- Ancestors, for each descendant table, should contain a superset of
-- all proximate tables that could have had an impact on the contents
-- of the descendant following execution of the statement.

type TableLineage = Map FQTN (Set FQTN)

class HasTableLineage q where
  getTableLineage :: q -> TableLineage

instance HasTableLineage (Statement d ResolvedNames a) where
  getTableLineage stmt = tableLineage stmt


mkFQTN :: FQTableName a -> FullyQualifiedTableName
mkFQTN (QTableName _ (Identity (QSchemaName _ (Identity (DatabaseName _ database)) schema _)) name) = FullyQualifiedTableName database schema name

-- a table with no rows (e.g. CREATE TABLE LIKE, TRUNCATE TABLE) has no ancestors.
emptyLineage :: FullyQualifiedTableName -> TableLineage
emptyLineage fqtn = M.singleton fqtn S.empty

-- Squash chained renames:
-- If `from` is a `to` in any lineage so far, then merge with that lineage.
-- Otherwise it's a new lineage.
squashTableLineage :: TableLineage -> TableLineage -> TableLineage
squashTableLineage old new =
    let new' = M.map (foldMap (\ s -> maybe (S.singleton s) id $ M.lookup s old)) new
     in M.union new' old

-- gives the lineage information implied by a statement, at table resolution
tableLineage :: Statement d ResolvedNames a -> TableLineage
tableLineage (QueryStmt _) = M.empty
tableLineage (InsertStmt Insert{insertTable = RTableName tableName _, ..}) = case insertValues of
    -- the contents of a table after an Insert depend on the contents of the
    -- table before the Insert, so the insertTable will always be its own
    -- ancestor.
    InsertExprValues _ _ -> filterByInsertBehavior soloAncestor
    InsertDefaultValues _ -> filterByInsertBehavior soloAncestor
    InsertDataFromFile _ _ -> filterByInsertBehavior soloAncestor
      -- the data in the table changed,
      -- but we can't know anything about its provenance :-/
    InsertSelectValues query ->
        let sources = S.insert fqtn $ getTables query
            ancestry = M.singleton fqtn sources
         in filterByInsertBehavior ancestry
   where
     fqtn = mkFQTN tableName
     soloAncestor = M.singleton fqtn $ S.singleton fqtn

     filterByInsertBehavior :: TableLineage -> TableLineage
     filterByInsertBehavior ancestry = case insertBehavior of
         InsertOverwrite _ -> M.adjust (S.delete fqtn) fqtn ancestry
         InsertAppend _ -> ancestry
         InsertOverwritePartition _ _ -> ancestry
         InsertAppendPartition _ _ -> ancestry

tableLineage (UpdateStmt Update{..}) =
    let RTableName table _ = updateTable
        fqtn = mkFQTN table
        sources = S.insert fqtn $ S.unions [ getTables updateFrom
                                           , getTables updateSetExprs
                                           , getTables updateWhere
                                           ]
     in M.singleton fqtn sources

tableLineage (DeleteStmt (Delete _ (RTableName table _) maybeExpr)) = case maybeExpr of
    -- if there's no WHERE clause, this is equivalent to a TRUNCATE TABLE
    Nothing -> emptyLineage fqtn
    -- otherwise, the contents after a delete depend on the contents before the
    -- delete, so `table` will be its own ancestor.
    Just expr ->
        let sources = S.insert fqtn $ getTables expr
         in M.singleton fqtn sources
    where fqtn = mkFQTN table

tableLineage (TruncateStmt (Truncate _ (RTableName table _))) =
    M.singleton (mkFQTN table) S.empty

tableLineage (CreateTableStmt CreateTable{createTableName = RCreateTableName tableName _, ..}) = case createTableDefinition of
    TableColumns _ _ -> emptyLineage fqtn
    TableLike _ _ -> emptyLineage fqtn
    TableAs _ _ query -> M.singleton fqtn $ getTables query
    TableNoColumnInfo _ -> emptyLineage fqtn
  where
    fqtn = mkFQTN tableName

tableLineage (DropTableStmt DropTable{dropTableNames = tables}) =
    F.foldl' (\acc v ->
                case v of
              RDropExistingTableName tableName _ -> M.insert (mkFQTN tableName) S.empty acc
              RDropMissingTableName _ -> acc
              ) M.empty tables

tableLineage (AlterTableStmt (AlterTableRenameTable _ (RTableName from _) (RTableName to _))) =
    let a = mkFQTN from
        d = mkFQTN to
     in M.fromList [(d, S.singleton a), (a, S.empty)]
tableLineage (AlterTableStmt (AlterTableRenameColumn _ _ _ _)) = M.empty
tableLineage (AlterTableStmt (AlterTableAddColumns _ _ _)) = M.empty

tableLineage (CreateViewStmt _) = M.empty
tableLineage (DropViewStmt _) = M.empty
-- TODO T590907
--
-- Lineage of views is tricky -- the data referenced by a view could change at
-- any time, asynchronously, under the hood. Probably, correct behavior would
-- be to emit descendant=createViewName, ancestors=(getTables on
-- createViewQuery) while also indicating that it's a view, so that in lineage
-- rollup we know not to treat the view as having static lineage.
--
-- It's subtly wrong (and probably more confusing overall) to emit lineage of
-- views without having a downstream method of accommodating their dynamic
-- lineage model, so let's just not emit anything -- at least then the results
-- should be obviously wrong (which is much better than subtly wrong!) :-)

tableLineage (CreateSchemaStmt _) = M.empty
tableLineage (GrantStmt _) = M.empty
tableLineage (RevokeStmt _) = M.empty
tableLineage (BeginStmt _) = M.empty
tableLineage (CommitStmt _) = M.empty
tableLineage (RollbackStmt _) = M.empty
tableLineage (ExplainStmt _ _) = M.empty
tableLineage (EmptyStmt _) = M.empty
