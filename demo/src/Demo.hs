{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

module Demo
    ( parse
    , parseAndResolve
    , catalog
    , demoTablesAccessed
    , demoColumnsAccessedByClause
    , demoJoins
    , demoTableLineage
    , demoAllAnalyses
    ) where

import Database.Sql.Type hiding (catalog)

import           Database.Sql.Util.Scope (runResolverWarn)
import qualified Database.Sql.Vertica.Parser as VP
import           Database.Sql.Vertica.Type (VerticaStatement, resolveVerticaStatement, Vertica)

import Database.Sql.Util.Tables
import Database.Sql.Util.Columns
import Database.Sql.Util.Joins
import Database.Sql.Util.Lineage.Table

import           Data.Either
import           Data.Functor (void)
import qualified Data.HashMap.Strict as HMS
import qualified Data.List as L
import qualified Data.Map as M
import           Data.Proxy
import qualified Data.Set as S
import qualified Data.Text.Lazy as TL

import Text.PrettyPrint


-- let's provide a really simple function to do parsing!
-- It will have ungraceful error handling.
parse :: TL.Text -> VerticaStatement RawNames ()
parse sql = case void <$> VP.parse sql of
    Right q -> q
    Left err -> error $ show err

-- and construct a catalog, with tables `foo` (columns a, b, and c) and `bar` (columns x, y, and z)
catalog :: Catalog
catalog = makeDefaultingCatalog catalogMap [defaultSchema] defaultDatabase
  where
    defaultDatabase :: DatabaseName ()
    defaultDatabase = DatabaseName () "defaultDatabase"

    defaultSchema :: UQSchemaName ()
    defaultSchema = mkNormalSchema "public" ()

    foo :: (UQTableName (), SchemaMember)
    foo = ( QTableName () None "foo", persistentTable [ QColumnName () None "a"
                                                      , QColumnName () None "b"
                                                      , QColumnName () None "c"
                                                      ] )

    bar :: (UQTableName (), SchemaMember)
    bar = ( QTableName () None "bar", persistentTable [ QColumnName () None "x"
                                                      , QColumnName () None "y"
                                                      , QColumnName () None "z"
                                                      ] )

    catalogMap :: CatalogMap
    catalogMap = HMS.singleton defaultDatabase $
                     HMS.fromList [ ( defaultSchema, HMS.fromList [ foo , bar ] ) ]

-- let's provide a really simple function that combines parsing + resolving.
-- We'll hardcode the catalog and leave the error handling ungraceful, still.
parseAndResolve :: TL.Text -> (VerticaStatement ResolvedNames (), [ResolutionError ()])
parseAndResolve sql = case runResolverWarn (resolveVerticaStatement $ parse sql) (Proxy :: Proxy Vertica) catalog of
    (Right queryResolved, resolutions) -> (queryResolved, lefts resolutions)
    (Left err, _) -> error $ show err

-- let's run some analyses!
demoTablesAccessed :: TL.Text -> Doc
demoTablesAccessed sql = draw $ getTables $ fst $ parseAndResolve sql
  where
    draw :: S.Set FullyQualifiedTableName -> Doc
    draw xs = case S.toList xs of
                  [] -> text "no tables accessed"
                  xs' -> vcat $ map drawFQTN xs'

demoColumnsAccessedByClause :: TL.Text -> Doc
demoColumnsAccessedByClause sql = draw $ getColumns $ fst $ parseAndResolve sql
  where
    draw :: S.Set (FullyQualifiedColumnName, Clause) -> Doc
    draw xs = case S.toList xs of
                  [] -> text "no columns accessed"
                  xs' -> vcat $ map drawCol xs'

    drawCol :: (FullyQualifiedColumnName, Clause) -> Doc
    drawCol (col, clause) = hcat [drawFQCN col, text "\t", text (TL.unpack clause)]

demoJoins :: TL.Text -> Doc
demoJoins sql = draw $ getJoins $ fst $ parseAndResolve sql
  where
    draw :: S.Set ((FullyQualifiedColumnName, [StructFieldName ()]), (FullyQualifiedColumnName, [StructFieldName ()])) -> Doc
    draw xs = case S.toList xs of
                  [] -> text "no joins"
                  xs' -> vcat $ map drawJoin xs'

    drawJoin :: ((FullyQualifiedColumnName, [StructFieldName ()]), (FullyQualifiedColumnName, [StructFieldName ()])) -> Doc
    drawJoin (f1, f2) = hsep [drawField f1, text "<->", drawField f2]

demoTableLineage :: TL.Text -> Doc
demoTableLineage sql = draw $ getTableLineage $ fst $ parseAndResolve sql
  where
    draw :: M.Map FQTN (S.Set FQTN) -> Doc
    draw xs = case M.assocs xs of
                  [] -> text "no tables modified"
                  xs' -> vcat $ map drawAssoc xs'

    drawAssoc :: (FQTN, S.Set FQTN) -> Doc
    drawAssoc (tgt, srcs) = case S.toList srcs of
                                [] -> hsep [drawFQTN tgt, text "no longer has data"]
                                srcs' -> hsep [ drawFQTN tgt
                                              , text "after the query depends on"
                                              , drawDeps srcs'
                                              , text "before the query"
                                              ]


    drawDeps :: [FQTN] -> Doc
    drawDeps srcs = hcat $ L.intersperse ", " $ map drawFQTN srcs

demoAllAnalyses :: TL.Text -> Doc
demoAllAnalyses sql = vcat
    -- note the absence of Column Lineage from this list: that analysis is a work in progress.
    [ text "Tables accessed:"
    , nest indent $ demoTablesAccessed sql
    , text "Columns accessed by clause:"
    , nest indent $ demoColumnsAccessedByClause sql
    , text "Joins:"
    , nest indent $ demoJoins sql
    , text "Table lineage:"
    , nest indent $ demoTableLineage sql
    ]
  where
    indent = 4

-- pretty printing helpers
drawFQTN :: FullyQualifiedTableName -> Doc
drawFQTN FullyQualifiedTableName{..} = hcat $ map (text . TL.unpack) $ L.intersperse "." [fqtnSchemaName, fqtnTableName]

drawFQCN :: FullyQualifiedColumnName -> Doc
drawFQCN FullyQualifiedColumnName{..} = hcat $ map (text . TL.unpack) $ L.intersperse "." [fqcnSchemaName, fqcnTableName, fqcnColumnName]

drawField :: (FullyQualifiedColumnName, [StructFieldName ()]) -> Doc
drawField (fqcn, fields) = foldl1 combineWithDot (drawFQCN fqcn : map drawStructFieldName fields)
  where
    combineWithDot x y = x <> text "." <> y
    drawStructFieldName (StructFieldName _ name) = text $ TL.unpack name
