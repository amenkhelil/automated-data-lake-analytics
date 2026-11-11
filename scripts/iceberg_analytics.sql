-- ==============================================================================
-- Perspective 2 : Requêtes Analytiques & Opérations ACID sur Apache Iceberg
-- Base de données : analytics_datalake_db
-- ==============================================================================

-- 1. Définition DDL de la Table Transactionnelle Apache Iceberg (Format Version 2)
CREATE TABLE IF NOT EXISTS "analytics_datalake_db"."clean_orders_iceberg" (
    transaction_id INT,
    date DATE,
    produit STRING,
    categorie STRING,
    prix DOUBLE,
    quantite INT,
    montant_total DOUBLE
)
LOCATION 's3://<VOTRE_BUCKET>/iceberg-warehouse/analytics_datalake_db/clean_orders_iceberg/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'format-version' = '2',
    'write_compression' = 'snappy'
);

-- 2. Interrogation Analytique Standard
SELECT 
    categorie,
    COUNT(*) AS total_commandes,
    ROUND(SUM(montant_total), 2) AS chiffre_affaires
FROM "analytics_datalake_db"."clean_orders_iceberg"
GROUP BY categorie
ORDER BY chiffre_affaires DESC;

-- 3. Opération ACID : Mise à jour ciblée (UPDATE sans réécrire la partition entière)
UPDATE "analytics_datalake_db"."clean_orders_iceberg"
SET categorie = 'Informatique & Bureau'
WHERE categorie = 'Electronique';

-- 4. Conformité RGPD : Suppression unitaire du droit à l'oubli (DELETE atomique)
DELETE FROM "analytics_datalake_db"."clean_orders_iceberg"
WHERE transaction_id = 1004;

-- 5. Audit Historique : Time Travel SQL (Voyage dans le temps)
-- 5.a Consulter la table telle qu'elle était avant la suppression ou la modification
SELECT * 
FROM "analytics_datalake_db"."clean_orders_iceberg" 
FOR SYSTEM_TIME AS OF (current_timestamp - interval '1' hour);

-- 5.b Interrogation par ID de snapshot de validation
-- SELECT * FROM "analytics_datalake_db"."clean_orders_iceberg" FOR SYSTEM_VERSION AS OF 123456789012345;

-- 6. Inspection des Méta-données Internes Iceberg
-- 6.a Historique complet des commits et transactions
SELECT * FROM "analytics_datalake_db"."clean_orders_iceberg$history";

-- 6.b Liste des instantanés (snapshots) et fichiers manifestes
SELECT snapshot_id, parent_id, operation, summary 
FROM "analytics_datalake_db"."clean_orders_iceberg$snapshots";

-- 6.c Inspection des fichiers de données actifs
SELECT file_path, file_format, record_count, file_size_in_bytes 
FROM "analytics_datalake_db"."clean_orders_iceberg$files";

