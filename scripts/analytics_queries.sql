-- ====================================================================
-- Projet : Data Lake Analytics sur AWS - Phase 1
-- Requêtes SQL Analytiques Amazon Athena
-- Base de données : analytics_datalake_db
-- Table cible     : clean_processed (générée par le Crawler depuis S3 processed/)
-- ====================================================================

-- 1 -- Vue d'ensemble des données transformées
SELECT * 
FROM "analytics_datalake_db"."clean_processed" 
LIMIT 10;

-- 2 -- Indicateurs clés de performance (KPIs Globaux)
--    Valide le nombre total de commandes nettoyées, le CA global et le panier moyen
SELECT 
    COUNT(*) AS total_commandes,
    ROUND(SUM(montant_total), 2) AS chiffre_affaires_global,
    ROUND(AVG(montant_total), 2) AS panier_moyen,
    ROUND(MIN(montant_total), 2) AS commande_min,
    ROUND(MAX(montant_total), 2) AS commande_max
FROM "analytics_datalake_db"."clean_processed";

-- 3 -- Chiffre d'affaires et volume par catégorie
--    Permet de vérifier le traitement des catégories (y compris 'Non spécifié')
SELECT 
    categorie,
    COUNT(*) AS nombre_articles,
    ROUND(SUM(montant_total), 2) AS chiffre_affaires_categorie,
    ROUND(AVG(montant_total), 2) AS panier_moyen_categorie
FROM "analytics_datalake_db"."clean_processed"
GROUP BY categorie
ORDER BY chiffre_affaires_categorie DESC;

-- 4 -- Top des produits les plus vendus en volume
SELECT 
    produit,
    categorie,
    SUM(quantite) AS quantite_totale_vendue,
    ROUND(SUM(montant_total), 2) AS montant_total_ventes
FROM "analytics_datalake_db"."clean_processed"
GROUP BY produit, categorie
ORDER BY quantite_totale_vendue DESC;
