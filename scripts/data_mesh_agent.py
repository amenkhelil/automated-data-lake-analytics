# -*- coding: utf-8 -*-
"""
Agent Data Mesh — Moteur de Génération PySpark
Point d'entrée unique de l'architecture :
  1. Inspecte l'échantillon de données brutes arrivant dans S3 raw/
  2. Identifie sémantiquement le domaine métier (Ventes, Finance, RH, Logistique)
  3. Génère dynamiquement le script PySpark 'active_etl.py' adapté au domaine :
     - Gouvernance par domaine : hachage SHA-256 PII/RGPD (RH), contrôle TVA (Finance),
       contrôle de prix (Ventes)
     - Isolement automatique des rejets vers la zone de quarantaine S3
     - Écriture Parquet partitionnée et synchronisation Apache Iceberg (ACID)
  4. Valide syntaxiquement et sécuritairement le script avant tout déploiement
  5. Archive chaque version produite avec horodatage UTC et empreinte SHA-256
"""

import ast
import csv
import hashlib
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

# ---------------------------------------------------------------------------
# Modules système interdits dans tout script généré.
# Toute violation déclenche le fallback sécurisé.
# ---------------------------------------------------------------------------
FORBIDDEN_IMPORTS: frozenset = frozenset({
    "os", "subprocess", "socket", "shutil", "requests",
    "urllib", "http", "ftplib", "smtplib", "telnetlib",
    "pickle", "shelve", "tempfile", "pty", "signal",
})

# Seuils de routage par score de confiance de classification.
CONFIDENCE_AUTONOMOUS = 90.0  # >= 90 % : exécution autonome complète
CONFIDENCE_WARNING    = 60.0  # 60-89 % : exécution avec avertissement
                               # <  60 % : routage vers révision humaine

# Script PySpark minimal utilisé lorsque la génération dynamique échoue.
# Ne dépend d'aucun module externe non standard.
_FALLBACK_TEMPLATE = """\
# -*- coding: utf-8 -*-
\"\"\"
AWS Glue ETL Job : Script de Sécurité
Appliqué lorsque la génération dynamique n'est pas utilisable.
Comportement garanti : lecture raw/, filtre prix > 0, écriture Parquet processed/.
\"\"\"

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, lit, when

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name     = args['S3_BUCKET_NAME']
raw_path        = f"s3://{bucket_name}/raw/"
processed_path  = f"s3://{bucket_name}/processed/fallback/"
quarantine_path = f"s3://{bucket_name}/quarantine/"

print("[FALLBACK ETL] Lecture des données brutes...")
df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)

if "Prix" in df_raw.columns:
    df_valid      = df_raw.filter(col("Prix").cast("double") > 0)
    df_quarantine = (
        df_raw.filter(~(col("Prix").cast("double") > 0))
              .withColumn("anomaly_reason", lit("Prix invalide ou absent"))
              .withColumn("quarantine_timestamp", current_timestamp())
    )
else:
    df_valid      = df_raw
    df_quarantine = spark.createDataFrame([], df_raw.schema)

valid_count      = df_valid.count()
quarantine_count = df_quarantine.count()
print(f"[FALLBACK ETL] Valides : {valid_count} | Quarantaine : {quarantine_count}")

if quarantine_count > 0:
    df_quarantine.repartition(1).write.mode("append").parquet(quarantine_path)

df_final = df_valid.withColumn("ingestion_timestamp", current_timestamp())
df_final.write.mode("overwrite").parquet(processed_path)

print("[FALLBACK ETL] Terminé avec succès.")
job.commit()
"""


# ---------------------------------------------------------------------------
# Classe principale
# ---------------------------------------------------------------------------

class DataMeshAgent:
    """Agent d'ingestion et de gouvernance autonome multi-domaines."""

    DOMAIN_POLICIES: Dict[str, Dict[str, Any]] = {
        "SALES_ECOMMERCE": {
            "database":        "analytics_datalake_db",
            "table":           "clean_processed",
            "s3_target":       "processed/sales/",
            "partition_col":   "Categorie",
            "governance_rules": [
                "Contrôle strict des prix unitaires (> 0)",
                "Imputation automatique des catégories manquantes en 'Non spécifié'",
                "Calcul de la métrique dérivée Montant_Total = Prix * Quantite",
            ],
        },
        "FINANCE": {
            "database":        "finance_datalake_db",
            "table":           "factures_consolidees",
            "s3_target":       "processed/finance/",
            "partition_col":   "Devise",
            "governance_rules": [
                "Validation comptable des montants HT (> 0)",
                "Contrôle et recalcul des montants de TVA et montants TTC",
                "Normalisation des devises bancaires ISO",
            ],
        },
        "HUMAN_RESOURCES": {
            "database":        "hr_datalake_db",
            "table":           "collaborateurs_conformes",
            "s3_target":       "processed/hr/",
            "partition_col":   "Departement",
            "governance_rules": [
                "Conformité RGPD / PII : anonymisation SHA-256 des identités (Nom, Prenom, SSN)",
                "Contrôle des salaires positifs",
                "Conversion stricte des dates d'embauche au format ISO yyyy-MM-dd",
            ],
        },
        "LOGISTICS": {
            "database":        "logistics_datalake_db",
            "table":           "expeditions_suivies",
            "s3_target":       "processed/logistics/",
            "partition_col":   "Transporteur",
            "governance_rules": [
                "Validation du numéro de suivi",
                "Contrôle du poids (> 0)",
                "Partitionnement par transporteur",
            ],
        },
    }

    # ------------------------------------------------------------------
    # Inspection & classification
    # ------------------------------------------------------------------

    @classmethod
    def inspect_sample(
        cls, filepath: str, max_rows: int = 5
    ) -> Tuple[List[str], List[Dict[str, str]]]:
        """Lit les en-têtes et les premières lignes du fichier entrant."""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Fichier introuvable : {filepath}")

        with open(filepath, mode="r", encoding="utf-8") as fh:
            reader      = csv.DictReader(fh)
            headers     = list(reader.fieldnames or [])
            sample_rows = [row for _, row in zip(range(max_rows), reader)]

        return headers, sample_rows

    @classmethod
    def classify_domain(cls, headers: List[str]) -> Tuple[str, float]:
        """
        Classifie le domaine métier par intersection lexicale sur les en-têtes.
        Retourne (domaine, score_de_confiance_en_pourcentage).
        """
        clean_headers = {h.strip().lower() for h in headers}

        keyword_map: Dict[str, frozenset] = {
            "SALES_ECOMMERCE": frozenset({
                "transaction_id", "order_id", "prix", "produit",
                "categorie", "quantite", "customer_id", "client",
            }),
            "FINANCE": frozenset({
                "invoice_id", "tva", "taux_tva", "vat",
                "montant_ht", "montant_ttc", "currency", "devise", "iban",
            }),
            "HUMAN_RESOURCES": frozenset({
                "employee_id", "nom", "prenom", "ssn", "salary",
                "salaire", "hire_date", "date_embauche", "departement", "department",
            }),
            "LOGISTICS": frozenset({
                "tracking_number", "shipment_id", "warehouse_id",
                "poids", "weight", "carrier", "transporteur",
            }),
        }

        scores      = {d: len(clean_headers & kw) for d, kw in keyword_map.items()}
        best_domain = max(scores, key=lambda d: scores[d])
        max_score   = scores[best_domain]

        if max_score == 0:
            return "GENERAL_DATA", 50.0

        # 3 mots-clés concordants équivalent à 100 % de confiance.
        confidence = min(round((max_score / 3.0) * 100.0, 1), 100.0)
        return best_domain, confidence

    # ------------------------------------------------------------------
    # Validation syntaxique
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_syntax(script_content: str) -> None:
        """
        Parse le contenu Python via le module ast.
        Lève SyntaxError si le script est syntaxiquement invalide.
        Appelé avant toute écriture sur disque.
        """
        ast.parse(script_content)

    # ------------------------------------------------------------------
    # Vérification des imports
    # ------------------------------------------------------------------

    @staticmethod
    def _check_security(script_content: str) -> None:
        """
        Parcourt l'AST à la recherche d'imports de modules interdits.
        Lève ImportError dès la première violation détectée.
        """
        tree = ast.parse(script_content)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Import, ast.ImportFrom)):
                continue
            for alias in node.names:
                root_module = alias.name.split(".")[0]
                if root_module in FORBIDDEN_IMPORTS:
                    raise ImportError(
                        f"Import interdit détecté : '{root_module}' — "
                        "ce module est exclu de la liste blanche de sécurité."
                    )

    # ------------------------------------------------------------------
    # Fallback sécurisé
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_fallback(reason: str, output_script_path: str) -> Dict[str, Any]:
        """
        Écrit le template minimal garanti dans output_script_path.
        Retourne un dict de résultat complet avec fallback_used=True.
        """
        print(f"[AGENT FALLBACK] Raison : {reason}")
        os.makedirs(os.path.dirname(os.path.abspath(output_script_path)), exist_ok=True)
        with open(output_script_path, "w", encoding="utf-8") as fh:
            fh.write(_FALLBACK_TEMPLATE)

        return {
            "domain":             "FALLBACK",
            "confidence":         0.0,
            "output_script":      output_script_path,
            "database":           "analytics_datalake_db",
            "table":              "fallback_data",
            "fallback_used":      True,
            "security_validated": False,
            "archive_path":       None,
            "status":             "FALLBACK_APPLIED",
            "reason":             reason,
        }

    # ------------------------------------------------------------------
    # Archivage horodaté
    # ------------------------------------------------------------------

    @staticmethod
    def _archive_script(script_content: str, domain: str) -> str:
        """
        Copie le contenu dans scripts/archive/<domaine>_<timestamp>_<hash8>.py.
        Retourne le chemin absolu de l'archive.
        """
        script_hash      = hashlib.sha256(script_content.encode("utf-8")).hexdigest()[:8]
        timestamp        = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        archive_dir      = Path("scripts") / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path     = archive_dir / f"{domain.lower()}_{timestamp}_{script_hash}.py"

        with open(archive_path, "w", encoding="utf-8") as fh:
            fh.write(script_content)

        return str(archive_path)

    # ------------------------------------------------------------------
    # Génération du script PySpark actif
    # ------------------------------------------------------------------

    @classmethod
    def generate_script_from_headers(
        cls,
        headers: List[str],
        output_script_path: str = None,
        source_name: str = "stream"
    ) -> Dict[str, Any]:
        """
        Génère et valide le script PySpark directement à partir des en-têtes.
        Utilisable aussi bien en local que dans un runtime serverless (AWS Lambda).
        """
        domain, confidence = cls.classify_domain(headers)
        policy = cls.DOMAIN_POLICIES.get(domain, cls.DOMAIN_POLICIES["SALES_ECOMMERCE"])

        partition_col = (
            policy["partition_col"]
            if policy["partition_col"] in headers
            else (headers[1] if len(headers) > 1 else (headers[0] if headers else "partition_date"))
        )
        database_name = policy["database"]
        table_name    = policy["table"]
        target_path   = policy["s3_target"]

        # --- Routage selon le score de confiance ---
        if confidence < CONFIDENCE_WARNING:
            return {
                "domain":             domain,
                "confidence":         confidence,
                "output_script":      None,
                "script_content":     None,
                "database":           database_name,
                "table":              table_name,
                "fallback_used":      False,
                "security_validated": False,
                "archive_path":       None,
                "status":             "REVIEW_REQUIRED",
            }

        confidence_status = "OK" if confidence >= CONFIDENCE_AUTONOMOUS else "WARNING"

        # --- Bloc de gouvernance spécifique au domaine ---
        if domain == "HUMAN_RESOURCES":
            governance_code = """\
# Gouvernance RH — Conformité RGPD : hachage irréversible SHA-256 des champs PII
from pyspark.sql.functions import sha2
for col_pii in ["Nom", "Prenom", "SSN"]:
    if col_pii in df_valid.columns:
        df_valid = df_valid.withColumn(col_pii, sha2(col(col_pii).cast("string"), 256))

if "Salaire" in df_valid.columns:
    df_valid = df_valid.filter(col("Salaire") > 0).withColumn("Salaire", col("Salaire").cast("double"))
if "Date_Embauche" in df_valid.columns:
    df_valid = df_valid.withColumn("Date_Embauche", to_date(col("Date_Embauche"), "yyyy-MM-dd"))"""
            quality_filter    = '(col("Salaire") > 0) if "Salaire" in df_raw.columns else lit(True)'
            quarantine_reason = 'when(col("Salaire") <= 0, lit("Salaire invalide ou <= 0")).otherwise(lit("Format invalide"))'

        elif domain == "FINANCE":
            governance_code = """\
# Gouvernance Finance — Validation comptable et recalcul automatique de la TVA
if "Montant_HT" in df_valid.columns:
    df_valid = df_valid.filter(col("Montant_HT") > 0).withColumn("Montant_HT", col("Montant_HT").cast("double"))
if "Taux_TVA" in df_valid.columns:
    df_valid = df_valid.withColumn("Taux_TVA", col("Taux_TVA").cast("double"))
    df_valid = df_valid.withColumn("Montant_TVA", round(col("Montant_HT") * col("Taux_TVA"), 2))
    df_valid = df_valid.withColumn("Montant_TTC", round(col("Montant_HT") * (lit(1.0) + col("Taux_TVA")), 2))"""
            quality_filter    = '(col("Montant_HT") > 0) if "Montant_HT" in df_raw.columns else lit(True)'
            quarantine_reason = 'when(col("Montant_HT") <= 0, lit("Montant HT <= 0")).otherwise(lit("Format invalide")) if "Montant_HT" in df_raw.columns else lit("Format invalide")'

        else:  # SALES_ECOMMERCE et domaines génériques
            governance_code = """\
# Gouvernance Ventes — Imputation des catégories, contrôle des prix, calcul du montant total
if "Prix" in df_valid.columns:
    df_valid = df_valid.withColumn("Prix", col("Prix").cast("double"))
if "Quantite" in df_valid.columns:
    df_valid = df_valid.withColumn(
        "Quantite",
        when((col("Quantite").isNull()) | (col("Quantite") <= 0), 1).otherwise(col("Quantite").cast("int"))
    )
if "Categorie" in df_valid.columns:
    df_valid = df_valid.withColumn(
        "Categorie",
        when(col("Categorie").isNull() | (col("Categorie") == ""), "Non spécifié").otherwise(col("Categorie"))
    )
if "Prix" in df_valid.columns and "Quantite" in df_valid.columns:
    df_valid = df_valid.withColumn("Montant_Total", round(col("Prix") * col("Quantite"), 2))"""
            quality_filter    = '((col("Prix").isNotNull()) & (col("Prix") > 0)) if "Prix" in df_raw.columns else lit(True)'
            quarantine_reason = 'when(~((col("Prix").isNotNull()) & (col("Prix") > 0)), lit("Prix unitaire <= 0 ou invalide")).otherwise(lit("Donnée corrompue")) if "Prix" in df_raw.columns else lit("Donnée corrompue")'

        # --- Construction du contenu PySpark complet ---
        governance_rules_str = ", ".join(policy["governance_rules"])
        generated_at         = datetime.utcnow().isoformat()

        script_content = f"""\
# -*- coding: utf-8 -*-
\"\"\"
AWS Glue ETL Job : Script PySpark Unifié
Domaine       : {domain} (Confiance : {confidence}%)
Table cible   : {database_name}.{table_name}
Règles        : {governance_rules_str}
Généré le     : {generated_at} UTC
\"\"\"

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, round, to_date, lit, current_timestamp

# 1. Initialisation de la session AWS Glue
raw_argv = sys.argv
opt_args = ['JOB_NAME', 'S3_BUCKET_NAME']
if any(a.startswith('--SOURCE_KEY') for a in raw_argv):
    opt_args.append('SOURCE_KEY')

args = getResolvedOptions(sys.argv, opt_args)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name     = args['S3_BUCKET_NAME']
source_key      = args.get('SOURCE_KEY', 'raw/')
raw_path        = f"s3://{{bucket_name}}/{{source_key}}"
processed_path  = f"s3://{{bucket_name}}/{target_path}"
quarantine_path = f"s3://{{bucket_name}}/quarantine/"
iceberg_path    = f"s3://{{bucket_name}}/iceberg-warehouse/{database_name}/{table_name}"

print(f"[ETL] Domaine : {domain} — Lecture de {{raw_path}}")

# 2. Extraction distribuée depuis S3
df_raw    = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)
total_raw = df_raw.count()
print(f"[ETL] Enregistrements bruts lus : {{total_raw}}")

# 3. Contrôle qualité et isolement des rejets
cond_valide   = {quality_filter}
df_valid      = df_raw.filter(cond_valide)
df_quarantine = (
    df_raw.filter(~cond_valide)
          .withColumn("anomaly_reason", {quarantine_reason})
          .withColumn("quarantine_timestamp", current_timestamp())
)

quarantine_count = df_quarantine.count()
valid_count      = df_valid.count()
print(f"[ETL] Valides : {{valid_count}} | Quarantaine : {{quarantine_count}}")

if quarantine_count > 0:
    print(f"[QUARANTAINE] Écriture de {{quarantine_count}} rejet(s) dans {{quarantine_path}}")
    df_quarantine.repartition(1).write.mode("append").parquet(quarantine_path)

# 4. Transformations et gouvernance spécifique au domaine
{governance_code}

df_final = df_valid.withColumn("ingestion_timestamp", current_timestamp())

# 5. Persistance Parquet partitionnée
print(f"[STOCKAGE] Écriture Parquet dans {{processed_path}}...")
(
    df_final.write
    .mode("overwrite")
    .partitionBy("{partition_col}")
    .parquet(processed_path)
)

# 6. Synchronisation Lakehouse Apache Iceberg (ACID)
iceberg_table = f"glue_catalog.{database_name}.{table_name}_iceberg"
try:
    print(f"[LAKEHOUSE] Initialisation du namespace : {database_name}...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{database_name}")
    print(f"[LAKEHOUSE] Synchronisation de la table Iceberg {{iceberg_table}}...")
    (
        df_final.writeTo(iceberg_table)
        .tableProperty("format-version", "2")
        .createOrReplace()
    )
    print(f"[LAKEHOUSE] Table Iceberg mise à jour avec succès dans {{iceberg_path}}.")
except Exception as exc:
    print(f"[LAKEHOUSE NOTE] Mode Parquet standard validé (Iceberg fallback) : {{exc}}")

print("[ETL] Job terminé avec succès.")
job.commit()
"""

        # --- Validation syntaxique AST ---
        try:
            cls._validate_syntax(script_content)
        except SyntaxError as exc:
            if output_script_path:
                return cls._apply_fallback(
                    reason=f"SyntaxError dans le script généré : {exc}",
                    output_script_path=output_script_path,
                )
            return {"status": "FALLBACK_APPLIED", "script_content": _FALLBACK_TEMPLATE, "fallback_used": True}

        # --- Vérification des imports ---
        try:
            cls._check_security(script_content)
        except ImportError as exc:
            if output_script_path:
                return cls._apply_fallback(
                    reason=str(exc),
                    output_script_path=output_script_path,
                )
            return {"status": "FALLBACK_APPLIED", "script_content": _FALLBACK_TEMPLATE, "fallback_used": True}

        archive_path = None
        if output_script_path:
            archive_path = cls._archive_script(script_content, domain)
            output_dir = os.path.dirname(os.path.abspath(output_script_path))
            os.makedirs(output_dir, exist_ok=True)
            with open(output_script_path, "w", encoding="utf-8") as fh:
                fh.write(script_content)

        return {
            "domain":             domain,
            "confidence":         confidence,
            "output_script":      output_script_path,
            "script_content":     script_content,
            "database":           database_name,
            "table":              table_name,
            "fallback_used":      False,
            "security_validated": True,
            "archive_path":       archive_path,
            "status":             confidence_status,
        }

    @classmethod
    def generate_active_pyspark_script(
        cls,
        filepath: str,
        output_script_path: str = "scripts/active_etl.py",
    ) -> Dict[str, Any]:
        """
        Analyse le fichier entrant et génère scripts/active_etl.py.
        """
        headers, _ = cls.inspect_sample(filepath)
        res = cls.generate_script_from_headers(
            headers=headers,
            output_script_path=output_script_path,
            source_name=os.path.basename(filepath)
        )
        if res.get("status") == "REVIEW_REQUIRED":
            review_path = str(Path(output_script_path).parent / "review" / Path(filepath).name)
            os.makedirs(os.path.dirname(os.path.abspath(review_path)), exist_ok=True)
            shutil.copy2(filepath, review_path)
            res["review_path"] = review_path

        if res.get("output_script"):
            print(f"[AGENT] Fichier analysé     : {os.path.basename(filepath)}")
            print(f"[AGENT] Domaine détecté     : {res['domain']} ({res['confidence']}%)")
            print(f"[AGENT] Script actif généré : {res['output_script']}")
            if res.get("archive_path"):
                print(f"[AGENT] Archive horodatée   : {res['archive_path']}")

        return res

        return {
            "domain":             domain,
            "confidence":         confidence,
            "output_script":      output_script_path,
            "database":           database_name,
            "table":              table_name,
            "fallback_used":      False,
            "security_validated": True,
            "archive_path":       archive_path,
            "status":             confidence_status,
        }


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 70)
    print("DÉCLENCHEMENT DE L'AGENT DATA MESH")
    print("=" * 70)
    result = DataMeshAgent.generate_active_pyspark_script("data/orders.csv")
    if result["status"] not in ("REVIEW_REQUIRED", "FALLBACK_APPLIED"):
        print(f"\nSUCCÈS : {result['output_script']} prêt pour déploiement AWS Glue.")
        print(f"Statut sécurité : {'VALIDÉ' if result['security_validated'] else 'FALLBACK'}")
    else:
        print(f"\nSTATUT : {result['status']} — Intervention requise.")
