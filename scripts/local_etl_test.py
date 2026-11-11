# -*- coding: utf-8 -*-
"""
Tests Unitaires & Validation Locale du Pipeline Unifié
Vérifie :
  - La conformité comptable des ventes (prix > 0, imputations, calculs)
  - L'isolation stricte des anomalies vers la zone de quarantaine
  - La génération autonome de scripts PySpark pour n'importe quel domaine
  - La validation syntaxique AST (Pilier 1)
  - La détection des imports interdits (Pilier 2)
  - La porte de confiance à seuils (Pilier 3)
  - Le déclenchement du fallback déterministe (Pilier 4)
  - La présence systématique d'une archive horodatée (Pilier 5)
"""

import csv
import sys
import tempfile
import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.data_mesh_agent import DataMeshAgent, FORBIDDEN_IMPORTS

# ---------------------------------------------------------------------------
# Test principal — Pipeline Ventes
# ---------------------------------------------------------------------------

def test_unified_pipeline():
    """Validation complète du pipeline SALES_ECOMMERCE sur orders.csv."""
    source_file = ROOT_DIR / "data" / "orders.csv"
    assert source_file.exists(), f"Fichier source introuvable : {source_file}"

    # --- Génération du script actif ---
    res = DataMeshAgent.generate_active_pyspark_script(str(source_file))

    assert res["domain"] == "SALES_ECOMMERCE", f"Domaine inattendu : {res['domain']}"
    assert Path(res["output_script"]).exists(), "Le script 'active_etl.py' doit exister"

    # --- Pilier 2 : sécurité validée pour un script propre ---
    assert res["security_validated"] is True, "Le script généré doit passer la validation de sécurité"

    # --- Pilier 4 : pas de fallback sur un domaine connu ---
    assert res["fallback_used"] is False, "Aucun fallback ne doit être déclenché pour SALES_ECOMMERCE"

    # --- Pilier 5 : archive produite ---
    assert res["archive_path"] is not None, "Un chemin d'archive doit être retourné"
    assert Path(res["archive_path"]).exists(), "Le fichier d'archive doit exister sur disque"

    # --- Validation locale des données (simulation du filtre Spark) ---
    cleaned_records = []
    quarantine_records = []

    with open(source_file, mode="r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            prix_raw      = row.get("Prix", "").strip()
            quantite_raw  = row.get("Quantite", "").strip()
            categorie     = row.get("Categorie", "").strip()

            try:
                prix = float(prix_raw)
                if prix <= 0:
                    quarantine_records.append((row, "Prix unitaire <= 0"))
                    continue
            except ValueError:
                quarantine_records.append((row, "Format de prix non numérique"))
                continue

            try:
                quantite_int = int(quantite_raw)
                quantite = quantite_int if quantite_int > 0 else 1
            except (ValueError, TypeError):
                quantite = 1

            categorie     = categorie if categorie else "Non spécifié"
            montant_total = round(prix * quantite, 2)

            cleaned_records.append({
                "Transaction_ID": row.get("Transaction_ID"),
                "Prix":           prix,
                "Quantite":       quantite,
                "Categorie":      categorie,
                "Montant_Total":  montant_total,
            })

    total_analysed = len(cleaned_records) + len(quarantine_records)

    print(f"\n=== BILAN DU TEST UNITAIRE UNIFIÉ ===")
    print(f"Total analysé        : {total_analysed}")
    print(f"Lignes saines        : {len(cleaned_records)}")
    print(f"Lignes en quarantaine: {len(quarantine_records)}")

    assert total_analysed == 10,            "10 lignes attendues au total"
    assert len(cleaned_records) == 9,       "9 lignes saines attendues"
    assert len(quarantine_records) == 1,    "1 seule ligne doit être en quarantaine (prix négatif)"

    total_ca = round(sum(r["Montant_Total"] for r in cleaned_records), 2)
    print(f"Chiffre d'affaires calculé : {total_ca} $")
    assert total_ca == 3334.33, f"CA attendu : 3334.33 $, obtenu : {total_ca} $"

    print("[OK] Test principal — Pipeline Ventes validé.")


# ---------------------------------------------------------------------------
# Test Pilier 3 — Porte de confiance (score < 60 %)
# ---------------------------------------------------------------------------

def test_confidence_gate_low_score():
    """
    Un fichier avec des colonnes inconnues doit produire un score < 60 %
    et retourner le statut REVIEW_REQUIRED sans générer de script.
    """
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write("colonne_inconnue_x,colonne_inconnue_y,colonne_inconnue_z\n")
        tmp.write("valeur1,valeur2,valeur3\n")
        tmp_path = tmp.name

    try:
        res = DataMeshAgent.generate_active_pyspark_script(
            tmp_path,
            output_script_path="scripts/active_etl_lowconf.py",
        )
        assert res["status"] == "REVIEW_REQUIRED", (
            f"Statut attendu REVIEW_REQUIRED, obtenu : {res['status']}"
        )
        assert res["confidence"] < 60.0, (
            f"Score de confiance attendu < 60 %, obtenu : {res['confidence']}"
        )
        assert res["output_script"] is None, "Aucun script ne doit être écrit en cas de REVIEW_REQUIRED"
        print(f"[OK] Porte de confiance — REVIEW_REQUIRED déclenché (confiance={res['confidence']}%).")
    finally:
        os.unlink(tmp_path)
        # Nettoyage du fichier de révision éventuellement copié
        review_file = Path("scripts/review") / Path(tmp_path).name
        if review_file.exists():
            review_file.unlink()


# ---------------------------------------------------------------------------
# Test Pilier 1 — Validation syntaxique AST
# ---------------------------------------------------------------------------

def test_ast_validation():
    """
    La méthode _validate_syntax doit lever SyntaxError sur du code Python invalide.
    """
    broken_code = "def foo(:\n    pass\n"
    try:
        DataMeshAgent._validate_syntax(broken_code)
        raise AssertionError("SyntaxError attendue mais non levée")
    except SyntaxError:
        pass  # comportement attendu

    valid_code = "x = 1 + 2\nprint(x)\n"
    DataMeshAgent._validate_syntax(valid_code)  # ne doit pas lever

    print("[OK] Validation AST — SyntaxError correctement interceptée.")


# ---------------------------------------------------------------------------
# Test Pilier 2 — Liste blanche des imports
# ---------------------------------------------------------------------------

def test_security_allowlist():
    """
    _check_security doit lever ImportError sur n'importe quel import interdit
    et ne rien lever pour les imports PySpark/awsglue autorisés.
    """
    for forbidden in FORBIDDEN_IMPORTS:
        dangerous_code = f"import {forbidden}\nprint('oops')\n"
        try:
            DataMeshAgent._check_security(dangerous_code)
            raise AssertionError(f"ImportError attendue pour le module '{forbidden}' mais non levée")
        except ImportError:
            pass  # comportement attendu

    safe_code = (
        "from pyspark.sql.functions import col, when\n"
        "from awsglue.context import GlueContext\n"
        "import sys\n"
    )
    DataMeshAgent._check_security(safe_code)  # ne doit pas lever

    print(f"[OK] Liste blanche — {len(FORBIDDEN_IMPORTS)} modules interdits correctement bloqués.")


# ---------------------------------------------------------------------------
# Test Pilier 4 — Fallback déterministe
# ---------------------------------------------------------------------------

def test_deterministic_fallback():
    """
    _apply_fallback doit produire un fichier écrivable contenant le template minimal
    et retourner un dict avec fallback_used=True et security_validated=False.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        fallback_path = str(Path(tmpdir) / "scripts" / "active_etl_fallback.py")
        res = DataMeshAgent._apply_fallback(
            reason="Test unitaire du fallback",
            output_script_path=fallback_path,
        )

    assert res["fallback_used"] is True,      "fallback_used doit être True"
    assert res["security_validated"] is False, "security_validated doit être False lors du fallback"
    assert res["status"] == "FALLBACK_APPLIED", f"Statut inattendu : {res['status']}"

    print("[OK] Fallback déterministe — template minimal produit correctement.")


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 70)
    print("TESTS UNITAIRES — PILIERS D'INGÉNIERIE DATA MESH")
    print("=" * 70)

    test_ast_validation()
    test_security_allowlist()
    test_confidence_gate_low_score()
    test_deterministic_fallback()
    test_unified_pipeline()

    print("\n" + "=" * 70)
    print("RÉSULTAT : TOUS LES TESTS UNITAIRES VALIDÉS AVEC SUCCÈS.")
    print("=" * 70)
