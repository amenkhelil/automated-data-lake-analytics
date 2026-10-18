"""
Tests unitaires et validation locale de la logique de transformation des données.
Permet de valider les règles de gestion avant le déploiement sur le cluster AWS Glue.
"""
import csv
from pathlib import Path

def test_transformation_pipeline():
    source_file = Path("data/orders.csv")
    if not source_file.exists():
        print(f"[ERREUR] Fichier source introuvable : {source_file}")
        return False

    cleaned_records = []
    rejected_records = []

    with open(source_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tx_id = row["Transaction_ID"]
            tx_date = row["Date"]
            produit = row["Produit"]
            categorie = row["Categorie"].strip()
            prix_raw = row["Prix"].strip()
            quantite_raw = row["Quantite"].strip()

            # Règle 1 : Contrôle de cohérence du prix unitaire
            try:
                prix = float(prix_raw)
                if prix <= 0:
                    rejected_records.append((row, "Prix unitaire inférieur ou égal à zéro"))
                    continue
            except ValueError:
                rejected_records.append((row, "Format numérique du prix invalide"))
                continue

            # Règle 2 : Normalisation des catégories non renseignées
            if not categorie:
                categorie = "Non spécifié"

            # Règle 3 : Imputation de quantité par défaut si valeur manquante ou nulle
            if not quantite_raw or quantite_raw.upper() == "NULL":
                quantite = 1
            else:
                try:
                    quantite = int(quantite_raw)
                    if quantite <= 0:
                        quantite = 1
                except ValueError:
                    quantite = 1

            # Règle 4 : Calcul de la métrique d'agrégation (Montant Total)
            montant_total = round(prix * quantite, 2)

            cleaned_records.append({
                "Transaction_ID": tx_id,
                "Date": tx_date,
                "Produit": produit,
                "Categorie": categorie,
                "Prix": prix,
                "Quantite": quantite,
                "Montant_Total": montant_total
            })

    print("=== Résultat de la validation locale ===")
    print(f"Lignes brutes analysées : {len(cleaned_records) + len(rejected_records)}")
    print(f"Lignes valides intégrées : {len(cleaned_records)}")
    print(f"Lignes rejetées          : {len(rejected_records)}")

    print("\nDétail des anomalies détectées :")
    for r, reason in rejected_records:
        print(f" - Transaction #{r.get('Transaction_ID')}: {reason} (Produit: {r.get('Produit')}, Prix: {r.get('Prix')})")

    assert len(cleaned_records) == 9, "Le nombre d'enregistrements valides attendu est de 9"
    assert len(rejected_records) == 1, "Une seule anomalie (prix négatif) doit être rejetée"
    print("\n[OK] Validation réussie : les règles de transformation sont conformes aux spécifications.")
    return True

if __name__ == "__main__":
    test_transformation_pipeline()
