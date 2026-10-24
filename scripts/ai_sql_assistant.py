"""
Interface Text-to-SQL pour Amazon Athena.
Traduction de requetes analytiques en langage naturel vers du SQL Presto/Trino
via l'API Gemini et execution directe avec boto3.
"""
import os
import sys
import time
import json
import urllib.request
import urllib.error

# Support de l'encodage UTF-8 sous Windows PowerShell / CMD
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

# Chargement automatique des variables d'environnement depuis .env si présent
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DATABASE_NAME = "analytics_datalake_db"
TABLE_NAME = "clean_processed"

SCHEMA_DESCRIPTION = f"""
Base de données : {DATABASE_NAME}
Table : {TABLE_NAME}
Format : Apache Parquet (stocké sur Amazon S3)

Colonnes et types :
- Transaction_ID (integer) : Identifiant unique de la transaction
- Date (date)              : Date d'achat au format YYYY-MM-DD
- Produit (string)         : Nom du produit vendu (ex: Laptop Pro, Souris Sans Fil, etc.)
- Categorie (string)       : Catégorie du produit (ex: Electronique, Mobilier, Non spécifié)
- Prix (double)            : Prix unitaire en euros
- Quantite (integer)       : Quantité commandée
- Montant_Total (double)   : Montant calculé de la commande (Prix * Quantite)
"""

SYSTEM_PROMPT = f"""Tu es un Data Analyst expert en SQL (dialecte Amazon Athena / Presto / Trino).
Ta mission est de traduire la question posée par l'utilisateur en une requête SQL valide et optimisée.

Contexte du schéma de données :
{SCHEMA_DESCRIPTION}

Règles strictes :
1. Génère UNIQUEMENT du SQL standard compatible Amazon Athena.
2. Utilise toujours le nom de table qualifié : "{DATABASE_NAME}"."{TABLE_NAME}"
3. N'inclus JAMAIS de commande destructive (interdiction de DROP, DELETE, INSERT, UPDATE, ALTER, TRUNCATE). Uniquement des requêtes SELECT.
4. Réponds UNIQUEMENT avec le bloc de code SQL, sans texte explicatif avant ou après.
"""

def fallback_sql_query(user_question: str) -> str:
    """Moteur de génération SQL de secours basé sur l'analyse sémantique locale."""
    q_lower = user_question.lower()

    # Détection de recherche par produit
    known_products = [
        ("laptop", "Laptop Pro"),
        ("souris", "Souris Sans Fil"),
        ("clavier", "Clavier Mecanique"),
        ("bureau", "Bureau Gamer"),
        ("chaise", "Chaise Ergonomique"),
        ("casque", "Casque Audio"),
        ("ecran", "Ecran 27 pouces"),
        ("écran", "Ecran 27 pouces"),
        ("lampe", "Lampe de Bureau"),
        ("tapis", "Tapis de Souris")
    ]
    for keyword, prod_name in known_products:
        if keyword in q_lower:
            return (
                f'SELECT produit, SUM(quantite) AS total_quantite, '
                f'ROUND(SUM(montant_total), 2) AS total_ca '
                f'FROM "{DATABASE_NAME}"."{TABLE_NAME}" '
                f'WHERE LOWER(produit) LIKE \'%{keyword}%\' '
                f'GROUP BY produit;'
            )

    if "catégorie" in q_lower or "categorie" in q_lower:
        return (
            f'SELECT categorie, COUNT(*) AS nb_commandes, '
            f'ROUND(SUM(montant_total), 2) AS ca_total '
            f'FROM "{DATABASE_NAME}"."{TABLE_NAME}" '
            f'GROUP BY categorie ORDER BY ca_total DESC;'
        )
    elif "top" in q_lower or "plus vendu" in q_lower or "meilleur" in q_lower:
        return (
            f'SELECT produit, SUM(quantite) AS total_quantite, '
            f'ROUND(SUM(montant_total), 2) AS total_ca '
            f'FROM "{DATABASE_NAME}"."{TABLE_NAME}" '
            f'GROUP BY produit ORDER BY total_quantite DESC LIMIT 3;'
        )
    elif "panier moyen" in q_lower or "moyenne" in q_lower:
        return (
            f'SELECT ROUND(AVG(montant_total), 2) AS panier_moyen, '
            f'ROUND(MIN(montant_total), 2) AS commande_min, '
            f'ROUND(MAX(montant_total), 2) AS commande_max '
            f'FROM "{DATABASE_NAME}"."{TABLE_NAME}";'
        )
    elif "total" in q_lower or "montant" in q_lower or "ca" in q_lower or "chiffre" in q_lower:
        return (
            f'SELECT COUNT(*) AS total_commandes, '
            f'ROUND(SUM(montant_total), 2) AS chiffre_affaires_global '
            f'FROM "{DATABASE_NAME}"."{TABLE_NAME}";'
        )
    else:
        return (
            f'SELECT COUNT(*) AS total_commandes, '
            f'ROUND(SUM(montant_total), 2) AS ca_global, '
            f'ROUND(AVG(montant_total), 2) AS panier_moyen '
            f'FROM "{DATABASE_NAME}"."{TABLE_NAME}";'
        )

def generate_sql_query(user_question: str) -> str:
    """Génère la requête SQL à partir de la question utilisateur via l'API Gemini avec repli automatique."""
    api_key = os.environ.get("GEMINI_API_KEY")

    if not api_key:
        print("\n[INFO] Mode local actif (clé GEMINI_API_KEY non configurée).")
        return fallback_sql_query(user_question)

    try:
        sql = None
        model_name = "gemini-flash-latest"

        # Option 1 : SDK officiel si installé
        try:
            from google import genai
            client = genai.Client(api_key=api_key)
            response = client.models.generate_content(
                model=model_name,
                contents=f"{SYSTEM_PROMPT}\n\nQuestion de l'utilisateur : {user_question}"
            )
            sql = response.text.strip()
        except ImportError:
            # Option 2 : Appel REST natif avec urllib
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
            payload = json.dumps({
                "contents": [{
                    "parts": [{"text": f"{SYSTEM_PROMPT}\n\nQuestion de l'utilisateur : {user_question}"}]
                }]
            }).encode("utf-8")

            req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=12) as resp:
                res_data = json.loads(resp.read().decode("utf-8"))
                sql = res_data["candidates"][0]["content"]["parts"][0]["text"].strip()

        # Nettoyage des balises markdown éventuelles
        if sql.startswith("```sql"):
            sql = sql[6:]
        if sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]
        return sql.strip()

    except Exception as e:
        print(f"[INFO] API distante indisponible ({e}).")
        print("[INFO] Génération de la requête SQL via le moteur de secours local...")
        return fallback_sql_query(user_question)

def execute_locally(sql_query: str):
    """Exécute la requête sur une base SQLite en mémoire chargée avec les données nettoyées."""
    import sqlite3
    import csv
    from pathlib import Path

    csv_path = Path("data/orders.csv")
    if not csv_path.exists():
        return

    try:
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE clean_processed (
                Transaction_ID INT,
                Date TEXT,
                Produit TEXT,
                Categorie TEXT,
                Prix REAL,
                Quantite INT,
                Montant_Total REAL
            );
        """)

        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    p = float(row["Prix"])
                    if p <= 0:
                        continue
                except (ValueError, TypeError):
                    continue

                cat = row["Categorie"].strip() or "Non spécifié"
                try:
                    q = int(row["Quantite"]) if row["Quantite"] and row["Quantite"].upper() != "NULL" else 1
                    if q <= 0:
                        q = 1
                except (ValueError, TypeError):
                    q = 1

                total = round(p * q, 2)
                cursor.execute(
                    "INSERT INTO clean_processed VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (row["Transaction_ID"], row["Date"], row["Produit"], cat, p, q, total)
                )
        conn.commit()

        # Adapter la syntaxe qualifiée Athena "database"."table" pour SQLite
        local_sql = sql_query.replace(f'"{DATABASE_NAME}"."{TABLE_NAME}"', "clean_processed")
        local_sql = local_sql.replace(f'{DATABASE_NAME}.{TABLE_NAME}', "clean_processed")

        cursor.execute(local_sql)
        rows = cursor.fetchall()
        col_names = [d[0] for d in cursor.description]

        print("RESULTATS DE LA REQUETE :")
        print("=" * 60)
        print(" | ".join(f"{h:<20}" for h in col_names))
        print("-" * 60)
        for r in rows:
            print(" | ".join(f"{str(v):<20}" for v in r))
        print("=" * 60 + "\n")
        conn.close()

    except Exception as e:
        print(f"[ERREUR EXECUTION] {e}")

def execute_athena_query(sql_query: str):
    """Exécute la requête SQL sur Amazon Athena via boto3 et affiche les résultats réels."""
    try:
        import boto3
        athena = boto3.client('athena')
        workgroup = os.environ.get("ATHENA_WORKGROUP", "analytics_workgroup")

        print(f"[ATHENA] Exécution sur le workgroup '{workgroup}'...")
        response = athena.start_query_execution(
            QueryString=sql_query,
            QueryExecutionContext={'Database': DATABASE_NAME},
            WorkGroup=workgroup
        )
        query_execution_id = response['QueryExecutionId']

        # Attente de la fin d'exécution
        while True:
            status_resp = athena.get_query_execution(QueryExecutionId=query_execution_id)
            state = status_resp['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)

        if state != 'SUCCEEDED':
            reason = status_resp['QueryExecution']['Status'].get('StateChangeReason', 'Erreur inconnue')
            print(f"[ERREUR ATHENA] Échec de la requête : {reason}")
            return

        # Récupération et affichage des résultats
        results = athena.get_query_results(QueryExecutionId=query_execution_id)
        rows = results['ResultSet']['Rows']

        print("\n" + "=" * 60)
        print("RESULTATS DE LA REQUETE (AMAZON ATHENA)")
        print("=" * 60)
        for i, row in enumerate(rows):
            line = [col.get('VarCharValue', 'NULL') for col in row['Data']]
            if i == 0:
                print(" | ".join(f"{h:<20}" for h in line))
                print("-" * 60)
            else:
                print(" | ".join(f"{v:<20}" for v in line))
        print("=" * 60 + "\n")

    except Exception:
        # En mode déconnecté / avant déploiement AWS, exécute en local pour afficher les vraies données
        print("[INFO] Connexion Athena inactive -> Calcul des résultats sur le dataset...")
        execute_locally(sql_query)

def main():
    print("=" * 70)
    print("Text-to-SQL CLI - Amazon Athena Data Lake")
    print("Questions en langage naturel sur la table 'clean_processed'.")
    print("Tapez 'exit' pour quitter.")
    print("=" * 70)

    while True:
        try:
            question = input("\n> Question : ").strip()
            if not question:
                continue
            if question.lower() in ['exit', 'quit', 'q']:
                break

            print("\nGeneration de la requete SQL...")
            sql = generate_sql_query(question)

            if sql:
                print("\n[SQL]")
                print(f"  {sql}\n")
                execute_athena_query(sql)

        except (KeyboardInterrupt, EOFError):
            print("\nInterruption.")
            break

if __name__ == "__main__":
    main()
