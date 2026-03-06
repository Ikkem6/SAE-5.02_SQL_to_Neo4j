import sqlite3
import time
from neo4j import GraphDatabase

# --- CONFIGURATION ---
SQLITE_DB = "C:/Users/mekki/Desktop/Test migration/crimes_police_gendarmerie.db"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "adminadmin123"

class Migrator:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_cypher(self, query, parameters=None):
        with self.driver.session() as session:
            session.run(query, parameters)

    def migrate(self):
        # --- DÉBUT DU CHRONO ---
        start_time = time.time() 
        
        conn = sqlite3.connect(SQLITE_DB)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        print("Début de la migration...")
        total_rows = 0

        # --- ÉTAPE 1 : CONTRAINTES ---
        print("1/7 Création des contraintes...")
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Departement) REQUIRE d.id_departement IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (u:Unite) REQUIRE u.id_unite IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Crime) REQUIRE c.id_crime IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Annee) REQUIRE a.annee IS UNIQUE"
        ]
        for c in constraints:
            self.execute_cypher(c)

        # --- ÉTAPES 2 À 6 (Simplifiées pour la lisibilité) ---
        # Note : On incrémente total_rows pour chaque insertion
        
        print("2/7 Importation des Départements...")
        cursor.execute("SELECT id_departement, lb_departement FROM departement")
        for row in cursor:
            self.execute_cypher("MERGE (d:Departement {id_departement: $id}) SET d.nom = $nom", 
                               {"id": row[0], "nom": row[1]})
            total_rows += 1

        print("3/7 Importation des Crimes...")
        cursor.execute("SELECT id_crime, lb_crime FROM crime")
        for row in cursor:
            self.execute_cypher("MERGE (c:Crime {id_crime: $id}) SET c.libelle = $libelle", 
                               {"id": row[0], "libelle": row[1]})
            total_rows += 1

        print("4/7 Importation des Années...")
        cursor.execute("SELECT DISTINCT annee FROM a_enregistre")
        for row in cursor:
            self.execute_cypher("MERGE (a:Annee {annee: $annee})", {"annee": row[0]})
            total_rows += 1

        print("5/7 Importation des Unités et rattachement...")
        cursor.execute("SELECT id_unite, lb_unite, id_departement FROM unite")
        for row in cursor:
            self.execute_cypher("""
                MATCH (d:Departement {id_departement: $id_dep})
                MERGE (u:Unite {id_unite: $id_u}) SET u.nom = $nom
                MERGE (u)-[:RATTACHE_A]->(d)
                """, {"id_dep": row[2], "id_u": row[0], "nom": row[1]})
            total_rows += 1

        print("6/7 Spécification Police/Gendarmerie...")
        cursor.execute("SELECT id_police, perimetre FROM police")
        for row in cursor:
            self.execute_cypher("MATCH (u:Unite {id_unite: $id}) SET u:Police, u.perimetre = $p", {"id": row[0], "p": row[1]})
            total_rows += 1
        
        cursor.execute("SELECT id_gendarmerie FROM gendarmerie")
        for row in cursor:
            self.execute_cypher("MATCH (u:Unite {id_unite: $id}) SET u:Gendarmerie", {"id": row[0]})
            total_rows += 1

        # --- ÉTAPE 7 : LES FAITS (Le gros morceau) ---
        print("7/7 Importation des faits (cela peut prendre du temps)...")
        cursor.execute("SELECT id_unite, id_crime, annee, nb_faits FROM a_enregistre")
        for row in cursor:
            self.execute_cypher("""
                MATCH (u:Unite {id_unite: $id_u})
                MATCH (c:Crime {id_crime: $id_c})
                MERGE (u)-[r:A_ENREGISTRE {annee: $annee}]->(c)
                SET r.nb_faits = $faits
                """, {"id_u": row[0], "id_c": row[1], "annee": row[2], "faits": row[3]})
            total_rows += 1

        # --- FIN ET CALCUL DU TEMPS ---
        end_time = time.time()
        duration = end_time - start_time
        
        print("\n" + "="*30)
        print(f"MIGRATION RÉUSSIE")
        print(f"Temps total : {duration:.2f} secondes")
        print(f"Éléments traités : {total_rows}")
        if duration > 0:
            print(f"Vitesse : {total_rows / duration:.1f} opérations/sec")
        print("="*30)
        
        conn.close()

if __name__ == "__main__":
    migration = Migrator(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        migration.migrate()
    finally:
        migration.close()