import sqlite3
import pandas as pd
import glob
import os
import re

# --- CONFIGURATION ---
FICHIE_ENTREE = "C:/Users/mekki/Desktop/Test migration/crimes-et-delits-enregistres-par-les-services-de-gendarmerie-et-de-police-depuis-2012.xlsx"
DB_NAME = "C:/Users/mekki/Desktop/Test migration/crimes_police_gendarmerie.db"

def excel_vers_csv_multiples(fichier_excel):
    if not os.path.exists(fichier_excel):
        print(f"Erreur : Le fichier '{fichier_excel}' est introuvable.")
        return

    print(f"Lecture du fichier : {fichier_excel} ...")
    xls = pd.ExcelFile(fichier_excel)
    onglets = xls.sheet_names

    compteur = 0
    for onglet in onglets:
        if onglet.strip().lower() in ["présentation", "presentation"]:
            continue
        
        print(f"-> Export CSV : {onglet}")
        df = pd.read_excel(xls, sheet_name=onglet, header=None)
        nom_sortie = f"{onglet}.csv"
        df.to_csv(nom_sortie, index=False, header=False, sep=',', encoding='utf-8')
        compteur += 1

# --- SCHEMA SQL ---
ddl_script = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS Annee (annee INT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS Departement (id_departement INT PRIMARY KEY, lb_departement VARCHAR(255));
CREATE TABLE IF NOT EXISTS Crime (id_crime INT PRIMARY KEY, lb_crime VARCHAR(255));

CREATE TABLE IF NOT EXISTS Unite (
    id_unite INTEGER PRIMARY KEY AUTOINCREMENT,
    lb_unite VARCHAR(255) NOT NULL,
    id_departement INT NOT NULL,
    FOREIGN KEY (id_departement) REFERENCES Departement(id_departement)
);

CREATE TABLE IF NOT EXISTS Gendarmerie (
    id_gendarmerie INT PRIMARY KEY, 
    FOREIGN KEY (id_gendarmerie) REFERENCES Unite(id_unite) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Police (
    id_police INT PRIMARY KEY, 
    perimetre VARCHAR(255), 
    FOREIGN KEY (id_police) REFERENCES Unite(id_unite) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS a_enregistre (
    id_unite INT, id_crime INT, annee INT, nb_faits INT,
    PRIMARY KEY (id_unite, id_crime, annee),
    FOREIGN KEY (id_unite) REFERENCES Unite(id_unite),
    FOREIGN KEY (id_crime) REFERENCES Crime(id_crime),
    FOREIGN KEY (annee) REFERENCES Annee(annee)
);
"""

def clean_dept_code(code):
    str_code = str(code).strip().replace('.0', '')
    if str_code in ['2A', '201']: return 201
    if str_code in ['2B', '202']: return 202
    try:
        return int(str_code)
    except:
        return 999

def run_migration():
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
        
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.executescript(ddl_script)
    conn.commit()

    cache_depts, cache_crimes, cache_annees, cache_units = set(), set(), set(), {}
    files = glob.glob("*.csv")

    for file_path in files:
        filename = os.path.basename(file_path)
        annee_match = re.search(r'20\d{2}', filename)
        if not annee_match: continue
        annee = int(annee_match.group(0))
        is_pn = "PN" in filename
        is_gn = "GN" in filename
        if not is_pn and not is_gn: continue

        print(f"Traitement : {filename}")
        if annee not in cache_annees:
            cursor.execute("INSERT OR IGNORE INTO Annee VALUES (?)", (annee,))
            cache_annees.add(annee)

        df_raw = pd.read_csv(file_path, header=None, low_memory=False, encoding='utf-8')

        # --- LOGIQUE D'EXTRACTION BASÉE SUR L'IMAGE ---
        col_mapping = {}
        
        if is_pn:
            # POLICE (3 lignes d'en-tête selon l'image)
            # Ligne 0: Dept | Ligne 1: Périmètre | Ligne 2: Libellé index \ CSP
            row_depts = df_raw.iloc[0, 2:].values
            row_perims = df_raw.iloc[1, 2:].values
            row_units = df_raw.iloc[2, 2:].values
            data_start_row = 3 # Les chiffres commencent à la ligne 3
        else:
            # GENDARMERIE (2 lignes d'en-tête classiques)
            row_depts = df_raw.iloc[0, 2:].values
            row_perims = ["N/A"] * len(row_depts)
            row_units = df_raw.iloc[1, 2:].values
            data_start_row = 2

        # Création des unités
        for i in range(len(row_units)):
            u_name = str(row_units[i]).strip()
            d_raw = row_depts[i]
            p_name = str(row_perims[i]).strip()
            col_idx = i + 2

            if u_name == "nan" or u_name == "": continue

            dept_id = clean_dept_code(d_raw)
            if dept_id not in cache_depts:
                cursor.execute("INSERT OR IGNORE INTO Departement VALUES (?, ?)", (dept_id, str(d_raw)))
                cache_depts.add(dept_id)

            unit_key = (u_name, dept_id, "GN" if is_gn else "PN")
            if unit_key not in cache_units:
                cursor.execute("INSERT INTO Unite (lb_unite, id_departement) VALUES (?, ?)", (u_name, dept_id))
                uid = cursor.lastrowid
                if is_gn:
                    cursor.execute("INSERT INTO Gendarmerie VALUES (?)", (uid,))
                else:
                    cursor.execute("INSERT INTO Police VALUES (?, ?)", (uid, p_name))
                cache_units[unit_key] = uid
            
            col_mapping[col_idx] = cache_units[unit_key]

        # Insertion des données (faits)
        df_data = df_raw.iloc[data_start_row:]
        batch_facts = []
        for _, row in df_data.iterrows():
            try:
                # Vérification que c'est une ligne de crime (colonne 0 est un nombre)
                c_id = int(float(str(row[0])))
                c_lib = str(row[1]).strip()

                if c_id not in cache_crimes:
                    cursor.execute("INSERT OR IGNORE INTO Crime VALUES (?, ?)", (c_id, c_lib))
                    cache_crimes.add(c_id)

                for c_idx, u_id in col_mapping.items():
                    try:
                        val = int(float(str(row[c_idx])))
                        if val > 0:
                            batch_facts.append((u_id, c_id, annee, val))
                    except:
                        continue
            except:
                continue

        if batch_facts:
            cursor.executemany("INSERT OR IGNORE INTO a_enregistre VALUES (?,?,?,?)", batch_facts)
            conn.commit()

    conn.close()
    print("\n--- TERMINE ---")
    print(f"Base de données générée : {os.path.abspath(DB_NAME)}")

if __name__ == "__main__":
    excel_vers_csv_multiples(FICHIE_ENTREE)
    run_migration()