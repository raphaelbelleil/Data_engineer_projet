# fichiers de fonctions

# import des librairies nécessaires
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import config
import cx_Oracle
import datetime


# FONCTIONS AVEC PANDAS, ANCIENNES FONCTIONS


def detection_nouvelles_valeurs(colonnes_fichier, cle, resultat_requete_selection_table, df, nom_flux):
    '''Fait la jointure entre le fichier et la table de la base de données et sélectionne les nouvelles valeurs.
    Entrée : colonnes_fichier, cle, resultat_requete_selection_table, df
    Return : Les nouvelles lignes du fichier à insérer'''
    # création colonne sans clé
    colonnes_sans_cle = colonnes_fichier[:]
    colonnes_sans_cle.remove(cle)
    # lignes dans la base de données
    resultat_requete_selection_table = pd.DataFrame(resultat_requete_selection_table, columns=colonnes_fichier)
    # jointure par la gauche entre le fichier et la base de données, où les lignes de la base de données n'existent pas
    jointure_fichier_table = pd.merge(df[colonnes_fichier], resultat_requete_selection_table,
                                       on=cle, how='left', suffixes=('_FICHIER', '_TABLE'))
    lignes_a_inserer = jointure_fichier_table[jointure_fichier_table[[col+'_TABLE' for col in colonnes_sans_cle]].isna().all(axis=1)]
    # définition des lignes à insérer en filtrant le fichier sur les lignes sont présentes dans la base de données
    lignes_a_inserer = df[df[cle].isin(lignes_a_inserer[cle].values)]
    print(f"{lignes_a_inserer.shape[0]} nouvelles lignes à insérer dans la table ODS_{nom_flux}.")
    return lignes_a_inserer

            

def insertion_nouvelles_valeurs(lignes_a_inserer, conn, cur, nom_flux):
    '''Insere les nouvelles valeurs dans la base de données'''
    colonnes_sql = ', '.join(list(lignes_a_inserer.columns))
    requete_insert = f"insert into ODS_{nom_flux}({colonnes_sql}) values (:1, :2, :3, :4, :5, :6)"
    df_row = list(lignes_a_inserer.itertuples(index=False, name=None))
    try : 
        cur.executemany(requete_insert, df_row)
        if conn :   
            conn.commit()
            print(f'{len(df_row)} nouvelles lignes insérées dans la table ODS_{nom_flux}')
    except cx_Oracle.DatabaseError as error :
        if conn :
            conn.rollback()
        print("Erreur lors de l'insertion des données", error)



def detection_valeurs_modifiees_jointure(colonnes_fichier, cle, resultat_requete_selection_table, df):
    '''Fait la jointure entre le fichier et la table de la base de données et sélectionne les valeurs modifiées,
    c'est à dire existant déja dans la base de données mais avec des valeurs différentes.
    Entrée : colonnes_fichier, cle, resultat_requete_selection_table, df
    Return : Les lignes modifiées dans le fichier'''
    # création colonne sans clé
    colonnes_sans_cle = colonnes_fichier[:]
    colonnes_sans_cle.remove(cle)
    # lignes dans la base de données
    resultat_requete_selection_table = pd.DataFrame(resultat_requete_selection_table, columns=colonnes_fichier)
    # jointure par la gauche entre le fichier et la base de données, où les lignes de la base de données n'existent pas
    jointure_fichier_table = pd.merge(df[colonnes_fichier], resultat_requete_selection_table,
                                       on=cle, how='inner', suffixes=('_FICHIER', '_TABLE'))
    return jointure_fichier_table



def detection_valeurs_modifiees_mask(jointure_fichier_table, mask, nom_flux, cle, df):
    '''Sélectionne les valeurs dans le fichier, le masque doit être définie au préalable et redéfinie pour chaque table.'''
    # définition des lignes à insérer en filtrant le fichier sur les lignes sont présentes dans la base de données
    lignes_a_modifier = df[df[cle].isin(jointure_fichier_table[mask][cle].values)]
    print(f"{lignes_a_modifier.shape[0]} lignes à modifier dans la table ODS_{nom_flux}.")
    return lignes_a_modifier



def modification_valeurs(lignes_a_modifier, conn, cur, nom_flux, cle):
    '''Modifie les valeurs dans la base de données'''
    colonne_sql = 'bl_ligne_active'
    requete_update = f"UPDATE ODS_{nom_flux} SET {colonne_sql} = 0 WHERE {cle} = :1"
    df_row = list(lignes_a_modifier.itertuples(index=False, name=None))
    try:
        for ligne in df_row:
            cur.execute(requete_update, (ligne[0],))
        if conn:
            conn.commit()
            print(f'{len(lignes_a_modifier)} lignes modifiées et rendu obsolètes dans la table ODS_{nom_flux}')
    except cx_Oracle.DatabaseError as error:
        if conn:
            conn.rollback()
        print("Erreur lors de la modification des données", error)



def insertion_nouvelles_valeurs_modifiees(lignes_a_modifier, conn, cur, nom_flux):
    '''Insère les nouvelles valeurs modifiées dans la base de données'''
    colonnes_sql = ', '.join(list(lignes_a_modifier.columns))
    requete_insert = f"insert into ODS_{nom_flux}({colonnes_sql}) values (:1, :2, :3, :4, :5, :6)"
    df_row = list(lignes_a_modifier.itertuples(index=False, name=None))
    try : 
        cur.executemany(requete_insert, df_row)
        if conn :   
            conn.commit()
            print(f'{len(df_row)} nouvelles lignes modifiées insérées dans la table ODS_{nom_flux}')
    except cx_Oracle.DatabaseError as error :
        if conn :
            conn.rollback()
        print("Erreur lors de l'insertion des données modifiées", error)