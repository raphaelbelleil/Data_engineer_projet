# fichiers de fonctions

# import des librairies nécessaires
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import config
import cx_Oracle
import datetime


# IMPORT


# fonction import fichier
def import_fichier(nom_flux):
    '''Importe un fichier,
    Entrée : nom du flux,
    Return : df, nom_fichier, nom_flux'''
    try :
        print(nom_flux)
        print('-'*100)
        nom_fichier = config.structure_fichier.replace('*', nom_flux)
        print(f'Fichier {nom_fichier} trouvé')
        print('-'*100)
        df = pd.read_csv(config.folder + nom_fichier, delimiter=config.delimiter)
        print(f'Fichier {nom_fichier} importé')
        print('-'*100)
        return df, nom_fichier, nom_flux
    
    except Exception as error :
        print ("Erreur lors de l'importation du fichier")
        print('-'*100)
        print("Message d'erreur : ", error)
        


# fonction scan fichier
def scan_dataframe_fichier(df, nom_fichier):
    '''Scanne le fichier :
    - Indique le nombre de ligne et de colonne
    - Indique le type de chaque colonne
    - Indique le nombre de lignes contenant des valeurs manquantes
    - Indique le nombre de valeurs manquantes par colonne,
    Entrée : df, nom_fichier
    '''
    print(f"Le fichier {nom_fichier} compte {df.shape[0]} lignes et {df.shape[1]} colonnes.")
    print('-'*100)
    print(f"Type de chaque colonne : {df.info()}")
    print('-'*100)
    print(f"Nombre de valeurs manquantes par colonnes : \n {df.isna().sum()}")
    print('-'*100)
    print(f"Nombre lignes avec au moins une valeur manquante : {df[df.isna().any(axis=1)].shape[0]}")
    print(f"Index de cette/ces ligne : {df[df.isna().any(axis=1)].index}")
    print('-'*100)


#######################################################################################################################

# TRAITEMENTS

# ajout colonne ligne
def ajout_col_num_ligne(df):
    """Ajout colonne numéro de ligne.
    Entrée : df,
    Return : df avec colonne num_ligne"""
    df['num_ligne'] = range(len(df))
    return df



# traitement des valeurs nulles
def traitement_valeurs_nulles(df):
    """Traite les lignes qui contiennent des valeurs manquantes.
    Génère un df sans valeurs manquantes et un autre avec les lignes où il y a des valeurs manquantes.
    Entrée : df,
    Return : df_sans_na, df_na"""
    df_sans_na = df.dropna()
    df_na = df[df.isna().any(axis=1)]
    nb_lignes_supprimees = df.shape[0] - df_sans_na.shape[0] 
    print(f"Nombre de lignes qui contiennent des valeurs nulles : {nb_lignes_supprimees}")
    return df_sans_na, df_na



# traitement des doublons
def traitement_doublons(df):
    """Traite les lignes qui contiennent des doublons.
    return : Génère un df sans doublons et un autre avec les doublons."""
    df_sans_doublons = df[~df.drop(columns='num_ligne').duplicated()]
    df_avec_doublons = df[df.drop(columns='num_ligne').duplicated()]
    nb_lignes_supprimees = df_avec_doublons.shape[0]
    print(f"Nombre de lignes qui contiennent des doublons : {nb_lignes_supprimees}")
    df_sans_doublons = df_sans_doublons.drop(columns='num_ligne')
    print('-'*100)
    return df_sans_doublons, df_avec_doublons



# table rejets valeurs nulles
def creation_rejet_valeurs_nulles(df_na, nom_fichier, nom_flux):
    """Traite les rejets, valeurs nulles
    Entrée : df_na, nom_fichier, nom_flux,
    Return : rejet_valeurs_nulles"""
    rejet_valeurs_nulles = pd.DataFrame()
    rejet_valeurs_nulles['num_ligne_rejet'] = df_na['num_ligne']
    rejet_valeurs_nulles['lb_nom_fichier'] = nom_fichier
    rejet_valeurs_nulles['lb_ligne_rejet'] = [str(row) for row in df_na.drop(columns='num_ligne').values]
    rejet_valeurs_nulles['lb_message_rejet'] = "Données qui contiennent des valeurs nulles"
    rejet_valeurs_nulles['lb_nom_flux'] = nom_flux
    rejet_valeurs_nulles['dt_rejet'] = datetime.datetime.now()
    return rejet_valeurs_nulles



# table rejet doublons
def creation_rejet_doublons(df_avec_doublons, nom_fichier, nom_flux):
    """Traite les rejets, doublons.
    Entrée : df_avec_doublons, nom_fichier, nom_flux,
    Return : rejet_doublons"""
    rejet_doublons = pd.DataFrame()
    rejet_doublons['num_ligne_rejet'] = df_avec_doublons['num_ligne']
    rejet_doublons['lb_nom_fichier'] = nom_fichier
    rejet_doublons['lb_ligne_rejet'] = [str(row) for row in df_avec_doublons.drop(columns='num_ligne').values]
    rejet_doublons['lb_message_rejet'] = "Données qui contiennent des doublons"
    rejet_doublons['lb_nom_flux'] = nom_flux
    rejet_doublons['dt_rejet'] = datetime.datetime.now()
    return rejet_doublons



# rassemblement des tables de rejets
def rassemblement_rejet(rejet_valeurs_nulles, rejet_doublons):
    """Rassemble les rejets valeurs nulles et doublons, s'ils existent.
    Entrée : rejet_valeurs_nulles, rejet_doublons,
    Return : rejets"""
    rejets = pd.concat((rejet_valeurs_nulles, rejet_doublons), axis=0)
    print(f"Nombre de lignes rejetées : {rejets.shape[0]}")
    print('-'*100)
    return rejets



# traitement pre_import
def traitement_pre_import(df, nom_fichier, nom_flux):
    df['lb_nom_fichier'] = nom_fichier
    df['dt_insertion'] = datetime.datetime.now()
    df['bl_ligne_active'] = 1
    df['lb_job_name'] = nom_flux
    colonnes_fichier = list(df.columns[:-4])
    return df, colonnes_fichier

#######################################################################################################################

# BASE DE DONNEES

# Connexion à la base de données
def open_connection():
    """ Ouvre le connection avec la base de données Oracle,
    Indique si la connexion est réussie ou non."""
    try : 
        dsn_tns = cx_Oracle.makedsn(config.host, config.port, service_name=config.service_name)
        conn = cx_Oracle.connect(user=config.user, password=config.password, dsn=dsn_tns)
        print('Connexion établie.')
        return conn
    except cx_Oracle.Error as error :
        print("Erreur de connexion à Oracle : ", error)
        return None
    


# Fermeture de la connexion à la base de données
def close_connection(conn):
    if conn :
        conn.close()
        print("Connexion fermée.")
    else :
        print("Pas de connexion d'ouverte.")



# Ouverture d'un curseur 
def open_cursor(conn):
    """ Ouvre un curseur dans la base de données Oracle,
    Indique si le curseur a bien été créé."""  
    try : 
        cur = conn.cursor()
        print("Curseur ouvert.")
        return cur
    except cx_Oracle.Error as error:
        print("Erreur lors de la création du curseur : ", error)
        return None



# Fermeture d'un curseur 
def close_cursor(cur):
    """ Ouvre un curseur dans la base de données Oracle,
    Indique si le curseur a bien été créé.""" 
    if cur : 
        cur.close()
        print("Curseur fermée.")
    else : 
        print("Pas de curseur d'ouvert.")


#######################################################################################################################
        
# INSERTION/SUPPRESSION/MODIFICATION DES DONNEES


def insertion_rejets(rejets, cur, conn):
    requete_sql_insertion = "INSERT INTO ODS_REJET(NUM_LIGNE_REJET, LB_NOM_FICHIER, LB_LIGNE_REJET, LB_MESSAGE_REJET, LB_NOM_FLUX, DT_REJET) VALUES (:1, :2, :3, :4, :5, :6)"
    table_row = list(rejets.itertuples(index=False, name=None))

    try:
        cur.executemany(requete_sql_insertion, table_row)
        if conn :
            conn.commit()
            print("Table rejets insérée")
    except cx_Oracle.DatabaseError as error: 
        print("Erreur lors de l'importation des rejets : ", error)
        if conn:
            conn.rollback()



def suppression_donnees_table(cur, nom_flux, conn):
    '''Supression des données d'une table.'''
    try : 
        cur.execute(f"delete from ODS_{nom_flux}")
        if conn:
            conn.commit()
            print(f"Données de la table ODS_{nom_flux} supprimées.")
    except cx_Oracle.DatabaseError as error:
        print(f"Erreur lors de la suppression des données de la table ODS_{nom_flux} : ", error)



def selection_table(colonnes_fichier, cur, nom_flux):
    '''Affiche le contenu de la table pour les lignes actives.
    Entrée : colonnes_fichier, cur, nom_flux
    Return : df resultat_requete'''
    colonnes = ', '.join(colonnes_fichier)
    fetchall = cur.execute(f"select {colonnes} from ODS_{nom_flux} where bl_ligne_active=1")
    resultat_requete_selection_table = pd.DataFrame(fetchall, columns=colonnes_fichier)
    print(f"Requête de sélection de la table ODS_{nom_flux} réussie, {resultat_requete_selection_table.shape[0]} lignes trouvées.")
    return resultat_requete_selection_table



def selection_table_entiere(colonnes_fichier, cur, nom_table):
    '''Affiche le contenu de la table.
    Entrée : colonnes_fichier, cur, nom_flux
    Return : df resultat_requete'''
    colonnes = ', '.join(colonnes_fichier)
    fetchall = cur.execute(f"select {colonnes} from {nom_table}")
    resultat_requete_selection_table = pd.DataFrame(fetchall, columns=colonnes_fichier)
    return resultat_requete_selection_table



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



#######################################################################################################################
        
# TABLE TEMPORAIRE

def insertion_fichier_table_temporaire(df, conn, cur, nom_flux):
    '''Insere les données du fichier dans une table temporaire.'''
    colonnes_sql = ', '.join(list(df.columns))
    requete_insert = f"insert into {nom_flux}_FICHIER({colonnes_sql}) values (:1, :2, :3, :4, :5, :6)"
    df_row = list(df.itertuples(index=False, name=None))
    try : 
        cur.executemany(requete_insert, df_row)
        if conn :   
            conn.commit()
            print(f'{len(df_row)} lignes insérées dans la table {nom_flux}_FICHIER')
    except cx_Oracle.DatabaseError as error :
        if conn :
            conn.rollback()
        print("Erreur lors de l'insertion des données", error)



def creation_table_temporaire_fichier(conn, cur, nom_flux, df):
    ''''Création d'une table temporaire pour intégrer le fichier.
    Intégration des données dans la table'''

    try : 
        cur.execute(f"drop table {nom_flux}_FICHIER")
        print(f"Suppression de la table {nom_flux}_FICHIER réussie.")
    except cx_Oracle.DatabaseError as error:
        print(f"Tentative de suppression de la table {nom_flux}_FICHIER échoué : {error}")


    requete_creation_table_temporaire = f'''
    create global temporary table {nom_flux}_FICHIER
    on commit preserve rows
    as 
    select * from ods_{nom_flux}
    where 1=0
    '''
    # création table temporaire
    try :
        cur.execute(requete_creation_table_temporaire)
        print(f"Table temporaire {nom_flux}_FICHIER créée.")
    except cx_Oracle.DatabaseError as error:
        print(f"Erreur lors de la création de la table temporaire {nom_flux}_FICHIER.")

    # insertion des données
    insertion_fichier_table_temporaire(df, conn, cur, nom_flux)




