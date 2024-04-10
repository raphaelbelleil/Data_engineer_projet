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
    df['num_ligne'] = [x+1 for x in range(len(df))]
    return df



# traitement des valeurs nulles
def traitement_valeurs_nulles(df, nom_flux):
    """Traite les lignes qui contiennent des valeurs manquantes.
    Génère un df sans valeurs manquantes et un autre avec les lignes où il y a des valeurs manquantes.
    Entrée : df,
    Return : df_sans_na, df_na"""
    if nom_flux in ['CATEGORIE', 'TYPE_CLIENT', 'SOUS_CATEGORIE', 'PRODUIT', 'VENTE'] :
        df_sans_na = df.dropna()
        df_na = df[df.isna().any(axis=1)]
        nb_lignes_supprimees = df.shape[0] - df_sans_na.shape[0] 
        print(f"Nombre de lignes qui contiennent des valeurs nulles : {nb_lignes_supprimees}")

    if nom_flux == 'CLIENT':
        df_sans_na = df.dropna(subset=df.drop(columns='CD_POSTAL_CLIENT').columns)
        df_na = df[df.drop(columns='CD_POSTAL_CLIENT').isna().any(axis=1)]
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

# traitement des types
def traitement_type(df, nom_flux):
    "Traite les types si nécessaires"
    if nom_flux == 'CLIENT':
        df['CD_POSTAL_CLIENT'] = df['CD_POSTAL_CLIENT'].astype('str')
    return df


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


def creation_rejet_dependance(lignes_rejets_dependance, nom_fichier, nom_flux, champs_dependance, flux_dependance):
    """Traite les rejets, dépendances.
    Entrée : lignes_rejets_dependance, nom_fichier, nom_flux,
    Return : rejet_dependance"""
    rejet_dependance = pd.DataFrame()
    rejet_dependance['num_ligne_rejet'] = lignes_rejets_dependance['num_ligne']
    rejet_dependance['lb_nom_fichier'] = nom_fichier
    rejet_dependance['lb_ligne_rejet'] = [str(row) for row in lignes_rejets_dependance.drop(columns='num_ligne').values]
    rejet_dependance['lb_message_rejet'] = f"La valeur {lignes_rejets_dependance[f'{champs_dependance}']} du champs {champs_dependance} n'est pas présent dans la table d'origine {flux_dependance}."
    rejet_dependance['lb_nom_flux'] = nom_flux
    rejet_dependance['dt_rejet'] = datetime.datetime.now()
    return rejet_dependance



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

    if nom_flux == 'VENTE':
        df = df.drop(columns='bl_ligne_active')
        colonnes_fichier = list(df.columns[:-3])

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
        
# INSERTION/SUPPRESSION/MODIFICATION DES DONNEES 1


def insertion_rejets(rejets, cur, conn):
    requete_sql_insertion = "INSERT INTO ODS_REJET(NUM_LIGNE_REJET, LB_NOM_FICHIER, LB_LIGNE_REJET, LB_MESSAGE_REJET, LB_NOM_FLUX, DT_REJET) VALUES (:1, :2, :3, :4, :5, :6)"
    table_row = list(rejets.itertuples(index=False, name=None))

    try:
        cur.executemany(requete_sql_insertion, table_row)
        if conn :
            conn.commit()
            print(f"Table rejets insérée ({len(table_row)})")
    except cx_Oracle.DatabaseError as error: 
        print("Erreur lors de l'importation des rejets : ", error)
        if conn:
            conn.rollback()



def suppression_donnees_table(cur, nom_flux, conn):
    '''Supression des données d'une table.'''
    try : 
        cur.execute(f"select count(*) from ODS_{nom_flux}")
        nombre_ligne = cur.fetchall()[0][0]
        cur.execute(f"delete from ODS_{nom_flux}")
        if conn:
            conn.commit()
            print(f"Données de la table ODS_{nom_flux} supprimées ({nombre_ligne}).")
    except cx_Oracle.DatabaseError as error:
        print(f"Erreur lors de la suppression des données de la table ODS_{nom_flux} : ", error)



def selection_table(colonnes_fichier, cur, nom_flux):
    '''Affiche le contenu de la table pour les lignes actives.
    Entrée : colonnes_fichier, cur, nom_flux
    Return : df resultat_requete'''
    colonnes = ', '.join(colonnes_fichier)
    try :
        fetchall = cur.execute(f"select {colonnes} from ODS_{nom_flux} where bl_ligne_active=1")
        resultat_requete_selection_table = pd.DataFrame(fetchall, columns=colonnes_fichier)
        print(f"Requête de sélection de la table ODS_{nom_flux} réussie, {resultat_requete_selection_table.shape[0]} lignes trouvées.")
        return resultat_requete_selection_table
    except cx_Oracle.DatabaseError as error:
        print(f"Erreur lors de la sélection de la table : {error}")
    


def selection_table_entiere(colonnes_fichier, cur, nom_table):
    '''Affiche le contenu de la table.
    Entrée : colonnes_fichier, cur, nom_flux
    Return : df resultat_requete'''
    colonnes = ', '.join(colonnes_fichier)
    try : 
        fetchall = cur.execute(f"select {colonnes} from {nom_table}")
        resultat_requete_selection_table = pd.DataFrame(fetchall, columns=colonnes_fichier)
        return resultat_requete_selection_table
    except cx_Oracle.DatabaseError as error:
        print(f"Erreur lors de la sélection de la table : {error}")



#######################################################################################################################
        
# VARIABLES ENTRE LES TABLES

def generer_cle(liste_cle):
    '''Si clé unique, renvoie la cle, sinon la liste de cle'''
    # clé et jointure clé
    if len(liste_cle) == 1:
        cle = liste_cle[0]
        cle_jointure = f"f.{cle} = t.{cle}"

    if len(liste_cle) > 1 :
        liste_cle2 = [f"t.{col}" for col in liste_cle]
        cle = f"concat({','.join(liste_cle2)})"
        cle_jointure = ' '.join([f"f.{col} = t.{col} and" for col in liste_cle[:-1]] + [f"f.{liste_cle[-1]} = t.{liste_cle[-1]}"])
      
    return cle, cle_jointure



def generer_condition_modif_table(nom_flux):
    '''Genère les conditions de modification des données en fonction de la table'''
    if nom_flux == 'CATEGORIE' :
        condition_modif_table = 'f.LB_CATEGORIE <> t.LB_CATEGORIE'
    if nom_flux == 'SOUS_CATEGORIE' :
        condition_modif_table = 'CONCAT(f.LB_SOUS_CATEGORIE, f.CD_CATEGORIE) <> CONCAT(t.LB_SOUS_CATEGORIE, t.CD_CATEGORIE)'
    if nom_flux == 'TYPE_CLIENT' :
        condition_modif_table = 'f.LB_TYPE_CLIENT <> t.LB_TYPE_CLIENT '
    if nom_flux == 'CLIENT' :
        condition_modif_table = '(f.NOM_CLIENT || f.PREN_CLIENT || f.CD_POSTAL_CLIENT || f.VILLE_CLIENT || f.PAYS_CLIENT || f.REGION_CLIENT || f.CD_TYPE_CLIENT) <> (t.NOM_CLIENT || t.PREN_CLIENT || t.CD_POSTAL_CLIENT || t.VILLE_CLIENT || t.PAYS_CLIENT || t.REGION_CLIENT || t.CD_TYPE_CLIENT)'
    if nom_flux == 'PRODUIT' :
        condition_modif_table = '(f.NOM_PRODUIT || f.PRIX_ACHAT_PRODUIT || f.PRIX_VENTE_PRODUIT || f.CD_SOUS_CATEGORIE) <> (t.NOM_PRODUIT || t.PRIX_ACHAT_PRODUIT || t.PRIX_VENTE_PRODUIT || t.CD_SOUS_CATEGORIE)'    
    if nom_flux == 'VENTE' :
        condition_modif_table = 'Pas de condition.'

    return condition_modif_table


#######################################################################################################################
        
# TABLE TEMPORAIRE

def insertion_fichier_table_temporaire(df, conn, cur, nom_flux):
    '''Insere les données du fichier dans une table temporaire.'''
    colonnes_sql = ', '.join(list(df.columns))
    references_colonnes_fichier = '(' + ', '.join([f":{x+1}" for x in range(len(df.columns))]) + ')'
    requete_insert = f"insert into {nom_flux}_FICHIER({colonnes_sql}) values {references_colonnes_fichier}"
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
    except Exception as error:
        print(f"Erreur lors de la création de la table temporaire {nom_flux}_FICHIER.")

    # insertion des données
    insertion_fichier_table_temporaire(df, conn, cur, nom_flux)


#######################################################################################################################
        
# INSERTION / MODIFICATION DES DONNEES
    
def insertion_nouvelles_valeurs(conn, cur, nom_flux, nom_fichier, cle_jointure, cle, df):
    '''Insere les nouvelles valeurs dans la base de données'''
    
    colonnes_sql = ', '.join(list(df.columns))
    references_colonnes_fichier = '(' + ', '.join([f":{x+1}" for x in range(len(df.columns))]) + ')'

    # insertion nouvelles donnees
    requete_nouvelles_valeurs = f'''
    with t as
    (select * from ODS_{nom_flux} t1 where t1.bl_ligne_active=1)
    select f.* from {nom_flux}_FICHIER f
    left join t on {cle_jointure} 
    where t.{cle} is null
    '''

    if nom_flux == 'VENTE':
        requete_nouvelles_valeurs = f'''
        select f.* from {nom_flux}_FICHIER f
        left join ODS_{nom_flux} t on {cle_jointure} 
        where {cle} is null
        '''      

    cur.execute(requete_nouvelles_valeurs)

    nouvelles_donnees = cur.fetchall()

    requete_insertion_nouvelles_valeurs = f'''
    insert into ODS_{nom_flux}({colonnes_sql}) 
    values {references_colonnes_fichier}
    '''

    if len(nouvelles_donnees)>0:
        try :
            cur.executemany(requete_insertion_nouvelles_valeurs, nouvelles_donnees)
            if conn :   
               conn.commit()
            print(f'{len(nouvelles_donnees)} nouvelles lignes insérées dans la table ODS_{nom_flux}')
        except cx_Oracle.DatabaseError as error :
            if conn :
                conn.rollback()
            print("Erreur lors de l'insertion des données", error)
    else :
        print(f"{len(nouvelles_donnees)} nouvelle données dans le fichier {nom_fichier}")



def modification_valeurs(conn, cur, nom_flux, cle, cle_jointure, condition_modif_table):
    '''Modifie les valeurs dans la base de données'''
    colonne_sql = 'bl_ligne_active'
    
    requete_anciennes_valeurs_modifiees = f'''
    select f.* from {nom_flux}_FICHIER f
    inner join ODS_{nom_flux} t on {cle_jointure} 
    where {condition_modif_table} and t.bl_ligne_active=1
    '''

    cur.execute(requete_anciennes_valeurs_modifiees)

    lignes_a_modifier = cur.fetchall()

    requete_update = f"UPDATE ODS_{nom_flux} SET {colonne_sql} = 0 WHERE {cle} = :1"
    
    try:
        for ligne in lignes_a_modifier:
            cur.execute(requete_update, (ligne[0],))
        if conn:
            conn.commit()
        print(f'{len(lignes_a_modifier)} lignes modifiées et rendu obsolètes dans la table ODS_{nom_flux}')
    except cx_Oracle.DatabaseError as error:
        if conn:
            conn.rollback()
        print("Erreur lors de la modification des données", error)

    return lignes_a_modifier



def insertion_nouvelles_valeurs_modifiees(conn, cur, nom_flux, df, cle_jointure, condition_modif_table, lignes_a_modifier):
    '''Insère les nouvelles valeurs modifiées dans la base de données'''

    colonnes_sql = ', '.join(list(df.columns))
    references_colonnes_fichier = '(' + ', '.join([f":{x+1}" for x in range(len(df.columns))]) + ')'

    requete_insert = f"insert into ODS_{nom_flux}({colonnes_sql}) values {references_colonnes_fichier}"

    try : 
        cur.executemany(requete_insert, lignes_a_modifier)
        if conn :   
            conn.commit()
            print(f'{len(lignes_a_modifier)} nouvelles lignes modifiées insérées dans la table ODS_{nom_flux}')
    except cx_Oracle.DatabaseError as error :
        if conn :
            conn.rollback()
        print("Erreur lors de l'insertion des données modifiées", error)



def gestion_dependance(conn, cur, nom_flux, df, nom_fichier, champs_dependance, flux_dependance):
    '''Gère les dépendances entre tables.'''

    # ligne à rejeter
    requete_dependance = f'''
    with 
    f as 
    (select f1.*, rownum as num_ligne from {nom_flux}_FICHIER f1),
    t as
    (select * from ODS_{flux_dependance} where bl_ligne_active=1)
    select f.* from f
    left join t
    on f.{champs_dependance} = t.{champs_dependance}
    where t.{champs_dependance} is null
    '''
    
    cur.execute(requete_dependance)
    resultat_requete_dependance = cur.fetchall()

    lignes_rejets_dependance = pd.DataFrame(resultat_requete_dependance, columns = list(df.columns) + ['num_ligne'])

    rejet_dependance = creation_rejet_dependance(lignes_rejets_dependance, nom_fichier, nom_flux, champs_dependance, flux_dependance)

    # Insertion des rejets s'il y en a
    if len(rejet_dependance) > 0 :
        # insertion rejet
        print(f"Problème de dépendances pour le fichier {nom_fichier} par rapport à la table {flux_dependance}")
        insertion_rejets(rejet_dependance, cur, conn)
        
        # suppression des rejets du fichier pour la suite
        requete_dependance_valide = f'''
        select f.* from {nom_flux}_FICHIER f
        inner join ODS_{flux_dependance} t
        on f.{champs_dependance} = t.{champs_dependance}
        where t.bl_ligne_active=1
        '''

        cur.execute(requete_dependance_valide)
        resultat_requete_dependance_valide = cur.fetchall()

        df_sans_rejets_dependances = pd.DataFrame(resultat_requete_dependance_valide, columns=df.columns)

        creation_table_temporaire_fichier(conn, cur, nom_flux, df_sans_rejets_dependances)

    else :
        print(f"Pas de problème de dépendances pour le fichier {nom_fichier} par rapport à la table {flux_dependance}")
    


#######################################################################################################################
        
# FACTORISATION

# EXTRACTION ET TRANSFORMATION     
def extract_transform(nom_flux, liste_cle):

    # import du fichier
    df, nom_fichier, nom_flux = import_fichier(nom_flux=nom_flux)

    # traitements df
    df = ajout_col_num_ligne(df)
    df, df_na = traitement_valeurs_nulles(df, nom_flux)
    df, df_avec_doublons = traitement_doublons(df)
    df = traitement_type(df, nom_flux)
    df, colonnes_fichier = traitement_pre_import(df, nom_fichier, nom_flux)

    # traitements rejets
    rejet_valeurs_nulles = creation_rejet_valeurs_nulles(df_na, nom_fichier, nom_flux)
    rejet_doublons = creation_rejet_doublons(df_avec_doublons, nom_fichier, nom_flux)
    rejets = rassemblement_rejet(rejet_valeurs_nulles, rejet_doublons)

    # CREATION DES VARIABLES SPECIFIQUES A LA TABLE

    cle, cle_jointure = generer_cle(liste_cle)
    condition_modif_table = generer_condition_modif_table(nom_flux)

    return df, nom_fichier, nom_flux, colonnes_fichier, rejets, cle, cle_jointure, condition_modif_table



# CHARGEMENT DANS LA BASE DE DONNEES

def load(df, nom_fichier, nom_flux, rejets, cle, cle_jointure, condition_modif_table):

    # ouverture de la connexion et du curseur
    conn = open_connection()
    cur = open_cursor(conn)

    # insertion des rejets s'il y en a
    if rejets.shape[0] > 0 : 
        insertion_rejets(rejets, cur, conn)
    else :
        print(f"Pas de rejets dans le fichier {nom_fichier}")

    # insertion du fichier dans une table temporaire pour inserer les données dans la base de données

    creation_table_temporaire_fichier(conn, cur, nom_flux, df)

    if nom_flux == 'SOUS_CATEGORIE' :
        champs_dependance = 'CD_CATEGORIE'
        flux_dependance = 'CATEGORIE'
        gestion_dependance(conn, cur, nom_flux, df, nom_fichier, champs_dependance, flux_dependance)

    if nom_flux == 'CLIENT' :
        champs_dependance = 'CD_TYPE_CLIENT'
        flux_dependance = 'TYPE_CLIENT'
        gestion_dependance(conn, cur, nom_flux, df, nom_fichier, champs_dependance, flux_dependance)

    if nom_flux == 'PRODUIT' :
        champs_dependance = 'CD_SOUS_CATEGORIE'
        flux_dependance = 'SOUS_CATEGORIE'
        gestion_dependance(conn, cur, nom_flux, df, nom_fichier, champs_dependance, flux_dependance) 

    if nom_flux == 'VENTE' :
        champs_dependance1 = 'CD_PRODUIT'
        flux_dependance1 = 'PRODUIT'
        gestion_dependance(conn, cur, nom_flux, df, nom_fichier, champs_dependance1, flux_dependance1) 
        champs_dependance2 = 'ID_CLIENT'
        flux_dependance2 = 'CLIENT'
        gestion_dependance(conn, cur, nom_flux, df, nom_fichier, champs_dependance2, flux_dependance2) 

    # insertion / modification des données dans la base de données

    insertion_nouvelles_valeurs(conn, cur, nom_flux, nom_fichier, cle_jointure, cle, df)

    if nom_flux in ['CATEGORIE', 'TYPE_CLIENT', 'SOUS_CATEGORIE', 'CLIENT', 'PRODUIT'] : 
        lignes_a_modifier = modification_valeurs(conn, cur, nom_flux, cle, cle_jointure, condition_modif_table)
        insertion_nouvelles_valeurs_modifiees(conn, cur, nom_flux, df, cle_jointure, condition_modif_table, lignes_a_modifier)

    # validation des modifications
    if conn :
        conn.commit()
