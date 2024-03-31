# fonctions personnelles
import functions as f

# AFFICHAGE CATEGORY
df_category, nom_fichier_category = f.import_fichier(nom_fichier = 'CATEGORIE')
print(df_category.head())

print('-'*100)

# SCAN CATEGORY
f.scan_dataframe_fichier(df=df_category, nom_fichier_entier=nom_fichier_category)

print(nom_fichier_category)

