import os
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from pymongo import MongoClient



# Chemin du fichier CSV
file_path = 'D:\\fezai_med_amine\\data\\data_project\\crypto_data.csv'
# Chargement des données
df = pd.read_csv(file_path)
# Fonction pour supprimer les doublons
def supprimerDoublons(df):
    df_cleaned = df.drop_duplicates()
    #df_cleaned.to_csv('crypto_data_cleaned.csv', index=False)
    return df_cleaned

# Fonction pour remplacer les valeurs "Non disponible" par des zéros
def remplacerZero(df):
    df.replace("Non disponible", 0, inplace=True)
    df.replace('',0,inplace=True)

# Fonction pour transformer le format de la colonne 'timestamp' en 'date' et 'heure'
def transformTime(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')  # gestion des erreurs de conversion
    df['date'] = df['timestamp'].dt.date
    df['heure'] = df['timestamp'].dt.time
    df.drop(columns=['timestamp'], inplace=True)  # suppression de la colonne timestamp après conversion

# Fonction pour nettoyer les colonnes
def nettoyer_colonne(colonne):
    # Enlever les symboles, les espaces, et convertir en float
    colonne = colonne.replace({'€': '', '\$': '', 'TND': '', '%': '', ',': '', ' ': ''}, regex=True)
    # Convertir en float avec gestion des erreurs
    colonne = pd.to_numeric(colonne, errors='coerce')
    return colonne

# Fonction spécifique pour nettoyer 'var_circulating_supply' avec suppression des lettres
def nettoyer_colonne1(colonne):
    colonne = colonne.replace({'€': '', '\$': '', 'TND': '', '%': '', ',': '', ' ': ''}, regex=True)
    colonne = colonne.str.replace(r'[a-zA-Z]', '', regex=True)  # suppression des lettres
    return pd.to_numeric(colonne, errors='coerce')

# Fonction pour appliquer le nettoyage sur les colonnes concernées
def nettoyerColonnes(df):
    df['price'] = nettoyer_colonne(df['price'])
    df['var_last_heure'] = nettoyer_colonne(df['var_last_heure'])
    df['var_last_day'] = nettoyer_colonne(df['var_last_day'])
    df['var_cap'] = nettoyer_colonne(df['var_cap'])
    df['var_volume_day'] = nettoyer_colonne(df['var_volume_day'])
    #on peut optimiser et utiliser nettoyer_colonne puis replace
    df['var_circulating_supply'] = nettoyer_colonne1(df['var_circulating_supply'])

def standardisationColonnes(df):
    scaler = MinMaxScaler()
    # Liste des colonnes à normaliser
    colonnes_a_normaliser = ['price', 'var_cap', 'var_volume_day','moyenne_day']
    # Application de la normalisation
    df[colonnes_a_normaliser] = scaler.fit_transform(df[colonnes_a_normaliser])
def enrechissement_par_crypto(df):
    df_crypto = df.copy()
    # Trier les données par heure
    df_crypto = df_crypto.sort_values(by=['date', 'heure']).reset_index(drop=True)
    # Décaler la colonne 'price' d'une heure en arrière pour comparer
    df_crypto['price_shifted_1h'] = df_crypto['price'].shift(1)
    df_crypto['timestamp_shifted_1h'] = df_crypto['heure'].shift(1)
    df_crypto['variation_1h']=(df_crypto['price'] -df_crypto['price_shifted_1h']) / (df_crypto['price_shifted_1h'] * 100)
    # Supprimer les colonnes temporaires
    df_crypto.drop(columns=['price_shifted_1h', 'timestamp_shifted_1h'], inplace=True)
    # Remplir les valeurs NaN pour les variations non calculées avec 0
    df_crypto['variation_1h'].fillna(0, inplace=True)
    return df_crypto

def enrechissement(df):
    # Appliquer la fonction 'enrechissement_par_crypto' pour chaque groupe de 'Symbol'
    df = df.groupby('Symbol').apply(enrechissement_par_crypto).reset_index(drop=True)
    # Calcul de la moyenne quotidienne du prix pour chaque crypto et chaque date
    df["moyenne_day"] = df.groupby(['Symbol', 'date'])["price"].transform('mean')
    
    return df
def load_data(df):
    #df.to_csv('crypto_data_cleaned.csv', index=False)
    client = MongoClient('localhost', 27017)
    db = client['webscrapingdb']
    collection = db['cryptomonnies']

    # Convertir le DataFrame en une liste de dictionnaires
    data = df.to_dict('records')

    # Insérer les données dans MongoDB
    collection.insert_many(data)



# Pipeline de nettoyage des données
df = supprimerDoublons(df)
remplacerZero(df)
nettoyerColonnes(df)
transformTime(df)
df=enrechissement(df)
remplacerZero(df)
standardisationColonnes(df)
# Enregistrer les données nettoyées
#load_data(df)
df.to_csv("D:\\fezai_med_amine\\data\\data_project\\crypto_cleaned_data.csv")
