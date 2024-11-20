
import os
import time
import pandas as pd 
from sklearn.preprocessing import MinMaxScaler
from pymongo import MongoClient

from playwright.sync_api import sync_playwright
import json
from datetime import datetime
import sys

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
    return df

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

def enrichissement_par_crypto(df, df_enrichment):
    # Charger les données enrichies et filtrer par 'date', 'heure' et 'Symbol'
    max_date = df_enrichment['date'].max()
    max_heure = df_enrichment[df_enrichment['date'] == max_date]['heure'].max()
    
    # Sélectionner les données correspondant à la dernière heure et au même 'Symbol'
    df_last_hour = df_enrichment[
        (df_enrichment['date'] == max_date) &
        (df_enrichment['heure'] == max_heure) &
        (df_enrichment['Symbol'] == df['Symbol'].iloc[0])
    ]
    
    # Concaténer les deux DataFrames
    df_crypto = pd.concat([df, df_last_hour], axis=0)
    
    # Trier par 'date' et 'heure'
    df_crypto = df_crypto.sort_values(by=['date', 'heure']).reset_index(drop=True)
    
    # Décaler le prix d'une heure
    df_crypto['price_shifted_1h'] = df_crypto['price'].shift(1)
    
    # Calculer la variation horaire en pourcentage
    df_crypto['variation_1h'] = (df_crypto['price'] - df_crypto['price_shifted_1h']) / df_crypto['price_shifted_1h']
    
    # Remplir les NaN avec 0 pour la variation
    df_crypto['variation_1h'] = df_crypto['variation_1h'].fillna(0)
    
    # Supprimer les colonnes temporaires
    df_crypto.drop(columns=['price_shifted_1h'], inplace=True)
    
    return df_crypto

def enrichissement(df):
    # Charger les données enrichies depuis un fichier CSV
    df_enrichment = pd.read_csv('D:\\fezai_med_amine\\data\\data_project\\data\\Crypto_Cleaned_Data.csv',index_col=0)
    
    # Appliquer 'enrichissement_par_crypto' pour chaque groupe de 'Symbol'
    df = df.groupby('Symbol', group_keys=False).apply(lambda group: enrichissement_par_crypto(group, df_enrichment))
    
    # Calculer la moyenne quotidienne du prix
    df['moyenne_day'] = df.groupby(['Symbol', 'date'])['price'].transform('mean')
    
    return df

def convert_dates_to_str(df):
    if 'date' in df.columns:
            df['date'] = df['date'].astype(str)
    if 'heure' in df.columns:
        df['heure'] = df['heure'].astype(str)
    return df 

# Fonction principale de scraping
def scraping():
    data_list = []
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto('https://coinmarketcap.com/')
            
            for i in range(5):
                page.mouse.wheel(0, 2000)
                page.wait_for_timeout(1000)

            trs_xpath = '//table[contains(@class, "cmc-table")]/tbody/tr'
            trs_list = page.query_selector_all(trs_xpath)

            for tr in trs_list:
                coin_dict = {}
                tds = tr.query_selector_all('td')
                if len(tds) > 1:
                    coin_dict['id'] = tds[1].inner_text()
                else:
                    coin_dict['id'] = 'Non disponible'
                coin_dict['Name'] = tds[2].query_selector('p[class*="coin-item-name"]').inner_text() if len(tds) > 2 else 'Non disponible'
                coin_dict['Symbol'] = tds[2].query_selector('p[class*="coin-item-symbol"]').inner_text() if len(tds) > 2 else 'Non disponible'
                coin_dict['price'] = tds[3].query_selector('div[class*="sc-b3fc6b7-0"]').inner_text() if len(tds) > 3 else 'Non disponible'
                coin_dict['var_last_heure'] = tds[4].query_selector('span[class*="ivvJzO"]').inner_text() if tds[4].query_selector('span[class*="ivvJzO"]') else 'Non disponible'
                coin_dict['var_last_day'] = tds[5].query_selector('span[class*="ivvJzO"]').inner_text() if tds[5].query_selector('span[class*="ivvJzO"]') else 'Non disponible'
                coin_dict['var_cap'] = tds[7].query_selector('span[class*="jfwGHx"]').inner_text() if tds[7].query_selector('span[class*="jfwGHx"]') else 'Non disponible'
                coin_dict['var_volume_day'] = tds[8].query_selector('p[class*="font_weight_500"]').inner_text() if tds[8].query_selector('p[class*="font_weight_500"]') else 'Non disponible'
                coin_dict['var_circulating_supply'] = tds[9].query_selector('p[class*="hhmVNu"]').inner_text() if tds[9].query_selector('p[class*="hhmVNu"]') else 'Non disponible'
                coin_dict['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                data_list.append(coin_dict)
            
            browser.close()

        except Exception as e:
            print("Erreur dans Playwright :", e)

    return data_list

# Fonction pour traiter les données
def traitement_donnees(data_list):
    df = pd.DataFrame(data_list)
    df = supprimerDoublons(df)
    remplacerZero(df)
    df=transformTime(df)
    nettoyerColonnes(df)
    #df_cleaned = pd.read_csv("mon_fichier.csv", sep=";")
    df = enrichissement(df)
    remplacerZero(df)
    #df=convert_dates_to_str(df)
    return df

# Exécution du pipeline
if __name__ == "__main__":
    try:
        data_list = scraping()
        df = traitement_donnees(data_list)
        
        # Convertir explicitement les dates en chaînes
        df=convert_dates_to_str(df)
        
        # Vérifier les types de colonnes
        print(df.dtypes)
        
        # Convertir en JSON et afficher
        data_dict = df.to_dict(orient='records')
        json.dump(data_dict, sys.stdout)  # Exporter au format JSON
        #print(df)
    except Exception as e:
        print("Erreur dans Playwright :", e)
        with open('logfile.log', 'a') as error_file:
            error_file.write(f"{datetime.now()} - {str(e)}\n")

