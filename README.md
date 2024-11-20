# Projet Data Pipeline 
Ce projet est un pipeline de données qui collecte en utilisant le un script python de web scraping, nettoie et enrichit des données de cryptomonnaies.  
Il utilise **Python** pour le traitement des données et **Apache NiFi** pour l'orchestration.

## Contenu
- **./** : Contient les scripts Python utilisés pour le scraping et le traitement des données.
- **nifi/** : Contient la configuration exportée d'Apache NiFi.

## Prérequis
1. Python 3.10.4
2. Apache NiFi installé
3. Librairies Python listées avec explication détaillé pour chaque étape dans `/requirements`.

## Instructions
1. Installez les dépendances :
   ```bash
   pip install -r requirements.txt

## fichier log
1. il y a un fichier log `logfile.log` pour les erreurs au niveau du web scraping
2. si il ya des erreurs au niveau d'installation des bibliothèques on peut utiliser un environnement virtuel Python  
