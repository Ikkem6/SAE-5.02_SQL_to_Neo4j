# SAE - 5.02 : Migration d'une base de données relationnelle vers un modèle graphe
Ce repository contient les travaux techniques réalisés dans le cadre du projet de BUT SD : Migration d'une base de données relationnelle vers un modèle graphe pour l’analyse des crimes et délits en France (2012-2021).

## Introduction

Ce projet porte sur la migration d’une base de données relationnelle vers un modèle orienté graphe afin d’améliorer l’analyse des données sur les crimes et délits enregistrés en France entre 2012 et 2021.

Les données proviennent d’un fichier source contenant des statistiques issues de la Police nationale et de la Gendarmerie nationale : https://www.data.gouv.fr/datasets/archive-crimes-et-delits-enregistres-par-les-services-de-gendarmerie-et-de-police-depuis-2012

Le travail a consisté à analyser ces données, à les intégrer dans une base relationnelle SQL, puis à les migrer vers une base orientée graphe avec Neo4j afin d’explorer une modélisation plus adaptée à l’analyse des relations entre entités.

## Travaux réalisés

Dans un premier temps, nous avons étudié les données brutes afin d’identifier les entités principales et les relations entre elles. À partir de cette analyse, nous avons conçu un modèle de données avec un MCD et un MLD réalisés avec le logiciel Looping.

Ensuite, nous avons mis en place une base de données relationnelle SQL et développé un script Python permettant de créer les tables et d’alimenter automatiquement la base à partir du fichier de données initial.

Après cela, nous avons étudié les limites du modèle relationnel et proposé une modélisation alternative sous forme de base orientée graphe. Nous avons défini les nœuds et les relations nécessaires pour représenter les données dans un environnement Neo4j.

Enfin, nous avons développé un script Python permettant de migrer les données depuis la base relationnelle vers la base graphe. Des tests ont ensuite été réalisés afin de vérifier la cohérence des données migrées et de comparer l’utilisation des deux modèles pour certaines analyses.

## Contenu du repository

Ce repository regroupe principalement les scripts utilisés pour :

- creer et alimenter la base de donnees relationnelle a partir des donnees brutes
- migrer les donnees de la base SQL vers une base graphe Neo4j

Il permet ainsi de retrouver l’ensemble du code utilise pour la transformation et la migration des donnees entre les deux systemes.
