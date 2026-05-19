# 🚀 Spark Scala — Exercices Pratiques (RDD & DataFrame)
### Niveau : Master 2 Data Engineering / Big Data
> **Pas de Streaming** — Uniquement **RDD** et **DataFrame**
> Données au format **CSV** — Du débutant à l'expert

---

## 📋 Table des Matières

1. [Exercice 1 — Analyse des Ventes (Niveau Débutant)](#exercice-1--analyse-des-ventes-niveau-débutant)
2. [Exercice 2 — Analyse RH & Salaires (Niveau Intermédiaire)](#exercice-2--analyse-rh--salaires-niveau-intermédiaire)
3. [Exercice 3 — Analyse E-Commerce Multi-Sources (Niveau Expert)](#exercice-3--analyse-e-commerce-multi-sources-niveau-expert)

---

## ⚙️ Prérequis & Configuration

```scala
// build.sbt
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql"  % "3.3.0"
)
```

```scala
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

val spark = SparkSession.builder()
  .appName("SparkExercices")
  .master("local[*]")
  .getOrCreate()

val sc = spark.sparkContext
sc.setLogLevel("WARN")
```

---

## Exercice 1 — Analyse des Ventes (Niveau Débutant)

### 🎯 Objectif
Manipuler les **RDD** et les **DataFrames** de base pour explorer un jeu de données de ventes commerciales.

---

### 📄 Données : `ventes.csv`

```csv
id_vente,produit,categorie,quantite,prix_unitaire,ville,date_vente
1,Laptop,Electronique,2,850.00,Dakar,2023-01-15
2,Chaise,Mobilier,5,120.00,Thiès,2023-01-16
3,Téléphone,Electronique,3,450.00,Dakar,2023-02-10
4,Bureau,Mobilier,1,300.00,Saint-Louis,2023-02-14
5,Casque,Electronique,10,75.00,Dakar,2023-03-05
6,Lampe,Mobilier,4,40.00,Thiès,2023-03-18
7,Tablette,Electronique,6,320.00,Ziguinchor,2023-04-01
8,Canapé,Mobilier,2,600.00,Dakar,2023-04-22
9,Souris,Electronique,15,25.00,Saint-Louis,2023-05-07
10,Étagère,Mobilier,3,90.00,Thiès,2023-05-19
11,Clavier,Electronique,8,60.00,Dakar,2023-06-03
12,Armoire,Mobilier,1,750.00,Ziguinchor,2023-06-25
```

---

### 📝 Questions — Partie A : Avec les RDD

**Q1.1 — Chargement & comptage**
Chargez le fichier `ventes.csv` avec `sc.textFile()`. Supprimez l'en-tête, puis affichez le nombre total de ventes.

**Q1.2 — Calcul du chiffre d'affaires total**
Calculez le chiffre d'affaires total de toutes les ventes.
> Formule : `CA = quantite × prix_unitaire`

**Q1.3 — Filtrer par catégorie**
Affichez uniquement les ventes de la catégorie `Electronique` et comptez-les.

**Q1.4 — Chiffre d'affaires par ville**
Calculez le chiffre d'affaires par ville et affichez les résultats triés du plus grand au plus petit.

**Q1.5 — Produit le plus vendu en quantité**
Trouvez le produit dont la quantité totale vendue est la plus élevée.

---

### 📝 Questions — Partie B : Avec les DataFrames

**Q1.6 — Chargement avec schéma automatique**
Chargez `ventes.csv` avec `spark.read.csv()`, option `header = true` et `inferSchema = true`. Affichez le schéma et les 5 premières lignes.

**Q1.7 — Ajout d'une colonne calculée**
Ajoutez une colonne `chiffre_affaires` = `quantite * prix_unitaire` au DataFrame.

**Q1.8 — Agrégation par catégorie**
Calculez pour chaque catégorie :
- La somme du chiffre d'affaires
- La quantité totale vendue
- Le nombre de transactions

**Q1.9 — Statistiques descriptives**
Utilisez `.describe()` pour afficher les statistiques (min, max, moyenne, écart-type) des colonnes `quantite` et `prix_unitaire`.

**Q1.10 — Filtrage combiné**
Sélectionnez les ventes où `prix_unitaire > 100` ET `ville = "Dakar"`, triées par `chiffre_affaires` décroissant.

---

## Exercice 2 — Analyse RH & Salaires (Niveau Intermédiaire)

### 🎯 Objectif
Maîtriser les **jointures**, les **agrégations avancées**, les **fonctions de fenêtrage (Window Functions)** et la conversion RDD ↔ DataFrame.

---

### 📄 Données : `employes.csv` & `departements.csv`

**`employes.csv`**
```csv
id_employe,nom,prenom,id_departement,salaire,anciennete_annees,genre,date_embauche
E001,Diallo,Mamadou,D01,650000,8,M,2015-03-10
E002,Sow,Fatou,D02,820000,12,F,2011-07-22
E003,Fall,Ibrahima,D01,540000,3,M,2020-01-15
E004,Ndiaye,Aminata,D03,910000,15,F,2008-06-01
E005,Ba,Ousmane,D02,730000,9,M,2014-11-30
E006,Sarr,Rokhaya,D01,610000,6,F,2017-09-14
E007,Diop,Cheikh,D03,1050000,18,M,2005-04-20
E008,Mbaye,Ndèye,D04,480000,2,F,2021-08-05
E009,Thiam,Lamine,D02,870000,11,M,2012-02-28
E010,Cissé,Marème,D04,520000,4,F,2019-05-17
E011,Gueye,Pape,D03,980000,14,M,2009-10-03
E012,Koné,Awa,D01,590000,5,F,2018-12-01
```

**`departements.csv`**
```csv
id_departement,nom_departement,localisation,budget_annuel
D01,Informatique,Dakar,45000000
D02,Finance,Dakar,60000000
D03,Direction,Dakar,80000000
D04,Marketing,Thiès,30000000
```

---

### 📝 Questions — Partie A : Jointures & Agrégations

**Q2.1 — Jointure RDD manuelle**
En utilisant uniquement l'API RDD, réalisez une jointure entre `employes.csv` et `departements.csv` sur `id_departement`. Affichez : `nom`, `prenom`, `nom_departement`, `salaire`.

**Q2.2 — Salaire moyen par département**
Calculez le salaire moyen par département (via DataFrame). Ajoutez aussi le min et le max.

**Q2.3 — Écart salarial Homme/Femme**
Calculez le salaire moyen par genre et par département. Affichez l'écart absolu et relatif (%) entre H et F pour chaque département.

**Q2.4 — Top 3 des salaires par département**
Pour chaque département, identifiez les 3 employés les mieux payés. Utilisez les **Window Functions**.

---

### 📝 Questions — Partie B : Window Functions & Analyses Avancées

**Q2.5 — Rang et percentile salarial**
Ajoutez au DataFrame les colonnes suivantes :
- `rang_departement` : rang du salarié dans son département (1 = le plus payé)
- `rang_global` : rang parmi tous les employés
- `percentile_salaire` : percentile de chaque salaire dans son département via `percent_rank()`

**Q2.6 — Salaire cumulatif**
Dans chaque département, calculez le salaire cumulatif (`running total`) trié par ancienneté croissante.

**Q2.7 — Détection des anomalies salariales**
Identifiez les employés dont le salaire est supérieur à **1,5 fois la médiane** de leur département (outliers potentiels). Utilisez `percentile_approx()`.

**Q2.8 — Budget utilisé vs alloué**
Joignez la masse salariale annuelle par département (`salaire × 12`) avec le `budget_annuel`. Calculez le **taux d'utilisation** du budget en %.

**Q2.9 — Conversion DataFrame → RDD → DataFrame**
Transformez le DataFrame résultant de Q2.2 en RDD, doublez le salaire moyen de chaque département, puis reconvertissez en DataFrame avec le même schéma.

---

## Exercice 3 — Analyse E-Commerce Multi-Sources (Niveau Expert)

### 🎯 Objectif
Résoudre des problèmes complexes : **jointures multiples**, **optimisation Spark** (partitionnement, cache, broadcast), **Spark SQL**, schémas explicites (`StructType`), et traitement de données imparfaites.

---

### 📄 Données : `commandes.csv`, `clients.csv`, `produits.csv`

**`commandes.csv`**
```csv
id_commande,id_client,id_produit,quantite,statut,date_commande,remise_pct
C001,CL05,P003,2,livré,2023-01-10,0.05
C002,CL12,P007,1,annulé,2023-01-15,0.00
C003,CL05,P001,3,livré,2023-02-03,0.10
C004,CL08,P005,5,en_cours,2023-02-18,0.00
C005,CL02,P003,1,livré,2023-03-01,0.05
C006,CL15,P009,2,livré,2023-03-22,0.15
C007,CL08,P002,4,livré,2023-04-11,0.00
C008,CL12,P006,1,retourné,2023-04-30,0.20
C009,CL02,P001,2,livré,2023-05-14,0.10
C010,CL05,P008,3,livré,2023-05-28,0.05
C011,CL20,P004,1,en_cours,2023-06-07,0.00
C012,CL15,P007,2,livré,2023-06-19,0.00
C013,CL08,P010,6,livré,2023-07-02,0.10
C014,CL02,P005,1,annulé,2023-07-25,0.00
C015,CL20,P003,4,livré,2023-08-14,0.05
```

**`clients.csv`**
```csv
id_client,nom_client,ville,segment,date_inscription
CL02,Khadija Mbaye,Dakar,Premium,2020-05-12
CL05,Aliou Diallo,Thiès,Standard,2021-03-08
CL08,Marème Fall,Dakar,Premium,2019-11-20
CL12,Ibou Sarr,Saint-Louis,Standard,2022-01-15
CL15,Coumba Ndiaye,Dakar,VIP,2018-07-04
CL20,Pape Sow,Ziguinchor,Standard,2021-09-30
```

**`produits.csv`**
```csv
id_produit,nom_produit,categorie,prix_base,fournisseur,stock_disponible
P001,Laptop Pro,Electronique,950000,TechAfrique,45
P002,Smartphone X,Electronique,420000,TechAfrique,120
P003,Chaise Ergonomique,Mobilier,185000,MeublesDakar,30
P004,Disque SSD 1TB,Electronique,85000,TechAfrique,200
P005,Bureau Standing,Mobilier,320000,MeublesDakar,15
P006,Casque Audio,Electronique,75000,SoundWave,80
P007,Tablette 10",Electronique,310000,TechAfrique,60
P008,Lampe LED Smart,Maison,45000,LumièreSN,150
P009,Cafetière Pro,Maison,95000,KitchenPlus,40
P010,Webcam HD,Electronique,62000,TechAfrique,90
```

---

### 📝 Questions — Partie A : Schémas Explicites & Qualité des Données

**Q3.1 — Définir les schémas manuellement**
Définissez un `StructType` explicite pour chacun des 3 fichiers CSV et chargez-les **sans** `inferSchema`. Gérez les types correctement (`IntegerType`, `DoubleType`, `DateType`).

**Q3.2 — Audit de la qualité des données**
Calculez pour chaque DataFrame :
- Le nombre de lignes contenant au moins une valeur nulle
- Le pourcentage de commandes par statut
- Les produits dont le stock est en dessous de 20 unités (alerte réapprovisionnement)

**Q3.3 — Nettoyage & enrichissement**
- Remplacez les valeurs nulles de `remise_pct` par `0.0`
- Ajoutez une colonne `annee_mois` au format `"YYYY-MM"` à partir de `date_commande`
- Ajoutez une colonne `jours_depuis_inscription` pour chaque client (différence avec la date du jour)

---

### 📝 Questions — Partie B : Analyses Métier Complexes

**Q3.4 — Chiffre d'affaires net par commande**
Réalisez la jointure entre `commandes`, `produits` et `clients`. Calculez le CA net uniquement pour les commandes `livré` :
> `CA_net = quantite × prix_base × (1 - remise_pct)`

**Q3.5 — Segmentation client RFM (simplifié)**
Calculez pour chaque client :
- **R** (Recency) : nombre de jours depuis sa dernière commande livrée
- **F** (Frequency) : nombre total de commandes livrées
- **M** (Monetary) : CA net total généré

Triez le résultat par **M** décroissant.

**Q3.6 — Analyse de panier (co-achats)**
Trouvez les paires de produits commandés le plus souvent par le **même client**. Affichez les 5 paires les plus fréquentes. Évitez les doublons symétriques `(A,B)` vs `(B,A)`.

**Q3.7 — Tendance mensuelle des ventes**
Calculez le CA net mensuel et le **taux de croissance mois sur mois** (MoM %) pour les commandes livrées. Utilisez `lag()` pour accéder au mois précédent.

---

### 📝 Questions — Partie C : Optimisation & Spark SQL

**Q3.8 — Broadcast Join**
Réécrivez la jointure de Q3.4 en forçant un **broadcast join** sur les DataFrames `produits` et `clients`. Comparez les plans d'exécution avant/après avec `.explain(true)`.

**Q3.9 — Partitionnement & Cache**
- Repartitionnez le DataFrame des commandes par `id_client` (4 partitions)
- Mettez en cache le DataFrame jointé de Q3.4 avec `StorageLevel.MEMORY_AND_DISK`
- Mesurez le temps d'exécution pour recalculer le CA total avant et après le cache

**Q3.10 — Spark SQL complet**
Enregistrez les 3 DataFrames comme vues temporaires. Écrivez une requête SQL pure produisant un rapport final contenant :
- Nom du client, segment, ville
- Nombre de commandes livrées
- CA net total
- Produit préféré (le plus commandé en quantité)
- Catégorie dominante achetée

**Q3.11 — Export des résultats**
Sauvegardez le rapport final en :
- **Parquet** partitionné par colonne `segment`
- **CSV** avec en-tête, en un seul fichier (`coalesce(1)`)

---

## 📊 Tableau Récapitulatif des Compétences

| Exercice | Niveau | Concepts Clés |
|----------|--------|---------------|
| Ex. 1 | ⭐ Débutant | `sc.textFile`, `map`, `filter`, `reduceByKey`, `spark.read.csv`, `withColumn`, `groupBy` |
| Ex. 2 | ⭐⭐ Intermédiaire | Jointure RDD & DataFrame, `Window Functions`, `rank`, `percent_rank`, `lag`, RDD ↔ DataFrame |
| Ex. 3 | ⭐⭐⭐ Expert | `StructType`, qualité de données, `broadcast`, `persist`, Spark SQL, export Parquet/CSV |

---

## 🧠 Ressources

- 📘 [Documentation Spark 3.x](https://spark.apache.org/docs/latest/)
- 📘 [Spark SQL Built-in Functions](https://spark.apache.org/docs/latest/api/sql/)
- 📘 [Scala API Reference](https://spark.apache.org/docs/latest/api/scala/)
- 💡 Exécutez dans **IntelliJ IDEA** (plugin Scala) ou **Databricks Community Edition** (gratuit)

---

> **Bon courage ! 🎓**
> *Maîtrisez ces exercices et vous serez prêts pour des projets Big Data en production.*
