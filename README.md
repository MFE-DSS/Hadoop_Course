# Cours de formation Apache Hadoop

Ce cours est destiné à aider les développeurs, les chefs de projets, les data-scientists, et les architectes à comprendre et à maîtriser l'écosystème Hadoop.

## Objectif de la formation : 
A l’issue de la formation, le stagiaire sera capable de développer des applications compatibles avec la plateforme Hadoop d'Apache pour traiter des
données Big Data.

### Objectifs pédagogiques:
Comprendre l’écosystème
Hadoop Cloudera/Hortonworks
Présenter les principes du
Framework Hadoop
Mettre en oeuvre des tâches
Hadoop pour extraire des éléments pertinents d'ensembles de données volumineux et variés
Développer des algorithmes parallèles efficaces avec MapReduce
Charger des données non structurées des systèmes HDFS et Hbase
Comprendre et mettre en œuvre un traitement temps réel
Public concerné : Développeurs, Chefs de projets,
Data-scientists, Architectes...

## Programme de formation:
### Introduction 
▪ Les fonctionnalités du framework Hadoop<br>
▪ Le projet et les modules : Hadoop Common, HDFS, YARN, MapReduce<br>
▪ Utilisation de yarn pour piloter les jobs mapreduce<br>
### MapReduce
▪ Principe et objectifs du modèle de programmation MapReduce<br>
▪ Fonctions map() et reduce() Couples (clés, valeurs)<br>
▪ Implémentation par le framework Hadoop<br>
▪ Etude de la collection d'exemples<br>
### Programmation
▪ Configuration des jobs, notion de configuration<br>
▪ Les interfaces principales : mapper, reducer,<br>
▪ La chaîne de production : entrées, input splits, mapper, combiner, shuffle/sort, reducer, sortie<br>
▪ Partitioner, outputcollector, codecs, compresseurs<br>
▪ Format des entrées et sorties d'un job MapReduce : InputFormat et OutputFormat<br>
### Streaming 
▪ Définition du streaming map/reduce<br>
▪ Création d'un job map/reduce en Python<br>
▪ Répartition sur la ferme<br>
▪ Avantage et inconvénients<br>
▪ Liaisons avec des systèmes externes<br>
▪ Introduction au pont HadoopR<br>
### Pig
▪ Pattern et best practices Map/reduce<br>
▪ Introduction à Pig<br>
▪ Caractéristiques du langage : latin<br>
▪ Les fonctions de bases<br>
▪ Ajouts de fonctions personnalisées<br>
▪ Les UDF<br>
▪ Mise en œuvre<br>
### Hive
  ▪ Simplification du requêtage<br>
  ▪ Syntaxe de base  <br>
  Sécurité en environnement Hadoop  : Mécanisme de gestion de l'authentification  <br>

## Jour 1

### Module 1 : Introduction à Hadoop

#### Objectifs pédagogiques :
- Comprendre le fonctionnement du Framework Hadoop
- Apprendre les principes des projets et modules Hadoop Common, HDFS, YARN, MapReduce
- Savoir utiliser YARN pour gérer les jobs MapReduce

#### Cours :

Hadoop est un framework open source qui facilite le stockage et le traitement de grands ensembles de données à travers des grappes de serveurs. Il est composé de quatre modules principaux :

Hadoop Common : Il s'agit des bibliothèques et des utilitaires communs requis par d'autres modules Hadoop.
Hadoop Distributed File System (HDFS) : Il s'agit d'un système de fichiers qui stocke les données sur les nœuds de la grappe, offrant une très haute bande passante de données.
Hadoop YARN : Il s'agit d'un gestionnaire de ressources et un système de planification de tâches.
Hadoop MapReduce : Il s'agit d'un algorithme de traitement en parallèle pour les grandes quantités de données.

#### Exercices :

**Exercice 1 : QCM**

Quelle est la fonction principale de Hadoop ?

a. Traitement des données en temps réel<br>
b. Traitement des transactions en ligne<br>
c. Stockage et analyse de grandes quantités de données<br>
d. Stockage de données relationnelles<br>

Réponse correcte : c. Stockage et analyse de grandes quantités de données

**Exercice 2 : Travaux Pratiques**
Créer un cluster Hadoop à partir de zéro en utilisant Hadoop Common, HDFS, YARN et MapReduce.

##### Installation Hortonworks

###### Problèmes de téléchargement de Hortonworks Data Platform

Si vous rencontrez des problèmes pour télécharger Hortonworks Data Platform...
Dans la prochaine leçon, nous allons installer un framework complet de "big data" - Hortonworks Data Platform - sur votre propre ordinateur de bureau !

Cependant, le processus de téléchargement sur le site web de Cloudera semble parfois fonctionner de manière peu fiable. Si vous constatez que vous ne pouvez pas télécharger l'image sandbox HDP 2.6.5 pour Virtualbox comme indiqué, essayez d'aller directement à cette page :

[Cloudera Hortonworks Sandbox](https://www.cloudera.com/downloads/hortonworks-sandbox/hdp.html)

Choisissez "Virtualbox" pour votre type d'installation, fournissez les informations qu'ils demandent, puis sélectionnez HDP 2.6.5 dans les "versions antérieures".

Si cela échoue également, vous pouvez essayer un lien direct vers leur téléchargement HDP 2.6.5 :

[HDP 2.6.5 Direct Download](https://archive.cloudera.com/hwx-sandbox/hdp/hdp-2.6.5/HDP_2.6.5_virtualbox_180626.ova)

Ou, si vous avez besoin de la version 2.5 à la place :

[HDP 2.5 Direct Download](https://archive.cloudera.com/hwx-sandbox/hdp/hdp-2.5.0/HDP_2.5_virtualbox.ova)

N'oubliez pas, vous avez besoin d'au moins 8 Go de RAM libre sur votre système pour exécuter HDP. Si vous n'avez pas autant de mémoire, ne vous inquiétez pas - vous pouvez toujours apprendre en regardant simplement les vidéos.

###### Avertissement pour les utilisateurs d'Apple M1

Si vous utilisez l'un des nouveaux ordinateurs Apple basés sur la puce M1, sachez que VirtualBox n'est pas compatible avec cette plateforme. Vous avez besoin de VirtualBox pour exécuter le bac à sable HDP utilisé dans les activités pratiques de ce cours.

Autant que nous puissions le dire, Oracle n'a pas l'intention de porter VirtualBox sur la plateforme M1 ; il nécessite une puce x86. Fondamentalement, HDP nécessite une puce x86 pour fonctionner - la virtualisation ne peut pas émuler une puce entièrement différente.

Si le seul système auquel vous avez accès est un système basé sur M1, nous vous recommandons de regarder simplement les vidéos des activités pratiques au lieu d'essayer de les suivre vous-même.

###### Emplacement de téléchargement alternatif pour MovieLens

Si vous rencontrez des problèmes pour télécharger l'ensemble de données ml-100k à partir de grouplens.org, utilisez cet emplacement de téléchargement à la place :

[Download ml-100k.zip](http://media.sundog-soft.com/es/ml-100k.zip)

###### Connect to Hadoop using CMD/Putty SSH

- SSH credentials : maria_dev@127.0.0.1
- port : 2222
- Password in CMD : maria_dev
  
###### Putting DATA using CMD : 
- hadoop fs -mkdir movies_data_cmd
- Get the data : git clone https://github.com/jamesyangwang/elasticsearch.git
- cd elasticsearch
- ls
- mv ml-latest-small ../movies_all
- cd ..
- ls
- cd movies_all
- hadoop fs -copyFromLocal movies.csv movies_data_cmd/movies.csv
- hadoop fs -copyFromLocal ratings.csv movies_data_cmd/ratings.csv
- hadoop fs -ls

### Module 2 : MapReduce

#### Objectifs pédagogiques :
- Comprendre le principe et les objectifs du modèle de programmation MapReduce
- Connaître les fonctions map() et reduce() et les couples (clés, valeurs)
- Savoir implémenter MapReduce avec le framework Hadoop

#### Cours :
MapReduce est un modèle de programmation pour le traitement en parallèle. Il divise une tâche en deux parties : une phase de "Map" où il effectue des opérations sur un ensemble de données, et une phase de "Reduce" où il agrège les résultats. En utilisant MapReduce, nous pouvons traiter de grandes quantités de données de manière distribuée sur un cluster de machines.

Dans la phase Map, les données en entrée sont divisées en paires clé-valeur. Ensuite, chaque paire clé-valeur est traitée et une sortie intermédiaire est générée. Dans la phase Reduce, les paires clé-valeur intermédiaires sont regroupées par clé et traitées pour générer la sortie finale.

## MRjob Installation for HDP 2.6.5 : 
#### Notes on MRJob Installation

This repository contains notes and instructions for installing the "mrjob" package for MapReduce on the HDP Sandbox.

##### Installation on HDP 2.65

To install mrjob on HDP 2.65, follow the steps below:

1. Save the HDP-SOLR-2.6-100 configuration:
  ```ruby
  yum-config-manager --save --setopt=HDP-SOLR-2.6-100.skip_if_unavailable=true
  ```

2. Install the required packages:
  ```ruby
  yum install https://repo.ius.io/ius-release-el7.rpm https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
  yum install python-pip
  ```

3. Install the "pathlib" package using pip:
  ```ruby
  pip install pathlib
  ```

4. Install mrjob version 0.7.4:
  ```ruby
  pip install mrjob==0.7.4
  ```

5. Install PyYAML version 5.4.1:
  ```ruby
  pip install PyYAML==5.4.1
  ```


6. Install the nano text editor:
  ```ruby
  yum install nano
  ```

7. Download the required files:
  ```ruby
  wget http://media.sundog-soft.com/hadoop/RatingsBreakdown.py
  wget http://media.sundog-soft.com/hadoop/ml-100k/u.data
  ```


##### Installation on HDP 2.5

If you encounter an error when running `pip install google-api-python-client==1.6.4` during the installation on HDP 2.5, follow these steps:

1. Update your sandbox to Python 2.7:
   
 ```ruby
  yum install scl-utils
  yum install centos-release-scl
  yum install python27
  scl enable python27 bash
 ```

2. After updating to Python 2.7, install the required package:

 ```ruby
  pip install google-api-python-client==1.6.4
 ```

## MRjob DEV : 

1. Get the data :
   ```ruby
   wget http://media.sundog-soft.com/hadoop/ml-100k/u.data
   wget http://media.sundog-soft.com/hadoop/ml-100k/u.item
   ```

2. Contenu de RatingsBreakdown.py :

 ```python
      from mrjob.job import MRJob
      from mrjob.step import MRStep
      
      class RatingsBreakdown(MRJob):
          def steps(self):
              return [
                  MRStep(mapper=self.mapper_get_ratings,
                         reducer=self.reducer_count_ratings)
              ]
      
          def mapper_get_ratings(self, _, line):
              (userID, movieID, rating, timestamp) = line.split('\t')
              yield rating, 1
      
          def reducer_count_ratings(self, key, values):
              yield key, sum(values)
      
      if __name__ == '__main__':
          RatingsBreakdown.run()
 ```

3. Contenu de MostPopularMovie.py :

```python
  from mrjob.job import MRJob
  from mrjob.step import MRStep
  
  class MostPopularMovie(MRJob):
      def steps(self):
          return [
              MRStep(mapper=self.mapper_get_ratings,
                     reducer=self.reducer_count_ratings),
              MRStep(reducer=self.reducer_sorted_output)
          ]
  
      def mapper_get_ratings(self, _, line):
          (userID, movieID, rating, timestamp) = line.split('\t')
          yield movieID, 1
  
      def reducer_count_ratings(self, movie, countList):
          yield None, (sum(countList), movie)
  
      def reducer_sorted_output(self, _, movies_count):
          for count, movie in sorted(movies_count, reverse=True):
              yield movie, count
  
  if __name__ == '__main__':
      MostPopularMovie.run()
```
4. Contenu de MostPopularMovie.py avec affichage des titres des films :

```python
from mrjob.job import MRJob
from mrjob.step import MRStep
import codecs

class MostPopularMovie(MRJob):
    def configure_args(self):
        super(MostPopularMovie, self).configure_args()
        self.add_file_arg('--movies', help='Path to the movies file')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sorted_output)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings(self, movie, countList):
        yield None, (sum(countList), movie)

    def reducer_sorted_output(self, _, movies_count):
        movie_names = self.load_movie_names()
        for count, movie in sorted(movies_count, reverse=True):
            if movie in movie_names:
                yield movie_names[movie], count

    def load_movie_names(self):
        movie_names = {}
        with codecs.open(self.options.movies, 'r', encoding='latin-1') as f:
            for line in f:
                fields = line.strip().split('|')
                if len(fields) >= 2:
                    movie_id = fields[0]
                    movie_name = fields[1]
                    movie_names[movie_id] = movie_name
        return movie_names

if __name__ == '__main__':
    MostPopularMovie.run()
```

5. Run MRJob in Local :
   ```ruby
    python MostPopularMovieSort.py u.data
   ```
6. Run MRJob in Cluster :
    ```ruby
     sudo python RatingsBreakdown.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar "hdfs:///usr/maria_dev/movies_data_cmd/u.data"
     ```

#### Exercices :

**Exercice 3 : QCM**
Quelle est la fonction de la phase Map dans MapReduce ?

a. Elle trie les données<br>
b. Elle réduit les données à un ensemble plus petit<br>
c. Elle divise les données en paires clé-valeur<br>
d. Elle fusionne les données en un seul ensemble<br>

Réponse correcte : c. Elle divise les données en paires clé-valeur

**Exercice 4 : Travaux Pratiques**
Développer un algorithme MapReduce pour compter le nombre de mots dans un texte. Utiliser le livre "The Project Gutenberg EBook of Alice's Adventures in Wonderland" comme ensemble de données.

## Jour 2

### Module 3 : Pig et Hive

#### Objectifs pédagogiques :
- Connaître les bases de Pig et Hive
- Savoir écrire des requêtes Pig et Hive

#### Cours :
[Contenu du cours]

#### Exercices :

**Exercice 7 : QCM**
Quel outil permet d'écrire des requêtes SQL pour extraire des données de Hadoop?<br>
a. Pig<br>
b. Hive<br>
c. HBase<br>
d. Spark<br>

Réponse correcte : b. Hive

**Exercice 8 : Travaux Pratiques**
Écrire un script Pig pour compter le nombre de mots dans un texte.

**Exercice 9 : Travaux Pratiques**
Écrire une requête Hive pour obtenir le nombre total de tweets contenant le mot "Hadoop".
