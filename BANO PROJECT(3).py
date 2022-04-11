# Databricks notebook source
# MAGIC %run ./functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import when,col
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Chargement des dataframes.<h4/>

# COMMAND ----------

departments_df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/edoumartial@gmail.com/departments.csv")
regions_df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/edoumartial@gmail.com/regions.csv")
cities_df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/edoumartial@gmail.com/cities.csv")
bano_df = spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/edoumartial@gmail.com/part_00000_tid_345560864559016770_d32fe7cc_4b89_4bc8_a6bc_aa131f5f056b_80_1_c000_snappy-1.parquet")
valeur_fonciere_df = spark.read.format("csv").option("header", "true").option("sep", "|").load("dbfs:/FileStore/shared_uploads/edoumartial@gmail.com/valeursfoncieres_2020.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h4> Renommage des colonnes.<h4/>

# COMMAND ----------

# bano
bano_df=bano_df.withColumnRenamed("_c0","id")\
.withColumnRenamed("_c1","numero_de_voie")\
.withColumnRenamed("_c2","nom_de_voie")\
.withColumnRenamed("_c3","post_code")\
.withColumnRenamed("_c4","cities_name")\
.withColumnRenamed("_c5","source")\
.withColumnRenamed("_c6","latitude")\
.withColumnRenamed("_c7","longitude")

#departments
departments_df=departments_df.select("region_code","code","name").withColumnRenamed("name","department_name").withColumnRenamed("code","department_code")
#departments_df=departments_df.withColumnRenamed("region_code","regions_code_in_dep_table")

#regions
regions_df=regions_df.select("code","name").withColumnRenamed("name","region_name").withColumnRenamed("code","region_code")

#cities
cities_df=cities_df.withColumnRenamed("zip_code","post_code")
cities_df=cities_df.select("department_code","post_code","name").withColumnRenamed("name","cities_name")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h4> Nouvelle colonne en respectant une structure <h4/>

# COMMAND ----------

bano_df=bano_df.withColumn("libellé_source", when(bano_df.source=="OSM","donnée directement issue d'OpenStreetMap")\
                          .when(bano_df.source=="OD","donnée provenant de source open data locales")\
                          .when(bano_df.source=="O+O","donnée opendata enrichie par OSM")\
                          .when(bano_df.source=="CAD","donnée directement issue du cadastre")\
                          .when(bano_df.source=="C+O","donnée du cadastre enrichie par OSM")\
                          .otherwise(bano_df.source))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h4> jointure des dataframes<h4/>
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h7> post_code dans bano devient zip_code dans cities. Department_code dans cities devient code dans depatments.region_code dans departments devient code dans regions <h7/>

# COMMAND ----------

bano_join_df=bano_df.join(cities_df,["post_code","cities_name"],how="left")\
.join(departments_df,["department_code"],"left")\
.join(regions_df,["region_code"],"left")                            

bano_join_df=bano_join_df.filter((bano_join_df["region_name"] == "Île-de-France") | (bano_join_df["region_name"] == "Hauts-de-France"))
                              
                       
#| (bano_join_df["region_name"] == "Bourgogne-Franche-Comté") | (bano_join_df["region_name"] == "Auvergne-Rhône-Alpes") | (bano_join_df["region_name"] == "Bretagne")) 

bano_join_df.count()

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC <h4>Suppréssion des tables .<h4/>

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/bano_db.db/",recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE  IF EXISTS bano_db.valeur_fonciere_table;
# MAGIC DROP TABLE  IF EXISTS bano_db.bano_table;
# MAGIC DROP TABLE  IF EXISTS bano_db.cities_table;
# MAGIC DROP TABLE  IF EXISTS bano_db.regions_table;
# MAGIC DROP TABLE  IF EXISTS bano_db.departments_table;
# MAGIC DROP TABLE  IF EXISTS bano_db.bano_final_table;

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC <h4>Suppréssion de la base de donnée.<h4/>

# COMMAND ----------

#%sql drop IF EXISTS database bano_db

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC <h4>Création de la base de donnée.<h4/>

# COMMAND ----------

# MAGIC %sql 
# MAGIC create database if not exists bano_db

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Création des tables.<h4/>

# COMMAND ----------

writedeltatable(regions_df,"overwrite",'bano_db.regions_table')
writedeltatable(cities_df,"overwrite",'bano_db.cities_table')
writedeltatable(departments_df,"overwrite",'bano_db.departments_table')
writedeltatable(bano_df,"overwrite",'bano_db.bano_table')
writedeltatable(valeur_fonciere_df,"overwrite",'bano_db.valeur_fonciere_table')
writedeltatable(bano_join_df,"overwrite",'bano_db.bano_final_table')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h4> La rue avec le plus grand nombre de caractères<h4/>

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct nom_de_voie
# MAGIC from bano_db.bano_table
# MAGIC where length(nom_de_voie)=(select max(length(nom_de_voie) )from bano_db.bano_table )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h4>le nombre de villes par département avec sql<h4/>

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select departments_table.department_name as ,count(cities_table.cities_name) as number_of_cities
# MAGIC from bano_db.departments_table join bano_db.cities_table on bano_db.departments_table.department_code=bano_db.cities_table.department_code
# MAGIC group by bano_db.departments_table.department_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h4>le nombre de villes par département avec spark<h4/>

# COMMAND ----------

nb_ville_depart_df=departments_df.join(cities_df,  ["department_code"],"left").groupBy("department_name").count()

nb_ville_depart_df=nb_ville_depart_df.withColumnRenamed("department_name", " department name").withColumnRenamed("count", " number of cities")

nb_ville_depart_df.display()
 

# COMMAND ----------

bano_join_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h4>le nombre de numéros dans chaque rue avec sql<h4/>

# COMMAND ----------

windowSpecAgg  = Window.partitionBy("nom_de_voie")
bano_join_df=bano_join_df.withColumn("number_of_road",count(col("nom_de_voie")).over(windowSpecAgg)) #.withColumn("sum", sum(col("salary")).over(windowSpecAgg))

bano_join_df.display()

# COMMAND ----------


