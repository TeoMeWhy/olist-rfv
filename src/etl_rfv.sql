-- Databricks notebook source
DROP TABLE IF EXISTS sandbox_apoiadores.tb_olist_rfv;
CREATE TABLE sandbox_apoiadores.tb_olist_rfv AS
WITH tb_rfv AS (

  SELECT 
         '2018-06-01' AS dtRef,
         t1.idSeller,
         MIN( DATEDIFF('2018-06-01', t3.dtApproved) ) AS vlRecencia,
         COUNT(DISTINCT t2.idOrder) as vlFrequencia,
         SUM(t2.vlPrice) AS vlValor

  FROM silver_olist.sellers AS t1

  LEFT JOIN silver_olist.order_items AS t2
  ON t1.idSeller = t2.idSeller

  LEFT JOIN silver_olist.orders AS t3
  ON t2.idOrder = t3.idOrder

  WHERE t3.dtApproved < '2018-06-01'
  AND t3.dtApproved >= date_sub('2018-06-01', 90)

  GROUP BY dtRef, t1.idSeller
  ORDER BY dtRef, t1.idSeller
),

tb_cluster AS (

  SELECT  *,
          CASE WHEN vlRecencia <= 30 THEN 3
               WHEN vlRecencia <= 60 THEN 2
               WHEN vlRecencia <= 90 THEN 1
          END AS descRecencia,

          CASE WHEN vlValor < 5000 AND vlFrequencia < 33 THEN 1
               WHEN vlValor < 5000 AND vlFrequencia < 100 THEN 2
               WHEN vlValor < 5000 AND vlFrequencia >= 100 THEN 3
               WHEN vlValor < 10000 AND vlFrequencia < 33 THEN 4
               WHEN vlValor < 10000 AND vlFrequencia < 100 THEN 5
               WHEN vlValor < 10000 AND vlFrequencia > 100 THEN 6
               WHEN vlValor > 10000 AND vlFrequencia < 33 THEN 7
               WHEN vlValor > 10000 AND vlFrequencia < 100 THEN 8
               WHEN vlValor > 10000 AND vlFrequencia > 100 THEN 9
           END AS vlRF

  FROM tb_rfv

)

SELECT *,
      INT(vlRF || descRecencia) AS vlCluster

FROM tb_cluster

-- COMMAND ----------

SELECT *

FROM sandbox_apoiadores.tb_olist_rfv


-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC 
-- MAGIC df = spark.table("sandbox_apoiadores.tb_olist_rfv").toPandas()
-- MAGIC 
-- MAGIC plt.scatter(df['vlFrequencia'], df['vlValor'], c=df['vlRF'])
-- MAGIC plt.xlabel("Frequencia")
-- MAGIC plt.ylabel("Valor")
-- MAGIC plt.hlines(y=10000, xmax=600, xmin=0)
-- MAGIC plt.hlines(y=5000, xmax=600, xmin=0)
-- MAGIC plt.vlines(x=33, ymax=80000, ymin=0)
-- MAGIC plt.vlines(x=100, ymax=80000, ymin=0)
-- MAGIC plt.grid(True)
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC import pandas as pd
-- MAGIC from sklearn import cluster
-- MAGIC from sklearn import preprocessing
-- MAGIC 
-- MAGIC minMax = preprocessing.MinMaxScaler()
-- MAGIC X = minMax.fit_transform(df[['vlFrequencia','vlValor']])
-- MAGIC 
-- MAGIC clusterModel = cluster.KMeans(n_clusters=5)
-- MAGIC clusterModel.fit(X)
-- MAGIC 
-- MAGIC df['cluster'] = clusterModel.labels_
-- MAGIC 
-- MAGIC clusterDistinct = df["cluster"].unique()
-- MAGIC 
-- MAGIC for c in clusterDistinct:
-- MAGIC     data = df[df['cluster']==c]
-- MAGIC     plt.scatter(data['vlFrequencia'], data['vlValor'])
-- MAGIC 
-- MAGIC plt.xlabel("Frequencia")
-- MAGIC plt.ylabel("Valor")
-- MAGIC plt.grid(True)
-- MAGIC plt.hlines(y=10000, xmax=600, xmin=0)
-- MAGIC plt.hlines(y=5000, xmax=600, xmin=0)
-- MAGIC plt.vlines(x=33, ymax=80000, ymin=0)
-- MAGIC plt.vlines(x=100, ymax=80000, ymin=0)
-- MAGIC 
-- MAGIC plt.legend(clusterDistinct)
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_summary = df.groupby(by=["cluster"]).agg({"vlValor":"sum", "idSeller":"count"}).reset_index()
-- MAGIC df_summary['pctValor'] = df_summary["vlValor"] / df_summary["vlValor"].sum()
-- MAGIC df_summary['pctSeller'] = df_summary["idSeller"] / df_summary["idSeller"].sum()
-- MAGIC 
-- MAGIC df_summary
