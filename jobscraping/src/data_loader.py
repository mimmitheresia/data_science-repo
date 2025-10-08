import os

import pandas as pd
import spark


def load_local_data(file_path="../data/bronze/", file_name="jobs.csv"):

    if os.path.exists(file_path):
        data = pd.read_csv(file_path + file_name)
    else:
        data = pd.DataFrame()
    return data


def unload_local_data(data, file_path="../data/bronze/", file_name="jobs.csv"):
    data.to_csv(file_path + file_name, index=False)


def load_databricks_data(table_name="workspace.bronze.jobs"):
    df_spark = spark.read.table(table_name)
    df = df_spark.toPandas()
    return df


def unload_databricks_data(df, table_name="workspace.bronze.jobs"):
    df_spark = spark.createDataFrame(df)
    df_spark.write.mode("overwrite").saveAsTable(table_name)
