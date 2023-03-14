# Databricks notebook source
import os
import sys
import logging
import json
from datetime import datetime
from pathlib import Path

import dbutils as dbutils
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType
from pytz import timezone
from delta import *
from pyspark.sql.types import *
# from delta.tables import *
from deltalake import DeltaTable


import time

# COMMAND ----------

# DBTITLE 1,Get / validate the ENV & YACHT Python API repo path and APP_ACRONYM as input arguments
try:
    dbutils.widgets.text("ENV", "", "")  # NOSONAR
    dbutils.widgets.text("APP_ACRONYM", "", "")
    dbutils.widgets.text("SOURCE_SYS", "", "")
    # dbutils.widgets.text("PROC_DATE","","")

    ENV = dbutils.widgets.getArgument("ENV")  # NOSONAR
    APP_ACRONYM = dbutils.widgets.getArgument("APP_ACRONYM")
    SOURCE_SYS = dbutils.widgets.getArgument("SOURCE_SYS")
    # PROC_DATE = dbutils.widgets.getArgument("PROC_DATE")

except Exception as e:
    ENV = "nprod_dev"
    APP_ACRONYM = "oma"
    SOURCE_SYS = "amac"
    # PROC_DATE = "20230227"

os.environ["YACHT_ENV"] = ENV


from yacht.yacht import Yachty
my_yachty = Yachty()

# COMMAND ----------

def load_mount_point(_containers, _settings):  # NOSONAR
    # Blob Configuration
    mount_path = "/mnt/"
    configs = ({"fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": _settings["Client_ID"],
                "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope=_settings["Secret_Scope"],
                                                                             key=_settings["Secret_Key"]),
                "fs.azure.account.oauth2.client.endpoint": _settings["Client_Endpoint"]})
    _mountpoint = {}

    for id, container in _containers.items():
        if not container["container"].startswith("dbfs:"):
            _mountpoint[id] = mount_path + container["container"]
            if not any(mount.mountPoint == _mountpoint[id] for mount in dbutils.fs.mounts()):
                dbutils.fs.mount(
                    source="abfss://" + container["container"] + "@%s.dfs.core.windows.net/" % container[
                        "Storage_Account"],
                    mount_point=mount_path + container["container"],
                    extra_configs=configs)
            _containers[id]["path"] = mount_path + container["path"]
    return _containers


# COMMAND ----------

# COMMAND ----------

# DBTITLE 1, Setup the log file
def config_logger(_settings):  # NOSONAR
    logger = logging.getLogger("databricks.deltalake")
    level = logging.getLevelName(_settings["Log_Level"]) if "Log_Level" in _settings else logging.getLevelName("INFO")
    logger.setLevel(level)

    log_formater = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(message)s")
    # Configure a console output
    consoleHandler = logging.StreamHandler()

    est = timezone("EST")
    now = datetime.now(est)
    logpath = _settings["Log_Path"] + str(now.year) + "/" + str(now.month) + "/"
    Log_FileName = _settings["Log_FileName"]

    Path(logpath).mkdir(parents=True, exist_ok=True)
    dfn = "%s_%s" % (logpath + Log_FileName, now.strftime("%Y_%m_%d_%H_%M_%S"))

    fileHandler = logging.FileHandler(dfn)
    consoleHandler.setFormatter(log_formater)
    fileHandler.setFormatter(log_formater)

    if not len(logger.handlers):
        logger.addHandler(fileHandler)
        logger.addHandler(consoleHandler)

    return logger


# COMMAND ----------

# COMMAND ----------

# DBTITLE 1, Create Delta Control Batch entry
def create_batch_entry(_settings, logger):
    batchname = _settings["AuditBatchName"]
    batchdesc = _settings["AuditBatchDesc"]
    user = _settings["Secret_Key"]

    #     est = timezone("EST")
    #     now = datetime.now(est)
    create_batch_r = my_yachty.create_batch_entry(batchname, batchdesc, user, "Ingestion")
    error_code = create_batch_r[0]
    error_desc = create_batch_r[1]
    curr_batch_id = create_batch_r[2]
    delta_start_dt = create_batch_r[4]
    delta_end_dt = create_batch_r[5]
    #     delta_end_dt = now.strftime("%Y-%m-%d %H:%M:%S")
    batch_start_ts = create_batch_r[7]

    if error_code:
        logger.error("Error creating batch entry - %s - %s " % (str(error_code), str(error_desc)))
        raise Exception("Error creating batch entry - %s - %s " % (str(error_code), str(error_desc)))

    logger.info("Current Batch Id - %s" % (str(curr_batch_id)))
    logger.info("Batch Start Ts - %s" % (str(batch_start_ts)))
    logger.info("Delta start dt - %s" % (str(delta_start_dt)))
    logger.info("Delta end dt - %s" % (str(delta_end_dt)))

    return curr_batch_id, batch_start_ts, delta_start_dt, delta_end_dt


# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,Upsert Delta Control Task Entry
def upsert_task_status(_settings, table_metadata, logger):
    source_system_cd = _settings["SourceSystemCD"]
    source_database = _settings["SourceDatabase"]
    source_table_key_column = _settings["SourceTableKeyColumn"]
    target_system_cd = _settings["TargetSystemCD"]
    target_database = _settings["TargetDatabase"]
    user = _settings["Secret_Key"]
    task_name = _settings["TaskName"]
    load_type = _settings["LoadType"]

    source_table = table_metadata["Source"]
    target_table = table_metadata["Target"]
    status = table_metadata["Status"]
    curr_batch_id = table_metadata["BatchId"]
    delta_start_dt = table_metadata["DeltaStart"]
    delta_end_dt = table_metadata["DeltaEnd"]
    source_count = table_metadata["SourceCnt"]
    target_count = table_metadata["TargetCnt"]

    payload = my_yachty.construct_payload(source_system_cd
                                          , source_database
                                          , source_table
                                          , source_table_key_column
                                          , target_system_cd
                                          , target_database
                                          , target_table
                                          , "PYSPARK"
                                          , load_type
                                          , status
                                          , source_count
                                          , target_count
                                          , "0"
                                          , task_name)

    create_task_r = my_yachty.upsert_task_entry(curr_batch_id, delta_start_dt, delta_end_dt, user, payload)
    error_code = create_task_r[0]
    error_desc = create_task_r[1]
    etl_control_task_id = create_task_r[2]
    task_status = create_task_r[3]

    if error_code:
        logger.error("Error creating task entry - %s - %s" % (str(error_code), str(error_desc)))
        raise Exception("Error creating task entry - %s - %s" % (str(error_code), str(error_desc)))
    elif task_status != status:
        logger.error("Error creating task entry - status mismatch - %s - %s" % (str(status), str(task_status)))
        raise Exception("Error creating task entry - status mismatch - %s - %s" % (str(status), str(task_status)))

    return etl_control_task_id, task_status


def data_cleanup(_settings, src_containers, tgt_containers, curr_batch_id, logger):

    for id, container in src_containers.items():
        for table in container["Tablelist"]:
            tgt_mount_path = tgt_containers[id]["path"] + table["SourceTableName"]
            delta_table = DeltaTable.forPath(spark, tgt_mount_path)
            delta_table.delete(f.col("curr_batch_id") == f.lit(curr_batch_id))

# COMMAND ----------

# COMMAND ----------

def vacuum_delta(_settings, src_containers, tgt_containers, logger):
    # read Vacuum_hrs from settings
    # run vacuum
    vacuum_hrs = _settings["Vacuum_hrs"]

    for id, container in src_containers.items():
        for table in container["Tablelist"]:
            tgt_mount_path = tgt_containers[id]["path"] + table["SourceTableName"]
            delta_table = DeltaTable.forPath(spark, tgt_mount_path)

            delta_table.vacuum(vacuum_hrs)


# COMMAND ----------

# COMMAND ----------

# DBTITLE 1, Close Delta Control Batch Status
def close_batch_status(_settings, curr_batch_id, src_containers, tgt_containers, logger):
    user = _settings["Secret_Key"]
    close_batch_r = my_yachty.close_batch_entry(curr_batch_id, user)

    error_code = close_batch_r[0]
    error_desc = close_batch_r[1]
    etl_control_batch_id = close_batch_r[2]
    previous_status = close_batch_r[3]
    batch_status = close_batch_r[4]

    if error_code:
        logger.error("Error closing batch status - %s - %s" % (str(error_code), str(error_desc)))
        data_cleanup(_settings, src_containers, tgt_containers, curr_batch_id, logger)
        raise Exception("Error closing batch status - %s - %s" % (str(error_code), str(error_desc)))
    elif batch_status != "SUCCESS":
        logger.error("Failure in current batch, check logs for more details")
        data_cleanup(_settings, src_containers, tgt_containers, curr_batch_id, logger)
        raise Exception("Failure in current batch, check logs for more details")
    else:
        vacuum_delta(_settings, src_containers, tgt_containers, logger)
    return etl_control_batch_id, previous_status, batch_status


# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,Read settings file
# GET SETTINGS
def get_settings():
    return json.load(
        open("/dbfs/configs/%s/delta_settings_%s_%s.json" % (APP_ACRONYM.lower(), SOURCE_SYS.lower(), ENV.lower())))


# COMMAND ----------

# DBTITLE 1,Main file - Copy "csv" files from raw containers as "delta" files in delta containers
from yacht.yacht import Yachty

my_yachty = Yachty()
try:
    spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
    # settings = json.loads(settings)
    settings = get_settings()

    logcontainer = settings["LogContainer"]
    logcontainer = load_mount_point(logcontainer, settings)

    logger = config_logger(settings)
    logger.info("Loading delta files for CSV raw files START")

    src_containers = settings["StagingContainer"]
    logger.info("Mounting the Source CSV RAW Containers")
    src_containers = load_mount_point(src_containers, settings)

    logger.info("Mounting the Target DELTA Containers")
    tgt_containers = settings["TargetContainer"]
    tgt_containers = load_mount_point(tgt_containers, settings)

    # Create Batch Entry
    curr_batch_id, batch_start_ts, delta_start_dt, delta_end_dt = create_batch_entry(settings, logger)
    # print("delta_start_dt": delta_start_dt)
    dt_partition = datetime.strptime(delta_end_dt, "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d%H%M%S")

    for id, container in src_containers.items():
        for table in container["Tablelist"]:

            src_mount_path = container["path"] + "/%s" % table["SourceTableName"]
            tgt_mount_path = tgt_containers[id]["path"] + table["TargetTableName"]
            format = table["Format"]

            # Create Task Entry
            table_metadata = {"Source": src_mount_path, "Target": tgt_mount_path, "Status": "STARTED",
                              "BatchId": curr_batch_id, "DeltaStart": delta_start_dt, "DeltaEnd": delta_end_dt,
                              "SourceCnt": "0", "TargetCnt": "0"}
            upsert_task_status(settings, table_metadata, logger)
            try:
                fdpaths = ["/dbfs" + src_mount_path + "/" + fd for fd in os.listdir("/dbfs" + src_mount_path)]
                for fdpath in fdpaths:
                    stat_info = os.stat(fdpath)

                    create_date = datetime.fromtimestamp(stat_info.st_ctime)
                    modified_date = datetime.fromtimestamp(stat_info.st_mtime)

                    # Create scr_dataframe by reading parquet files generated between delta_start_dt and delta_end_dt
                    logger.info("Reading delta file for %s table into a dataframe for date range %s to %s " % (
                    table["SourceTableName"], delta_start_dt, delta_end_dt))
                    source_count = 0

                    emptyRDD = spark.sparkContext.emptyRDD()
                    emptySchema = StructType([])
                    scr_dataframe = spark.createDataFrame(emptyRDD, emptySchema)

                    filepaths = ["/dbfs" + src_mount_path + "/" + fdpath.split("/")[-1] + "/" + fp for fp in
                                 os.listdir("/dbfs" + src_mount_path + "/" + fdpath.split("/")[-1] + "/")]
                    for filepath in filepaths:
                        fmodified_date = datetime.fromtimestamp(os.stat(filepath).st_mtime)
                        if (fmodified_date >= datetime.strptime(delta_start_dt, "%Y-%m-%d %H:%M:%S")) & (
                                fmodified_date < datetime.strptime(delta_end_dt, "%Y-%m-%d %H:%M:%S")):
                            # read source files into dataframes
                            print("FILE MODIFIED DATE :" + str(fmodified_date))
                            print("****IN DATE RANGE****")
                            file_path = src_mount_path + "/" + fdpath.split("/")[-1]
                            if (format.lower() == "csv"):
                                scr_dataframe_temp = spark.read.format(format).option("header", "true").load(file_path)
                            else:
                                scr_dataframe_temp = spark.read.format(format).load(file_path)
                            scr_dataframe = scr_dataframe.unionByName(scr_dataframe_temp, allowMissingColumns=True)

                source_count = scr_dataframe.count()
                # Add delta file at the target with additional partition column "delta_end_dt", only when there's data in the source corresponding parquet file
                if table["LoadType"] == "FULL_REFRESH":
                    tgt_dataframe = scr_dataframe.withColumn("delta_end_dt", f.lit(dt_partition))
                    tgt_dataframe.write.format("delta").mode("overwrite").option("mergeSchema", True).save(
                        tgt_mount_path)
                    logger.info(
                        "%s rows of Delta records written successfully for %s table in the target container %s" % (
                        source_count, table["SourceTableName"], tgt_mount_path))
                else:
                    tgt_dataframe = scr_dataframe.withColumn("delta_end_dt", f.lit(dt_partition))
                    tgt_dataframe.write.format("delta").partitionBy("delta_end_dt").mode("append").option("mergeSchema",
                                                                                                          True).save(
                        tgt_mount_path)
                    logger.info(
                        "%s rows of Delta records written successfully for %s table in the target container %s on delta_end_dt = %s" % (
                        source_count, table["SourceTableName"], tgt_mount_path, delta_end_dt))

                # Update Task Entry as SUCCESS
                table_metadata = {"Source": src_mount_path, "Target": tgt_mount_path, "Status": "SUCCESS",
                                  "BatchId": curr_batch_id, "DeltaStart": delta_start_dt, "DeltaEnd": delta_end_dt,
                                  "SourceCnt": str(source_count), "TargetCnt": str(source_count)}
                upsert_task_status(settings, table_metadata, logger)
                logger.info("Task status updated as SUCCESS for %s" % table["SourceTableName"])
            except Exception as ex:
                # Update Task Entry as FAILED
                table_metadata = {"Source": src_mount_path, "Target": tgt_mount_path, "Status": "FAILED",
                                  "BatchId": curr_batch_id, "DeltaStart": delta_start_dt, "DeltaEnd": delta_end_dt,
                                  "SourceCnt": str(source_count), "TargetCnt": "0"}
                upsert_task_status(settings, table_metadata, logger)
                logger.info("Task status updated as FAILED for %s" % table["SourceTableName"])
                logger.error("%s" % (ex))

    # Close Batch Entry
    close_batch_status(settings, curr_batch_id, src_containers, tgt_containers, logger)
except Exception as e:
    logger.error("%s" % (e))
    print(e)
    close_batch_status(settings, curr_batch_id, src_containers, tgt_containers, logger)
    sys.exit(1)