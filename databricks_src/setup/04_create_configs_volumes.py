{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "height": "181",
     "inputWidgets": {},
     "nuid": "8a1f151d-7f81-4648-905a-8d0ec90de89c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": "",
     "width": "938"
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE EXTERNAL LOCATION IF NOT EXISTS configs_managed\n",
    "  URL 'abfss://configs@ukpropertyintelligencedl.dfs.core.windows.net/'\n",
    "  WITH (STORAGE CREDENTIAL `uk-property-market-intelligence-platform-sc`)\n",
    "  COMMENT 'The configs container. Governs the watermark ADF reads before any compute exists, and the per-run Bronze failure markers.';"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "e6d0452e-c970-4fc1-95ff-8652fd88d9fe",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# Run after 01_create_schemas.\n",
    "dbutils.fs.ls(\"abfss://configs@ukpropertyintelligencedl.dfs.core.windows.net/\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "39657515-6dc5-47cc-bfb8-e5948917bae0",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "CREATE EXTERNAL VOLUME IF NOT EXISTS uk_property_intel.configs.watermark\n",
    "  LOCATION 'abfss://configs@ukpropertyintelligencedl.dfs.core.windows.net/'\n",
    "  COMMENT 'The configs container. watermark.json is the single JSON array ADF reads through its Lookup; log/ holds one marker per failed Bronze copy and is cleared at the start of every run, so an empty folder means every copy succeeded.';"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "ea79dbbe-4cbf-427e-a417-d3a77a0859a0",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Verify"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "implicitDf": true,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "015ca0de-3cd1-477d-9730-e0084045e958",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "%sql\n",
    "-- SHOW VOLUMES lists names only.\n",
    "SELECT volume_name, volume_type, storage_location\n",
    "FROM uk_property_intel.information_schema.volumes\n",
    "WHERE volume_schema = 'configs'\n",
    "ORDER BY volume_name;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "8034725e-fbc3-4d40-af29-6a808e7cb4cf",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "# End-to-end path resolution: the Silver BoE notebook reads this exact path.\n",
    "dbutils.fs.ls(\"/Volumes/uk_property_intel/configs/watermark/\")"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "5"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "mostRecentlyExecutedCommandWithImplicitDF": {
     "commandId": 7134870319965842,
     "dataframes": [
      "_sqldf"
     ]
    },
    "pythonIndentUnit": 4
   },
   "notebookName": "04_create_configs_volumes",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
