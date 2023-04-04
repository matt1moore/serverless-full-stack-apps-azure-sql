import logging
import os
from typing import Any

import azure.functions as func
import pyodbc
import json
import requests

from datetime import datetime
from bus_data_process import (
    get_bus_data_from_feed,
    get_geo_fences,
    get_monitored_format,
    get_monitored_routes,
    get_route_id,
    trigger_logic_app,
)

logging.info("Entering the environment variable region")

# AZURE_CONN_STRING: str = os.environ["AzureSQLConnectionString"]
# GTFS_REAL_TIME_FEED: str = os.environ["RealTimeFeedUrl"]
# LOGIC_APP_URL: str = os.environ.get("LogicAppUrl", "")

LOGIC_APP_URL = "https://prod-85.eastus.logic.azure.com:443/workflows/e19848d570e040368d1b398891c05ee6/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=oZeNhs8jR_ogAw56rvzRf-Y5KwKdjpIVBsJAS2zeBrA"
GTFS_REAL_TIME_FEED = "https://s3.amazonaws.com/kcm-alerts-realtime-prod/vehiclepositions_enhanced.json"
AZURE_CONN_STRING = "Driver={ODBC Driver 17 for SQL Server};Server=tcp:demo-server929254.database.windows.net;Database=demo-sql-db;Encrypt=yes;UID=cloudadmin;PWD=Demopass1234!;"


def main(GetBusData =  func.TimerRequest):
    """Retrieve the routes we want to monitor from the SQL Database"""
    logging.info("Entering the main function")
    conn = pyodbc.connect(AZURE_CONN_STRING)
    monitored_routes = get_monitored_routes(conn)
    logging.info(f"{len(monitored_routes)} routes to check against")
    entities = get_bus_data_from_feed(GTFS_REAL_TIME_FEED)['entity']
    logging.info(monitored_routes)

    # reformat the bus_feed to match the format of the monitored_routes
    monitored_buses = [get_monitored_format(bus['vehicle']) for bus in entities if get_route_id(bus) in monitored_routes]
    logging.info(f"{len(entities)} buses found. {len(monitored_buses)} buses monitored.")
    print(monitored_buses)
    if not monitored_buses:
        logging.info("No Monitored Bus Routes Detected")
        return

    geo_fences = get_geo_fences(conn, monitored_buses) or list()
    logging.info(geo_fences)

    ## Send notifications. 
    for fence in geo_fences:
        logging.info(f"Vehicle {fence['VehicleId']} , route {fence['RouteId']}, status: {fence['GeoFenceStatus']} at {fence['TimestampUTC']} UTC")
        trigger_logic_app(fence, LOGIC_APP_URL)

if __name__ == '__main__':
    main(GetBusData=True)