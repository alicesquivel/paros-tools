import argparse
import influxdb_client
import pickle
import os
import pytz
import datetime
from scipy.io import savemat
import pandas as pd

def cliArguments():
    """
    Parse CLI arguments using argparse
    """
    parser = argparse.ArgumentParser()

    parser.description = "Script to download data from influxdb from PAROS sensors. Can output python, matlab, and csv files"

    parser.add_argument("start_time", help="Start time in ISO format: YYYY-MM-DDTHH:MM:SS", type=str)
    parser.add_argument("end_time", help="End time in ISO format: YYYY-MM-DDTHH:MM:SS", type=str)
    parser.add_argument("output_file", help="Output data file with .csv, .mat, or .pickle extension", type=str)
    parser.add_argument("--box-id", help="Box ID to pull data from. For multiple boxes comma separate the list", type=str)
    parser.add_argument("--sensor-id", help="Sensor ID to pull data from. For multiple sensors comma separate the list", type=str)
    parser.add_argument("--bucket", help="Set a custom bucket to use", type=str, default="parosbox")
    parser.add_argument("--input-zone", help="Set time zone of input timestamps", type=str, default="Etc/UTC")
    parser.add_argument("--output-zone", help="Set time zone of output timestamps", type=str, default="Etc/UTC")
    parser.add_argument("--creds", help="Custom path to credentials pickle file", type=str, default="influx-creds.pickle")

    args = parser.parse_args()
    return args

def loadInfluxClient(pickle_path):
    """
    Creates influxdb client from pickle credentials file
    """
    if os.path.isfile(pickle_path):
        with open(pickle_path, "rb") as f:
            influx_dict = pickle.load(f)

            client = influxdb_client.InfluxDBClient(
                url = influx_dict["idb_url"],
                token = influx_dict["idb_token"],
                org = influx_dict["idb_org"],
                timeout = 100000_000
            )

            query_api = client.query_api()

            return client,query_api
    else:
        print("InfluxDB credentials file not found. Run influxdb-setup.py to generate one.")
        exit(1)

def processInfluxDF(df, output_tz):
    cur_box = df["_measurement"].iloc[0]
    cur_id = df["id"].iloc[0]
    id_str = f"{cur_box}_{cur_id}"

    out_df = df.drop(columns=["result", "table", "_measurement", "id"])

    if "baro_time" in out_df:
        out_df.drop(columns=["baro_time"], inplace=True)

    if "err" in out_df:
        err_list = pd.Series(list("err")).unique()
        if len(err_list) > 1:
            print("WARNING: Some anemometer values were recorded with an error code")

        out_df.drop(columns=["err"], inplace=True)

    out_df.rename(columns={'_time': 'time'}, inplace=True)
    out_df["time"] = out_df["time"].dt.tz_convert(output_tz)
    out_df["time"] = out_df["time"].dt.tz_localize(None)
    # create a unix time column
    out_df["time"] = (out_df["time"] - pd.Timestamp("1970-01-01")) / pd.Timedelta('1s')

    return id_str,out_df

def createFluxFilters(col_name, in_str):
    if in_str is None:
        return ""

    if ',' in in_str:
        # user provided a list
        in_list = in_str.split(',')
    else:
        in_list = [in_str]

    filter_list = []
    for i in in_list:
        filter_list.append(f'r["{col_name}"] == "{i}"')

    filter_list_str = " or ".join(filter_list)

    if len(filter_list) > 0:
        flux_line = f'|> filter(fn: (r) => {filter_list_str})'
    else:
        flux_line = ""

    return flux_line

def main():
    """
    Main runner
    """

    # Process Args
    args = cliArguments()

    # Create InfluxDB Client
    idb_client,idb_query_api = loadInfluxClient(args.creds)

    # process timezones
    input_tz = pytz.timezone(args.input_zone)
    output_tz = pytz.timezone(args.output_zone)

    start_time = datetime.datetime.fromisoformat(args.start_time)
    start_time = input_tz.localize(start_time)
    start_time = start_time.astimezone(datetime.timezone.utc)
    start_time = start_time.replace(tzinfo=None)
    end_time = datetime.datetime.fromisoformat(args.end_time)
    end_time = input_tz.localize(end_time)
    end_time = end_time.astimezone(datetime.timezone.utc)
    end_time = end_time.replace(tzinfo=None)

    # process filters
    box_filters = createFluxFilters("_measurement", args.box_id)
    sensor_filters = createFluxFilters("id", args.sensor_id)

    # from idb query
    idb_query = f'''from(bucket: "{args.bucket}")
        |> range(start: {start_time.isoformat()}Z, stop: {end_time.isoformat()}Z)\n'''
    
    if box_filters:
        idb_query += f'\t{box_filters}\n'

    if sensor_filters:
        idb_query += f'\t{sensor_filters}\n'

    idb_query += '''\t|> drop(columns: ["_start", "_stop"])
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'''

    print("\nRunning InfluxDB Query...\n")
    print(f"{idb_query}\n")

    idb_result = idb_query_api.query_data_frame(query=idb_query)
    
    dfs_list = []
    if isinstance(idb_result, list):
        for r in idb_result:
            dfs_list += [r for _, r in r.groupby('table')]
    else:
        dfs_list.append(idb_result)

    # process dataframes
    out_df = {}
    for df in dfs_list:
        cur_idstr,cur_df = processInfluxDF(df, args.output_zone)

        out_df[cur_idstr] = cur_df

    print("\nPreviewing Dataframes...\n")
    print(out_df)

    # create output
    output_name, output_type = os.path.splitext(args.output_file)
    if output_type == ".csv":
        # csv files split by df
        for cur_name,cur_df in out_df.items():
            output_filename = f"{output_name}_{cur_name}.csv"
            cur_df.to_csv(output_filename, header=False, index=False)
            print(f"Saved file {output_filename} successfully.")
    elif output_type == ".mat":
        mat_df = {key: df.values for key, df in out_df.items()}

        output_filename = f"{output_name}.mat"
        savemat(output_filename, mat_df)
        print(f"Saved file {output_filename} successfully. Load in matlab.")
    elif output_type == ".pickle":
        output_filename = f"{output_name}.pickle"
        with open(output_filename, "wb") as f:
            pickle.dump(out_df, f, protocol=pickle.HIGHEST_PROTOCOL)
            print(f"\nSaved file {output_filename} successfully. Serialized as a dictionary of dataframes.")

if __name__ == "__main__":
    main()
