import pickle
import os

def main():
    creds_file = "influx-creds.pickle"

    cur_influxdb_url = ""
    cur_influxdb_org = ""
    cur_influxdb_token = ""

    # check if existing exists
    if os.path.isfile(creds_file):
        with open(creds_file, "rb") as f:
            existing_dict = pickle.load(f)

            cur_influxdb_url = existing_dict['idb_url']
            cur_influxdb_org = existing_dict['idb_org']
            cur_influxdb_token = existing_dict['idb_token']

    influxdb_url = ""
    while not influxdb_url:
        influxdb_url = input(f"Enter InfluxDB URL [{cur_influxdb_url}]: ")
        if not influxdb_url:
            if not cur_influxdb_url:
                print("InfluxDB URL must be specified")
            else:
                influxdb_url = cur_influxdb_url

    influxdb_org = ""
    while not influxdb_org:
        influxdb_org = input(f"Enter InfluxDB Org [{cur_influxdb_org}]: ")
        if not influxdb_org:
            if not cur_influxdb_org:
                print("InfluxDB Org must be specified")
            else:
                influxdb_org = cur_influxdb_org

    influxdb_token = ""
    while not influxdb_token:
        influxdb_token = input(f"Enter InfluxDB Token [{cur_influxdb_token}]: ")
        if not influxdb_token:
            if not cur_influxdb_token:
                print("InfluxDB Token must be specified")
            else:
                influxdb_token = cur_influxdb_token

    output_dict = {
        "idb_url": influxdb_url,
        "idb_org": influxdb_org,
        "idb_token": influxdb_token
    }

    with open(creds_file, "wb") as f:
        pickle.dump(output_dict, f, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"Saved file {creds_file} successfully. Re-run this script anytime you need to update it")

if __name__ == "__main__":
    main()
