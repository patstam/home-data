import boto3
import collections
import csv
import json
import os
import sys
import tempfile
import zipfile

def extract_zip(zip_path, extract_path):
    # Create the extraction directory if it doesn't exist
    os.makedirs(extract_path, exist_ok=True)
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extract all contents
            zip_ref.extractall(extract_path)
        return True
    except zipfile.BadZipFile:
        print("Error: File is not a zip file or is corrupted")
        sys.exit()
    except Exception as e:
        print(f"Error extracting zip file: {str(e)}")
        sys.exit()

def parse_pse_csv(csv_path, usage_data):
    with open(csv_path, 'r') as f:
        csv_reader = csv.reader(f)

        # Skip extra headers at top. Example:
        # Name,JOHN DOE
        # Address,"ADDRESS"
        # Account Number,1234
        # Service,Service 1
        #
        # TYPE,DATE,START TIME,END TIME,USAGE (kWh),NOTES
        while True:
            row = next(csv_reader)
            if len(row) > 0 and row[0] == "TYPE":
                break

        for row in csv_reader:
            date = row[1]
            start = date + " " + row[2]
            end = date + " " + row[3]
            usage = float(row[4])

            usage_data[row[0]].append((start, end, usage))

def parse_govee_csv(csv_path, govee_data):
    with open(csv_path, 'r') as f:
        csv_reader = csv.reader(f)

        # Skip headers
        next(csv_reader)

        name = os.path.basename(csv_path).split('_')[0].lower()
        for row in csv_reader:
            timestamp = row[0]
            temp = float(row[1])
            humidity = float(row[2])

            govee_data[name + "_temp"].append((timestamp, temp, humidity))

def get_data_range_dates(data):
    first = data[0][0]
    last = data[-1][0]
    first_date = first.split()[0].replace('-', '')
    last_date = last.split()[0].replace('-', '')
    return (first_date, last_date)

def convert_files(files, outdir):
    pse_data = collections.defaultdict(list)
    govee_data = collections.defaultdict(list)

    # Parse all input files
    for path in files:
        if path.endswith(".zip"):
            # Assume PSE export ZIP
            with tempfile.TemporaryDirectory() as extract_dir:
                extract_zip(path, extract_dir)
                for name in os.listdir(extract_dir):
                    if name.endswith(".csv"):
                        parse_pse_csv(os.path.join(extract_dir, name), pse_data)
        if path.endswith(".csv"):
            # Assume Govee export CSV
            parse_govee_csv(path, govee_data)

        
    PSE_USAGE_TYPES = {
        "Electric usage": ("electricity", "kwh"),
        "Natural gas usage": ("gas", "ccf")
    }

    # Write PSE outputs
    for usage_type in pse_data:
        type_name, units = PSE_USAGE_TYPES[usage_type]

        data = pse_data[usage_type]
        first_date, last_date = get_data_range_dates(data)

        with open(os.path.join(outdir, f"{type_name}_{first_date}_{last_date}.csv"), 'w') as of:
            csv_writer = csv.writer(of)
            csv_writer.writerow(["start", "end", f"usage_{units}"])

            for row in data:
                csv_writer.writerow(row)

    # Write Govee outputs
    for sensor in govee_data:
        data = govee_data[sensor]
        first_date, last_date = get_data_range_dates(data)

        with open(os.path.join(outdir, f"{sensor}_{first_date}_{last_date}.csv"), 'w') as of:
            csv_writer = csv.writer(of)
            csv_writer.writerow(["timestamp", "temp_degf", "relative_humidity"])

            for row in data:
                csv_writer.writerow(row)

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    outbucket = os.environ['OUTPUT_BUCKET']

    s3 = boto3.client('s3')

    with tempfile.TemporaryDirectory() as indir:
        local_path = os.path.join(indir, os.path.basename(key))

        try:
            # Download the file
            s3.download_file(bucket, key, local_path)

            outkeys = []
            with tempfile.TemporaryDirectory() as outdir:
                convert_files([local_path], outdir)
                for file in os.listdir(outdir):
                    if not file.endswith(".csv"):
                        continue

                    s3.upload_file(os.path.join(outdir, file), outbucket, file)
                    outkeys.append(file)

            # Clean up the file we just processed
            s3.delete_object(Bucket=bucket, Key=key)

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'bucket': outbucket,
                    'keys': outkeys,
                }),
            }
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': f"{e}",
                }),
            }

if __name__ == "__main__":
    if sys.argv[1] == 'aws':
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': { 'name': 'patstam-home-data-input' },
                        'object': { 'key': 'Master_export202412260840.csv' }
                    }
                }
            ]
        }
        os.environ['OUTPUT_BUCKET'] = 'patstam-home-data-output'
        print(json.dumps(lambda_handler(event, {})))
    else:
        convert_files(sys.argv[1:], ".")

