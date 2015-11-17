import json
import time
import datetime
import gzip
import queue
import urllib.request, urllib.error, urllib.parse
import threading
import os.path

USERNAME = "xxx"
PASSWORD = "xxx"
START_DATE = datetime.datetime(2015, 8, 1, 0)
END_DATE = datetime.datetime(2015, 8, 2, 0)
OUTPUT_FILE = 'geo.gz'
DOWNLOAD_THREADS = 3
PARSE_THREADS = 4
DOWNLOAD_PATH = "http://data.blitzortung.org/Data_3/" + \
                "Protected/Strokes/{year}/{month:0>2d}" + \
                "/{day:0>2d}/{hour:0>2d}/{minute:0>2d}.json"

download_queue = queue.Queue()
downloaded_files = queue.Queue()
strikes = []

# create a password manager
password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
password_mgr.add_password(None, 'http://data.blitzortung.org', USERNAME, PASSWORD)
handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
opener = urllib.request.build_opener(handler)
urllib.request.install_opener(opener)

def convert_to_geostrike(raw_strike):
    strike = json.loads(raw_strike)
    strike_time = time.localtime(strike["time"]/1000000000)
    return {"type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [strike["lon"], strike["lat"]]},
            "properties": {
                "name": "Strike",
                "time": time.strftime('%Y-%m-%d %H:%M:%S', strike_time)}}

def parse_strikes(input_file):
    try:
        with gzip.open(input_file, 'r') as f:
            for strike_data in f:
                strikes.append(convert_to_geostrike(strike_data.decode("utf-8")))
    except:
        print ("Unable to parse " + input_file)

def save_output(geo_strikes):
    print("Writing to disk...")
    geo_file = {"type": "FeatureCollection", "features": geo_strikes}
    with gzip.open(OUTPUT_FILE, 'w') as f:
        for chunk in json.JSONEncoder().iterencode(geo_file):
            f.write(chunk.encode("utf-8"))

def time_range(start, end):
    date_list = []
    while start <= end:
        date_list.append(start)
        start = start + datetime.timedelta(minutes=10)
    return date_list

def download_file(strike_time):
    file_name = str(strike_time) + '.gz'
    file_name = file_name.replace(":", "-") #for windows files

    url = DOWNLOAD_PATH.format(year=strike_time.year,
                               month=strike_time.month,
                               day=strike_time.day,
                               hour=strike_time.hour,
                               minute=strike_time.minute)

    if os.path.isfile(file_name):
        print("File Exists: " + file_name)
        downloaded_files.put(file_name)
    else:
        is_gzip = True
        data = None

        #first see if there's a gz archive
        try:
            response = urllib.request.urlopen(url + ".gz")
            data = response.read()
            response.close()
        except urllib.error.HTTPError as err:
            if err.code == 404:
                try:
                    #It might be a newer uncompressed file
                    time.sleep(5) #Server is touchy about rapid requests
                    response = urllib.request.urlopen(url)
                    data = response.read()
                    response.close()
                    is_gzip = False
                except urllib.error.HTTPError:
                    print("Download Failed: " + url)
            else:
                print("Download Failed: " + url + ".gz")

        if data:
            if is_gzip: #file is already compressed
                with open(file_name, 'wb') as output:
                    output.write(data)
            else: #file needs to be compressed
                with gzip.open(file_name, 'wb') as output:
                    output.write(data)
            print("Downloaded: " + url + (".gz" if is_gzip else ''))
            downloaded_files.put(file_name)

def download_worker():
    while download_queue.qsize() > 0:
        strike_time = download_queue.get(block=False)
        download_file(strike_time)
        download_queue.task_done()

def parse_worker():
    while downloaded_files.qsize() > 0:
        f = downloaded_files.get(block=False)
        parse_strikes(f)
        downloaded_files.task_done()

if __name__ == "__main__":
    for t in time_range(START_DATE, END_DATE):
        download_queue.put(t)

    download_workers = [threading.Thread(target=download_worker) for i in range(DOWNLOAD_THREADS)]
    for t in download_workers:
        t.daemon = True
        t.start()

    for t in download_workers:
        t.join()

    print("Processing downloaded files...")
    parse_workers = [threading.Thread(target=parse_worker) for i in range(PARSE_THREADS)]
    for t in parse_workers:
        t.daemon = True
        t.start()

    for t in parse_workers:
        t.join()

    save_output(strikes)

