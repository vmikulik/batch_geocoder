"""
This is a batch geocoder for processing large address datasets.

It keeps a memory of its progress between runs, so if it freezes or times out,
data is not lost.

To use:

edit the config
delete "status.json" if starting for the first time on a new datset
run "python main.py"

Vladimir Mikulik, 2018
"""

import pandas as pd
import geopy
import time
import os
import saving
from config import *

geo = geopy.geocoders.GoogleV3(api_key=API_KEY, timeout=5)
data = pd.read_csv(INPUT_PATH)

def reset_geocodes():
    return {'address':[], 'longitude':[], 'latitude':[]}

status = saving.load_progress()
current_index = status['current_index']

print("starting from entry {}...".format(current_index))
retries = 0
geocodes = reset_geocodes()
while current_index < len(data):
    address = data[ADDRESS_COL].ix[current_index]
    try:
        geocode = geo.geocode(address)
    except geopy.exc.GeocoderQuotaExceeded:
        #wait for some time and check if quota has reset
        time.sleep(60 * QUOTA_PING_TIME)
    except:
        #general geocoding failures
        if retries < MAX_RETRIES:
            retries += 1
            time.sleep(2)
        else:
            raise Exception("Failed too many times, aborting!")
    else:
        retries = 0

        #add geocode to working chunk
        geocodes['address'].append(address)
        if geocode != None:
            geocodes['latitude'].append(geocode.latitude)
            geocodes['longitude'].append(geocode.longitude)
        else:
            geocodes['latitude'].append(None)
            geocodes['longitude'].append(None)
        current_index += 1

        if current_index % LOGGING_RATE == 0:
            #log progress to console
            print("{} geocoded. ({:.1%})".format(current_index, current_index/len(data)))

        if (current_index % CHUNK_SIZE == 0) or (current_index == len(data)):
            #save this chunk
            print("saving chunk...")
            df = pd.DataFrame(geocodes)
            i_low = current_index - CHUNK_SIZE
            i_high = current_index - 1
            filepath = OUTPUT_FOLDER + "{}-{}.csv".format(i_low, i_high)
            df.to_csv(filepath, index=False)
            print("saved at {}".format(filepath))
            #resetting working chunk
            geocodes = reset_geocodes()
            status['current_index'] = current_index
            saving.save_progress(status)
