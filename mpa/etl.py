import h5py
import os
import boto3 
import pandas as pd
import time
import glob
from joblib import Parallel, delayed

def process_h5_file(h5_file):
    """Do the processing of what fields you'll use here.

     For example, to get the artist familiarity, refer to:

     https://github.com/tbertinmahieux/MSongsDB/blob/master/PythonSrc/hdf5_getters.py

     So we see that it does h5.root.metadata.songs.cols.artist_familiarity[songidx] 
     and it would translate to:

       num_songs = len(file['metadata']['songs'])
       file['metadata']['songs'][:num_songs]['artist_familiarity']

     Since there is one song per file, it simplifies to:

       file['metadata']['songs'][:1]['artist_familiarity']

     I recommend downloading one file, opening it with h5py, and explore/practice

     To see the datatype and shape:

     http://millionsongdataset.com/pages/field-list/
     http://millionsongdataset.com/pages/example-track-description/
     """
    row = [ ]
    label = h5_file['metadata']['songs'][:1]['song_hotttnesss'][0]
    if isinstance(label, float) and label > -1:
        row.append(label)
        row.append(h5_file['metadata']['songs'][:1]['artist_id'][0].decode('UTF-8'))
        row.append(h5_file['metadata']['songs'][:1]['artist_name'][0].decode('UTF-8'))
        row.append(h5_file['metadata']['songs'][:1]['artist_familiarity'][0])
        row.append(h5_file['metadata']['songs'][:1]['artist_hotttnesss'][0])
        row.append(h5_file['metadata']['songs'][:1]['artist_latitude'][0])
        row.append(h5_file['metadata']['songs'][:1]['artist_longitude'][0])
        row.append(h5_file['metadata']['songs'][:1]['artist_location'][0].decode('UTF-8'))
        row.append(h5_file['metadata']['songs'][:1]['title'][0].decode('UTF-8'))
        row.append(h5_file['metadata']['songs'][:1]['genre'][0].decode('UTF-8'))
        string = ','.join(h5_file['metadata']['artist_terms'][:].astype(str))
        row.append(string)
        string = ','.join(h5_file['metadata']['artist_terms_freq'][:].astype(str))
        row.append(string)
        string = ','.join(h5_file['metadata']['artist_terms_weight'][:].astype(str))
        row.append(string)
        row.append(h5_file['musicbrainz']['songs'][:1]['year'][0])
        row.append(h5_file['analysis']['songs'][:1]['danceability'][0])
        row.append(h5_file['analysis']['songs'][:1]['energy'][0])
        row.append(h5_file['analysis']['songs'][:1]['duration'][0])
        row.append(h5_file['analysis']['songs'][:1]['start_of_fade_out'][0])
        row.append(h5_file['analysis']['songs'][:1]['end_of_fade_in'][0])
        row.append(h5_file['analysis']['songs'][:1]['key'][0])
        row.append(h5_file['analysis']['songs'][:1]['key_confidence'][0])
        row.append(h5_file['analysis']['songs'][:1]['loudness'][0])
        row.append(h5_file['analysis']['songs'][:1]['mode'][0])
        row.append(h5_file['analysis']['songs'][:1]['mode_confidence'][0])
        row.append(h5_file['analysis']['songs'][:1]['tempo'][0])
        row.append(h5_file['analysis']['songs'][:1]['time_signature'][0])
        row.append(h5_file['analysis']['songs'][:1]['time_signature_confidence'][0])
    return row 

def transform_local(path):
    try:
        with h5py.File(path) as h5:
            return process_h5_file(h5)
    except Exception:
        return []
        
def rows_to_file(rows, chunk_idx, bucket='roborace-imaronna'):
    # Write some code to save a list of rows into a temporary CSV (or whatever format)
    # for example using pandas.
    df = pd.DataFrame(rows)
    df.dropna(how='all', axis=0, inplace=True)
    df.to_csv("temp.csv", header=False, index=False)

if __name__ == '__main__':
    # If using the local volume:
    data_dir = "songs/data"
    bucket = '10805-miniproject'
    chunk_size = 1000
    max_retries = 3
    chunk_idx = 0
    files = glob.glob("songs/data/**/*.h5", recursive=True)
    while chunk_idx < 1000:
        processed = Parallel(n_jobs=2)(delayed(transform_local)(filepath) 
                                for filepath in files[chunk_idx*chunk_size:chunk_idx*chunk_size+chunk_size])
        rows_to_file(processed, chunk_idx) 
        object_name = "csv/%d.csv"%chunk_idx
        s3_client = boto3.client('s3')
        for i in range(0, max_retries):
            while True:
                try:
                    response = s3_client.upload_file("temp.csv", bucket, object_name)
                except:
                    print("Client error")
                    time.sleep(10)
                break            
        print("Chunk ID: %d /1000"%chunk_idx)
        chunk_idx += 1