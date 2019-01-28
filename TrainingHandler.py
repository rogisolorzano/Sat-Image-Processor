import mgrs
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
from datetime import date
from collections import deque
import json
import sys
import boto3
import os
from botocore.client import Config

#custom exception to throw if upload fails
class UploadException(Exception):
    pass

# Initialize a session using DigitalOcean Spaces.
session = boto3.session.Session()
client = session.client('s3',
                        region_name='nyc3',
                        endpoint_url='https://nyc3.digitaloceanspaces.com',
                        aws_access_key_id='<SPACES_ACCESS_KEY>',
                        aws_secret_access_key='<SPACES_SECRET_KEY>')

#initialize MGRS to LatLon converter
mgrs = mgrs.MGRS()

class TrainingHandler:

    # @param Int index : hardcode an index to start from if you want
    def __init__(self,index=None):

        #initialize sentinelsat
        self.api = SentinelAPI('username', 'password', 'https://scihub.copernicus.eu/dhus')

        #prepare queue for pending satellite products
        self.queue = deque([])

        # if no index is set, load from indexLog.txt file
        if index is None:
            self.index = int(open('indexLog.txt').readline())
            print("Loaded previous index from file. Starting from index "+ str(self.index))
        else:
            self.index = int(index)

        #load fire training dataset file
        with open('fires.json') as f:
            self.data = json.load(f)

    def next(self):
        if len(self.queue) > 0:
            return self.api.download(self.queue.popleft())
        else:
            #get 2 more
            dataSample = self.data[self.index]
            urlData = extractURLData(dataSample["aws_s3"][0])
            # search by polygon, time, and SciHub query keywords
            footprint = geojson_to_wkt(toFeatureCollection(mgrs.toLatLon(str.encode(urlData["mgrs"]))))
            products = self.api.query(footprint,
                                 date=(urlData["date"]["from"], urlData["date"]["to"]),
                                 platformname='Sentinel-2',
                                 cloudcoverpercentage=(0, 30))
            #get first 2 keys which are the product IDs you need to download
            products = list(products)[:2]
            print('PRODUCTS ',products)

            if len(products) > 0:
                self.updateIndex(self.index)
                if len(products) > 1:
                    # if found 2 products, add one to the queue
                    self.queue.append(products[0])
                    imageData = self.api.download(products[1])
                    return products[1]
                else:
                    imageData = self.api.download(products[0])
                    return products[0]
            else:
                # if didn't find any products, updateIndex and go to next data
                self.updateIndex(self.index)
                self.next()

    # @param Int index : current index, will be updated by this function
    def updateIndex(self,index):
        if index < len(self.data):
            index += 1
            self.index = index
            fh = open("indexLog.txt", "w")
            fh.write(str(index))
            fh.close()
        else:
            sys.exit('DONE')

    # @param String sourcePath : the current path of the processed image in the filesystem
    # @param String fileName : the unique name of the file in object storage
    def upload(self,sourcePath,fileName):
        try:
            print('Uploading...')
            client.upload_file(sourcePath, 'training-data', fileName )
            os.remove(sourcePath)
            print('Done.')
            return True
        except:
            #throws exception if upload failed
            raise
            #raise UploadException('Upload of index '+ sourcePath +' failed')
            return False

    #check if there are any more wildfire images in dataset
    def hasNext(self):
        return self.index < len(self.data) or len(self.queue) > 0

######## END TrainingHandler Class


#####
# HELPER FUNCTIONS BELOW
#####

# @param String url :
# url is an Amazon URL like https://sentinel-s2-l1c.s3.amazonaws.com/tiles/11/S/KU/2017/12/13/0/B01.jp2
# @return Dict { "mgrs": "{tile-name}", "date": { "from": "2017-12-13", "to": "2018-12-14""  }
def extractURLData(url):
    parts = url.split("amazonaws.com/tiles/")[1]
    parts = parts.split('/')
    mgrs =  "".join(parts[0:3])
    ymd = parts[3:6]
    if len(ymd[1]) == 1:
        ymd[1] = "0" + ymd[1]
    if len(ymd[2]) == 1:
        ymd[2] = "0" + ymd[2]
    dateFrom = ymd[0] + "-" + ymd[1] + "-" + ymd[2] + "T00:00:00.000Z"
    dateTo = ymd[0] + "-" + ymd[1] + "-" + ymd[2] + "T23:59:59.000Z"

    return { "mgrs" : mgrs, "date": { "from": dateFrom, "to": dateTo } }

# @param LatLng ll : is a [latitude,longitude] array
# @return GeoJSON FeatureCollection
def toFeatureCollection(ll):
    return {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "properties": {},
          "geometry": {
            "type": "Point",
            "coordinates": [
              ll[1],
              ll[0]
            ]
          }
        }
      ]
    }
