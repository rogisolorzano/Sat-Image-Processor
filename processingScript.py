from TrainingHandler import *
import numpy as np
import os
import glob
import zipfile
import mgrs
from skimage import io
from skimage.transform import rescale
from skimage import img_as_ubyte, img_as_uint, img_as_float
from skimage import exposure
from memory_profiler import profile
import io as byt
import re

# initialize my training handler that will manage all the getting and uploading of data
trainingHandler = TrainingHandler()

@profile
def getPreprocessedSatelliteImage(band_scheme, archive,scale):
    if band_scheme == "natural":
        b_r = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B04.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        b_g = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B03.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        b_b = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B02.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        return b_r, b_g, b_b
    elif band_scheme == "infrared":
        b_r = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B08.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        b_g = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B04.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        b_b = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B03.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        return b_r, b_g, b_b
    elif band_scheme == "urban":
        b_r = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B12.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        b_g = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B11.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        b_b = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B04.jp2$", fileName) != None, archive.namelist()))[0]))), (10/20) * scale)
        return b_r, b_g, b_b
    elif band_scheme == "agr":
        b_r = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B11.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        b_g = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B08.jp2$", fileName) != None, archive.namelist()))[0]))), (10/20) * scale)
        b_b = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B02.jp2$", fileName) != None, archive.namelist()))[0]))), (10/20) * scale)
        return b_r, b_g, b_b
    elif band_scheme == "atmospheric":
        b_r = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B12.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        b_g = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B11.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        b_b = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B8A.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        return b_r, b_g, b_b
    elif band_scheme == "healthy_veg":
        b_r = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B08.jp2$", fileName) != None, archive.namelist()))[0]))), (10/20) * scale)
        b_g = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B11.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        b_b = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B02.jp2$", fileName) != None, archive.namelist()))[0]))), (10/20) * scale)
        return b_r, b_g, b_b
    elif band_scheme == "land_water":
        b_r = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B08.jp2$", fileName) != None, archive.namelist()))[0]))), (10/20) * scale)
        b_g = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B11.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        b_b = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B04.jp2$", fileName) != None, archive.namelist()))[0]))), (10/20) * scale)
        return b_r, b_g, b_b
    elif band_scheme == "natural_atmospheric":
        b_r = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B12.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        b_g = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B08.jp2$", fileName) != None, archive.namelist()))[0]))), (10/20) * scale)
        b_b = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B03.jp2$", fileName) != None, archive.namelist()))[0]))), (10/20) * scale)
        return b_r, b_g, b_b
    elif band_scheme == "short_infrared":
        b_r = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B12.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        b_g = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B08.jp2$", fileName) != None, archive.namelist()))[0]))), (10/20) * scale)
        b_b = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B04.jp2$", fileName) != None, archive.namelist()))[0]))), (10/20) * scale)
        return b_r, b_g, b_b
    elif band_scheme == "veg_analyses":
        b_r = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B11.jp2$", fileName) != None, archive.namelist()))[0]))), scale)
        b_g = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B08.jp2$", fileName) != None, archive.namelist()))[0]))), (10/20) * scale)
        b_b = rescale(io.imread(byt.BytesIO(archive.read(list(filter(lambda fileName: re.search(r"B04.jp2$", fileName) != None, archive.namelist()))[0]))), (10/20) * scale)
        return b_r, b_g, b_b

@profile
def intensityScaleBandwidths(b_r, b_g, b_b, intensity):
    stack_image = np.dstack((b_r, b_g, b_b))
    p1, p2 = np.percentile(stack_image, (intensity, 100-intensity))
    stack_image[:,:,0] = exposure.rescale_intensity(stack_image[:,:,0], in_range=(p1, p2))
    stack_image[:,:,1] = exposure.rescale_intensity(stack_image[:,:,1], in_range=(p1, p2))
    stack_image[:,:,2] = exposure.rescale_intensity(stack_image[:,:,2], in_range=(p1, p2))
    return stack_image


# while there is another image to be downloaded
fileNameCounter = 0
while trainingHandler.hasNext():

    # trainingHandler.next() will download next image to disk and return data about it
    # don't forget to run os.remove(imageZipPath) after your processing is done
    # this is actually returning the product name now
    productName = trainingHandler.next()
    print('CURR PRODUCT ',productName) #debugging

    # find zipfile in directory
    zipName = list(filter(lambda fileName: re.search(r".zip$", fileName) != None, os.listdir()))[0]

    # get zipfile archive without unpacking it
    currentArchive = zipfile.ZipFile(zipName, "r")

    #--------------INFRARED IMAGE-------------------------------
    # get red, green, blue channels for infrared image
    redInfrared, greenInfrared, blueInfrared = getPreprocessedSatelliteImage("infrared", currentArchive, 1)
    # create image out of channels
    infraredImage = intensityScaleBandwidths(redInfrared, greenInfrared, blueInfrared, 2)
    infraredPath = productName + "-infrared.jpeg"
    io.imsave("./" + infraredPath, infraredImage)

    #--------------SWIR IMAGE-------------------------------
    # get red, green, blue channels for infrared image
    redSWIR, greenSWIR, blueSWIR = getPreprocessedSatelliteImage("short_infrared", currentArchive, 1)
    # create image out of channels
    swirImage = intensityScaleBandwidths(redSWIR, greenSWIR, blueSWIR, 2)
    swirPath = productName + "-swir.jpeg"
    io.imsave("./" + swirPath, swirImage)

    uploadStatusInfrared = trainingHandler.upload("./" + infraredPath, infraredPath)
    uploadStatusSWIR = trainingHandler.upload("./" + swirPath, swirPath)

    # then delete large zip file safely
    if uploadStatusInfrared == True:
        fileNameCounter += 1;
        try:
            for zipPath in glob.iglob(os.path.join('./', '*.zip')):
                print('zipPath ', zipPath)
                os.remove(zipPath)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    #
    # BELOW IS JUST AN EXAMPLE OF WHAT imageData LOOKS LIKE
    # {
    #     'downloaded_bytes': 819438534,
    #     'title': 'S2B_MSIL1C_20180716T183929_N0206_R070_T11SKA_20180716T222217',
    #     'url': "https://scihub.copernicus.eu/dhus/odata/v1/Products('f2de340c-5866-4f67-9580-721aa8094a3a')/$value",
    #     'footprint': 'POLYGON((-120.3713182092037 36.99866808026183,-119.13850562461279 37.02730768000537,-119.11143123293881 36.03812799310683,-120.32869364809063 36.01049725766069,-120.3713182092037 36.99866808026183))',
    #     'date': datetime.datetime(2018, 7, 16, 18, 39, 29, 24000),
    #     'path': './S2B_MSIL1C_20180716T183929_N0206_R070_T11SKA_20180716T222217.zip',
    #     'md5': 'EE7D40F99847B42EC58B987CB027B10B',
    #     'id': 'f2de340c-5866-4f67-9580-721aa8094a3a',
    #     'size': 819438534
    # }
    #
