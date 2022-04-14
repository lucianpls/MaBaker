#
# Copyright 2019-2022 Esri
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Contributors:  Lucian Plesea
# Created: 12/09/2019
#

from osgeo import gdal
from osgeo import gdal_array as garray
from osgeo import gdalconst as gconst
import numpy
import math
import os
import os.path as path
import time
from subprocess import call,PIPE,Popen
import sys
import logging as log
import mabaker

def bsky_torgba8(tifin, tifout):
    "Converts input tif from RGBI uint16 to RGBA ubyte, where A is 0 when R,G,B and I are 0"
    ds_in = gdal.Open(tifin)
    raw = ds_in.ReadAsArray()
    nbands, rows, cols = raw.shape
    assert nbands == 4, "Wrong input format"
    b = list(raw[i,:,:] for i in range(nbands))
    mask = (((b[0] != 0) 
             | (b[1] != 0) 
             | (b[2] != 0) 
             | (b[3] != 0)).astype(numpy.ubyte) * 255).astype(numpy.ubyte)
    bands = list((band >> 8).astype(numpy.ubyte) for band in b[0:3])
    bands.append(mask)
    ds = garray.SaveArray(numpy.array(bands), tifout)
    ds.SetProjection(ds_in.GetProjection())
    ds.SetGeoTransform(ds_in.GetGeoTransform())
    ds_in = None
    ds = None

def buildMRF(inputvrt, output, compress = "LERC"):
    "Create an MRF, byte, but don't copy the data"
    format ='MRF'
    creationOptions = [
        "BLOCKSIZE=500",
        "COMPRESS=" + compress,
        "NOCOPY=TRUE"]
    topt = gdal.TranslateOptions(format = format
        ,outputType = gconst.GDT_Byte
        ,creationOptions = creationOptions)
    gdal.Translate(output, inputvrt, options=topt)
    return True

def remove(pth):
    try:
        gdal.Unlink(pth)
    except:
        pass

def insertMRF(filelist, mrf):
    mrfInsertExe = "mrf_insert"
    remove(temp_tif)
    # This could be threaded, with conversion taking place while the previous one is inserted
    for f in filelist:
        temp_tif = f + "temp"
        bsky_torgba8(f, temp_tif)
        call([mrfInsertExe, temp_tif, mrf])
        remove(temp_tif)

#
# If common_path is set, it applies to all files
# Some temporary files will still be created in the working folder
#
def project(files_txt, 
            common_path="",  # For the source
            cache_path="",  # Where the output L?? folders
            proj_mrf="project.mrf", 
            bbox_vrt="bbox.vrt",
            wm_vrt = "wm.vrt",
            warp_resample = "bilinear",
            base_level = 21,
            baseLevelQuality = 75,
            baseLevelSampling = "bilinear",
            OvrLevelQuality = 85,
            OvrLevelSampling = "bilinear",
            nprocs = None):

    base_level = int(base_level)
    with open(path.join(common_path, files_txt), "r") as listf:
        filelist = list(path.join(common_path, f.strip()) for f in listf)

    #call buildvrt
    STime = time.time()
    gdal.BuildVRT(bbox_vrt, filelist)
    print("BuildVRT took {} seconds".format(str(time.time()- buildVRT_STime)))

    #rebuild the project mrf
    proj_mrf = path.join(common_path, proj_mrf)
    for ext in ".mrf", ".lrc", ".idx", ".til", ".mrf.aux.xml":
        remove(path.splitext(proj_mrf)[0] + ext)

    STime = time.time()
    buildMRF(bbox_vrt, proj_mrf, compress = "None")
    remove(bbox_vrt)
    log.info("BuildMRF took {} seconds".format(str(time.time() - STime)))

    STime = time.time()
    insertMRF(filelist, proj_mrf)
    log.info("InsertMRF took {} seconds".format(str(time.time() - STime)))

    # Create the warping vrt
    wm_vrt = path.join(common_path, wm_vrt)
    mabaker.toWebMerc(proj_mrf, wm_vrt, resample = warp_resample)

    # bundle output folder
    baseleveldir = path.join(cache_path, f"L{base_level:02d}")
    try:
        os.makedirs(baseleveldir)
    except:
        pass

    compression = 'JPNG'
    bands = 4
    STime = time.time()
    mabaker.tobundles(wm_vrt, baseleveldir,
            quality = baseLevelQuality, level = base_level,
            resample = baseLevelSampling,
            compression = compression, nprocs = nprocs)
    log.info(f"mabaker base level time = {str(time.time()-STime)} seconds")

    mabaker.overviews(base_level-1, endlevel = 16, p = cache_path,
        quality = OvrLevelQuality, resample = OvrLevelSampling,
        compression = compression, bands = bands, nprocs = nprocs)
    log.info("mabaker total time = {} seconds".format(str(time.time()-STime)))

if __name__ == '__main__':
    import multiprocessing as mp
    nprocs = mp.cpu_count()
    
    #Set loging options via LOGLEVEL environment variable
    #INFO is equivalent to verbose
    # log.basicConfig(level = os.environ.get("LOGLEVEL", "WARNING"))

#    log.basicConfig(level = log.INFO)  # or log.DEBUG
    log.basicConfig(filename = f"run.log", level = os.environ.get("LOGLEVEL", "WARNING"))

    # project("files.txt", 
    #         common_path = r"/raid/bsky/19", 
    #         cache_path = r"/raid/bsky/19/cache/Layers",
    #         nprocs = nprocs)

    # mabaker.tobundles("/data/mrf/brunsli/la.mrf", "/data/L21", level=21, 
    #     options = "OPTIMIZE:1 JFIF:1", compression="JPEG", nprocs = nprocs)
    mabaker.overviews(20, endlevel=8, p="/data/", quality = 85, bands = 3,
         compression = "JPEG", options = "OPTIMIZE:1 JFIF:1", nprocs = nprocs)