#
# Name: mabaker.py
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

import xml.etree.ElementTree as ET
import math
import array
import struct
import sys
import os
import os.path as path
import glob
import copy
from datetime import datetime as DT
import time
import multiprocessing as mp
import logging
from random import randint
from osgeo import gdal
gdal.UseExceptions()


_BSZ = 128                  # Bundle size in tiles
_PSZ = 256                  # Tile size in pixels
_PPB = _BSZ * _PSZ          # Bundle size in pixels
_BSZ2 = _BSZ * _BSZ         # Tiles in a bundle
_IDXSZ = _BSZ2 * 8          # Index record size in bytes
_HSZ = 64                   # V2 bundle header size
_OBITS = 40                 # Bits for offset in the bundle index
_EOFF = 36                  # The offset for empty tiles

log = logging.getLogger(__name__)

def _bhead(maxRecord = 0, fileSize = 0, 
        index : bytes = None, bsz = _BSZ):
    "Initializes the header for a V2 bundle file"
    bsz2 = bsz * bsz
    idxsz = bsz2 * 8
    header = struct.pack("<4I3Q6I",
        3,          # Version
        bsz2,       # numRecords
        maxRecord,  # maxRecord Size
        5,          # Offset Size
        0,          # Slack Space
        fileSize,   # File Size
        40,         # User Header Offset
        20 + idxsz, # User Header Size
        3,          # Legacy 1
        16,         # Legacy 2
        bsz2,       # Legacy 3
        5,          # Legacy 4
        idxsz       # Index Size
    )
    if index is not None:
        header += index
    return header


class _XY(object):
    def __init__(self,X,Y):
        self.X = X
        self.Y = Y
    def Opposite(self):
        return _XY(-self.X, -self.Y)


class _WebMercatorCS(object):
    "Predefined CS object"
    def __init__(self):
        radius = 6378137
        onef = 298.257223563
        self.WKT = f'''PROJCS["WGS_1984_Web_Mercator_Auxiliary_Sphere",
            GEOGCS["GCS_WGS_1984",
                DATUM["D_WGS_1984",
                    SPHEROID["WGS_1984",{radius},{onef}]],
                PRIMEM["Greenwich",0],
                UNIT["Degree",{math.pi/180}]],
            PROJECTION["Mercator_Auxiliary_Sphere"],
            PARAMETER["False_Easting",0],
            PARAMETER["False_Northing",0],
            PARAMETER["Central_Meridian",0],
            PARAMETER["Standard_Parallel_1",0],
            PARAMETER["Auxiliary_Sphere_Type",0],
            UNIT["Meter",1],
            AUTHORITY["EPSG",3857]]'''
        self.WKID = 3857
        self.Origin = _XY(-math.pi*radius, math.pi*radius)


class _LatLonCS(object):
    "Predefined GCS"
    def __init__(self):
        radius = 6378137
        onef = 298.257223563
        self.WKT = f'''GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS_1984",{radius},{onef}]],
            PRIMEM["Greenwich",0],UNIT["degree", {math.pi/180}]]'''
        self.WKID = 4326
        self.Origin = _XY(-180, 90)


class Cache(object):
    def __init__(self, CS = _WebMercatorCS(), levels = 24, psz = _PSZ, bsz = _BSZ):
        self.CS = CS
        self.folder = None
        self.format = None
        self.Quality = None
        self.Antialiasing = True
        self.PacketSize = bsz
        self.TileCols = psz
        self.TileRows = psz
        self.DPI = 96
        self.Origin = self.CS.Origin
        self.End = self.Origin.Opposite()
        Corner = self.CS.Origin.Y
        self.LODS = {i : Corner / 2**(i + math.log(bsz,2))
                     for i in range(levels)}

    def __getitem__(self, key):
        return self.LODS[key]


class GFile(object):
    "Context manager for gdal.VSIFile"
    def __init__(self, fname :str, mode : str):
        self.vsifile = gdal.VSIFOpenL(fname, mode)
    def __enter__(self):
        return self.vsifile
    def __exit__(self, type, value, traceback):
        gdal.VSIFCloseL(self.vsifile)


def nameToRC(name : str):
    "Bundle name to Row Col"
    if '.' not in name:
        name += '.'
    return (int('0x' + name[1:name.find('C')], 0),
            int('0x' + name[name.find('C') + 1: name.find('.')], 0))


def RCname(r, c):
    "Row Col to bundle name"
    return f"R{r:04x}C{c:04x}"


def patchlist(outb, level = 13, scheme = Cache(_WebMercatorCS())):
    "A list with l, r, c touples affected by a bundle at a specific level"
    assert level in scheme.LODS, "No such level in cache scheme"
    # Check that bundle is inside the source level...
    r, c = nameToRC(outb)
    bsz = scheme.PacketSize
    mask = bsz - 1
    assert (0 == r & mask) and (0 == c & mask), "No such bundle"
    r, c = r // bsz, c // bsz
    val = list()
    for l in range(level - 1, -1, - 1):
        r, c = r // 2, c // 2
        val.append((l, r * bsz, c * bsz))
    return val if len(val) else None


def toWebMerc(source, dst ,resample = "bilinear"):
    "Create a vrt warped to web mercator"
    wopt = gdal.WarpOptions(
            format = "VRT"
            ,resampleAlg = resample
            ,dstSRS = "EPSG:3857"
    )
    gdal.Warp(dst, source, options = wopt)


def createBundle(name):
    "Create an empty V2 bundle"
    with GFile(name, "wb") as data_file:
        header = _bhead()
        gdal.VSIFWriteL(header, len(header), 1, data_file)
        emptyidx = array.array('Q', (_EOFF,) * _BSZ2)
        if sys.byteorder != "little":
            emptyidx.byteswap()
        gdal.VSIFWriteL(emptyidx.tobytes(), len(emptyidx), 
            emptyidx.itemsize, data_file)

def _readbidx(bfile):
    "Read a bundle index from a bundle file"
    gdal.VSIFSeekL(bfile, _HSZ, os.SEEK_SET)
    idx = array.array("Q")
    idx.frombytes(gdal.VSIFReadL(_BSZ2, idx.itemsize, bfile))
    if sys.byteorder != "little":
        idx.byteswap()
    return idx


def headerfix(name, _BSZ2 = _BSZ2):
    "Update the maxrecord and file size fields of a V2 bundle"
    with GFile(name, "r+b") as bundle:
        idx = _readbidx(bundle)
        maxsize = max(ix >> _OBITS for ix in idx)
        stat = gdal.VSIStatL(name)
        header = _bhead(maxsize, stat.size)
        gdal.VSIFSeekL(bundle, 0, os.SEEK_SET)
        gdal.VSIFWriteL(header, len(header), 1, bundle)


def mrfbundlefix(name, bsz2 = _BSZ2, v2 = True):
    "Convert an MRF index into a V2 bundle one"
    # if v2 is false the bundle is V2 without the size tile prefix
    base = path.splitext(name)[0]

    idx = array.array('Q')
    with GFile(base + ".idx", "rb") as idxf:
        idx.frombytes(gdal.VSIFReadL(bsz2, idx.itemsize * 2, idxf))
    if sys.byteorder != "big":
        idx.byteswap()

    it = iter(idx)
    oidx = array.array('Q')
    maxsize = 0
    for i in range(bsz2):
        offset = next(it)
        size = next(it)
        maxsize = max(maxsize, size)
        if size == 0:
            offset = _EOFF
        oidx.append(offset + (size << _OBITS))
    if sys.byteorder != "little":
        oidx.byteswap()

    stat = gdal.VSIStatL(name)
    header = _bhead(maxsize, stat.size)
    with GFile(name, "r+b") as data_file:
        gdal.VSIFWriteL(header, len(header), 1, data_file)
        gdal.VSIFWriteL(oidx.tobytes(), len(oidx),
            oidx.itemsize, data_file)
        if not v2:
            return

        # Patch in the tile sizes, no checks
        it = iter(idx)
        for i in range(_BSZ2):
            offset = next(it)
            size = next(it)
            if size != 0:
                gdal.VSIFSeekL(data_file, offset - 4, os.SEEK_SET)
                gdal.VSIFWriteL(struct.pack("<I", size), 4, 1, data_file)


def towms(bundle, pth, compression = "PNG", bands = "4"):
    "Bundle to WMS"
    name = path.splitext(bundle)[0]
    wms_root = ET.Element("GDAL_WMS")
    service = ET.SubElement(wms_root, "Service",
                            {"name" : "MRF", "type" : "bundle"})
    ET.SubElement(service,"ServerUrl").text = "file://" + path.join(pth,bundle)
    ET.SubElement(wms_root,"BandsCount").text = str(bands)
    wmsname = name + ".wms"
    with GFile(wmsname, "wb") as wms_file:
        mxml = ET.tostring(wms_root)
        gdal.VSIFWriteL(mxml, 1, len(mxml), wms_file)
    return wmsname


def tomrf(bundle, compression = "JPNG", bands = 4):
    "Bundle to MRF"
    # Convert the index to an MRF one
    with GFile(bundle, "rb") as bfile:
        inidx = _readbidx(bfile)

    outidx = array.array("Q")
    for idx in inidx:
        outidx.append(idx % (1 << _OBITS))
        outidx.append(idx >> _OBITS)
    if sys.byteorder != "big":
        outidx.byteswap()
    name = path.splitext(bundle)[0]
    with GFile(name + ".idx", "wb") as idxfile:
        gdal.VSIFWriteL(outidx.tobytes(), len(outidx), outidx.itemsize, idxfile)

    # Create a minimal mrf metadata file pointing to this bundle
    mrf_root = ET.Element('MRF_META')
    raster = ET.SubElement(mrf_root, 'Raster')
    ET.SubElement(raster, "Size",
                  {"x" : str(_PPB), "y" : str(_PPB), "c" : str(bands)})
    ET.SubElement(raster, "PageSize",
                  {"x" : str(_PSZ), "y" : str(_PSZ), "c" : str(bands)})
    ET.SubElement(raster, "Compression").text = compression
    # It might be a relative path
    ET.SubElement(raster, "DataFile").text = bundle
    mrfname = name + ".mrf"
    mxml = ET.tostring(mrf_root)
    with GFile(mrfname, "wb") as mrf_file:
        gdal.VSIFWriteL(mxml, 1, len(mxml), mrf_file)
    return mrfname


def underview(src,
              dst = None,
              resample = "average",
              compression = "JPEG",
              bands = 3,
              quality = 75,
              options = None,
              v2 = True
              ):
    "Oversample a bundle to the next higher resolution level"

    def quart(name, bottom, right):
        createBundle(name)
        creationOptions = [f"BLOCKSIZE={_PSZ}",
            f"DATANAME={name}",
            f"COMPRESS={compression}",
            f"QUALITY={quality}"]
        if options:
            creationOptions.append(f"OPTIONS={options}")
        if v2:
            creationOptions.append("SPACING=4")

        hsize = _PPB // 2
        srcWin = [hsize if right else 0, hsize if bottom else 0, hsize, hsize]
        topt = gdal.TranslateOptions(
            format = "MRF"
            ,resampleAlg = resample
            ,width = _PPB
            ,height = _PPB
            ,srcWin = srcWin
            ,creationOptions = creationOptions)

        # Translate and close, purging data
        omrfname = path.splitext(d)[0] + ".mrf"
        gdal.Translate(omrfname, srcmrf, options = topt)
        mrfbundlefix(name, v2 = v2)
        for f in glob.iglob(path.splitext(name)[0] + "*.???"):
            gdal.Unlink(f)
        # no empty bundles
        empty = (gdal.VSIStatL(name).size == _IDXSZ + _HSZ)
        if empty:
            gdal.Unlink(name)
        return not empty

    inRC = nameToRC(path.basename(src))
    if dst is None:
        dst = []
        dname = path.dirname(src)
        lfolder = path.basename(dname)
        if "L" == lfolder[0]:
            level = int(lfolder[1:]) + 1
            dname = path.join(path.dirname(dname), f"L{level:02d}")
        for r in 0, 1:
            r = inRC[0] * 2 + r * _BSZ
            for c in 0, 1:
                c = inRC[1] * 2 + c * _BSZ
                dst.append(path.join(dname, RCname(r, c) + ".bundle"))
    elif isinstance(dst, str):
        dst = [dst]

    srcmrf = tomrf(src, compression = compression, bands = bands)
    generated = []
    for d in dst:
        outRC = nameToRC(path.basename(d))
        r = outRC[0] - inRC[0] * 2
        c = outRC[1] - inRC[1] * 2
        assert (0 == r or _BSZ == r) and (0 == c or _BSZ == c), "Invalid output bundle"
        if quart(d, r, c):
            generated.append(path.basename(d))
    for f in glob.iglob(path.splitext(src)[0] + "*.???"):
        gdal.Unlink(f)
    return generated


def underlevel(source_path
        , bundles = None  # An iterable of input bundle file names
        , options = "OPTIMIZE:1"
        , quality = 75
        , resample = "average"
        , compression = "JPEG"
        , bands = 3
        , v2 = True # Generate full V2 bundles, faster without it
        , nprocs = None # Defaults to cores/2
        , dryrun = False # Don't do the work, returns the bundle names to be built
        , clean = True   # set to false for debuging only
    ):
    "Generate underviews for a list of input bundles, or for all bundles in the source path"

    def endajob(jobs, tau = 0.1):
        "Loop over jobs every tau s until one stops, return the index"
        while True:
            for j in range(len(jobs)):
                job = jobs[j]
                state = job["state"]
                if state.ready():
                    elapsed = DT.now() - job["stime"]
                    name = path.basename(job["in"])
                    if state.successful():
                        log.info(f"{elapsed} {name} {state.get()}")
                    else:
                        # The use of _value is not safe, but get() would raise an exception
                        log.error(f"{name} failed {state._value}")
                    return j
            time.sleep(tau)

    def makeslot(jobs):
        if len(jobs) != nprocs:
            return
        if len(jobs):
            jobs.pop(endajob(jobs))

    def submit(name, jobs):
        "starts a job, add it to the jobs list"
        kwds = { "resample" : resample
            , "compression" : compression
            , "bands" : bands
            , "quality" : quality
            , "v2" : v2
            , "options" : options
        }
        jobs.append({
             "stime" : DT.now()
            , "in" : name
            , "state" : pool.apply_async(underview,
                            args = [name],
                            kwds = kwds
                        )
            })

    #underlevel function body
    level = int(path.basename(source_path)[1:])
    dst_path = path.join(path.dirname(source_path), f"L{(level + 1):02d}")
    try:
        os.mkdir(dst_path)
    except:
        pass
    if not bundles:
        bundles = glob.glob(path.join(source_path, "*.bundle"))
    if dryrun:
        return list(bundles)

    jobs = []
    nprocs = nprocs if nprocs else max(1, mp.cpu_count() // 2)
    with mp.Pool(nprocs) as pool:
        for name in bundles:
            makeslot(jobs)
            submit(path.join(source_path, name), jobs)
        while 0 != len(jobs):
            jobs.pop(endajob(jobs))


def overview(src, dst=None,
        quality=None,
        resample="bilinear",
        compression="JPNG",
        bands=4,
        options = None,
        v2 = True):
    "push a source into an overview bundle"

    def quart_index(bundle, bottom=False, right=False):
        "for an overview bundle, patches the index from an mrf quad index"
        name = path.splitext(bundle)[0]
        with GFile(bundle, "r+b") as bfile:
            gdal.VSIFSeekL(bfile, 8, os.SEEK_SET)
            maxRecord = struct.unpack("<I", gdal.VSIFReadL(4, 1, bfile))[0]
            idx = _readbidx(bfile)
            # Read the input mrf index also, should be quarter size
            mrfidx = array.array("Q")
            with GFile(name + ".idx", "rb") as idxfile:
                mrfidx.frombytes(gdal.VSIFReadL(_BSZ2 // 2, mrfidx.itemsize, idxfile))
            if sys.byteorder != "big":
                mrfidx.byteswap()

            # Patch this quarter
            QSZ = _BSZ // 2
            left = 0 if not right else QSZ
            top = 0 if not bottom else QSZ
            it = iter(mrfidx)
            for r in range(top, top + QSZ):
                for c in range(left, left + QSZ):
                    offset = next(it)
                    size = next(it)
                    maxRecord = max(maxRecord, size)
                    if size == 0:
                        offset = _EOFF
                    idx[r * _BSZ + c] = offset + (size << _OBITS)

            # Put the index back into the bundle
            if sys.byteorder != "little":
                idx.byteswap()
            gdal.VSIFSeekL(bfile, 8, os.SEEK_SET)
            gdal.VSIFWriteL(struct.pack("<I", maxRecord), 4, 1, bfile)
            gdal.VSIFSeekL(bfile, _HSZ, os.SEEK_SET)
            gdal.VSIFWriteL(idx.tobytes(), len(idx), idx.itemsize, bfile)

            if not v2:
                return
            it = iter(mrfidx)
            for r in range(top, top + QSZ):
                for c in range(left, left + QSZ):
                    offset = next(it)
                    size = next(it)
                    if size != 0:
                        gdal.VSIFSeekL(bfile, offset - 4, os.SEEK_SET)
                        gdal.VSIFWriteL(struct.pack("<I", size), 4, 1, bfile)

    inRC = nameToRC(path.basename(src))
    if dst is None:
        outRC = tuple((v // (2 * _BSZ)) * _BSZ for v in inRC)
        dst = RCname(*outRC) + ".bundle"
    else:
        outRC = nameToRC(path.basename(dst))

    if gdal.VSIStatL(dst) is None:
        createBundle(dst)

    mrf_name = tomrf(src, compression = compression, bands = bands)
    creationOptions = [
        f"BLOCKSIZE={_PSZ}",
        f"DATANAME={dst}",
        f"COMPRESS={compression}"]
    if options:
        creationOptions.append(f"OPTIONS={options}")
    if quality is not None:
        creationOptions.append(f"QUALITY={quality}")
    if v2:
        creationOptions.append("SPACING=4")

    topt = gdal.TranslateOptions(
        format = "MRF"
        ,resampleAlg = resample
        ,width = _PPB / 2
        ,height = _PPB / 2
        ,creationOptions = creationOptions
        )

    # Translate and close, purging data
    omrfname = path.splitext(dst)[0] + ".mrf"
    gdal.Translate(omrfname, mrf_name, options = topt)

    bottom = (inRC[0] & 0x80) != 0
    right = (inRC[1] & 0x80) != 0
    quart_index(dst, right = right, bottom = bottom)
    headerfix(dst)
    for d in (dst, src):
        for f in glob.iglob(path.splitext(d)[0] + "*.???"):
            gdal.Unlink(f)


def ovrbundle(srcpath, dst,
        quality = None,
        resample = "bilinear",
        compression = 'JPNG',
        bands = 4,
        v2 = True):
    "Build a single overview bundle, if any of the source bundles exist"

    if path.splitext(dst)[1] != ".bundle":
        dst += ".bundle"
    outRC = nameToRC(path.basename(dst))
    inRC = tuple((v * 2) for v in outRC)
    STime = DT.now()
    for dr in 0, _BSZ:
        for dc in 0, _BSZ:
            src = path.join(srcpath, RCname(inRC[0] + dr, inRC[1] + dc)) + ".bundle"
            if gdal.VSIStatL(src) is None:
                continue
            overview(src, dst,
                    quality = quality,
                    resample = resample,
                    compression = compression,
                    bands = bands,
                    options = options,
                    v2 = v2)
    log.info(f"{DT.now() - STime} {dst}")


def ovrlevel(srcpath, dstpath, quality=None,
        compression="JPNG",
        bands=4,
        v2=True,
        options = None,
        resample = "bilinear",
        nprocs = None):

    def generates(name):
        "lower level bundle name from the upper one"
        inRC = nameToRC(name)
        return RCname(*tuple((v // (2 * _BSZ)) * _BSZ for v in inRC))

    def endajob(jobs, sleepsec = 1):
        "Loop over jobs until one stops, return its index"
        while True:
            for j in range(len(jobs)):
                job = jobs[j]
                if job["state"].ready():
                    entry = job["in"]
                    log.info(f"{DT.now() - job['stime']} {entry} -> {generates(entry)}")
                    return j
            time.sleep(sleepsec)

    def submit(entry):
        "Call only when a job slot is available and not blocked"
        inname = path.join(srcpath, entry)
        outname = path.join(dstpath, generates(entry)) + ".bundle"
        jobs.append({
            "stime" : DT.now(),
            "in" : entry,
            "state" : pool.apply_async(overview,
                        args = (inname, outname),
                        kwds = {
                            "quality" : quality,
                            "compression" : compression,
                            "resample" : resample,
                            "bands" : bands,
                            "v2" : v2,
                            "options" : options
                            })
            })

    def makeslot(drain = False):
        "drain = True if jobs are to be drained"
        if len(jobs) != nprocs:
            if not drain:
                return
        if len(jobs) != 0:
            jobs.pop(endajob(jobs))
        # Pending jobs have priority, at least one can proceed
        if len(pending) == 0:
            return
        blocked = tuple(generates(job["in"]) for job in jobs)
        for i in range(len(pending)):
            entry = pending[i]
            if generates(entry) not in blocked:
                pending.pop(i)
                submit(entry)
                # Job slots are full again, recurse
                makeslot(drain)
                return

    #ovrlevel function body
    nprocs = nprocs if nprocs else max(1, mp.cpu_count() // 2)
    jobs = list() # running jobs
    pending = list() # blocked jobs
    with mp.Pool(nprocs) as pool:
        for entry in glob.iglob(path.join(srcpath, "*.bundle")):
            entry = path.basename(entry)
            blocked = tuple(generates(job["in"]) for job in jobs)
            if generates(entry) in blocked:
                pending.append(entry)
            else:
                makeslot()
                submit(entry)
        # No more entries, but we still have jobs
        while 0 != len(jobs):
            makeslot(drain = True)

def tobundle(src, dst
        , compression = "JPNG"
        , quality = None
        , v2 = True
        , options = None
        , clean = True
    ):
    "Create a single bundle, dst is without extention"

    bundle = dst + ".bundle"
    createBundle(bundle)
    creationOptions = [
            f"BLOCKSIZE={_PSZ}"
            ,f"COMPRESS={compression}"
            ,f'DATANAME={bundle}'
        ]
    if v2:
        creationOptions.append("SPACING=4")
    if quality:
        creationOptions.append(f"QUALITY={quality}")
    if options:
        creationOptions.append(f"OPTIONS={options}")
    topt = gdal.TranslateOptions(
            format = "MRF",
            creationOptions = creationOptions
            )
    gdal.Translate(dst + ".mrf", src, options = topt)
    empty = (gdal.VSIStatL(bundle).size == _IDXSZ + _HSZ)
    if empty:
        gdal.Unlink(bundle)
    else:
        mrfbundlefix(bundle, v2 = v2)
    if clean or empty:
        for f in glob.iglob(dst + "*.???"):
            gdal.Unlink(f)


def tobundles(source
        , dfolder = "."
        , bundles = None  # An iterable of names
        , level = None
        , options = "OPTIMIZE:1"
        , quality = None
        , resample = "bilinear"
        , compression = "JPNG"
        , scheme = Cache(_WebMercatorCS()) # source projection should match
        , v2 = True # Generate full V2 bundles, faster without it
        , nprocs = None # Defaults to cores/2
        , clean = True   # Only set to false for debuging
        , dryrun = False # Don't do the work, returns the bundle names to be built
    ):
    "Convert a source to bundles"

    def endajob(jobs, tau = 0.1):
        "Loop over jobs every tau s until one stops, return the index"
        while True:
            for j in range(len(jobs)):
                job = jobs[j]
                if job["state"].ready():
                    elapsed = DT.now() - job["stime"]
                    name = path.basename(job["out"])
                    log.info(f"{elapsed} {name} {path.exists(job['out'] + '.bundle')}")
                    return j
            time.sleep(tau)

    def makeslot(jobs, drain = False):
        # drain = True if jobs are to be drained
        if len(jobs) != nprocs:
            return
        if len(jobs) != 0:
            jobs.pop(endajob(jobs))

    def submit(name, jobs):
        "starts a job, call only when a slot is available"
        br, bc = (v // _BSZ for v in nameToRC(name))
        # Chop the bundle from input
        vopt = gdal.BuildVRTOptions(
                outputBounds = (
                    xcorner + bc * bundle_size,
                    ycorner - (br + 1) * bundle_size,
                    xcorner + (bc + 1) * bundle_size,
                    ycorner - br * bundle_size
                    )
                ,xRes = resolution
                ,yRes = resolution
                ,resampleAlg = resample
                )
        fullname = path.join(dfolder, name)
        vrtname = fullname + ".vrt"
        gdal.BuildVRT(vrtname, source, options =  vopt)
        jobs.append({
              "stime" : DT.now()
            , "out" : fullname
            , "state" : pool.apply_async(tobundle,
                        args = (vrtname, fullname),
                        kwds = {
                            "quality" : quality
                            ,"compression" : compression
                            ,"v2" : v2
                            ,"options" : options
                            ,"clean" : clean
                            }
                        )
            })

    ids = gdal.Open(source)
    left, dx, nothing , top, nothing, dy = ids.GetGeoTransform()
    right = left + dx * ids.RasterXSize
    bottom = top + dy * ids.RasterYSize
    ids = None

    if level is None:
        for level in scheme.LODS:
            if scheme[level] <= dx:
                break
    assert level in scheme.LODS, f"Can't pick level for resolution {dx}"
    resolution = scheme[level]
    bundle_size = resolution * _PPB
    xcorner = scheme.Origin.X
    ycorner = scheme.Origin.Y
    start_bundle_col = int((left - xcorner) / bundle_size)
    start_bundle_row = int((ycorner - top) / bundle_size)
    end_bundle_col = 1 + int((right - xcorner) / bundle_size)
    end_bundle_row = 1 + int((ycorner - bottom) / bundle_size)
    width = end_bundle_col - start_bundle_col
    height = end_bundle_row - start_bundle_row
    log.info(f"Generating L{level:02d}")
    log.info(f"Input {left},{bottom} {right},{top}")
    log.info(f"Resample {resample}")
    log.info(f"{width * height} bundles {width} by {height}")
    log.info("{} to {}".format(RCname(start_bundle_row * _BSZ, start_bundle_col * _BSZ),
            RCname((end_bundle_row -1) * _BSZ, (end_bundle_col -1) * _BSZ)))

    names = (RCname(br * _BSZ, bc * _BSZ)
                for br in range(start_bundle_row, end_bundle_row)
                for bc in range(start_bundle_col, end_bundle_col))
    if dryrun:
        return names

    # Do the actual job, using "tobundle" in parallel via a job queue
    jobs = list()
    nprocs = nprocs if nprocs else max(1, mp.cpu_count() // 2)
    with mp.Pool(nprocs) as pool:
        for name in names:
            if bundles and name not in bundles:
                continue
            makeslot(jobs)
            submit(name, jobs)
        while 0 != len(jobs):
            jobs.pop(endajob(jobs))

def isfull(fname):
    "Do all the tiles in this bundle exist"
    with GFile(fname, "rb") as bfile:
        idx = _readbidx(bfile)
    return 0 < min(i >> _OBITS for i in idx)


def fillbundle(srcfile, bfile):
    "Fill missing tiles from a source bundle"
    idx = _readbidx(bfile)
    maxsize = max(idx >> _OBITS for idx in idx)
    gdal.VSIFSeekL(bfile, 0, os.SEEK_END)
    offset = gdal.VSIFTellL(bfile)
    modified = False
    sidx = _readbidx(srcfile)
    gdal.VSIFSeekL(srcfile, 0, os.SEEK_END)
    srcsize = gdal.VSIFTellL(srcfile)
    for i in range(len(idx)):
        if (idx[i] >> _OBITS) != 0 or (sidx[i] >> _OBITS) == 0:
            continue
        size = sidx[i] >> _OBITS
        soffset = sidx[i] - (size << _OBITS)
        maxsize = max(size, maxsize)
        if soffset + size > srcsize:
            raise Exception("Corrupt bundle")
        gdal.VSIFSeekL(srcfile, soffset, os.SEEK_SET)
        record = gdal.VSIFReadL(size, 1, srcfile)
        gdal.VSIFWriteL(struct.pack("<I", size), 4, 1, bfile)
        offset += 4
        gdal.VSIFWriteL(record, len(record), 1, bfile)
        idx[i] = (size << _OBITS) + offset
        offset += len(record)
        modified = True

    # Leave the dst untouched if no tiles were filled
    if modified:
        gdal.VSIFSeekL(bfile, 0, os.SEEK_SET)
        header = _bhead(maxsize, offset)
        gdal.VSIFWriteL(header, len(header), 1, bfile)
        if sys.byteorder != "little":
            idx.byteswap()
        gdal.VSIFWriteL(idx.tobytes(), len(idx), idx.itemsize, bfile)



def filltilespath(srcpath : str, dstpath : str):
    "Given two paths, fills in tiles for all matching bundle names"
    for entry in glob.iglob(path.join(dstpath,"*.bundle")):
        if isfull(entry):
            continue
        srcname = path.join(srcpath, path.basename(entry))
        if gdal.VSIStatL(srcname) is None:
            continue
        with GFile(entry, "r+b") as dstfile:
            STime = DT.now()
            with GFile(srcname, "rb") as srcfile:
                fillbundle(srcfile, dstfile)
            log.info(f"{path.basename(entry)} {DT.now() - STime}")


def filltilesurl(srcurl : str, dstpath : str):
    "Fill with tiles from a service, source should end with ../<level>"
    import requests
    # Could check the service here
    s = requests.session()
    tpos = srcurl.find("tile/")
    if tpos is None:
        raise TypeError("Service URL should end in ../<level>")
    eurl = srcurl[:tpos + 4]
    empty = requests.get(f"{eurl}/-1/0/0")
    if empty:
        log.info(f"Empty tile size {len(empty.content)}")
    else:
        log.info(f"No empty tile found")
    for entry in glob.iglob(path.join(dstpath,"*.bundle")):
        bn = path.basename(entry)
        if isfull(entry):
            log.warning(f"Bundle {bn} is full")
            continue

        STime = DT.now()
        r0, c0 = nameToRC(bn)
        with GFile(entry, "r+b") as bfile:
            idx = _readbidx(bfile)
            maxsize = max(idx >> _OBITS for idx in idx)
            gdal.VSIFSeekL(bfile, 0, os.SEEK_END)
            offset = gdal.VSIFTellL(bfile)
            modified = 0
            for i in range(len(idx)):
                if (idx[i] >> _OBITS) != 0:
                    continue
                row, column = r0 + (i // _BSZ), c0 + (i % _BSZ)
                url = f"{srcurl}/{row}/{column}"
                tile = s.get(url)
                if not tile:
                    continue
                if (tile.headers["Content-type"] != "image/jpeg" and
                    tile.headers["Content-type"] != "image/png"):
                    raise TypeError(f"Response is not an image, from {url}")
                record = tile.content
                size = len(record)
                maxsize = max(size, maxsize)
                if len(empty.content) == size and record == empty.record:
                    continue
                gdal.VSIFWriteL(struct.pack("<I", size), 4, 1, bfile)
                offset += 4
                gdal.VSIFWriteL(record, size, 1, bfile)
                idx[i] = (size << _OBITS) + offset
                offset += len(record)
                modified += 1

            # Write the index only if bundle was modified
            if modified > 0:
                gdal.VSIFSeekL(bfile, 0, os.SEEK_SET)
                header = _bhead(maxsize, offset)
                gdal.VSIFWriteL(header, len(header), 1, bfile)
                if sys.byteorder != "little":
                    idx.byteswap()
                gdal.VSIFWriteL(idx.tobytes(), len(idx), idx.itemsize, bfile)
                log.info(f"{bn} loaded {modified}, {DT.now() - STime}")


def filltiles(source : str, target : str):
    if (source.startswith("http://") or source.startswith("https://")):
        filltilesurl(source, target)
    else:
        filltilespath(source, target)


def overviews(startlevel
        , endlevel = -1
        , p = "."
        , quality = None
        , compression = "JPNG"
        , bands = 4
        , resample = "bilinear"
        , v2 = True
        , nprocs = None
        , fillfrom = None
        , options = None
        ):
    "Build multiple cache overviews levels"

    for l in range(startlevel, endlevel, -1):
        dst = path.join(p, f"L{l:02}")
        src = path.join(p, f"L{(l+1):02}")
        try:
            os.mkdir(dst)
        except:
            pass
        ovrlevel(src, dst, quality = quality, bands = bands,
                compression = compression, resample = resample,
                v2 = v2, nprocs = nprocs, options = options)
        if fillfrom:
            if src.startswith("http"):
                filltiles(fillfrom + f"/{l}", dst)
            else:
                filltiles(path.join(fillfrom, f"L{l:02}"), dst)

def testbundle(src : str, temploc = "/vsimem/", samples = 10, clean = True):
    """
    Test an output bundle with only a couple of random sample tiles
    If the sample tiles are not empty returns true
    """
    # Arbitrary limit, to keep the test time low
    assert samples <= _BSZ, "Too many samples requested"
    from osgeo import gdal_array as gdarr
    from osgeo import gdalconst as gconst

    def randomtiles(n):
        'Generate a set of n random values between 0 and _BSZ2'
        numbers = set()
        while len(numbers) < n:
            numbers.add(randint(0, _BSZ2 -1))
        return numbers

    dst = temploc + "testing"
    bundle = dst + ".test"
    topt = gdal.TranslateOptions(format = "MRF", 
                                 creationOptions = [f"BLOCKSIZE={_PSZ}"
                                     ,"COMPRESS=NONE"
                                     ,"NOCOPY=True"
                                     ,f"DATANAME={bundle}"]
                                 )
    mname = dst + ".mrf"
    gdal.Translate(mname, src, options = topt)
    dsin = gdal.Open(src)
    dsout = gdal.Open(mname, gconst.GA_Update)
    for tilen in randomtiles(samples):
        r, c = tilen // _BSZ , tilen % _BSZ
        dsout.WriteRaster(c * _PSZ, r * _PSZ, _PSZ, _PSZ,
          dsin.ReadRaster(c * _PSZ, r * _PSZ, _PSZ, _PSZ))
    dsout = None
    dsin = None
    val = (gdal.VSIStatL(bundle).size != 0)
    if clean:
        fnames = gdal.ReadDir(temploc)
        for f in fnames:
            if "testing" in f:
                gdal.Unlink(temploc + f)
    return val

def _check(names, source, level, scheme = Cache(), samples = 10):
    resolution = scheme[level]
    origin = scheme.Origin
    bundle_size = resolution * _PPB
    data = list()
    for name in names:
        br, bc = (v // _BSZ for v in nameToRC(name))
        # Chop the bundle from input
        # Ignore the resampling, it's only a test
        vopt = gdal.BuildVRTOptions(
                outputBounds = (
                    origin.X + bc * bundle_size,
                    origin.Y - (br + 1) * bundle_size,
                    origin.X + (bc + 1) * bundle_size,
                    origin.Y - br * bundle_size
                    )
                ,xRes = resolution 
                ,yRes = resolution
                )
        fullname = path.join("/vsimem/", name)
        vrtname = fullname + ".vrt"
        gdal.BuildVRT(vrtname, source, options =  vopt)
        STime = DT.now()
        if testbundle(vrtname, samples = samples):
            data.append(name)
            log.debug(f"{DT.now() - STime} Test {name}")
        gdal.Unlink(vrtname)
    log.debug(f"Got {len(data)} out of {len(names)} {data}")
    return data

def checknames(names, source, level, scheme = Cache(), samples = 10, nprocs = None):
    'Checks the names using nprocs, fifty at a time'
    def endajob(jobs, tau = .1):
        while True:
            for j in range(len(jobs)):
                if jobs[j]["state"].ready():
                    data.extend(jobs[j]["state"].get())
                    return j
            time.sleep(tau)

    def makeslot(jobs):
        if len(jobs) != nprocs:
            return
        if len(jobs) != 0:
            jobs.pop(endajob(jobs))

    def submit(subnames, jobs):
        jobs.append({
              "stime" : DT.now()
            , "state" : pool.apply_async(_check,
                        args = (subnames, source, level),
                        kwds = {"scheme" : scheme, "samples" : samples})
            })

    data = list()
    jobs = list()
    nprocs = nprocs if nprocs else max(1, mp.cpu_count() // 2)
    with mp.Pool(nprocs) as pool:
        for chunk in range(0, len(names), 50):
            makeslot(jobs)
            submit(names[chunk:chunk + 50], jobs)
            log.info(f"{DT.now()} Left {len(names) - chunk}, Found {len(data)}")
        while 0 != len(jobs):
            jobs.pop(endajob(jobs))
            log.info(f"{DT.now()} Found {len(data)}")
    return data

def getoutputs(source, dst, level, scheme = Cache(), samples = 16):
    "In memory only test, returns a list of output bundles that will have data"

    def candidates(certains, potentials):
        "retuns a list of neighbors of certains that are in potentials"
        if len(certains) == 0: # Only at startup
            return potentials
        neighbors = list()
        for name in certains:
            r, c = nameToRC(name)
            cnames = (RCname(tr, tc)
                      for tr in (r - _BSZ, r, r + _BSZ)
                      for tc in (c - _BSZ, c, c + _BSZ))
            neighbors = list(name for name in cnames
                 if ((name not in neighbors) and (name in potentials) and (name not in certains)))
        return neighbors

    #Use lists for consistent results, otherwise sets might be faster but are not ordered
    names = list(tobundles(source, level = level, dryrun = True))
    assert len(names), "Can't generate output bundle names"

    certains = list()
    while True:
        STime = DT.now()
        neighbors = candidates(certains, names)
        log.debug(f"{DT.now() - STime} Candidates are {len(neighbors)} {neighbors}")
        STime = DT.now()
        found = checknames(neighbors, source, level, scheme = scheme, samples = samples)
        log.info(f"{DT.now() - STime} check pass {samples}, got {len(found)}")

        if len(found) == 0:
            break; # Converged
        # Increase tile samples from second turn on
        samples = _BSZ
        log.debug(f"Got extra {found}")
        certains.extend(found)
    return certains

if __name__ == "__main__":
    print ("MaBaker is a module, not a program")