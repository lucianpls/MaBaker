# Converts a level of V1 bundles into a single MRF, by copying tiles

import os
import os.path as path
import glob
import struct
import array
import logging as log
import sys

# It doesn't really need gdal, os.truncate would work fine
import gdal

BSZ = 128
NTILES = BSZ * BSZ

class GFile(object):
    "Context manager for gdal.VSIFile"
    def __init__(self, fname :str, mode : str):
        self.vsifile = gdal.VSIFOpenL(fname, mode)
    def __enter__(self):
        return self.vsifile
    def __exit__(self, type, value, traceback):
        gdal.VSIFCloseL(self.vsifile)

def rcfromname(fname):
    cpos = fname.find('C')
    return int(fname[1:cpos], 16), int(fname[cpos+1:fname.find('.')], 16)

def bigbox(location):
    rows, cols = [], []
    for fname in glob.glob( location + "/*.bundle"):
        row, col = rcfromname(path.basename(fname))
        rows.append(row)
        cols.append(col)
    return min(cols), min(rows), max(cols) + BSZ, max(rows) + BSZ

def getinfo(box, srcpath):
    mx, my, Mx, My = bigbox(srcpath)
    width = Mx - mx
    height = My - my
    return width, height

def getoffsets(fname):
    with open(fname, "rb") as fidx:
        idx = fidx.read()
    assert len(idx) == NTILES * 5 + 16 + 16, "Incorrect external index size"
    offs = array.array("Q")
    # Unpack 5 byte int, low endian
    for i in range(NTILES):
        offs.append(0xffffffffff & struct.unpack_from("<Q", idx, 16 + 5 * i)[0])
    return offs

def cache2mrf(srcpath, dst, version = 2):

    #if sys.platform != "win32":
    #    with open(dst + "mrf_info", "w") as infofile:
    #        startx = -20050000
    #        starty = 30200000
    #        res = 0.26458386250105836 * 256
    #        ox = startx + mx * res
    #        oy = starty - my * res
    #        OX = startx + Mx * res
    #        OY = starty - My * res
    #        infofile.write(f"{mx*256}, {my*256}, {Mx*256}, {My*256}\n")
    #        infofile.write(f"size {width * 256} {height * 256}\n")
    #        infofile.write(f"{ox}, {oy}, {OX}, {OY}\n")

    ## Buckeye
    #if sys.platform != "win32":
    #    with open(dst + "mrf_info", "w") as infofile:
    #        startx = -200
    #        starty = 200
    #        res = .0006091420174925517
    #        ox = startx + mx * res
    #        oy = starty - my * res
    #        OX = startx + Mx * res
    #        OY = starty - My * res
    #        infofile.write(f"{mx*256}, {my*256}, {Mx*256}, {My*256}\n")
    #        infofile.write(f"size {width * 256} {height * 256}\n")
    #        infofile.write(f"{ox}, {oy}, {OX}, {OY}\n")

    width, height = getinfo(bigbox(srcpath))
    idxsz = width * height * 16
    with GFile(dst + ".idx", "wb") as outdata:
        gdal.VSIFTruncateL(outdata, idxsz)
    
    ooff = 0
    bcount = 0
    with open(dst + ".idx", "r+b") as idxf:
        with open(dst + ".pjp", "wb") as datf:
            for fname in glob.glob(srcpath + "/*.bundle"):
                bname = path.splitext(path.basename(fname))[0]
                brow, bcol = rcfromname(path.basename(fname))
                with open(fname, "rb") as f:
                    data : bytes = f.read()
                if version == 1:
                    offsets = getoffsets(f"{srcpath}/{bname}.bundlx")
                for r in range(BSZ):
                    oidx = array.array('Q', (0,) * BSZ * 2)
                    for c in range(BSZ):
                        if version == 1:
                            # offsets are in column major order
                            inoff = offsets[c * BSZ + r]
                            size = struct.unpack_from("<I", data, inoff)[0]
                            inoff += 4
                        else: # index is in row major order, contains offset and size
                            off = 64 + 8 * (r * BSZ + c)
                            inoff = struct.unpack_from("<Q", data, off)[0]
                            size = inoff >> 40 # Top three bytes
                            inoff &= 0xffffffffff # Bottom five bytes
                        if 0 != size:
                            datf.write(data[inoff: inoff + size])
                            oidx[2 * c] = ooff
                            oidx[2 * c + 1] = size
                            ooff += size
                    # Write this index line
                    oidx.byteswap()
                    idxf.seek(16 * ((brow + r - my) * width + bcol - mx))
                    oidx.tofile(idxf)

                bcount += 1
                if 0 == bcount % 10:
                    log.debug(f"{bname} {ooff}")


if __name__ == "__main__":
    srcpath = "//esri.com/Departments/ProfessionalServices/NATO-Imagery_and_Data/DigitalGlobeCache/DigitalGlobe_AFG_Cache/_alllayers/L29"
    dst = "Z:/NATO/GDL29"
    log.basicConfig(level = log.DEBUG, format = "%(asctime)-15s %(message)s")
    if len(sys.argv) > 1:
        srcpath = sys.argv[1]
    if len(sys.argv) > 2:
        dst = sys.argv[2]
    cache2mrf(srcpath, dst, version = 1)
