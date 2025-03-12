#
# Name: 122.py
#
# Copyright 2025 Esri
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
# Description: Converts a V1 esri bundle to a V2 bundle
#
# Contributors:  Lucian Plesea
# Created: 2025-03-11
#

from argparse import ArgumentParser
from os import path
from sys import byteorder
from array import array
from mmap import mmap, ACCESS_READ
import struct

def v1offsets(index):
    # Should be 16384*5 bytes long little endian offsets + 16 bytes of header and 16 of footer
    assert len(index) == 81952, "Invalid index length"
    idx = [int.from_bytes(index[i:i+5], "little") for i in range(16, 81936, 5)]
    # Transpose rows and columns
    return [idx[col * 128 + row] for row in range(128) for col in range(128)]

def v2header(maxRecord = 0, fileSize = 0, 
        index : bytes = None, bsz = 128) -> bytes:
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

def readbidx(bfile):
    "Read a V2 bundle index from a bundle file"
    bfile.seek(64)
    idx = array("Q")
    idx.fromfile(bfile, 16384)
    return idx

def createBundle(name):
    _EOFF = 36                  # The offset for empty tiles
    _BSZ = 128                  # The bundle size
    "Create an empty V2 bundle"
    with open(name, "wb") as data_file:
        data_file.write(v2header())
        array('Q', (_EOFF,) * _BSZ * _BSZ).tofile(data_file)

def headerfix(name):
    "Update the maxrecord and file size fields of a V2 bundle"
    with open(name, "r+b") as bundle:
        idx = readbidx(bundle)
        maxsize = max(x >> 40 for x in idx)
        bundle.seek(0, 2)
        header = v2header(maxsize, bundle.tell())
        bundle.seek(0)
        bundle.write(header)

def process(args):
    # Get the source path without the extension
    base = path.splitext(args.source)[0]
    basename = path.basename(args.source)
    # Read the index file
    with open(base + ".bundlx", "rb") as f:
        offsets = v1offsets(f.read())
    with open(base + ".bundle", "rb") as f:
        data = mmap(f.fileno(), 0, access=ACCESS_READ)
    # Create the output bundle
    outname = path.join(args.destination + basename)
    createBundle(outname)
    with open(outname, "+rb") as outbundle:
        # Get the current output size
        outidx = readbidx(outbundle) # These are 64 bits
        outbundle.seek(0, 2)
        outoffset = outbundle.tell()
        for i in range(len(outidx)):
            off = offsets[i]
            size = int.from_bytes(data[off:off+4], "little")
            if size:
                outbundle.write(data[off : off + 4 + size])
                outidx[i] = outoffset + 4 + (size << 40)
                outoffset += 4 + size
        # Write the filled index, at offset 64
        outbundle.seek(64)
        outidx.tofile(outbundle)
    data.close()
    headerfix(outname)

def main():
    assert byteorder == "little", "This script only works on little endian machines"
    parser = ArgumentParser(description="Converts a V1 cache to V2 cache, no checks")
    parser.add_argument("source", help="Source bundle file")
    parser.add_argument("destination", help="Destination path")
    args = parser.parse_args()
    process(args)

if __name__ == "__main__":
    main()
