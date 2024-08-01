#
# Name: convert_to_jpg.py
#
#  Converts an existing cache from PNG to JPG
#  Run in each folder than contains bundles
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

import mabaker
import glob
from osgeo import gdal
import os.path as path
import os

def main(opath = 'dest'):
    for f in glob.glob("*/*.bundle"):
        print(f)
#        mabaker.tomrf(f, compression='PNG', bands='3')
        mabaker.towms(f, os.getcwd(), compression='PNG', bands='4')
        base = path.splitext(f)[0]
        src = base + ".wms"
        dst = path.join(opath, base)
        bname = dst + ".bundle"
        mabaker.createBundle(bname)

        topt = gdal.TranslateOptions(
                format = 'MRF',
                bandList = [ 1, 2, 3],
                creationOptions = [
                    "BLOCKSIZE=256",
                    "COMPRESS=JPEG",
                    "SPACING=4",
                    "DATANAME={}".format(bname),
                    "OPTIONS='JFIF:1 OPTIMIZE:1'",
                    "QUALITY=75"
                    ]
                )

        print(src, dst)

        gdal.Translate(dst + ".mrf", src, options=topt)

        mabaker.mrfbundlefix(bname)


if __name__ == "__main__":
    main()
