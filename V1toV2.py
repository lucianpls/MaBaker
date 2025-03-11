from argparse import ArgumentParser
from os import path
from mabaker import headerfix, createBundle, _readbidx, GFile

def v1offsets(index):
    # Should be 16384*5 bytes long little endian offsets + 16 bytes of header and 16 of footer
    assert len(index) == 81952, "Invalid index length"
    idx = [index[i] + (index[i+1] << 8) + (index[i+2] << 16) + (index[i+3] << 24) + (index[i+4] << 32)
        for i in range(16, 81936, 5)]
    # Change from col major to row major
    return [idx[col * 128 + row] for row in range(128) for col in range(128)]

def process(args):
    # Get the source path without the extension
    base = path.splitext(args.source)[0]
    basename = path.basename(args.source)
    # Read the index file
    with open(base + ".bundlx", "rb") as f:
        offsets = v1offsets(f.read())
    with open(base + ".bundle", "rb") as f:
        data = f.read()
    # Create the output bundle
    outname = path.join(args.destination + basename)
    createBundle(outname)
    with GFile(outname, "rb") as outbundle:
        outidx = _readbidx(outbundle) # These are 64 bits
    with open(outname, "+rb") as outbundle:
        # Get the current output size
        outbundle.seek(0, 2)
        outoffset = outbundle.tell()
        for i in range(len(outidx)):
            # swap rows w columns
            in_idx = ((i % 128) << 7) + (i // 128)
            off = offsets[in_idx]
            size = data[off] + (data[off + 1] << 8) + (data[off + 2] << 16) + (data[off + 3] << 24)
            if size:
                outbundle.write(size.to_bytes(4, "little") + data[off + 4 : off + 4 + size])
                outidx[i] = outoffset + 4 + (size >> 40)
                outoffset += 4 + size
        # Write the filled index, at offset 64
        outbundle.seek(64)
        outidx.tofile(outbundle)
    headerfix(outname)

def main():
    parser = ArgumentParser(description="Converts a V1 cache to V2 cache, no checks")
    parser.add_argument("source", help="Source bundle file")
    parser.add_argument("destination", help="Destination path")
    args = parser.parse_args()
    process(args)

if __name__ == "__main__":
    main()