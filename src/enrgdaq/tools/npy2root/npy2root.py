#!/usr/bin/env python

import argparse
import io
import os

import lz4.frame
import numpy as np
import uproot


def read_npy_lz4(file_path):
    """
    Reads a .npy.lz4 file and yields numpy arrays.
    The file is a sequence of (4-byte length)(lz4-compressed numpy array).
    """
    with open(file_path, "rb") as f:
        while True:
            len_bytes = f.read(4)
            if not len_bytes:
                break
            frame_len = int.from_bytes(len_bytes, "little")
            compressed_frame = f.read(frame_len)
            frame = lz4.frame.decompress(compressed_frame)
            with io.BytesIO(frame) as bio:
                # The saved object is a dictionary of numpy arrays, so allow pickle
                yield np.load(bio, allow_pickle=True).item()


def main():
    parser = argparse.ArgumentParser(
        description="Convert .npy.lz4 file to a ROOT file."
    )
    parser.add_argument("input_file", help="Path to the input .npy.lz4 file.")
    parser.add_argument("output_file", help="Path to the output .root file.")
    parser.add_argument(
        "-t",
        "--tree-name",
        default="digitizer_waveforms",
        help="Name of the tree in the ROOT file.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.input_file):
        print(f"Error: Input file not found: {args.input_file}")
        return

    first_chunk = True
    with uproot.recreate(args.output_file) as f:
        for i, data_chunk in enumerate(read_npy_lz4(args.input_file)):
            if first_chunk:
                # For the first chunk, create the tree
                f[args.tree_name] = data_chunk
                first_chunk = False
            else:
                # For subsequent chunks, extend the tree
                f[args.tree_name].extend(data_chunk)
            print(f"Processed chunk {i+1}", end="\r")

    print(f"\nSuccessfully converted {args.input_file} to {args.output_file}")


if __name__ == "__main__":
    main()
