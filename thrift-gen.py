#! /usr/bin/python3

import os
import sys

# todo: change to env
thrift_bin="/usr/local/bin/thrift"
prefix="github.com/Qthai16/go-playground/gen-go/"
in_dir="thrift"
out_dir="gen-go"

def gen_thrift(in_path, out_path):
    os.system("mkdir -p %s" % (out_path))
    for root, _, files in os.walk(in_path):
        for file in files:
            if file.endswith(".thrift"):
                fullPath = os.path.join(root, file)
                print("gen thrift file: %s to: %s" % (fullPath, out_path))
                os.system("%s -r --gen go:skip_remote,package_prefix=%s -out %s %s" %(thrift_bin, prefix, out_path, fullPath))

if __name__ == "__main__":
    gen_thrift(in_dir, out_dir)
