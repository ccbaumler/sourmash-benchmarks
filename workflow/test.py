#! usr/bin/env python

import sourmash
import inspect

import os
import gzip
import shutil
import screed
import glob
import json
import csv
import screed
import sourmash
from sourmash import MinHash
from sourmash.sbt import SBT, Node
from sourmash.sbtmh import SigLeaf, load_sbt_index
from sourmash.command_compute import ComputeParameters
from sourmash.cli.compute import subparser
from sourmash.cli import SourmashParser
from sourmash import manifest

from sourmash import signature
from sourmash import VERSION

import argparse

parser = argparse.ArgumentParser()

print(sourmash.commands())

#sourmash('sketch', 'translate', '-p', 'scaled=-5', 'input/raw/SRR1976948_1.fastq.gz')

#inspect.getargspec(sourmash.command_sketch)
#print(inspect.getargspec(sourmash))

import os

indir = 'input/raw'
print(indir)

sample_list = []
for root, dirs, files in os.walk(f'{indir}'):
    for file in files:
        print(file)
    sample_list.extend(files)
print(sample_list)

sample_set = set(sample_list)
print(sample_set)

#SAMPLES=[ os.path.basename(x) for x in sample_set ]
#print(SAMPLES)
SAMPLES=[ os.path.splitext(x)[0] for x in sample_set ]
print(SAMPLES)


for root, dirs, files in os.walk(f'{indir}'):
    simple= [ str.split(file, sep='_')[0] for file in files ]
print(simple)

# Does this work even more eloquently?
#sam = []
#for root, dirs, files in os.walk(indir):
#    sam.extend(str.split(files, sep='_')[0])


import glob

#for root, dirs, files in walk
print(glob.glob(glob.escape(indir) +'/*'))
print(glob.glob(glob.escape(indir) + '/.*_[12].fast(a|q)(.gz|zip|bz2)?'))
