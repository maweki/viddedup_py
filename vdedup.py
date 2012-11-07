# -*- coding: utf-8 -*-
"""
Based on a ruby script by hackeron<https://github.com/hackeron>
https://github.com/hackeron/ruby_experiments/blob/master/dedup.rb

This script searches for duplicate videos in a folder.
It grabs frames from a video, transforms them and compares them to
other frames to find duplicate videos.
I rewrote hackeron's script in python because I could not get parallel
processing to work in ruby and I didn't like the output.
Python chews through the videos in massive speed.

Prerequisites: Linux, mplayer, mogrify

@author: maweki
@license: GPL
"""

import os
import mimetypes
from multiprocessing import Pool
import subprocess
import tempfile
import hashlib
import multiprocessing
import argparse

parser = argparse.ArgumentParser(description='Finds video duplicates')
parser.add_argument('--dir', metavar='vid_path', default=os.path.dirname(os.path.realpath(__file__)))
parser.add_argument('--frames', metavar='frames_to_grab', type=int, default=100)
parser.add_argument('--starttime', metavar='grab_start_time', default="00:01:00")

try:
	cpus = multiprocessing.cpu_count()*2
except:
	cpus = 2
parser.add_argument('--threads', metavar='threads', type=int, default=cpus)
args = parser.parse_args()
cpus = args.threads


vid_path = args.dir
print vid_path

def getfiles(directory):
    mimetypes.init()
    f = []
    for g in os.listdir(directory):
        d = os.path.join(directory,g)
        if os.path.isdir(d):
            f.extend(getfiles(d))
        else:
            mime = mimetypes.guess_type(d)
            if mime[0] != None and mime[0].startswith('video'):
                f.append(d)
            else:
                print "ignored:", d, mime
    return f
    
def md5Checksum(filePath):
    fh = open(filePath, 'rb')
    m = hashlib.md5()
    while True:
        data = fh.read(8192)
        if not data:
            break
        m.update(data)
    return m.hexdigest()

md5_hash = {}
dup_hash = {}

def workvideo(video):
    
    nulldevice = open(os.devnull, "w")

    global args    
    
    md5_hash = {}
            
    TEST_FRAMES = args.frames
    START_TIME = args.starttime
    tmpdir = tempfile.mkdtemp()
    print "Working on",video,"in",tmpdir
    ret = subprocess.call(['mplayer', '-nosound', '-vo', 'jpeg:outdir='+tmpdir, '-ss', START_TIME,
              '-frames', str(TEST_FRAMES), video], stdout=nulldevice, stderr=nulldevice)
    if (ret != 0):
        print video, "...failed, skipping...\n"
    
    ret = subprocess.call(['mogrify', '-resize', '32x32!', '-threshold', '50%',
              '-format', 'bmp', os.path.join(tmpdir, '*.jpg')], stdout=nulldevice, stderr=nulldevice)    
    
    for g in os.listdir(tmpdir):
        if g.endswith('.bmp'):
            f = os.path.join(tmpdir, g)
            md5sum = md5Checksum(f)
            if (md5_hash.has_key(md5sum)):
                md5_hash[md5sum].append(video)
            else:
                md5_hash[md5sum] = [video]
    subprocess.call(['rm', '-rf', tmpdir], stdout=nulldevice, stderr=nulldevice)
    return md5_hash
    
files = getfiles(vid_path)


workers = Pool(cpus)
n = len(files)
md5sums = workers.map(workvideo, files)


for one_dict in md5sums:
    for key in one_dict.keys():
        val = one_dict[key]
        if (md5_hash.has_key(key)):
            md5_hash[key].extend(val)
        else:
            md5_hash[key] = val
            
dups = []
            
for key in md5_hash.keys():
    setlist = set(md5_hash[key])
    if (len(setlist) > 1) and (setlist not in dups):
        print "Possible dup", setlist
        dups.append(setlist)