#!/usr/bin/python3 -d
# -*- coding: utf-8 -*-
## Format data gathered from qMp nodes in GuifiSants
## http://dsg.ac.upc.edu/qmpsu/index.php
## meshmon-main.py
## (c) Llorenç Cerdà-Alabern, May 2020.
## debug: import pdb; pdb.set_trace()

import os
import sys
import csv
import json
import importlib
from operator import itemgetter
import click   ## https://click.palletsprojects.com/en/7.x
import fnmatch
import re
import gzip

# wd
pwd = os.getcwd()
if not os.path.exists("meshmon-parser.py"):
    wd = os.environ['HOME'] + '/meshmon/scripts'
    if os.path.exists(wd):
        os.chdir(wd)

# local modules
imported = {}
def force_import(name):
    global imported
    if name not in imported:
        imported[name] = importlib.import_module(name)
    else:
        importlib.reload(imported[name])
    return imported[name]

# sys.path.append('parser')
cmn = force_import("common")
uid = force_import("uid")
par = force_import("meshmon-parser")
fmt = force_import("meshmon-format")
indent_json = None

cmn.error('pwd: ' + pwd)

def get_date(f):
    # re_pat = re.compile('(?P<date>\d\d-\d\d-\d\d_\d\d-\d\d-\d\d)', re.VERBOSE)
    re_pat = re.compile('(?P<date>\d\d-\d\d-\d\d_\d\d-\d\d-\d\d(?:-GMT)?)', re.VERBOSE)
    match = re_pat.search(f)
    if match:
        return(match.group('date'))

def read_qmpdb(f):
    par.parse_file(f)
    uid.add_uid(par.qmpdb)

def build_graph(f):
    read_qmpdb(f)
    res = True
    try:
        fmt.build_graph(par.qmpdb)
    except:
        res = False
    return res

def build_rt(f):
    read_qmpdb(f)
    fmt.build_rt(par.qmpdb)

def get_ofile_name(f, odir, suf, skip):
    date = get_date(f)
    odir = check_dir(odir)
    if date:
        ofile =  odir + '/' + date + '-' + suf + ".json"
        ofilegz = odir + '/' + date + '-' + suf + ".json.gz"
        if (os.path.isfile(ofile) or os.path.isfile(ofilegz)) and skip:
            cmn.error("skip "+ofile)
        else:
            return ofile

def save_json(ofile, data):
    cmn.say(ofile)
    if(re.search(r'gz$', ofile)):
        with gzip.open(ofile, 'wt') as f:
            f.write(json.dumps(data, indent=2))
    else:
        with open(ofile, 'w') as f:
            f.write(json.dumps(data))
            # f.write(json.dumps(data, indent=2))

def save_raw_json(f, save, odir, skip):
    global indent_json
    if save:
        ofile = get_ofile_name(f, odir, "meshmon-raw", skip) + '.gz'
        if ofile:
            read_qmpdb(f)
            save_json(ofile, par.qmpdb)
    else:
        read_qmpdb(f)
        print(json.dumps(par.qmpdb, indent=indent_json))

def save_graph_json(f, save, odir, skip):
    global indent_json
    if save:
        ofile = get_ofile_name(f, odir, "meshmon-graph", skip) + '.gz'
        cmn.say("save_graph_json: {}".format(ofile))
        if ofile:
            if build_graph(f):
                save_json(ofile, fmt.graph)
            else:
                cmn.error("could not build graph, skipping " + f)
    else:
        build_graph(f)
        print(json.dumps(fmt.graph, indent=indent_json))

def save_rt(f, save, odir, skip):
    global indent_json
    if save:
        ofile = get_ofile_name(f, odir, "meshmon-rt", skip)
        if ofile:
            build_rt(f)
            save_json(ofile, fmt.tabs)
    else:
        build_rt(f)
        print(json.dumps(fmt.tabs, indent=indent_json))

def save_csvm(f, save, odir, skip):
    global indent_json
    if save:
        ofile = get_ofile_name(f, odir, "meshmon-csvm", skip)
        if ofile:
            build_rt(f)
            save_csv(ofile, fmt.tabs)
    else:
        build_rt(f)
        print(json.dumps(fmt.tabs, indent=indent_json))

def proc_file(f, save, odir, skip, outformat):
    """
    """
    save_func = "save_%s" % outformat
    try:
        if callable(getattr(sys.modules[__name__], save_func)):
            getattr(sys.modules[__name__], save_func)(f, save, odir, skip)
    except:
        cmn.error("{}:{}?".format(outformat, f))

def check_dir(dir):
    global pwd
    if not os.path.isdir(dir):
        if os.path.isdir(pwd + '/' + dir):
            dir = pwd + '/' + dir
        else:
            cmn.abort(dir + '?')
    return dir

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.argument('file', type=str, nargs=-1)
@click.option('-c', '--count', type=int, default=None, help='Number of files to process (-1 for all)')
@click.option('-d', '--dir', default='mmdata', show_default=True, type=str, help='File dir')
@click.option('-s', '--save/--no-save', default=True, show_default=True, help='Save output file')
@click.option('-i', '--indent/--no-indent', default=True, show_default=True, help='indent json')
@click.option('-v', '--verbose/--no-verbose', default=False, show_default=True, help='Verbose')
@click.option('-o', '--odir', type=str, default=pwd, show_default=True, help='Output dir')
@click.option('-k', '--skip/--no-skip', default=True, show_default=True, help='Skip existing files')
@click.option('-f', '--outformat', type=str, default='graph_json', show_default=True, 
              help='Output format [raw_json graph_json rt]')
def process(file, count, dir, save, indent, verbose, odir, skip, outformat):
    global indent_json
    if indent: indent_json = 2
    cmn.verbose = verbose
    dir = check_dir(dir)
    subdir = next(os.walk(dir))
    if len(subdir[1]) > 1:
        dir = dir + '/' + sorted(subdir[1], reverse=True)[0]
    if file:
        file_list = file
        if not count: count = -1
        # dirname = os.path.dirname(file_list[0])
        # if dirname: dir = None
    else:
        file_list = fnmatch.filter(os.listdir(dir), 
                                   '*-qmpnodes-gather-meshmon*.data.gz')
    if not count: count = 1
    file_list = sorted(file_list, reverse=False)
    if(count > 0): file_list = file_list[0:count]
    for f in file_list:
        f = dir + '/' + f
        if os.path.isfile(f):
            cmn.error(f)
            if outformat in ['raw_json', 'graph_json', 'rt', 'csvm']:
                proc_file(f, save, odir, skip, outformat)                
            else:
                cmn.abort(outformat + '?')
        else:
            cmn.error(f + '?')

if __name__ == '__main__':
    process()

sys.exit()

# Local Variables:
# mode: python
# coding: utf-8
# python-indent-offset: 4
# python-indent-guess-indent-offset: t
# End:
