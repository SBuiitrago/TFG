#!/usr/bin/env python3
# -*- coding: utf-8 -*-
## Format data gathered from qMp nodes in GuifiSants
## http://dsg.ac.upc.edu/qmpsu/index.php
## jsonp-main.py
## (c) Llorenç Cerdà-Alabern, May 2020.
## debug: import pdb; pdb.set_trace()

import json
import importlib
import sys
import click   ## https://click.palletsprojects.com/en/7.x
import os
import fnmatch

# sys.path.append('json-proc')
cmn = importlib.import_module("common")
links = importlib.import_module("jsonp-links")

def load_json(jf):
    if os.path.exists(jf):
        uidDB = json.load(open(uidDBfileName))
    else:
        sys.exit("mmdta-uid? " + uidDBfileName)

def get_ofile_name(date, odir, suf, skip):
    if date:
        ofile = odir + '/' + date + '-' + suf + ".csv"
        ofilegz = odir + '/' + date + '-' + suf + ".csv.gz"
        if (os.path.isfile(ofile) or os.path.isfile(ofilegz)) and skip:
            cmn.abort("skip "+ofile)
        else:
            return ofile

def proc_file(f, dir, save, odir, skip, outformat, node):
    """
    """
    if save:
        date = cmn.get_date_ym(f[0]) if len(f) > 1 else cmn.get_date(f[0])
        ofile = get_ofile_name(date, odir, "meshmon-" + outformat, skip)
    else:
        ofile = None
    if outformat.find("links") == 0:
        ll = outformat.split("_")
        try:
            links.build_csv_links(f, dir, ll[1], node, ofile)
        except:
            print(outformat + ": not in scope!")
    else:
        build_csv_func = "build_csv_%s" % outformat
        try:
            if callable(getattr(sys.modules['jsonp-links'], build_csv_func)):
                getattr(sys.modules['jsonp-links'], build_csv_func)(f, dir, node, ofile)
        except:
            cmn.error(build_csv_func + ": not in scope!")

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.argument('file', type=str, nargs=-1)
@click.option('-c', '--count', type=int, default=-1, help='Number of files to process (-1 for all)')
@click.option('-d', '--dir', type=str, default='./', show_default=True, help='File dir')
@click.option('-s', '--save/--no-save', default=True, show_default=True, help='Save output file')
@click.option('-v', '--verbose/--no-verbose', default=False, show_default=True, help='Verbose')
@click.option('-o', '--odir', type=str, default='.', show_default=True, help='Output dir')
@click.option('-k', '--skip/--no-skip', default=True, show_default=True, help='Skip existing files')
@click.option('-f', '--outdata', type=str, default='links_wifi', show_default=True, 
              help='Output data [links_wifi, links_eth, ifaces, state]')
@click.option('-n', '--node', type=int, default=-1, show_default=True, help='Choose node n, -1 for all')
def process(file, count, dir, save, verbose, odir, skip, outdata, node):
    cmn.verbose = verbose
    if file:
        file_list = file
        if not count: count = -1
        dirname = os.path.dirname(file_list[0])
        if dirname: dir = None
    else:
        file_list = fnmatch.filter(os.listdir(dir), 
                                   '*-meshmon-graph.json*')
    if not count: count = 1
    file_list = sorted(file_list, reverse=False)
    if(count > 0): file_list = file_list[0:count]
    if outdata in ['links_wifi', 'links_eth', 'ifaces', 'state']:
        proc_file(file_list, dir, save, odir, skip, outdata, node)
    else:
        cmn.abort(outdata + '?')

if __name__ == '__main__':
    process()

exit()

from subprocess import call
# call(["./jsonp-main.py", "-f", "state", "--no-save", "23-12-12_14-40-00-GMT-meshmon-graph.json.gz"])
call(["./jsonp-main.py", "-f", "state", "23-12-12_14-40-00-GMT-meshmon-graph.json.gz"])

##
## testing
##

# Local Variables:
# mode: python
# coding: utf-8
# python-indent-offset: 4
# python-indent-guess-indent-offset: t
# End:
