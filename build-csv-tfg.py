#!/usr/bin/env python3
# -*- coding: utf-8 -*-
## Build per node CSV files for gufisants use case
## http://dsg.ac.upc.edu/qmpsu/index.php
## build-csv.py

## imports
import numpy as np
import pandas as pd
import json
import importlib
import sys
import click   ## https://click.palletsprojects.com/en/7.x
import os
import fnmatch
import pickle
import gzip
import re
import datetime as dt
import importlib
# import tarfile

imported = {}
def force_import(name):
    if name not in imported:
        imported[name] = importlib.import_module(name)
    else:
        importlib.reload(imported[name])
    return imported[name]
##

##
pwd = os.getcwd()
cmn = force_import("common")
uid = force_import("uid")
##
def load_csvgz_data(f):
    if os.path.exists(f):
        return(pd.read_csv(f, compression='gzip', header=0, sep=','))
    error("file? " + f)
    return None

def read_data_file(fname, callf):
    if os.path.isfile(fname):
        say("reading file: " + fname)
        with open(fname, 'rb') as filehandle:
            return pickle.load(filehandle)
    error("building file: " + fname)
    res = eval(callf)
    if res != None:
        with open(fname, 'wb') as filehandle:
            pickle.dump(res, filehandle)
    else:
        error("eval? " + callf)
    return res

def build_mmdata_df(csvf):
    mmdata = load_csvgz_data(csvf)
    mmdata['date'].fillna(method='ffill', inplace=True)
    return mmdata

def date2POSIXct(d):
  return re.sub(r"(\d\d-\d\d-\d\d)_(\d\d)-(\d\d)-(\d\d)", r"20\1 \2:\3:\4", d)

def date_to_datetime(d):
    val = d[0] if type(d).__name__ == 'ndarray' else d
    fmt = r'%y-%m-%d_%H-%M-%S-GMT' if re.search(r'GMT', val) else r'%y-%m-%d_%H-%M-%S'
    return pd.to_datetime(d, format=fmt)

def date_to_epoch(d):
    val = d[0] if type(d).__name__ == 'ndarray' else d
    fmt = r'%y-%m-%d_%H-%M-%S-GMT' if re.search(r'GMT', val) else r'%y-%m-%d_%H-%M-%S'
    return (pd.to_datetime(d, format=fmt) - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")

## start SplitByNode
class NodeDF:
    def __init__(self, ndf):
        self.df = ndf.copy()
    def __call__(self):
        return self.df
    def add_timestamp_to_df(self):
        self.df.index = date_to_datetime(self.df['date'].values)
        if sum(self.df.index.duplicated()) > 0:
            cmn.error("dropping duplicated dates ({}): ".format(sum(self.df.index.duplicated())))
            print(self.df.index[self.df.index.duplicated()])
            self.df = self.df.loc[~self.df.index.duplicated(),:]
        self.df.drop('date',  axis='columns', inplace=True)
    def field_to_rate(self, field, f_den=None, replace=True):
        def compute_rate(nr, f_num, f_den):
            if f_den == None:
                return (self.df[f_num].iloc[1:nr].values-self.df[f_num].iloc[0:(nr-1)])/(
                    (self.df.index[1:nr].values-self.df.index[0:(nr-1)]).total_seconds())
            else:
                return (self.df[f_num].iloc[1:nr].values-self.df[f_num].iloc[0:(nr-1)])/(
                    self.df[f_den].iloc[1:nr].values-self.df[f_den].iloc[0:(nr-1)])
        if any(self.df[field] == -1): # remove unitialized values (-1)
            for f in field:
                if all(self.df[f] == -1):
                    self.df[f] = 0
            self.df = self.df[(self.df[field] >= 0).all(1)]
        nr = len(self.df.index)
        if nr > 2:
            for f in field:
                if replace:
                    self.df.loc[:, f] = compute_rate(nr, f, f_den)
                    self.df.rename(columns={f:f+'.rate'}, inplace=True)
                else:
                    self.df.insert(0, f+'.rate', compute_rate(nr, f, f_den))
            self.df = self.df[:-1] # last row has no rate
            # remove rows with negative rates (restarted counters)
            self.df = self.df[(self.df[[f+'.rate' for f in field]] >= 0).all(1)]
            return True
        return False

class SplitByNode:
    sn = {}
    typef = None
    mmdata = pd.DataFrame()
    csvf = None
    def __init__(self, csvf, node, force=False):
        if (self.csvf == None) | (self.csvf != csvf) | force:
            self.csvf = csvf
            if not self.mmdata.empty:
                del self.mmdata
                self.mmdata = pd.DataFrame()
        rate_fields = {
            'wifi': ['ltxb', 'lrxb', 'ltxp', 'lrxp'], #
            'ifaces': ['txe', 'rxe', 'txb', 'rxb', 'txp', 'rxp'], #
            'eth': [], #
            'state': ["system", "idle", "user", "nice", "btime",
                    "irq", "ctxt", "softirq", "iowait", "intr",
                    "eth.txe", "eth.rxe", "eth.txb", "eth.rxb", "eth.txp", "eth.rxp",
                    "wifi.txe", "wifi.rxe", "wifi.txb", "wifi.rxb", "wifi.txp", "wifi.rxp"]
        }
        for typef in rate_fields.keys():
            if re.search(typef, csvf):
                self.typef = typef
                break
        if self.typef == None:
            cmn.abort("file type? " + csvf)
        if self.mmdata.empty:
            cmn.say("Reading file " + csvf)
            self.mmdata = build_mmdata_df(csvf)
        cmn.say("Splitting by node")
        if self.typef == 'state':
            self.split_state_by_node(node)
        else:
            self.split_link_by_node()
        cmn.say("Adding timestamps")
        self.add_timestamp_to_dict()
        # cmn.say("Adding rates")
        # for src in self.sn.keys():
        #     if self.typef == 'state':
        #         self.field_to_rate(rate_fields[self.typef], src)
        #     else:
        #         for dst in self.sn[src].keys():
        #             self.field_to_rate(rate_fields[self.typef], src, dst)
        #             if self.typef == 'wifi':
        #                 self.sn[src][dst].field_to_rate(['rtx'], 'ltxp')
    def __call__(self):
        return self.sn
    def get_field(self, f):
        for t in f:
            if t in self.mmdata.columns:
                return(t)
        cmn.abort("get_field: f.src? "+f)
    def split_link_by_node(self):
        f_dst = self.get_field(['dst.uid', 'if.name'])
        f_src = self.get_field(['src.uid', 'uid'])
        if f_src == None:
            cmn.abort("split_link_by_node: f_src?")
        self.sn = {src: {dst: NodeDF(self.mmdata.loc[(self.mmdata[f_src]==src) & 
                                                     (self.mmdata[f_dst]==dst),:])
                         for dst in np.unique(self.mmdata[f_dst][self.mmdata[f_src]==src])}
                   for src in np.unique(self.mmdata[f_src])}
    def split_state_by_node(self, node):
        f_src = self.get_field(['uid'])
        if f_src == None:
            cmn.abort("split_state_by_node: f_src?")
        if node > -1:
            self.sn = {node: NodeDF(self.mmdata.loc[self.mmdata[f_src]==node,:])}
        else:
            self.sn = {src: NodeDF(self.mmdata.loc[self.mmdata[f_src]==src,:])
                       for src in np.unique(self.mmdata[f_src])}
    def add_timestamp_to_dict(self):
        for src in self.sn.keys():
            if self.typef == 'state':
                self.sn[src].add_timestamp_to_df()
            else:
                for dst in self.sn[src].keys():
                    self.sn[src][dst].add_timestamp_to_df()
    def field_to_rate(self, field, src, dst=None):
        if dst == None:
            if not self.sn[src].field_to_rate(field):
                cmn.error("field.to.rate: empty df? ({}/{})".format(src, uid.uid2hname(src)[0]))
        else:
            if not self.sn[src][dst].field_to_rate(field):
                cmn.error("field.to.rate: empty df? ({},{})".format(src, dst))
## end SplitByNode

def proc_file(f, odir, save, node, skip):
    date = cmn.get_date(f)
    of = odir + '/' + date + '-state' + ".csv.gz"
    if os.path.isfile(of) and skip:
        cmn.say("skipping file " + of)
        return
    cmn.say("proc_file: "+f)
    bn = os.path.basename(f)
    m = re.search('^(..-..)', bn)
    if m:
        month = m.group(1)
    ss = SplitByNode(f, node)
    res = pd.DataFrame()
    for n in ss().keys():
        df = ss()[n].df.copy()
        df.columns = df.columns + '-' + str(n)
        res = pd.concat([res, df], axis=1)
    cmn.say("building csv file")
    if ss.typef == 'state':
        if save:
            cmn.say(" " + of)
            res.to_csv(of, sep=',', header=True, 
                       index=True, index_label='datetime', compression='gzip')
        else:
            res.to_csv(sys.stdout, sep=',', header=True, 
                      index=True, index_label='datetime')
    else:
        cmn.abort('type not yet implemented '+ss.typef)

@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.argument('file', type=str, nargs=-1)
@click.option('-o', '--odir', type=str, default='.', show_default=True, help='Output dir')
@click.option('-s', '--save/--no-save', default=True, show_default=True, help='Save output file')
@click.option('-n', '--node', type=int, default=-1, show_default=True, help='Choose node n, -1 for all')
@click.option('-k', '--skip/--no-skip', default=True, show_default=True, help='Skip existing files')
def process(file, odir, save, node, skip):
    cmn.say("cwd: "+os.getcwd())
    file = pwd + '/' + file[0]
    odir = pwd + '/' + odir
    if not file:
        cmn.abort('file?')
    if os.path.isfile(file):
        if os.path.isdir(odir):
            proc_file(file, odir, save, node, skip)
        else:
            cmn.abort('odir? '+odir)
    else:
        cmn.abort('file? '+file)

if __name__ == '__main__':
    process()

exit()

##
## testing
##

from subprocess import call
call(["./build-csv-tfg.py", "--no-save", "23-12-12_14-40-00-GMT-meshmon-state.csv.gz"])
# call(["./build-csv-tfg.py", "23-12-12_14-40-00-GMT-meshmon-state.csv.gz"])

# Local Variables:
# mode: python
# coding: utf-8
# python-indent-offset: 4
# python-indent-guess-indent-offset: t
# End:
