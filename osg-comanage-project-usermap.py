#!/usr/bin/env python3

import os
import sys
import getopt
import collections
import comanage_utils as utils


SCRIPT = os.path.basename(__file__)
ENDPOINT = "https://registry.cilogon.org/registry/"
OSG_CO_ID = 7


_usage = f"""\
usage: [PASS=...] {SCRIPT} [OPTIONS]

OPTIONS:
  -u USER[:PASS]      specify USER and optionally PASS on command line
  -c OSG_CO_ID        specify OSG CO ID (default = {OSG_CO_ID})
  -d passfd           specify open fd to read PASS
  -f passfile         specify path to file to open and read PASS
  -e ENDPOINT         specify REST endpoint
                        (default = {ENDPOINT})
  -o outfile          specify output file (default: write to stdout)
  -g filter_group     filter users by group name (eg, 'ap1-login')
  -i inputfiles       specify a comma-delineated list of local mapfiles to merge into outfile
  -h                  display this help text

PASS for USER is taken from the first of:
  1. -u USER:PASS
  2. -d passfd (read from fd)
  3. -f passfile (read from file)
  4. read from $PASS env var
"""

def usage(msg=None):
    if msg:
        print(msg + "\n", file=sys.stderr)

    print(_usage, file=sys.stderr)
    sys.exit()


class Options:
    endpoint = ENDPOINT
    user = "co_7.project_script"
    osg_co_id = OSG_CO_ID
    outfile = None
    authstr = None
    filtergrp = None
    inputfiles = []


options = Options()


# api call results massagers

def get_osg_co_groups__map():
    #print("get_osg_co_groups__map()")
    resp_data = utils.get_osg_co_groups(options.osg_co_id, options.endpoint, options.authstr)
    data = utils.get_datalist(resp_data, "CoGroups")
    return { g["Id"]: g["Name"] for g in data }


def co_group_is_ospool(gid):
    #print(f"co_group_is_ospool({gid})")
    resp_data = utils.get_co_group_identifiers(gid, options.endpoint, options.authstr)
    data = utils.get_datalist(resp_data, "Identifiers")
    return any( i["Type"] == "ospoolproject" for i in data )


def get_co_group_members__pids(gid):
    #print(f"get_co_group_members__pids({gid})")
    resp_data = utils.get_co_group_members(gid,  options.endpoint, options.authstr)
    data = utils.get_datalist(resp_data, "CoGroupMembers")
    # For INF-1060: Temporary Fix until "The Great Project Provisioning" is finished
    return [ m["Person"]["Id"] for m in data if m["Member"] == True]


def get_co_person_osguser(pid):
    #print(f"get_co_person_osguser({pid})")
    resp_data = utils.get_co_person_identifiers(pid, options.endpoint, options.authstr)
    data = utils.get_datalist(resp_data, "Identifiers")
    typemap = { i["Type"]: i["Identifier"] for i in data }
    return typemap.get("osguser")


def parse_options(args):
    try:
        ops, args = getopt.getopt(args, 'u:c:d:f:g:e:o:i:h')
    except getopt.GetoptError:
        usage()

    if args:
        usage("Extra arguments: %s" % repr(args))

    passfd = None
    passfile = None

    for op, arg in ops:
        if op == '-h': usage()
        if op == '-u': options.user       = arg
        if op == '-c': options.osg_co_id  = int(arg)
        if op == '-d': passfd             = int(arg)
        if op == '-f': passfile           = arg
        if op == '-e': options.endpoint   = arg
        if op == '-o': options.outfile    = arg
        if op == '-g': options.filtergrp  = arg
        if op == '-i': options.inputfiles = arg.split(",")

    try:
        user, passwd = utils.getpw(options.user, passfd, passfile)
        options.authstr = utils.mkauthstr(user, passwd)
    except PermissionError:
        usage("PASS required")


def gid_pids_to_osguser_pid_gids(gid_pids, pid_osguser):
    pid_gids = collections.defaultdict(set)

    for gid in gid_pids:
        for pid in gid_pids[gid]:
            if pid_osguser[pid] is not None:
                pid_gids[pid].add(gid)

    return pid_gids


def filter_by_group(pid_gids, groups, filter_group_name):
    groups_idx = { v: k for k,v in groups.items() }
    filter_gid = groups_idx[filter_group_name]  # raises KeyError if missing
    filter_group_pids = set(get_co_group_members__pids(filter_gid))
    return { p: g for p,g in pid_gids.items() if p in filter_group_pids }


def get_osguser_groups(filter_group_name=None):
    groups = get_osg_co_groups__map()
    ospool_gids = filter(co_group_is_ospool, groups)
    gid_pids = { gid: get_co_group_members__pids(gid) for gid in ospool_gids }
    all_pids = set( pid for gid in gid_pids for pid in gid_pids[gid] )
    pid_osguser = { pid: get_co_person_osguser(pid) for pid in all_pids }
    pid_gids = gid_pids_to_osguser_pid_gids(gid_pids, pid_osguser)
    if filter_group_name is not None:
        pid_gids = filter_by_group(pid_gids, groups, filter_group_name)

    return { pid_osguser[pid]: sorted(map(groups.get, gids))
             for pid, gids in pid_gids.items() }


def parse_inputfile(inputfile: str):
    user_groupmap = dict()
    with open(inputfile) as file:
        for line in file:
            split_line = line.split()
            if split_line[0] == "*" and len(split_line) >= 3:
                user_groupmap[split_line[1]] = split_line[2].split(",")
    return user_groupmap


def merge_maps(maps: list[dict[str, list]]):
    while len(maps) > 1:
        merge_target = maps[0]
        merge_material = maps.pop(1)
        for key in merge_material.keys():
            if key in merge_target:
                merge_target[key].extend(merge_material[key])
                merge_target[key] = list(set(merge_target[key]))
            else:
                merge_target[key] = merge_material[key]
    return maps[0]


def merge_inputfiles(osguser_groups):
    maps = [osguser_groups]
    for inputfile in options.inputfiles:
        maps.append(parse_inputfile(inputfile))
    return merge_maps(maps)

def print_usermap_to_file(osguser_groups, file):
    for osguser, groups in sorted(osguser_groups.items()):
        print("* {} {}".format(osguser, ",".join(group.strip() for group in groups)), file=file)


def print_usermap(osguser_groups):
    if options.outfile:
        with open(options.outfile, "w") as w:
            print_usermap_to_file(osguser_groups, w)
    else:
        print_usermap_to_file(osguser_groups, sys.stdout)


def main(args):
    parse_options(args)

    osguser_groups = get_osguser_groups(options.filtergrp)
    osguser_groups_merged = merge_inputfiles(osguser_groups)
    print_usermap(osguser_groups_merged)


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except Exception as e:
        sys.exit(e)
