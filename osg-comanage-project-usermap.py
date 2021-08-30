#!/usr/bin/env python3

import os
import sys
import json
import getopt
import collections
import urllib.request


ENDPOINT = "https://registry-test.cilogon.org/registry/"


_usage = """\
usage: [PASS=...] {script} [OPTIONS]

OPTIONS:
  -u USER[:PASS]      specify USER and optionally PASS on command line
  -d passfd           specify open fd to read PASS
  -f passfile         specify path to file to open and read PASS
  -e ENDPOINT         specify REST endpoint
  -o outfile          specify output file (default: write to stdout)
  -h                  display this help text

PASS for USER is taken from the first of:
  1. -u USER:PASS
  2. -d passfd (read from fd)
  3. -f passfile (read from file)
  4. read from $PASS env var

ENDPOINT defaults to {ENDPOINT}
"""

def usage(msg=None):
    if msg:
        print(msg + "\n", file=sys.stderr)

    script = os.path.basename(__file__)
    print(_usage.format(script=script, ENDPOINT=ENDPOINT), file=sys.stderr)
    sys.exit()


class Options:
    endpoint = ENDPOINT
    user = "co_8.project_script"
    outfile = None
    authstr = None


options = Options()


def getpw(user, passfd, passfile):
    if ':' in user:
        user, pw = user.split(':', 1)
    elif passfd is not None:
        pw = os.fdopen(passfd).readline().rstrip('\n')
    elif passfile is not None:
        pw = open(passfile).readline().rstrip('\n')
    elif 'PASS' in os.environ:
        pw = os.environ['PASS']
    else:
        usage("PASS required")
    return user, pw


def mkauthstr(user, passwd):
    from base64 import encodebytes
    raw_authstr = '%s:%s' % (user, passwd)
    return encodebytes(raw_authstr.encode()).decode().replace('\n', '')


def mkrequest(target, **kw):
    url = os.path.join(options.endpoint, target)
    if kw:
        url += "?" + "&".join( "{}={}".format(k,v) for k,v in kw.items() )
    req = urllib.request.Request(url)
    req.add_header("Authorization", "Basic %s" % options.authstr)
    req.get_method = lambda: 'GET'
    return req


def call_api(target, **kw):
    req = mkrequest(target, **kw)
    resp = urllib.request.urlopen(req)
    payload = resp.read()
    return json.loads(payload) if payload else None


def get_osg_co_groups():
    OSG_CO_ID = 8
    return call_api("co_groups.json", coid=OSG_CO_ID)


# primary api calls

def get_co_group_identifiers(gid):
    return call_api("identifiers.json", cogroupid=gid)


def get_co_group_members(gid):
    return call_api("co_group_members.json", cogroupid=gid)


def get_co_person_identifiers(pid):
    return call_api("identifiers.json", copersonid=pid)


def get_datalist(data, listname):
    return data[listname] if data else []


# api call results massagers

def get_osg_co_groups__map():
    #print("get_osg_co_groups__map()")
    data = get_datalist(get_osg_co_groups(), "CoGroups")
    return { g["Id"]: g["Name"] for g in data }


def co_group_is_ospool(gid):
    #print(f"co_group_is_ospool({gid})")
    data = get_datalist(get_co_group_identifiers(gid), "Identifiers")
    return any( i["Type"] == "ospoolproject" for i in data )


def get_co_group_members__pids(gid):
    #print(f"get_co_group_members__pids({gid})")
    data = get_datalist(get_co_group_members(gid), "CoGroupMembers")
    return [ m["Person"]["Id"] for m in data ]


def get_co_person_osguser(pid):
    #print(f"get_co_person_osguser({pid})")
    data = get_datalist(get_co_person_identifiers(pid), "Identifiers")
    typemap = { i["Type"]: i["Identifier"] for i in data }
    return typemap.get("osguser")


def parse_options(args):
    try:
	ops, args = getopt.getopt(args, 'u:d:f:e:o:h')
    except getopt.GetoptError:
	usage()

    if args:
        usage("Extra arguments: %s" % repr(args))

    passfd = None
    passfile = None

    for op, arg in ops:
        if op == '-h': usage()
        if op == '-u': options.user     = arg
        if op == '-d': passfd           = int(arg)
        if op == '-f': passfile         = arg
        if op == '-e': options.endpoint = arg
        if op == '-o': options.outfile  = arg

    user, passwd = getpw(options.user, passfd, passfile)
    options.authstr = mkauthstr(user, passwd)


def gid_pids_to_osguser_pid_gids(gid_pids, pid_osguser):
    pid_gids = collections.defaultdict(set)

    for gid in gid_pids:
        for pid in gid_pids[gid]:
            if pid_osguser[pid] is not None:
                pid_gids[pid].add(gid)

    return pid_gids


def get_osguser_groups():
    groups = get_osg_co_groups__map()
    ospool_gids = filter(co_group_is_ospool, groups)
    gid_pids = { gid: get_co_group_members__pids(gid) for gid in ospool_gids }
    all_pids = set( pid for gid in gid_pids for pid in gid_pids[gid] )
    pid_osguser = { pid: get_co_person_osguser(pid) for pid in all_pids }
    pid_gids = gid_pids_to_osguser_pid_gids(gid_pids, pid_osguser)

    return { pid_osguser[pid]: sorted(map(groups.get, gids))
             for pid, gids in pid_gids.items() }


def print_usermap_to_file(osguser_groups, file):
    for osguser, groups in osguser_groups.items():
        print("* {} {}".format(osguser, ",".join(groups)), file=file)


def print_usermap(osguser_groups):
    if options.outfile:
        with open(options.outfile, "w") as w:
            print_usermap_to_file(osguser_groups, w)
    else:
        print_usermap_to_file(osguser_groups, sys.stdout)


def main(args):
    parse_options(args)

    osguser_groups = get_osguser_groups()
    print_usermap(osguser_groups)


if __name__ == "__main__":
    main(sys.argv[1:])

