#!/usr/bin/env python3

import os
import re
import sys
import json
import getopt
import collections
import urllib.error
import urllib.request


SCRIPT = os.path.basename(__file__)
ENDPOINT = "https://registry.cilogon.org/registry/"
USER = "co_7.group_fixup"
OSG_CO_ID = 7

GET    = "GET"
PUT    = "PUT"
POST   = "POST"
DELETE = "DELETE"


_usage = f"""\
usage: [PASS=...] {SCRIPT} [OPTIONS] COGroupNameOrId ProjectName

OPTIONS:
  -u USER[:PASS]      specify USER and optionally PASS on command line
  -c OSG_CO_ID        specify OSG CO ID (default = {OSG_CO_ID})
  -d passfd           specify open fd to read PASS
  -f passfile         specify path to file to open and read PASS
  -e ENDPOINT         specify REST endpoint
                        (default = {ENDPOINT})
  -h                  display this help text

Adds an identifier of type ospoolproject named Yes-ProjectName to
a COGroup based on its Name or CO Group Id.

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
    endpoint  = ENDPOINT
    osg_co_id = OSG_CO_ID
    user      = USER
    authstr   = None
    gid       = None
    gname     = None
    project   = None


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
    return mkrequest2(GET, target, **kw)


def mkrequest2(method, target, **kw):
    return mkrequest3(method, target, data=None, **kw)


def mkrequest3(method, target, data, **kw):
    url = os.path.join(options.endpoint, target)
    if kw:
        url += "?" + "&".join( "{}={}".format(k,v) for k,v in kw.items() )
    req = urllib.request.Request(url, json.dumps(data).encode("utf-8"))
    req.add_header("Authorization", "Basic %s" % options.authstr)
    req.add_header("Content-Type", "application/json")
    req.get_method = lambda: method
    return req


def call_api(target, **kw):
    return call_api2(GET, target, **kw)


def call_api2(method, target, **kw):
    return call_api3(method, target, data=None, **kw)


def call_api3(method, target, data, **kw):
    req = mkrequest3(method, target, data, **kw)
    resp = urllib.request.urlopen(req)
    payload = resp.read()
    return json.loads(payload) if payload else None


# primary api calls


def get_osg_co_groups():
    return call_api("co_groups.json", coid=options.osg_co_id)


def get_co_group_identifiers(gid):
    return call_api("identifiers.json", cogroupid=gid)


def get_co_group_members(gid):
    return call_api("co_group_members.json", cogroupid=gid)


def get_co_person_identifiers(pid):
    return call_api("identifiers.json", copersonid=pid)


def get_co_group(gid):
    grouplist = call_api("co_groups/%d.json" % gid) | get_datalist("CoGroups")
    if not grouplist:
        raise RuntimeError("No such CO Group Id: %s" % gid)
    return grouplist[0]


def get_identifier(id_):
    idfs = call_api("identifiers/%s.json" % id_) | get_datalist("Identifiers")
    if not idfs:
        raise RuntimeError("No such Identifier Id: %s" % id_)
    return idfs[0]


# @rorable
# def foo(x): ...
# x | foo -> foo(x)
class rorable:
    def __init__(self, f): self.f = f
    def __call__(self, *a, **kw): return self.f(*a, **kw)
    def __ror__ (self, x): return self.f(x)


def get_datalist(listname):
    def get(data):
        return data[listname] if data else []
    return rorable(get)


# script-specific functions

def add_project_identifier_to_group(gid, project_name):
    identifier_name = "Yes-%s" % project_name
    type_ = "ospoolproject"
    return add_identifier_to_group(gid, type_, identifier_name)


def add_identifier_to_group(gid, type_, identifier_name):
    new_identifier_info = {
        "Version"    : "1.0",
        "Type"       : type_,
        "Identifier" : identifier_name,
        "Login"      : False,
        "Person"     : {"Type": "Group", "Id": str(gid)},
        "Status"     : "Active"
    }
    data = {
      "RequestType" : "Identifiers",
      "Version"     : "1.0",
      "Identifiers" : [new_identifier_info]
    }
    return call_api3(POST, "identifiers.json", data)


def gname_to_gid(gname):
    groups = get_osg_co_groups() | get_datalist("CoGroups")
    matching = [ g for g in groups if g["Name"] == gname ]

    if len(matching) > 1:
        raise RuntimeError("Multiple groups found with Name '%s'" % gname)
    elif not matching:
        raise RuntimeError("No group found with Name '%s'" % gname)

    group = matching[0]
    return group["Id"]


# CLI


def parse_options(args):
    try:
        ops, args = getopt.getopt(args, 'u:c:d:f:e:h')
    except getopt.GetoptError:
        usage()

    if len(args) != 2:
        usage()

    cogroup, project = args
    if re.fullmatch(r'\d+', cogroup):
        options.gid = int(cogroup)
    else:
        options.gname = cogroup
    options.project = project

    passfd = None
    passfile = None

    for op, arg in ops:
        if op == '-h': usage()
        if op == '-u': options.user      = arg
        if op == '-c': options.osg_co_id = int(arg)
        if op == '-d': passfd            = int(arg)
        if op == '-f': passfile          = arg
        if op == '-e': options.endpoint  = arg

    user, passwd = getpw(options.user, passfd, passfile)
    options.authstr = mkauthstr(user, passwd)


def main(args):
    parse_options(args)

    if options.gname:
        options.gid = gname_to_gid(options.gname)
    else:
        options.gname = get_co_group(options.gid)["Name"]

    print('Creating new Identifier for project "%s"\n'
          'for CO Group "%s" (%s)'
          % (options.project, options.gname, options.gid))
    print("")

    resp = add_project_identifier_to_group(options.gid, options.project)

    print("Server Response:")
    print(json.dumps(resp, indent=2, sort_keys=True))

    new_identifier = get_identifier(resp["Id"])
    print("")
    print("New Identifier Object:")
    print(json.dumps(new_identifier, indent=2, sort_keys=True))

    # no exceptions, must have worked
    print("")
    print(":thumbsup:")


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except (RuntimeError, urllib.error.HTTPError) as e:
        print(e, file=sys.stderr)
        sys.exit(1)

