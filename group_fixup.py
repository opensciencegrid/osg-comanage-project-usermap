#!/usr/bin/env python3

import os
import sys
import json
import getopt
import collections
import urllib.error
import urllib.request


SCRIPT = os.path.basename(__file__)
ENDPOINT = "https://registry-test.cilogon.org/registry/"
OSG_CO_ID = 8

GET    = "GET"
PUT    = "PUT"
POST   = "POST"
DELETE = "DELETE"


_usage = f"""\
usage: [PASS=...] {SCRIPT} [OPTIONS]

OPTIONS:
  -u USER[:PASS]      specify USER and optionally PASS on command line
  -c OSG_CO_ID        specify OSG CO ID (default = {OSG_CO_ID})
  -d passfd           specify open fd to read PASS
  -f passfile         specify path to file to open and read PASS
  -e ENDPOINT         specify REST endpoint
                        (default = {ENDPOINT})
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
    user = "co_8.project_script"
    osg_co_id = OSG_CO_ID
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


def get_osg_co_groups():
    return call_api("co_groups.json", coid=options.osg_co_id)


# primary api calls

def get_co_group_identifiers(gid):
    return call_api("identifiers.json", cogroupid=gid)


def get_co_group_members(gid):
    return call_api("co_group_members.json", cogroupid=gid)


def get_co_person_identifiers(pid):
    return call_api("identifiers.json", copersonid=pid)


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


# api call results massagers









def parse_options(args):
    try:
        ops, args = getopt.getopt(args, 'u:c:d:f:e:o:h')
    except getopt.GetoptError:
        usage()

    if args:
        usage("Extra arguments: %s" % repr(args))

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



if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except urllib.error.HTTPError as e:
        print(e, file=sys.stderr)
        sys.exit(1)

