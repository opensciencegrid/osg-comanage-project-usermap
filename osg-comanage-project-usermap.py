#!/usr/bin/env python3

import os
import sys
import json
import getopt
import urllib.request


ENDPOINT = "https://registry-test.cilogon.org/registry/"


_usage = """\
usage: [PASS=...] {script} [-u USER[:PASS]] [-d passfd] [-e ENDPOINT]

PASS for USER is taken from the first of:
  1. -u USER:PASS
  2. -d passfd (read from fd)
  3. read from $PASS env var

ENDPOINT defaults to {ENDPOINT}
"""

def usage(msg=None):
    if msg:
        print(msg + "\n", file=sys.stderr)

    script = os.path.basename(__file__)
    print(_usage.format(script=script, ENDPOINT=ENDPOINT))
    sys.exit()


class Options:
    endpoint = ENDPOINT
    user = "co_8.project_script"
    authstr = None


options = Options()


def getpw(user, passfd=None):
    if ':' in user:
        user, pw = user.split(':', 1)
    elif passfd is not None:
        pw = os.fdopen(passfd).readline().rstrip('\n')
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
    url = options.endpoint + target
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
    data = get_datalist(get_osg_co_groups(), "CoGroups")
    return { g["Id"]: g["Name"] for g in data }


def co_group_is_ospool(gid):
    data = get_datalist(get_co_group_identifiers(gid), "Identifiers")
    return any( i["Type"] == "ospoolproject" for i in data )


def get_co_group_members__pids(gid):
    data = get_datalist(get_co_group_members(gid), "CoGroupMembers")
    return [ m["Person"]["Id"] for m in data ]


def get_co_person_osguser(pid):
    data = get_datalist(get_co_person_identifiers(pid), "Identifiers")
    typemap = { i["Type"]: i["Identifier"] for i in data }
    return typemap.get("osguser")


def main(args):
    ops, args = getopt.getopt(args, 'u:d:')
    ops = dict(ops)

    passfd = None

    if '-u' in ops: options.user     =     ops['-u']
    if '-d' in ops: passfd           = int(ops['-d'])
    if '-e' in ops: options.endpoint =     ops['-e']

    options.user, passwd = getpw(options.user, passfd)
    options.authstr = mkauthstr(options.user, passwd)


