#!/usr/bin/env python3

import os
import re
import sys
import json
import getopt
import urllib.error
import urllib.request

SCRIPT = os.path.basename(__file__)
ENDPOINT = "https://registry-test.cilogon.org/registry/"
OSG_CO_ID = 8
MINTIMEOUT = 5
MAXTIMEOUT = 625
TIMEOUTMULTIPLE = 5

GET = "GET"
PUT = "PUT"
POST = "POST"
DELETE = "DELETE"

OSPOOL_PROJECT_PREFIX_STR = "Yes-"
PROJECT_GIDS_START = 200000

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
  -t minTimeout       set minimum timeout, in seconds, for API call (default to {MINTIMEOUT})
  -T maxTimeout       set maximum timeout, in seconds, for API call (default to {MAXTIMEOUT})
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
    user = "co_8.william_test"
    osg_co_id = OSG_CO_ID
    outfile = None
    authstr = None
    min_timeout = MINTIMEOUT
    max_timeout = MAXTIMEOUT
    project_gid_startval = PROJECT_GIDS_START


options = Options()


def getpw(user, passfd, passfile):
    if ":" in user:
        user, pw = user.split(":", 1)
    elif passfd is not None:
        pw = os.fdopen(passfd).readline().rstrip("\n")
    elif passfile is not None:
        pw = open(passfile).readline().rstrip("\n")
    elif "PASS" in os.environ:
        pw = os.environ["PASS"]
    else:
        usage("PASS required")
    return user, pw


def mkauthstr(user, passwd):
    from base64 import encodebytes

    raw_authstr = "%s:%s" % (user, passwd)
    return encodebytes(raw_authstr.encode()).decode().replace("\n", "")


def mkrequest(method, target, data, **kw):
    url = os.path.join(options.endpoint, target)
    if kw:
        url += "?" + "&".join("{}={}".format(k, v) for k, v in kw.items())
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
    req = mkrequest(method, target, data, **kw)
    trying = True
    currentTimeout = options.min_timeout
    while trying:
        try:
            resp = urllib.request.urlopen(req, timeout=currentTimeout)
            payload = resp.read()
            trying = False
        except urllib.error.URLError as exception:
            if currentTimeout < options.max_timeout:
                currentTimeout *= TIMEOUTMULTIPLE
            else:
                sys.exit(
                    f"Exception raised after maximum timeout {options.max_timeout} seconds reached. "
                    + f"Exception reason: {exception.reason}.\n Request: {req.full_url}"
                )

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


def get_datalist(data, listname):
    return data[listname] if data else []


def identifier_index(id_list, id_type):
    id_type_list = [id["Type"] for id in id_list]
    try:
        return id_type_list.index(id_type)
    except ValueError:
        return -1


def identifier_matches(id_list, id_type, regex_string):
    pattern = re.compile(regex_string)
    index = identifier_index(id_list, id_type)
    return (index != -1) & (pattern.match(id_list[index]["Identifier"]) is not None)


def add_identifier_to_group(gid, type, identifier_name):
    new_identifier_info = {
        "Version": "1.0",
        "Type": type,
        "Identifier": identifier_name,
        "Login": False,
        "Person": {"Type": "Group", "Id": str(gid)},
        "Status": "Active",
    }
    data = {
        "RequestType": "Identifiers",
        "Version": "1.0",
        "Identifiers": [new_identifier_info],
    }
    return call_api3(POST, "identifiers.json", data)


def parse_options(args):
    try:
        ops, args = getopt.getopt(args, "u:c:d:f:e:o:t:T:h")
    except getopt.GetoptError:
        usage()

    if args:
        usage("Extra arguments: %s" % repr(args))

    passfd = None
    passfile = None

    for op, arg in ops:
        if op == "-h":
            usage()
        if op == "-u":
            options.user = arg
        if op == "-c":
            options.osg_co_id = int(arg)
        if op == "-d":
            passfd = int(arg)
        if op == "-f":
            passfile = arg
        if op == "-e":
            options.endpoint = arg
        if op == "-o":
            options.outfile = arg
        if op == "-t":
            options.min_timeout = float(arg)
        if op == "-T":
            options.max_timeout = float(arg)

    user, passwd = getpw(options.user, passfd, passfile)
    options.authstr = mkauthstr(user, passwd)


def main(args):
    parse_options(args)

    # get groups with 'OSPool project name' matching "Yes-*" that don't have a 'OSG GID'

    co_groups = get_osg_co_groups()["CoGroups"]
    highest_osggid = 0
    projects_to_assign_identifiers = []

    for group in co_groups:
        gid = group["Id"]
        identifier_data = get_co_group_identifiers(gid)

        if identifier_data:
            identifier_list = identifier_data["Identifiers"]

            project_id_index = identifier_index(identifier_list, "ospoolproject")
            if project_id_index != -1:
                project_id = str(identifier_list[project_id_index]["Identifier"])
                is_project = re.compile(OSPOOL_PROJECT_PREFIX_STR + "*").match(project_id) is not None
            else:
                is_project = False

            osggid_index = identifier_index(identifier_list, "osggid")
            if osggid_index != -1:
                highest_osggid = max(highest_osggid, int(identifier_list[osggid_index]["Identifier"]))
            elif is_project is True:
                project_name = project_id.replace(OSPOOL_PROJECT_PREFIX_STR, "", 1).lower()
                projects_to_assign_identifiers.append(tuple([gid, project_name]))

    for gid, project_name in projects_to_assign_identifiers:
        # for each, set a 'OSG GID' starting from 200000 and a 'OSG Group Name' that is the group name
        osggid_to_assign = max(highest_osggid + 1, options.project_gid_startval)
        highest_osggid = osggid_to_assign
        add_identifier_to_group(gid, type="osggid", identifier_name=osggid_to_assign)
        add_identifier_to_group(gid, type="osggroup", identifier_name=project_name)
        print(f"project {project_name}: added osggid {osggid_to_assign} and osg project name {project_name}")


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except urllib.error.HTTPError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
