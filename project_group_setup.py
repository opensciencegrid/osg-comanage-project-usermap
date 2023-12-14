#!/usr/bin/env python3

import os
import re
import sys
import json
import getopt
import urllib.error
import urllib.request
from ldap3 import Server, Connection, ALL, ALL_ATTRIBUTES, SAFE_SYNC

SCRIPT = os.path.basename(__file__)
ENDPOINT = "https://registry-test.cilogon.org/registry/"
LDAP_SERVER = "ldaps://ldap.cilogon.org"
LDAP_USER = "uid=readonly_user,ou=system,o=OSG,o=CO,dc=cilogon,dc=org"
OSG_CO_ID = 8
UNIX_CLUSTER_ID = 10
LDAP_TARGET_ID = 9
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
  -g CLUSTER_ID       specify UNIX Cluster ID (default = {UNIX_CLUSTER_ID})
  -l LDAP_TARGET      specify LDAP Provsion ID (defult = {LDAP_TARGET_ID})
  -p LDAP authtok     specify LDAP server authtok
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
    ucid = UNIX_CLUSTER_ID
    provision_target = LDAP_TARGET_ID
    outfile = None
    authstr = None
    ldap_authtok = None
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
                    f"Exception raised after maximum retrys and timeout {options.max_timeout} seconds reached. "
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


def get_unix_cluster_groups(ucid):
    return call_api("unix_cluster/unix_cluster_groups.json", unix_cluster_id=ucid)


def get_unix_cluster_groups_ids(ucid):
    unix_cluster_groups = get_unix_cluster_groups(ucid)
    return set(group["CoGroupId"] for group in unix_cluster_groups["UnixClusterGroups"])


def get_datalist(data, listname):
    return data[listname] if data else []


def get_ldap_groups():
    ldap_group_osggids = set()
    server = Server(LDAP_SERVER, get_info=ALL)
    connection = Connection(server, LDAP_USER, options.ldap_authtok, client_strategy=SAFE_SYNC, auto_bind=True)
    _, _, response, _ = connection.search("ou=groups,o=OSG,o=CO,dc=cilogon,dc=org", "(cn=*)", attributes=ALL_ATTRIBUTES)
    for group in response:
        ldap_group_osggids.add(group["attributes"]["gidNumber"])
    return ldap_group_osggids


def identifier_from_list(id_list, id_type):
    id_type_list = [id["Type"] for id in id_list]
    try:
        id_index = id_type_list.index(id_type)
        return id_list[id_index]["Identifier"]
    except ValueError:
        return None


def identifier_matches(id_list, id_type, regex_string):
    pattern = re.compile(regex_string)
    value = identifier_from_list(id_list, id_type)
    return (value is not None) & (pattern.match(value) is not None)


def add_identifier_to_group(gid, type, identifier_value):
    new_identifier_info = {
        "Version": "1.0",
        "Type": type,
        "Identifier": identifier_value,
        "Login": False,
        "Person": {"Type": "Group", "Id": str(gid)},
        "Status": "Active",
    }
    data = {
        "RequestType": "Identifiers",
        "Version": "1.0",
        "Identifiers": [new_identifier_info],
    }
    call_api3(POST, "identifiers.json", data)


def add_unix_cluster_group(gid):
    request = {
        "RequestType": "UnixClusterGroups",
        "Version": "1.0",
        "UnixClusterGroups": [{"Version": "1.0", "UnixClusterId": options.ucid, "CoGroupId": gid}],
    }
    call_api3(POST, "unix_cluster/unix_cluster_groups.json", request)


def ldap_provision_group(gid):
    call_api2(POST, f"co_provisioning_targets/provision/{options.provision_target}/cogroupid:{gid}.json")


def parse_options(args):
    try:
        ops, args = getopt.getopt(args, "u:c:g:l:p:d:f:e:o:t:T:h")
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
        if op == "-g":
            options.ucid = int(arg)
        if op == "-l":
            options.provision_target = int(arg)
        if op == "-p":
            options.ldap_authtok = arg
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


def append_if_project(project_groups, group):
    # If this group has a ospoolproject id, and it starts with "Yes-", it's a project
    if identifier_matches(group["ID_List"], "ospoolproject", (OSPOOL_PROJECT_PREFIX_STR + "*")):
        # Add a dict of the relavent data for this project to the project_groups list
        project_groups.append(group)


def update_highest_osggid(highest_osggid, group):
    # Get the value of the osggid identifier, if this group has one
    osggid = identifier_from_list(group["ID_List"], "osggid")
    # If this group has a osggid, keep a hold of the highest one we've seen so far
    if osggid is not None:
        return max(highest_osggid, int(osggid))
    else:
        return highest_osggid


def get_comanage_data():
    comanage_data = {"Projects": [], "highest_osggid": 0}

    co_groups = get_osg_co_groups()["CoGroups"]
    for group_data in co_groups:
        try:
            identifier_list = get_co_group_identifiers(group_data["Id"])["Identifiers"]
            # Store this groups data in a dictionary to avoid repeated API calls
            group = {"Gid": group_data["Id"], "Name": group_data["Name"], "ID_List": identifier_list}

            append_if_project(comanage_data["Projects"], group)

            comanage_data["highest_osggid"] = update_highest_osggid(comanage_data["highest_osggid"], group)
        except TypeError:
            pass
    return comanage_data


def get_projects_needing_identifiers(project_groups):
    projects_needing_identifiers = []
    for project in project_groups:
        # If this project doesn't have an osggid already assigned to it...
        if identifier_from_list(project["ID_List"], "osggid") is None:
            # Prep the project to have the proper identifiers added to it
            projects_needing_identifiers.append(project)
    return projects_needing_identifiers


def get_projects_needing_cluster_groups(project_groups):
    # CO Groups associated with a UNIX Cluster Group
    clustered_group_ids = get_unix_cluster_groups_ids(options.ucid)
    try:
        # All project Gids
        project_gids = set(project["Gid"] for project in project_groups)
        # Project Gids for projects without UNIX cluster groups
        project_gids_lacking_cluster_groups = project_gids.difference(clustered_group_ids)
        # All projects needing UNIX cluster groups
        projects_needing_unix_groups = (
            project
            for project in project_groups
            if project["Gid"] in project_gids_lacking_cluster_groups
        )
        return projects_needing_unix_groups
    except TypeError:
        return set()
    

def get_projects_needing_provisioning(project_groups):
    # project groups provisioned in LDAP
    ldap_group_osggids = get_ldap_groups()
    try:
        # All project osggids
        project_osggids = set(identifier_from_list(project["ID_List"], "osggid") for project in project_groups)
        # project osggids not provisioned in ldap
        project_osggids_to_provision = project_osggids.difference(ldap_group_osggids)
        # All projects that aren't provisioned in ldap
        projects_to_provision = (
            project
            for project in project_groups
            if identifier_from_list(project["ID_List"], "osggid") in project_osggids_to_provision
        )
        return projects_to_provision
    except TypeError:
        return set()


def get_projects_to_setup(project_groups):
    projects_to_setup = {
        "Need Identifiers": get_projects_needing_identifiers(project_groups),
        "Need Cluster Groups": get_projects_needing_cluster_groups(project_groups),
        "Need Provisioning": get_projects_needing_provisioning(project_groups),
    }
    return projects_to_setup


def add_missing_group_identifier(project, id_type, value):
    # If the group doesn't already have an id of this type...
    if identifier_from_list(project["ID_List"], id_type) is None:
        add_identifier_to_group(project["Gid"], id_type, value)
        print(f'project {project["Gid"]}: aded id {value} of type {id_type}')


def assign_identifiers_to_project(project, id_dict):
    for k, v in id_dict.items():
        # Add an identifier of type k and value v to this group, if it dones't have them already
        add_missing_group_identifier(project, k, v)


def assign_identifiers(project_list, highest_osggid):
    highest = highest_osggid
    for project in project_list:
        # Project name identifier is the CO Group name in lower case
        project_name = project["Name"].lower()

        # Determine what osggid to assign this project,
        # based on the starting range and the highest osggid seen in existing groups
        osggid_to_assign = max(highest + 1, options.project_gid_startval)
        highest = osggid_to_assign

        identifiers_to_add = {"osggid": osggid_to_assign, "osggroup": project_name}

        assign_identifiers_to_project(project, identifiers_to_add)


def create_unix_cluster_groups(project_list):
    for project in project_list:
        add_unix_cluster_group(project["Gid"])
        print(f'project group {project["Gid"]}: added UNIX Cluster Group')


def provision_groups(project_list):
    for project in project_list:
        ldap_provision_group(project["Gid"])
        print(f'project group {project["Gid"]}: Provisioned Group')


def main(args):
    parse_options(args)

    comanage_data = get_comanage_data()
    projects_to_setup = get_projects_to_setup(comanage_data["Projects"])

    assign_identifiers(projects_to_setup["Need Identifiers"], comanage_data["highest_osggid"])
    create_unix_cluster_groups(projects_to_setup["Need Cluster Groups"])
    provision_groups(projects_to_setup["Need Provisioning"])


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except OSError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
