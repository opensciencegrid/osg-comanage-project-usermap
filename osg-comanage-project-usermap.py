#!/usr/bin/env python3

import os
import sys
import time
import getopt
import urllib.error
import urllib.request
import comanage_scripts_utils as utils


SCRIPT = os.path.basename(__file__)
ENDPOINT = "https://registry-test.cilogon.org/registry/"
LDAP_SERVER = "ldaps://ldap-test.cilogon.org"
LDAP_USER = "uid=registry_user,ou=system,o=OSG,o=CO,dc=cilogon,dc=org"
OSG_CO_ID = 8
CACHE_FILENAME = "COmanage_Projects_cache.txt"
CACHE_LIFETIME_HOURS = 0.5


_usage = f"""\
usage: [PASS=...] {SCRIPT} [OPTIONS]

OPTIONS:
  -u USER[:PASS]      specify USER and optionally PASS on command line
  -c OSG_CO_ID        specify OSG CO ID (default = {OSG_CO_ID})
  -s LDAP_SERVER      specify LDAP server to read data from
  -l LDAP_USER        specify LDAP user for reading data from LDAP server
  -a ldap_authfile    specify path to file to open and read LDAP authtok
  -d passfd           specify open fd to read PASS
  -f passfile         specify path to file to open and read PASS
  -e ENDPOINT         specify REST endpoint
                        (default = {ENDPOINT})
  -o outfile          specify output file (default: write to stdout)
  -g filter_group     filter users by group name (eg, 'ap1-login')
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
    outfile = None
    authstr = None
    ldap_server = LDAP_SERVER
    ldap_user = LDAP_USER
    ldap_authtok = None
    filtergrp = None


options = Options()


# api call results massagers

def get_osg_co_groups__map():
    #print("get_osg_co_groups__map()")
    resp_data = utils.get_osg_co_groups(options.osg_co_id, options.endpoint, options.authstr)
    data = utils.get_datalist(resp_data, "CoGroups")
    return { g["Id"]: g["Name"] for g in data }


def co_group_is_project(gid):
    #print(f"co_group_is_ospool({gid})")
    resp_data = utils.get_co_group_identifiers(gid, options.endpoint, options.authstr)
    data = utils.get_datalist(resp_data, "Identifiers")
    return any( i["Type"] == "ospoolproject" for i in data )


def get_co_group_osggid(gid):
    resp_data = utils.get_co_group_identifiers(gid, options.endpoint, options.authstr)
    data = utils.get_datalist(resp_data, "Identifiers")
    return list(filter(lambda x : x["Type"] == "osggid", data))[0]["Identifier"]


def get_co_group_members__pids(gid):
    #print(f"get_co_group_members__pids({gid})")
    resp_data = utils.get_co_group_members(gid,  options.endpoint, options.authstr)
    data = utils.get_datalist(resp_data, "CoGroupMembers")
    return [ m["Person"]["Id"] for m in data ]


def get_co_person_osguser(pid):
    #print(f"get_co_person_osguser({pid})")
    resp_data = utils.get_co_person_identifiers(pid, options.endpoint, options.authstr)
    data = utils.get_datalist(resp_data, "Identifiers")
    typemap = { i["Type"]: i["Identifier"] for i in data }
    return typemap.get("osguser")


def parse_options(args):
    try:
        ops, args = getopt.getopt(args, 'u:c:s:l:a:d:f:g:e:o:h')
    except getopt.GetoptError:
        usage()

    if args:
        usage("Extra arguments: %s" % repr(args))

    passfd = None
    passfile = None
    ldap_authfile = None

    for op, arg in ops:
        if op == '-h': usage()
        if op == '-u': options.user       = arg
        if op == '-c': options.osg_co_id  = int(arg)
        if op == '-s': options.ldap_server= arg
        if op == '-l': options.ldap_user  = arg
        if op == '-a': ldap_authfile      = arg
        if op == '-d': passfd             = int(arg)
        if op == '-f': passfile           = arg
        if op == '-e': options.endpoint   = arg
        if op == '-o': options.outfile    = arg
        if op == '-g': options.filtergrp  = arg

    try:
        user, passwd = utils.getpw(options.user, passfd, passfile)
        options.authstr = utils.mkauthstr(user, passwd)
        options.ldap_authtok = utils.get_ldap_authtok(ldap_authfile)
    except PermissionError:
        usage("PASS required")


def get_ldap_group_members_dict():
    group_data_dict = dict()
    for group_gid in utils.get_ldap_groups(options.ldap_server, options.ldap_user, options.ldap_authtok):
        group_members = utils.get_ldap_group_members(group_gid, options.ldap_server, options.ldap_user, options.ldap_authtok)
        group_data_dict[group_gid] = group_members

    return group_data_dict


def create_user_to_projects_map(project_to_user_map, active_users, osggids_to_names):
    users_to_projects_map = dict()
    for osggid in project_to_user_map:
        for user in project_to_user_map[osggid]:
            if user in active_users:
                if user not in users_to_projects_map:
                    users_to_projects_map[user] = [osggids_to_names[osggid]]
                else:
                    users_to_projects_map[user].append(osggids_to_names[osggid])

    return users_to_projects_map


def get_groups_data_from_api():
    groups = get_osg_co_groups__map()
    project_osggids_to_name = dict()
    for id,name in groups.items():
        if co_group_is_project(id):
            project_osggids_to_name[get_co_group_osggid(id)] = name
    return project_osggids_to_name


def get_co_api_data():
    try:
        r = open(CACHE_FILENAME, "r")
        lines = r.readlines()
        if float(lines[0]) >= (time.time() - (60 * 60 * CACHE_LIFETIME_HOURS)):
            entries = lines[1:len(lines)]
            project_osggids_to_name = dict()
            for entry in entries:
                osggid_name_pair = entry.split(":")
                if len(osggid_name_pair) == 2:
                    project_osggids_to_name[int(osggid_name_pair[0])] = osggid_name_pair[1].strip()
            r.close()
        else:
            r.close()
            raise OSError
    except OSError:
        with open(CACHE_FILENAME, "w") as w:
            project_osggids_to_name = get_groups_data_from_api()
            print(time.time(), file=w)
            for osggid, name in project_osggids_to_name.items():
                print(f"{osggid}:{name}", file=w)

    return project_osggids_to_name


def get_osguser_groups(filter_group_name=None):
    project_osggids_to_name = get_co_api_data()
    ldap_groups_members = get_ldap_group_members_dict()
    ldap_users = utils.get_ldap_active_users(options.ldap_server, options.ldap_user, options.ldap_authtok, filter_group_name)

    active_project_osggids = set(ldap_groups_members.keys()).intersection(set(project_osggids_to_name.keys()))
    project_to_user_map = {
        osggid : ldap_groups_members[osggid]
        for osggid in active_project_osggids
        }
    all_project_users = set(
        username for osggid in project_to_user_map for username in project_to_user_map[osggid]
        )
    all_active_project_users = all_project_users.intersection(ldap_users)
    usernames_to_project_map = create_user_to_projects_map(
                                project_to_user_map,  
                                all_active_project_users,
                                project_osggids_to_name,
                                )

    return usernames_to_project_map


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
    print_usermap(osguser_groups)


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except urllib.error.HTTPError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
