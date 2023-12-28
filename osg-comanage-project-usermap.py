#!/usr/bin/env python3

import re
import os
import sys
import json
import time
import getopt
import subprocess
import urllib.error
import urllib.request
import comanage_scripts_utils as utils


SCRIPT = os.path.basename(__file__)
ENDPOINT = "https://registry-test.cilogon.org/registry/"
OSG_CO_ID = 8
MINTIMEOUT = 5
MAXTIMEOUT = 625
TIMEOUTMULTIPLE = 5
CACHE_FILENAME = "COmanage_Projects_cache.txt"
CACHE_LIFETIME_HOURS = 0.5

LDAP_AUTH_COMMAND = [
    "awk", "/ldap_default_authtok/ {print $3}", "/etc/sssd/conf.d/0060_domain_CILOGON.ORG.conf",
]

LDAP_GROUP_MEMBERS_COMMAND = [
    "ldapsearch",
    "-H",
    "ldaps://ldap.cilogon.org",
    "-D",
    "uid=readonly_user,ou=system,o=OSG,o=CO,dc=cilogon,dc=org",
    "-w", "{auth}",
    "-b",
    "ou=groups,o=OSG,o=CO,dc=cilogon,dc=org",
    "-s",
    "one",
    "(cn=*)",
]

LDAP_ACTIVE_USERS_COMMAND = [
    "ldapsearch",
    "-LLL",
    "-H", "ldaps://ldap.cilogon.org",
    "-D", "uid=readonly_user,ou=system,o=OSG,o=CO,dc=cilogon,dc=org",
    "-x",
    "-w",  "{auth}",
    "-b", "ou=people,o=OSG,o=CO,dc=cilogon,dc=org",
    "{filter}", "voPersonApplicationUID",
    "|", "grep", "voPersonApplicationUID",
    "|", "sort",
]

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
    resp_data = get_co_group_identifiers(gid)
    data = get_datalist(resp_data, "Identifiers")
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
        ops, args = getopt.getopt(args, 'u:c:d:f:g:e:o:h')
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

    try:
        user, passwd = utils.getpw(options.user, passfd, passfile)
        options.authstr = utils.mkauthstr(user, passwd)
    except PermissionError:
        usage("PASS required")


def get_ldap_group_members_data():
    gidNumber_str = "gidNumber: "
    gidNumber_regex = re.compile(gidNumber_str)
    member_str = "hasMember: "
    member_regex = re.compile(member_str)

    auth_str = subprocess.run(
            LDAP_AUTH_COMMAND,
            stdout=subprocess.PIPE
            ).stdout.decode('utf-8').strip()
    
    ldap_group_members_command = LDAP_GROUP_MEMBERS_COMMAND
    ldap_group_members_command[LDAP_GROUP_MEMBERS_COMMAND.index("{auth}")] = auth_str

    data_file = subprocess.run(
        ldap_group_members_command, stdout=subprocess.PIPE).stdout.decode('utf-8').split('\n')

    search_results = list(filter( 
        lambda x: not re.compile("#|dn:|cn:|objectClass:").match(x),
        (line for line in data_file)))
        
    search_results.reverse()

    group_data_dict = dict()
    index = 0
    while index < len(search_results) - 1:
        while not gidNumber_regex.match(search_results[index]):
            index += 1
        gid = search_results[index].replace(gidNumber_str, "")
        members_list = []
        while search_results[index] != "":
            if member_regex.match(search_results[index]):
                members_list.append(search_results[index].replace(member_str, ""))
            index += 1
        group_data_dict[gid] = members_list
        index += 1

    return group_data_dict


def get_ldap_active_users(filter_group_name):
    auth_str = subprocess.run(
            LDAP_AUTH_COMMAND,
            stdout=subprocess.PIPE
            ).stdout.decode('utf-8').strip()

    filter_str = ("(isMemberOf=CO:members:active)" if filter_group_name is None 
                  else f"(&(isMemberOf={filter_group_name})(isMemberOf=CO:members:active))")
    
    ldap_active_users_command = LDAP_ACTIVE_USERS_COMMAND
    ldap_active_users_command[LDAP_ACTIVE_USERS_COMMAND.index("{auth}")] = auth_str
    ldap_active_users_command[LDAP_ACTIVE_USERS_COMMAND.index("{filter}")] = filter_str

    active_users = subprocess.run(ldap_active_users_command, stdout=subprocess.PIPE).stdout.decode('utf-8').split('\n')
    users = set(line.replace("voPersonApplicationUID: ", "") if re.compile("dn: voPerson*") 
                else "" for line in active_users)
    return users


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
                    project_osggids_to_name[osggid_name_pair[0]] = osggid_name_pair[1]
        else:
            raise OSError
    except OSError:
        with open(CACHE_FILENAME, "w") as w:
            project_osggids_to_name = get_groups_data_from_api()
            print(time.time(), file=w)
            for osggid, name in project_osggids_to_name.items():
                print(f"{osggid}:{name}", file=w)
    finally:
        if r:
            r.close()

    return project_osggids_to_name


def get_osguser_groups(filter_group_name=None):
    project_osggids_to_name = get_co_api_data()
    ldap_groups_members = get_ldap_group_members_data()
    ldap_users = get_ldap_active_users(filter_group_name)

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
