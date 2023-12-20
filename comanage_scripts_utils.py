#!/usr/bin/env python3

import os
import re
import sys
import json
import urllib.error
import urllib.request
from ldap3 import Server, Connection, ALL, ALL_ATTRIBUTES, SAFE_SYNC

MIN_TIMEOUT = 5
MAX_TIMEOUT = 625
TIMEOUTMULTIPLE = 5

GET    = "GET"
PUT    = "PUT"
POST   = "POST"
DELETE = "DELETE"


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
        raise PermissionError
        #when script needs to say PASS required, raise a permission error
        #usage("PASS required")
    return user, pw


def mkauthstr(user, passwd):
    from base64 import encodebytes
    raw_authstr = "%s:%s" % (user, passwd)
    return encodebytes(raw_authstr.encode()).decode().replace("\n", "")


def mkrequest(method, target, data, endpoint, authstr, **kw):
    url = os.path.join(endpoint, target)
    if kw:
        url += "?" + "&".join("{}={}".format(k,v) for k,v in kw.items())
    req = urllib.request.Request(url, json.dumps(data).encode("utf-8"))
    req.add_header("Authorization", "Basic %s" % authstr)
    req.add_header("Content-Type", "application/json")
    req.get_method = lambda: method
    return req


def call_api(target, endpoint, authstr, **kw):
    return call_api2(GET, target, endpoint, authstr, **kw)


def call_api2(method, target, endpoint, authstr, **kw):
    return call_api3(method, target, data=None, endpoint=endpoint, authstr=authstr, **kw)


def call_api3(method, target, data, endpoint, authstr, **kw):
    req = mkrequest(method, target, data, endpoint, authstr, **kw)
    trying = True
    currentTimeout = MIN_TIMEOUT
    while trying:
        try:
            resp = urllib.request.urlopen(req, timeout=currentTimeout)
            payload = resp.read()
            trying = False
        except urllib.error.URLError as exception:
            if currentTimeout < MAX_TIMEOUT:
                currentTimeout *= TIMEOUTMULTIPLE
            else:
                sys.exit(
                    f"Exception raised after maximum retrys and/or timeout {MAX_TIMEOUT} seconds reached. "
                    + f"Exception reason: {exception.reason}.\n Request: {req.full_url}"
                )

    return json.loads(payload) if payload else None


def get_osg_co_groups(osg_co_id, endpoint, authstr):
    return call_api("co_groups.json", endpoint, authstr, coid=osg_co_id)


def get_co_group_identifiers(gid, endpoint, authstr):
    return call_api("identifiers.json", endpoint, authstr, cogroupid=gid)


def get_co_group_members(gid, endpoint, authstr):
    return call_api("co_group_members.json", endpoint, authstr, cogroupid=gid)


def get_co_person_identifiers(pid, endpoint, authstr):
    return call_api("identifiers.json", endpoint, authstr, copersonid=pid)


def get_co_group(gid, endpoint, authstr):
    resp_data = call_api("co_groups/%s.json" % gid, endpoint, authstr)
    grouplist = get_datalist(resp_data, "CoGroups")
    if not grouplist:
        raise RuntimeError("No such CO Group Id: %s" % gid)
    return grouplist[0]


def get_identifier(id_, endpoint, authstr):
    resp_data = call_api("identifiers/%s.json" % id_, endpoint, authstr)
    idfs = get_datalist(resp_data, "Identifiers")
    if not idfs:
        raise RuntimeError("No such Identifier Id: %s" % id_)
    return idfs[0]


def get_unix_cluster_groups(ucid, endpoint, authstr):
    return call_api("unix_cluster/unix_cluster_groups.json", endpoint, authstr, unix_cluster_id=ucid)


def get_unix_cluster_groups_ids(ucid, endpoint, authstr):
    unix_cluster_groups = get_unix_cluster_groups(ucid, endpoint, authstr)
    return set(group["CoGroupId"] for group in unix_cluster_groups["UnixClusterGroups"])


def delete_identifier(id_, endpoint, authstr):
    return call_api2(DELETE, "identifiers/%s.json" % id_, endpoint, authstr)


def get_datalist(data, listname):
    return data[listname] if data else []


def get_ldap_groups(ldap_server, ldap_user, ldap_authtok):
    ldap_group_osggids = set()
    server = Server(ldap_server, get_info=ALL)
    connection = Connection(server, ldap_user, ldap_authtok, client_strategy=SAFE_SYNC, auto_bind=True)
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


def rename_co_group(gid, group, newname, endpoint, authstr):
    # minimal edit CoGroup Request includes Name+CoId+Status+Version
    new_group_info = {
        "Name"    : newname,
        "CoId"    : group["CoId"],
        "Status"  : group["Status"],
        "Version" : group["Version"]
    }
    data = {
        "CoGroups"    : [new_group_info],
        "RequestType" : "CoGroups",
        "Version"     : "1.0"
    }
    return call_api3(PUT, "co_groups/%s.json" % gid, data, endpoint, authstr)


def add_identifier_to_group(gid, type, identifier_value, endpoint, authstr):
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
    return call_api3(POST, "identifiers.json", data, endpoint, authstr)


def add_unix_cluster_group(gid, ucid, endpoint, authstr):
    data = {
        "RequestType": "UnixClusterGroups",
        "Version": "1.0",
        "UnixClusterGroups": [{"Version": "1.0", "UnixClusterId": ucid, "CoGroupId": gid}],
    }
    return call_api3(POST, "unix_cluster/unix_cluster_groups.json", data, endpoint, authstr)


def provision_group(gid, provision_target, endpoint, authstr):
    path = f"co_provisioning_targets/provision/{provision_target}/cogroupid:{gid}.json"
    data = {
        "RequestType" : "CoGroupProvisioning",
        "Version"     : "1.0",
        "Synchronous" : True
    }
    return call_api3(POST, path, data, endpoint, authstr)

def provision_group_members(gid, prov_id, endpoint, authstr):
    data = {
        "RequestType" : "CoPersonProvisioning",
        "Version"     : "1.0",
        "Synchronous" : True
    }
    responses = {}
    for member in get_co_group_members(gid, endpoint, authstr)["CoGroupMembers"]:
        if member["Person"]["Type"] == "CO":
            pid = member["Person"]["Id"]
            path = f"co_provisioning_targets/provision/{prov_id}/copersonid:{pid}.json"
            responses[pid] = call_api3(POST, path, data, endpoint, authstr)
    return responses
