import uuid
import os

def is_valid_guid(guid_str):
    """Utility to check valid Guid."""
    try:
        # Attempt to create a UUID object
        uuid_obj = uuid.UUID(guid_str, version=4)
        # Check if the string representation matches the UUID object
        return str(uuid_obj) == guid_str
    except ValueError:
        return False

############################################################

def __CS__exit(m,*var):
    force_terminate=True
    m = "[!] " + m
    if len(var)>0:
        for e in var:
            m+=f" {e}"
    print(m)
    if force_terminate:
        exit(-1)
    else:
        raise("[!] Aborting operation")
    
WF_COMM_DIS_KEY="COMMUNITIES_DISABLED"
DIS_EXT_RETRY="NO_EXTRACTION_RETRIES"
EMBED_RELS="REL_DEC_EMBED"

def __CS__env(e,ret=False):
    if e in os.environ:
        return True if not ret else os.environ[e]
    return False if not ret else None

def CS__env(e,ret=False): #for data classes
    return __CS__env(e,ret)