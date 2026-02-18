import re
from typing import Dict, List, Any, Union

URN_PATTERN = re.compile(
    r"^[Uu][Rr][Nn]:"                          # "urn:" prefix (case-insensitive)
    r"(?![Uu][Rr][Nn]-)"                       # NID cannot start with "urn-"
    r"[a-zA-Z0-9][a-zA-Z0-9-]{1,31}:"          # NID: 2-32 chars
    r"(?:[a-zA-Z0-9()+,\-.:=@;$_!*']"          # NSS trans characters
    r"|%[0-9A-Fa-f]{2})+"                      # or percent-encoded
    r"$"
)

def is_valid_urn(urn: str) -> bool:
    return URN_PATTERN.match(urn) is not None

def get_type_from_urn(urn: str) -> str:
    if not is_valid_urn(urn):
        raise ValueError(f'Invalid URN: {urn}') 
    second_colon = urn.index(':', urn.index(':') + 1)
    last_colon = urn.rfind(':')
    return urn[second_colon + 1:last_colon]

def get_id_from_urn(urn: str) -> str:
    if not is_valid_urn(urn):
        raise ValueError(f'Invalid URN: {urn}') 
    return urn[urn.rfind(':') + 1:]

def map_project_name(
    obj: Union[Dict, List, str, Any], 
    from_urn: str, 
    to_urn: str
) -> Union[Dict, List, str, Any]:
    if isinstance(obj, dict):
        return {key: map_project_name(value, from_urn, to_urn) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [map_project_name(item, from_urn, to_urn) for item in obj]
    elif isinstance(obj, str) and obj.startswith(from_urn):
        return obj.replace(from_urn, to_urn)
    return obj
