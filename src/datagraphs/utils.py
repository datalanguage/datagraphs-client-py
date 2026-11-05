"""URN parsing and project-name mapping utilities."""

import re
from typing import Any

_URN_PATTERN = re.compile(
    r"^[Uu][Rr][Nn]:"                          # "urn:" prefix (case-insensitive)
    r"(?![Uu][Rr][Nn]-)"                       # NID cannot start with "urn-"
    r"[a-zA-Z0-9][a-zA-Z0-9-]{1,31}:"          # NID: 2-32 chars
    r"(?:[a-zA-Z0-9()+,\-.:=@;$_!*']"          # NSS trans characters
    r"|%[0-9A-Fa-f]{2})+"                      # or percent-encoded
    r"$"
)

def is_valid_urn(urn: str) -> bool:
    """Check whether *urn* matches the RFC 2141 URN syntax.

    :param urn: The string to test.
    :returns: ``True`` if *urn* is a valid URN.
    """
    return _URN_PATTERN.match(urn) is not None

# URN namespaces owned by the DataGraphs platform rather than by a project:
# datatypes and validation rules (``urn:datagraphs:``) and legacy model IDs
# (``urn:models:``).
_PLATFORM_URN_NAMESPACES = frozenset({'datagraphs', 'models'})

def is_platform_urn(urn: str) -> bool:
    """Check whether *urn* is in a DataGraphs platform namespace.

    Only the namespace is inspected (case-insensitively, as RFC 2141
    requires): use `is_valid_urn` to check the URN as a whole.

    :param urn: The string to test.
    :returns: ``True`` if *urn* starts ``urn:datagraphs:`` or ``urn:models:``.
    """
    parts = urn.split(':', 2)
    return len(parts) == 3 and parts[0].lower() == 'urn' and parts[1].lower() in _PLATFORM_URN_NAMESPACES

def get_type_from_urn(urn: str) -> str:
    """Extract the type segment from a URN.

    Returns the segment between the second and last colons.

    :param urn: A valid URN string.
    :returns: The type segment.
    :raises ValueError: If *urn* is not a valid URN.
    """
    if not is_valid_urn(urn):
        raise ValueError(f'Invalid URN: {urn}') 
    second_colon = urn.index(':', urn.index(':') + 1)
    last_colon = urn.rfind(':')
    return urn[second_colon + 1:last_colon]

def get_project_from_urn(urn: str) -> str:
    """Extract the project name from a URN.

    Returns the segment between the first and second colons.

    :param urn: A valid URN string.
    :returns: The project name.
    :raises ValueError: If *urn* is not a valid URN.
    """
    if not is_valid_urn(urn):
        raise ValueError(f'Invalid URN: {urn}') 
    first_colon = urn.index(':')
    second_colon = urn.index(':', first_colon + 1)
    return urn[first_colon + 1:second_colon]    

def get_id_from_urn(urn: str) -> str:
    """Extract the trailing identifier from a URN.

    Returns the segment after the last colon.

    :param urn: A valid URN string.
    :returns: The identifier.
    :raises ValueError: If *urn* is not a valid URN.
    """
    if not is_valid_urn(urn):
        raise ValueError(f'Invalid URN: {urn}') 
    return urn[urn.rfind(':') + 1:]

def map_project_name(
    obj: dict | list | str | Any, 
    from_urn: str, 
    to_urn: str
) -> dict | list | str | Any:
    """Recursively replace a URN prefix in all string values of *obj*.

    :param obj: The object to process (dict, list, str, or other).
    :param from_urn: URN prefix to replace.
    :param to_urn: Replacement URN prefix.
    :returns: A copy of *obj* with URN prefixes replaced.
    """
    if isinstance(obj, dict):
        return {key: map_project_name(value, from_urn, to_urn) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [map_project_name(item, from_urn, to_urn) for item in obj]
    elif isinstance(obj, str) and obj.startswith(from_urn):
        return obj.replace(from_urn, to_urn)
    return obj

def rewrite_urns_for_project(obj: dict | list | str | Any, project_name: str) -> dict | list | str | Any:
    """Recursively rewrite URNs in *obj* to match the target project.

    Every valid URN string has its project segment (between the first and
    second colons) replaced with *project_name*, so ``urn:my-test:path1:id1``
    becomes ``urn:<project_name>:path1:id1``. Platform URNs (see
    `is_platform_urn`) and non-URN strings are left as-is.

    :param obj: The object to process (dict, list, str, or other).
    :param project_name: The project name to write into each URN.
    :returns: A copy of *obj* with URNs rewritten to the target project.
    """
    if isinstance(obj, dict):
        return {key: rewrite_urns_for_project(value, project_name) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [rewrite_urns_for_project(item, project_name) for item in obj]
    elif isinstance(obj, str) and is_valid_urn(obj) and not is_platform_urn(obj):
        project_start = obj.index(':') + 1
        project_end = obj.index(':', project_start)
        return f'{obj[:project_start]}{project_name}{obj[project_end:]}'
    return obj
