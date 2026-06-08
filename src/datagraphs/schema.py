"""Schema definition and manipulation for DataGraphs domain models."""

import json
import datetime
from collections import deque
from typing import Optional, Self, Union
from datagraphs.enums import DATATYPE, REPORT_FORMAT
from datagraphs.utils import SchemaTransformer
from datagraphs.schema_report import build_change_report
from datagraphs.schema_tracker import ChangeTracker


class SchemaError(Exception):
    """Base exception for Schema-related errors."""
    pass

class ClassNotFoundError(SchemaError):
    """Raised when a class is not found in the schema."""
    pass

class PropertyNotFoundError(SchemaError):
    """Raised when a property is not found in a class."""
    pass

class PropertyExistsError(SchemaError):
    """Raised when attempting to create a property that already exists."""
    pass

class InvalidInversePropertyError(SchemaError):
    """Raised when an invalid inverse property is specified."""
    pass

class Schema:
    """In-memory representation of a DataGraphs domain model schema.

    A ``Schema`` instance tracks every change applied to it over its lifetime
    and exposes :meth:`change_report` to emit a deterministic, net-effect,
    semantically-annotated changelog relative to the state at construction.
    Tracking is always on and adds negligible overhead for typical schema sizes.

    Every public mutating method is **atomic (all-or-nothing)**: a rollback
    transaction is opened at the outermost call boundary and replayed on any
    exception, so a method that raises leaves the schema completely unchanged —
    never a partial write. The transaction is scoped to the operation's footprint
    (a shallow class-list snapshot plus a property-granular undo journal), so its
    cost is proportional to what the operation touches, not to the schema size —
    building an N-class schema is O(N), not O(N²). Compound mutations (e.g.
    :meth:`create_subclass`, or any ``apply_to_subclasses`` cascade) are covered
    as a single unit: their inner self-calls share the outer transaction and never
    open a nested one. Because a rolled-back operation records nothing,
    :meth:`change_report` never surfaces a change for an operation the caller saw
    raise.
    """

    ALL_CLASSES = '__all_classes__'

    def __init__(self, name: str = "", version: str = "") -> None:
        """Create a new empty schema.

        :param name: Model name. Defaults to ``'Domain Model'`` if empty.
        :param version: Schema version. Defaults to ``'1.0'`` if empty.
        :raises TypeError: If a ``dict`` is passed instead of keyword arguments.
        """
        if isinstance(name, dict):
            raise TypeError("Schema constructor expects keyword arguments, not a dict. Use Schema.create_from() to create a schema from a dict.")
        now = datetime.datetime.now(datetime.UTC).isoformat()
        self._schema = {
            "name": "",
            "createdDate": now,
            "lastModifiedDate": now,
            "classes": [],
        }
        # Apply construction-time metadata via the untracked core, then bind the
        # tracker to the finished schema. Its baseline is therefore the
        # fully-constructed model, so construction edits are never reported as
        # user changes — no throwaway tracker, no re-baselining. The change-
        # tracking and atomic-transaction subsystem itself lives in its own
        # collaborator (see schema_tracker.ChangeTracker); Schema only encodes the
        # model and drives the tracker from its public mutating methods.
        self._apply_metadata(name, version)
        self._tracker = ChangeTracker(self._schema)

    @staticmethod
    def create_from(data: dict, version: str = "") -> Self:
        """Create a `Schema` from a dictionary.

        Automatically detects and converts legacy-format schemas.

        :param data: Schema dictionary (new or legacy format).
        :param version: Schema version override.
        :returns: A new `Schema` instance.
        :raises SchemaError: If the dict is missing required keys.
        """
        if Schema._is_legacy_format(data):
            data = SchemaTransformer.old_to_new(data)
        schema = Schema(version=version)
        schema._set_internal_schema(data, version)
        # _set_internal_schema rebinds schema._schema to the post-transform dict;
        # a fresh tracker re-baselines from it (and re-points at the new dict) so
        # legacy conversion and construction-time metadata never appear as changes.
        schema._tracker = ChangeTracker(schema._schema)
        return schema

    def update_schema_metadata(self, name: str = "", version: str = "") -> None:
        """Update the schema's name, version, and last modified date.

        :param name: New name for the schema. If empty, the name is unchanged unless it was previously empty, in which case it defaults to 'Domain Model'.
        :param version: New version string. If empty, the version is unchanged unless it was previously empty, in which case it defaults to '1.0'.
        """
        with self._tracker.track() as outermost, self._tracker.atomic(outermost):
            self._apply_metadata(name, version)
            if outermost:
                self._tracker.record("update_schema_metadata", name=name, version=version)

    def _apply_metadata(self, name: str = "", version: str = "") -> None:
        """Mutate the schema's name/version/last-modified — the untracked core.

        Shared by :meth:`update_schema_metadata` (which wraps this in the
        tracking + atomic guards and records the op) and by construction paths
        (``__init__`` / :meth:`_set_internal_schema`), which apply metadata
        *before* a tracker exists and must not record a change.

        :param name: New model name (see :meth:`update_schema_metadata`).
        :param version: New version string (see :meth:`update_schema_metadata`).
        """
        self._version = version or '1.0'
        self._schema['lastModifiedDate'] = datetime.datetime.now(datetime.UTC).isoformat()
        if name or version or len(self._schema.get('name', '')) == 0:
            self._schema['name'] = f"{name or 'Domain Model'} v{self.version}"

    @staticmethod
    def _is_legacy_format(schema: dict) -> bool:
        """Detect whether a schema dict uses the legacy (old) format."""
        classes = schema.get('classes', [])
        if classes:
            first = classes[0]
            return 'objectProperties' in first or ('label' in first and 'type' not in first)
        return 'guid' in schema

    def _set_internal_schema(self, data: dict, version: str) -> None:
        self._validate_schema(data)
        self._apply_metadata(version=version)
        self._schema = data

    def _validate_schema(self, schema: dict) -> None:
        required_keys = {'name', 'createdDate', 'lastModifiedDate', 'classes'}
        if not all(key in schema for key in required_keys):
            missing_keys = required_keys - set(schema.keys())
            raise SchemaError(f"Invalid schema. Missing keys: {', '.join(missing_keys)}")

    @property
    def classes(self) -> list[dict]:
        """The list of class definitions in the schema."""
        return self._schema["classes"]

    @property
    def version(self) -> str:
        """The schema version string."""
        return self._version
    
    def _make_description(self, text: str) -> dict:
        """Create a description dict in the new format."""
        return {"en": text, "@none": text}

    def _get_description_text(self, desc: Union[str, dict]) -> str:
        """Extract plain text from a description (handles both str and dict)."""
        if isinstance(desc, dict):
            return desc.get('@none', desc.get('en', ''))
        return desc or ''

    def create_class(
        self,
        class_name: str,
        description: str = "",
        parent_class_name: str = "",
        label_prop_name: str = "label",
        is_label_prop_lang_string: bool = True,
    ) -> None:
        """Create a new class in the schema.

        :param class_name: Name of the new class.
        :param description: Human-readable description.
        :param parent_class_name: Name of the parent class (for inheritance).
        :param label_prop_name: Name of the label property created by default.
        :param is_label_prop_lang_string: Whether the label property supports
            multiple languages.
        :raises SchemaError: If a class with the same name already exists.
        """
        with self._tracker.track() as outermost, self._tracker.atomic(outermost):
            existing_class = self.find_class(class_name)
            if existing_class is not None:
                raise SchemaError(f"The class '{class_name}' already exists in the schema")
            class_def = {
                "type": "Class",
                "name": class_name,
                "labelProperty": label_prop_name,
                "identifierProperty": "id",
                "properties": [
                    {
                        "type": "DatatypeProperty",
                        "name": label_prop_name,
                        "range": "text",
                        "isOptional": False,
                        "isArray": False,
                        "isLangString": is_label_prop_lang_string,
                        "isLabelSynonym": False
                    }
                ],
                "isAbstract": False,
            }
            if description:
                class_def['description'] = self._make_description(description)
            if parent_class_name:
                class_def['subClassOf'] = parent_class_name
            self._schema['classes'].append(class_def)
            if outermost:
                self._tracker.record("create_class", class_name=class_name)

    def create_subclass(self, class_name: str, description: str, parent_class_name: str) -> None:
        """Create a subclass that inherits all properties from the parent class.

        :param class_name: Name of the new subclass.
        :param description: Description for the subclass.
        :param parent_class_name: Name of the parent class to inherit from.
        :raises ClassNotFoundError: If the parent class does not exist.
        """
        with self._tracker.track() as outermost, self._tracker.atomic(outermost):
            class_def = self.find_class(parent_class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Parent class '{parent_class_name}' not found")
            label_prop_name = class_def['labelProperty']
            label_prop_def = self.find_property(class_def['properties'], label_prop_name)
            self.create_class(class_name, description, parent_class_name, label_prop_name, label_prop_def.get('isLangString', False))
            for prop_def in class_def['properties']:
                if prop_def['name'] != label_prop_name:
                    validation_rules = prop_def.get('validationRules', [])
                    range_value = prop_def['range']
                    try:
                        datatype = DATATYPE(range_value)
                    except ValueError:
                        datatype = range_value
                    enums = validation_rules[0].get('value', []) if validation_rules else []
                    desc = self._get_description_text(prop_def.get('description'))
                    self.create_property(
                        class_name,
                        prop_def['name'],
                        datatype,
                        desc,
                        prop_def.get('isOptional', True),
                        prop_def.get('isArray', False),
                        prop_def.get('isNestedObject', False),
                        prop_def.get('isLangString', False),
                        prop_def.get('inverseOf', ''),
                        enums,
                        prop_def.get('isLabelSynonym', False),
                        prop_def.get('isFilterable', None),
                        apply_to_subclasses=False,
                    )
            if outermost:
                # Capture the names of the properties INHERITED from the parent
                # AT CREATION TIME (op-time intent, consistent with how
                # apply_to_subclasses captures its target set).  The report uses
                # this set — not the subclass's live property count — so a
                # property added to the subclass AFTER create_subclass is NOT
                # mislabelled "inherited" and instead surfaces as its own
                # ``added`` record (round-4 B2).
                inherited_properties = [
                    p["name"] for p in class_def.get("properties", []) if "name" in p
                ]
                self._tracker.record(
                    "create_subclass",
                    class_name=class_name,
                    parent_class_name=parent_class_name,
                    inherited_properties=inherited_properties,
                )

    def update_class(self, class_name: str, new_name: str = "", new_description: str = "", parent_class_name: str = "") -> None:
        """Update a class's name, description, or parent class.

        :param class_name: Current class name.
        :param new_name: New class name, or empty to leave unchanged.
        :param new_description: New description, or empty to leave unchanged.
        :param parent_class_name: New parent class. Empty string removes the parent.
        :raises ClassNotFoundError: If the class does not exist.
        """
        with self._tracker.track() as outermost, self._tracker.atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            self._tracker.stage(class_def)
            if new_name:
                class_def["name"] = new_name
            if parent_class_name:
                class_def['subClassOf'] = parent_class_name
            elif 'subClassOf' in class_def:
                del class_def['subClassOf']
            if new_description:
                class_def['description'] = self._make_description(new_description)
            if outermost:
                self._tracker.record("update_class", class_name=class_name, new_name=new_name)

    def delete_class(self, class_name: str, include_linked_properties: bool = False, cascade_to_subclasses: bool = True) -> None:
        """Delete a class from the schema.

        :param class_name: Name of the class to delete.
        :param include_linked_properties: If ``True``, also removes ObjectProperties
            on other classes that reference this class.
        :param cascade_to_subclasses: If ``True``, removes ``subClassOf`` links
            from any subclasses of the deleted class.
        :raises ClassNotFoundError: If the class does not exist.
        """
        with self._tracker.track() as outermost, self._tracker.atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            self._schema["classes"].remove(class_def)
            if include_linked_properties:
                self._delete_linked_properties(class_name)
            if cascade_to_subclasses:
                for other_def in self._schema["classes"]:
                    if other_def.get("subClassOf") == class_name:
                        self._tracker.stage(other_def)
                        other_def.pop("subClassOf", None)
            if outermost:
                self._tracker.record("delete_class", class_name=class_name, cascade_to_subclasses=cascade_to_subclasses)

    def assign_label_property(self, class_name: str, prop_name: str, is_lang_string: bool = True) -> None:
        """Designate an existing property as the label property for a class.

        The property is also marked as required (``isOptional=False``).

        :param class_name: Class name.
        :param prop_name: Property name to use as the label.
        :param is_lang_string: Whether the label supports multiple languages.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If the property does not exist on the class.
        """
        with self._tracker.track() as outermost, self._tracker.atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            self._tracker.stage(class_def)
            class_def["labelProperty"] = prop_name
            prop_def = self.find_property(class_def["properties"], prop_name)
            if prop_def is None:
                raise PropertyNotFoundError(f"Property '{prop_name}' not found in class '{class_name}'")
            prop_def["isOptional"] = False
            prop_def["isLangString"] = is_lang_string
            if outermost:
                self._tracker.record("assign_label_property", class_name=class_name, prop_name=prop_name)

    def assign_label_autogen(self, class_name: str, pattern: str) -> None:
        """Set an auto-generation pattern on the label property of a class.

        :param class_name: Class name.
        :param pattern: Auto-generation expression.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If the label property does not exist.
        """
        with self._tracker.track() as outermost, self._tracker.atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            self._tracker.stage(class_def)
            prop_name = class_def["labelProperty"]
            prop_def = self.find_property(class_def["properties"], prop_name)
            if prop_def is None:
                raise PropertyNotFoundError(f"Label property '{prop_name}' not found in class '{class_name}'")
            prop_def['propertyValuePattern'] = pattern
            if outermost:
                self._tracker.record("assign_label_autogen", class_name=class_name)

    def assign_baseclass(self, class_name: str, parent_class_name: str) -> None:
        """Set or change the parent (base) class for an existing class.

        :param class_name: The class to modify.
        :param parent_class_name: The new parent class name.
        :raises ClassNotFoundError: If *class_name* does not exist.
        """
        with self._tracker.track() as outermost, self._tracker.atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            self._tracker.stage(class_def)
            class_def['subClassOf'] = parent_class_name
            if outermost:
                self._tracker.record("assign_baseclass", class_name=class_name, parent_class_name=parent_class_name)

    def assign_class_description(self, class_name: str, description: str) -> None:
        """Set or clear the description of a class.

        :param class_name: Class name.
        :param description: New description. Pass an empty string to remove it.
        :raises ClassNotFoundError: If the class does not exist.
        """
        with self._tracker.track() as outermost, self._tracker.atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            self._tracker.stage(class_def)
            if description:
                class_def['description'] = self._make_description(description)
            else:
                class_def.pop('description', None)
            if outermost:
                self._tracker.record("assign_class_description", class_name=class_name)

    def _delete_linked_properties(self, class_name: str) -> None:
        for class_def in self._schema["classes"]:
            properties_to_remove = [
                prop_def for prop_def in class_def["properties"]
                if (prop_def.get("type") == "ObjectProperty"
                    and prop_def.get("range") == class_name)
            ]
            if properties_to_remove:
                self._tracker.stage(class_def)
            for prop_def in properties_to_remove:
                class_def["properties"].remove(prop_def)

    def create_property(
        self,
        class_name: str,
        prop_name: str,
        datatype: Union[DATATYPE, str],
        description: str = "",
        is_optional: bool = True,
        is_array: bool = False,
        is_nested: bool = False,
        is_lang_string: bool = True,
        inverse_of: str = "",
        enums: Optional[list] = None,
        is_synonym: bool = False,
        is_filterable: Optional[bool] = None,
        apply_to_subclasses: bool = False,
    ) -> None:
        """Create a new property on a class.

        :param class_name: Class to add the property to.
        :param prop_name: Property name.
        :param datatype: A `DATATYPE` enum value for primitive types, or a class
            name string for object (relationship) properties.
        :param description: Human-readable description.
        :param is_optional: Whether the property is optional.
        :param is_array: Whether the property holds multiple values.
        :param is_nested: Whether an object property is nested (embedded).
        :param is_lang_string: For text properties, whether to support multiple
            languages.
        :param inverse_of: Name of the inverse property on the target class
            (object properties only).
        :param enums: Allowed values for ``DATATYPE.ENUM`` properties.
        :param is_synonym: Whether this property is a label synonym.
        :param is_filterable: Whether the property is available as a facet/filter.
        :param apply_to_subclasses: If ``True``, also creates the property on all
            existing subclasses.
        :raises ClassNotFoundError: If the class (or referenced class) does not exist.
        :raises PropertyExistsError: If a property with the same name already exists.
        :raises InvalidInversePropertyError: If the inverse property specification
            is invalid.
        """
        with self._tracker.track() as outermost, self._tracker.atomic(outermost):
            if enums is None:
                enums = []
            if not (hasattr(datatype, 'value') and (datatype.value in set(i.value for i in DATATYPE))) and not isinstance(datatype, str):
                raise TypeError(f"Unspecified datatype for {class_name}.{prop_name}")

            # Build the by-name and parent->children indices ONCE (FIX round-4 B3):
            # a single O(C) pass replaces the per-level O(C) find_subclasses /
            # find_class scans that made the cascade O(C^2).  The target set is the
            # parent plus its transitive descendants in BFS order; it also serves
            # as the op-log's op-time intent set (FIX VR-B3).
            if apply_to_subclasses:
                by_name, children = self._class_indices()
                target_names = [class_name] + self._descendants(class_name, children)
            else:
                cdef = self.find_class(class_name)
                by_name = {class_name: cdef} if cdef is not None else {}
                target_names = [class_name]

            # Pre-validate existence / duplicate up front (cheap, O(targets) via the
            # O(1) index) so the common conflict cases raise before any mutation.
            target_defs: list[dict] = []
            for name in target_names:
                cdef = by_name.get(name)
                if cdef is None:
                    raise ClassNotFoundError(f"Class '{name}' not found")
                if self.find_property(cdef["properties"], prop_name) is not None:
                    raise PropertyExistsError(
                        f"The property '{prop_name}' already exists in the class: {name}"
                    )
                target_defs.append(cdef)

            # Apply to every target iteratively (no Python recursion, so a
            # 1000s-deep subClassOf chain cannot RecursionError; FIX B4).  This is
            # ALL-OR-NOTHING: pre-validation cannot cover every mid-apply raise
            # (inverse_of / object-range / enum / datatype validity is per-target
            # and resolved here), so each appended property is journalled
            # (`_tracker.track_added_prop`, inside the core) and the outermost
            # `_tracker.atomic` guard rolls back on ANY exception — no raise leaves
            # a partial write, and `_tracker.record` (below, success-only) never
            # lies about it.
            for cdef in target_defs:
                self._create_property_on_class(
                    cdef, cdef["name"], prop_name, datatype, description,
                    is_optional, is_array, is_nested, is_lang_string,
                    inverse_of, enums, is_synonym, is_filterable,
                )

            # The op-time intent set is the cascade footprint minus the parent
            # itself (the subclasses the op actually touched), in BFS order.
            applied_subclasses = target_names[1:] if (outermost and apply_to_subclasses) else []
            if outermost:
                self._tracker.record(
                    "create_property",
                    class_name=class_name,
                    prop_name=prop_name,
                    apply_to_subclasses=bool(apply_to_subclasses),
                    applied_subclasses=applied_subclasses,
                )

    def _create_property_on_class(
        self, class_def: dict, owner_class_name: str, prop_name: str,
        datatype: Union[DATATYPE, str], description: str, is_optional: bool,
        is_array: bool, is_nested: bool, is_lang_string: bool, inverse_of: str,
        enums: list, is_synonym: bool, is_filterable: Optional[bool],
    ) -> None:
        """Create one property on one already-resolved class dict.

        The single-class core shared by :meth:`create_property` and its cascade.
        The caller pre-validates existence/duplicate; this core may still raise
        mid-apply (``_assign_datatype`` on a missing object range,
        ``_assign_inverse_of`` on an invalid inverse) AFTER appending the
        half-built dict — the caller's outermost ``_tracker.atomic`` guard rolls
        the model back on any such raise, so the overall create is all-or-nothing.
        ``inverse_of`` is resolved against *owner_class_name* (each target's own
        class name, matching the prior per-subclass recursion's inverse validation
        exactly).
        """
        prop_def = {"name": prop_name}
        class_def["properties"].append(prop_def)
        # Journal the append so a mid-build raise (or a later cascade target's
        # failure) rolls it back in O(1) — no whole-class deep copy.
        self._tracker.track_added_prop(class_def["properties"], prop_def)
        self._assign_datatype(prop_def, datatype, is_nested, is_lang_string)
        self._assign_property_description(prop_def, description)
        self._assign_is_optional(prop_def, is_optional)
        self._assign_is_array(prop_def, is_array)
        self._assign_inverse_of(prop_def, owner_class_name, inverse_of, datatype)
        self._assign_enum(prop_def, datatype, enums)
        self._assign_is_synonym(prop_def, is_synonym)
        if is_filterable is not None:
            self._assign_is_filterable(prop_def, is_filterable)

    def _assign_property_description(self, prop_def: dict, description: str) -> None:
        if description:
            prop_def["description"] = self._make_description(description)
        else:
            prop_def.pop("description", None)

    def _assign_is_optional(self, prop_def: dict, is_optional: bool = False) -> None:
        prop_def["isOptional"] = is_optional

    def _assign_is_array(self, prop_def: dict, is_array: bool = False) -> None:
        prop_def["isArray"] = is_array

    def _assign_datatype(self, prop_def: dict, datatype: Union[DATATYPE, str], is_nested: bool = False, is_lang_string: bool = True) -> None:
        if datatype in DATATYPE:
            prop_def["type"] = "DatatypeProperty"
            prop_def["range"] = str(datatype)
            if datatype == DATATYPE.TEXT:
                prop_def["isLangString"] = is_lang_string
            else:
                prop_def["isLangString"] = False
            prop_def.pop("isNestedObject", None)
            prop_def.pop("inferLocation", None)
            prop_def.pop("isSymmetric", None)
        else:
            if self.find_class(datatype) is None:
                raise ClassNotFoundError(f"Class '{datatype}' not found for property datatype")
            prop_def["type"] = "ObjectProperty"
            prop_def["range"] = str(datatype)
            prop_def["isNestedObject"] = is_nested
            prop_def.setdefault("inferLocation", False)
            prop_def.setdefault("isSymmetric", False)
            prop_def.pop("isLangString", None)

    def _assign_inverse_of(self, prop_def: dict, class_name: str, inverse_of: str, datatype: Union[DATATYPE, str]) -> None:
        if inverse_of and self._is_valid_inverse_of(class_name, inverse_of, datatype):
            prop_def["inverseOf"] = inverse_of

    def _assign_enum(self, prop_def: dict, datatype: Union[DATATYPE, str], enums: list) -> None:
        if datatype == DATATYPE.ENUM:
            prop_def["validationRules"] = [{
                "type": "enumeration",
                "value": enums,
            }]

    def _assign_is_filterable(self, prop_def: dict, is_filterable: Optional[bool] = None) -> None:
        if is_filterable is not None:
            prop_def["isFilterable"] = is_filterable

    def _assign_is_synonym(self, prop_def: dict, is_synonym: bool) -> None:
        if is_synonym is not None:
            prop_def["isLabelSynonym"] = is_synonym

    def _is_valid_inverse_of(self, class_name: str, inverse_of: str, datatype: Union[DATATYPE, str]) -> bool:
        is_valid = False
        if datatype not in DATATYPE:
            class_def = self.find_class(datatype)
            if class_def is not None:
                prop_def = self.find_property(class_def["properties"], inverse_of)
                if prop_def is None:
                    raise InvalidInversePropertyError(f"Inverse property '{inverse_of}' not found in class '{datatype}'")
                elif prop_def.get("type") != "ObjectProperty":
                    raise InvalidInversePropertyError(f"Inverse property '{inverse_of}' in class '{datatype}' has no range defined, expected '{class_name}'")
                elif prop_def.get("range") != class_name:
                    raise InvalidInversePropertyError(f"Inverse property '{inverse_of}' in class '{datatype}' does not point back to class '{class_name}'")
                else:
                    is_valid = True
            else:
                raise InvalidInversePropertyError(f"Inverse property refers to non-existent class '{datatype}'")
        else:
            raise InvalidInversePropertyError(f"Inverse property can only be set for properties with concept datatype, not '{datatype}'")
        return is_valid

    def update_property(
        self,
        class_name: str,
        prop_name: str,
        datatype: Union[DATATYPE, str] = None,
        description: str = None,
        is_optional: bool = None,
        is_array: bool = None,
        is_nested: bool = None,
        is_lang_string: bool = None,
        inverse_of: str = "",
        enums: Optional[list] = None,
        is_synonym: bool = False,
        is_filterable: bool = None,
        apply_to_subclasses: bool = None,
    ) -> None:
        """Update an existing property on a class.

        Only parameters that are explicitly provided (non-``None``) will be
        changed.

        :param class_name: Class containing the property.
        :param prop_name: Property name to update.
        :param datatype: New data type.
        :param description: New description.
        :param is_optional: Whether the property is optional.
        :param is_array: Whether the property holds multiple values.
        :param is_nested: Whether an object property is nested.
        :param is_lang_string: Whether the property supports multiple languages.
        :param inverse_of: Name of the inverse property on the target class.
        :param enums: Allowed enumeration values.
        :param is_synonym: Whether this property is a label synonym.
        :param is_filterable: Whether the property is available as a filter.
        :param apply_to_subclasses: If ``True``, also updates the property on all
            existing subclasses.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If the property does not exist.
        """
        with self._tracker.track() as outermost, self._tracker.atomic(outermost):
            # Build the by-name and parent->children indices ONCE (FIX round-4 B3):
            # one O(C) pass, not a find_subclasses/find_class scan per level.
            if apply_to_subclasses:
                by_name, children = self._class_indices()
                target_names = [class_name] + self._descendants(class_name, children)
            else:
                cdef = self.find_class(class_name)
                by_name = {class_name: cdef} if cdef is not None else {}
                target_names = [class_name]

            # Pre-validate existence / presence up front (cheap, O(targets) via the
            # O(1) index) so the common not-found cases raise before any mutation.
            targets: list[tuple[dict, dict]] = []
            for name in target_names:
                cdef = by_name.get(name)
                if cdef is None:
                    raise ClassNotFoundError(f"Class '{name}' not found")
                pdef = self.find_property(cdef["properties"], prop_name)
                if pdef is None:
                    raise PropertyNotFoundError(
                        f"Property '{prop_name}' not found in class '{name}'"
                    )
                targets.append((cdef, pdef))

            # Apply iteratively (no Python recursion; FIX B4).  ALL-OR-NOTHING:
            # inverse_of / object-range / enum / datatype validity is per-target
            # and resolved here, so a mid-apply raise is rolled back by the
            # outermost `_tracker.atomic` guard — no partial write, and
            # `_tracker.record` (below, success-only) never lies about a raised op.
            for cdef, pdef in targets:
                self._tracker.stage_prop(pdef)
                self._update_property_on_class(
                    cdef, pdef, cdef["name"], datatype, description, is_optional,
                    is_array, is_nested, is_lang_string, inverse_of, enums,
                    is_synonym, is_filterable,
                )

            applied_subclasses = target_names[1:] if (outermost and apply_to_subclasses) else []
            if outermost:
                self._tracker.record(
                    "update_property",
                    class_name=class_name,
                    prop_name=prop_name,
                    apply_to_subclasses=bool(apply_to_subclasses),
                    applied_subclasses=applied_subclasses,
                )

    def _update_property_on_class(
        self, class_def: dict, prop_def: dict, owner_class_name: str,
        datatype: Union[DATATYPE, str], description, is_optional, is_array,
        is_nested, is_lang_string, inverse_of, enums, is_synonym, is_filterable,
    ) -> None:
        """Update one already-resolved property on one already-resolved class.

        The single-class core shared by :meth:`update_property` and its cascade.
        Only explicitly-provided (non-``None``) fields are changed, exactly as
        the public method.  ``inverse_of`` is resolved against *owner_class_name*
        (each target's own class name, matching the prior per-subclass recursion).
        """
        if description is not None:
            self._assign_property_description(prop_def, description)
        if is_optional is not None:
            self._assign_is_optional(prop_def, is_optional)
        if is_array is not None:
            self._assign_is_array(prop_def, is_array)
        if datatype is not None:
            self._assign_datatype(prop_def, datatype, is_nested, is_lang_string)
        if inverse_of is not None:
            self._assign_inverse_of(prop_def, owner_class_name, inverse_of, datatype)
        if enums is not None:
            existing_datatype = prop_def["range"]
            self._assign_enum(prop_def, existing_datatype, enums)
        if is_filterable is not None:
            self._assign_is_filterable(prop_def, is_filterable)
        if is_synonym is not None:
            self._assign_is_synonym(prop_def, is_synonym)

    def rename_property(self, class_name: str, old_prop_name: str, new_prop_name: str) -> None:
        """Rename a property.

        If the property is the class's label property, the label property
        reference is updated automatically.

        :param class_name: Class containing the property.
        :param old_prop_name: Current property name.
        :param new_prop_name: New property name.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If *old_prop_name* does not exist.
        :raises PropertyExistsError: If *new_prop_name* is already in use.
        """
        with self._tracker.track() as outermost, self._tracker.atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            prop_def = self.find_property(class_def["properties"], old_prop_name)
            if prop_def is None:
                raise PropertyNotFoundError(f"Property '{old_prop_name}' not found in class '{class_name}'")
            conflict_prop_def = self.find_property(class_def["properties"], new_prop_name)
            if conflict_prop_def is not None:
                raise PropertyExistsError(f"The new property name '{new_prop_name}' is already in use")
            self._tracker.stage(class_def)
            prop_def["name"] = new_prop_name
            if class_def["labelProperty"] == old_prop_name:
                class_def["labelProperty"] = new_prop_name
            if outermost:
                self._tracker.record("rename_property", class_name=class_name, old_prop_name=old_prop_name, new_prop_name=new_prop_name)

    def delete_property(self, class_name: str, prop_name: str) -> None:
        """Remove a property from a class.

        :param class_name: Class containing the property.
        :param prop_name: Property name to delete.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If the property does not exist.
        """
        with self._tracker.track() as outermost, self._tracker.atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            prop_def = self.find_property(class_def["properties"], prop_name)
            if prop_def is None:
                raise PropertyNotFoundError(f"Property '{prop_name}' not found in class '{class_name}'")
            self._tracker.stage(class_def)
            class_def["properties"].remove(prop_def)
            if outermost:
                self._tracker.record("delete_property", class_name=class_name, prop_name=prop_name)

    def find_class(self, name: str) -> Optional[dict]:
        """Find a class definition by name.

        :param name: The class name to look up.
        :returns: The class dict, or ``None`` if not found.
        """
        return next((x for x in self._schema["classes"] if x['name'] == name), None)

    def find_subclasses(self, baseclass: str) -> list[dict]:
        """Find all direct subclasses of a given class.

        :param baseclass: The parent class name.
        :returns: A list of class dicts whose ``subClassOf`` matches *baseclass*.
        """
        return [x for x in self._schema["classes"] if x.get('subClassOf') == baseclass]

    def _children_index(self) -> dict[str, list[str]]:
        """Build the parent-name -> direct-children-names index in ONE O(C) pass.

        Built once per outermost cascade so the iterative descendant walk is
        O(descendants) rather than an O(C) ``find_subclasses`` scan per level
        (FIX round-4 B3 — the relocated op-time quadratic).
        """
        return self._class_indices()[1]

    def _class_indices(self) -> tuple[dict[str, dict], dict[str, list[str]]]:
        """Build the (name -> class_def) and (parent -> children) indices in ONE pass.

        Both indices back the cascade in O(descendants): the name index makes the
        atomic pre-validation O(targets) (an O(1) lookup per target rather than an
        O(C) ``find_class`` scan), and the children index drives the iterative
        descendant walk — together eliminating the O(C^2) cascade (FIX round-4 B3).
        """
        by_name: dict[str, dict] = {}
        children: dict[str, list[str]] = {}
        for cls in self._schema["classes"]:
            name = cls.get("name")
            if name is None:
                continue
            by_name[name] = cls
            parent = cls.get("subClassOf")
            if parent is not None:
                children.setdefault(parent, []).append(name)
        return by_name, children

    @staticmethod
    def _descendants(baseclass: str, children: dict[str, list[str]]) -> list[str]:
        """Transitive descendants of *baseclass* in BFS order, off a prebuilt index.

        ITERATIVE (explicit queue), so a ``subClassOf`` chain thousands of levels
        deep cannot exceed Python's recursion limit (FIX round-4 B4).  Each class
        is visited at most once (cycle-safe).
        """
        result: list[str] = []
        seen: set[str] = {baseclass}
        queue: deque[str] = deque(children.get(baseclass, []))
        while queue:
            name = queue.popleft()
            if name in seen:
                continue
            seen.add(name)
            result.append(name)
            queue.extend(children.get(name, []))
        return result

    def _transitive_subclass_names(self, baseclass: str) -> list[str]:
        """Names of every transitive subclass of *baseclass*, in BFS order.

        Mirrors the cascade footprint of ``apply_to_subclasses=True`` (direct
        children, their children, and so on).  A class is visited at most once
        (cycle-safe).  Builds the children index once and walks it iteratively.
        """
        return self._descendants(baseclass, self._children_index())

    def find_property(self, props: list, name: str) -> Optional[dict]:
        """Find a property by name within a list of property dicts.

        :param props: List of property dicts to search.
        :param name: The property name to look up.
        :returns: The property dict, or ``None`` if not found.
        """
        return next((x for x in props if x['name'] == name), None)

    def assign_property_orders(self, property_orders: dict) -> None:
        """Reorder properties within classes.

        Properties not listed in the order are appended at the end.

        :param property_orders: A dict mapping class names to ordered lists of
            property names.
        """
        with self._tracker.track() as outermost, self._tracker.atomic(outermost):
            for class_def in self._schema['classes']:
                if class_def['name'] in property_orders:
                    self._tracker.stage(class_def)
                    ordered_names = property_orders[class_def['name']]
                    props_by_name = {p['name']: p for p in class_def['properties']}
                    ordered = [props_by_name[n] for n in ordered_names if n in props_by_name]
                    remaining = [p for p in class_def['properties'] if p['name'] not in set(ordered_names)]
                    class_def['properties'] = ordered + remaining
            if outermost:
                self._tracker.record("assign_property_orders", property_orders={k: list(v) for k, v in property_orders.items()})

    def clone(self) -> Self:
        """Create a deep copy of the schema.

        :returns: A new independent `Schema` instance.
        """
        return Schema.create_from(ChangeTracker._capture_baseline(self._schema))

    def change_report(
        self, fmt: REPORT_FORMAT = REPORT_FORMAT.TEXT
    ) -> "str | list[dict]":
        """Return a net-effect changelog of all changes since construction.

        Computes the structural delta between the baseline (state at
        construction) and the current schema, then annotates it with semantic
        intent from the op-log (renames, reorders, compound ops, label-property
        assignments).  The result is deterministic: identical mutation sequences
        always yield byte-identical text and equal records regardless of dict
        insertion order.

        This method is **strictly read-only**: it never mutates ``_schema``,
        ``_baseline``, or ``_change_log``.

        **Supported surface / guarantees.**  ``fmt="records"`` is the
        fully-supported, guaranteed output: deterministic and complete — every
        structural change since the baseline is present, with its full
        ``from``/``to``/``fields``/``detail`` payload, for programmatic
        consumption.  ``fmt="text"`` is a **best-effort human-readable** rendering
        of the same change set; it is NOT guaranteed to round-trip user-supplied
        field content (e.g. a ``description`` containing newlines may produce
        additional or ambiguous lines in the text changelog) — a documented known
        limitation.  Cross-subclass annotation of ``apply_to_subclasses`` cascade
        ops in the report is likewise **best-effort**.  Prefer ``fmt="records"``
        whenever the output is parsed or relied upon.

        **Cost.**  For cascade-heavy edit histories the report is approximately
        ``O(L*C)`` (``L`` cascade ops over a parent of ``C`` subclasses): a cascade
        op genuinely fans out to one record per annotated subclass, so the report
        size — and therefore its cost — is inherent to annotating ``C`` subclasses.

        :param fmt: Output format, a :class:`~datagraphs.enums.REPORT_FORMAT`.
            As that enum is a :class:`~enum.StrEnum`, the equivalent string
            (``"text"`` / ``"records"``) is also accepted.

            * :attr:`~datagraphs.enums.REPORT_FORMAT.TEXT` *(default)*: returns a
              deterministic plain-text changelog ``str`` with a header count line
              and per-class grouping.  Best-effort human rendering — see
              *Supported surface* above.
            * :attr:`~datagraphs.enums.REPORT_FORMAT.RECORDS`: returns a
              ``list[dict]`` of structured change records for programmatic
              consumption — the supported, guaranteed output.  See *Record shape*
              below.

        :returns: A ``str`` for ``REPORT_FORMAT.TEXT``; a ``list[dict]`` for
            ``REPORT_FORMAT.RECORDS``.  Returns ``""`` (text) or ``[]`` (records)
            when nothing has changed since construction.
        :raises ValueError: If *fmt* is not a member (or value) of
            :class:`~datagraphs.enums.REPORT_FORMAT`.

        .. note::

            **Untracked edits via** :meth:`to_dict` **— graceful degradation.**
            :meth:`to_dict` returns the live internal dict; mutations applied
            directly to that dict bypass the op-log entirely.  Those changes
            are still captured by the structural diff and appear in
            ``change_report`` output, but *without* semantic intent labels:
            a property rename done through the dict appears as a remove + add
            rather than a single ``renamed`` entry, an unlogged reorder does
            not become a ``reordered`` entry, and so on.  Use the public
            mutating methods to preserve full semantic annotation.

        **Record shape** (``fmt="records"``)

        Each dict always carries:

        * ``"target"`` (``str``) — dotted path of the changed entity, using
          the current name: ``"ClassName"`` for class/metadata changes or
          ``"ClassName.propName"`` for property changes.
        * ``"kind"`` (``str``) — ``"class"``, ``"property"``, or
          ``"metadata"``.
        * ``"op"`` (``str``) — one of ``"added"``, ``"removed"``,
          ``"modified"``, ``"renamed"``, ``"reordered"``,
          ``"subclass_created"``.

        The following keys are **omitted** (not ``None``) when they do not
        apply to the entry:

        * ``"from"`` (``str``) — previous name; present only when
          ``op="renamed"``.
        * ``"to"`` (``str``) — new name; present only when ``op="renamed"``.
        * ``"fields"`` (``list[dict]``) — field-level before/after list, each
          entry ``{"field": str, "before": Any, "after": Any}``; present on
          ``op="modified"`` and on ``op="renamed"`` when field-level changes
          accompany the rename.
        * ``"detail"`` (``dict``) — supplementary annotation dict; present for
          compound or annotated entries:

          - ``op="subclass_created"``: ``{"parent": str, "inherited": int}``
          - ``op="reordered"``: ``{"order": list[str]}``
          - ``op="added"`` / ``op="modified"`` with ``apply_to_subclasses``:
            ``{"applied_to_subclasses": list[str]}``
          - ``op="modified"`` (label-property assignment):
            ``{"label_property": str}``
        """
        # The whole diff/annotate/render pipeline lives in schema_report; this
        # method only supplies the tracking state it consumes, read straight from
        # the tracker. Read-only: the facade never writes back to the schema, the
        # baseline, or the op-log.
        return build_change_report(
            self._tracker.baseline, self._tracker.change_log, self._schema, fmt
        )

    def to_dict(self) -> dict:
        """Convert the schema to a plain dictionary.

        :returns: The schema as a dict.
        """
        return self._schema

    def to_json(self) -> str:
        """Serialise the schema to a JSON string.

        :returns: A JSON-formatted string.
        """
        return json.dumps(self._schema, ensure_ascii=False, indent=2)

