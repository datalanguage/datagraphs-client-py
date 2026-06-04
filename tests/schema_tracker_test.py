"""Tests for the change-tracking and atomic-transaction subsystem (ChangeTracker).

Covers the machinery that sits between schema encoding and reporting: baseline
capture, op-log recording per public mutation, re-entrancy depth, and the
all-or-nothing atomic rollback (including its footprint-scoped snapshot cost).
These exercise the tracker through ``Schema``'s public mutating API and its
read-only tracking views (``_change_log`` / ``_baseline`` / ``_tracking_depth``).

The pure diff/annotate/render pipeline that *consumes* the op-log is tested in
``tests/schema_report_test.py``; schema encoding in ``tests/schema_test.py``.
"""

import copy
import pytest
from datagraphs.schema import (
    Schema as DatagraphsSchema,
    PropertyExistsError,
    InvalidInversePropertyError,
    SchemaError,
    ClassNotFoundError,
    PropertyNotFoundError,
)
from datagraphs.enums import DATATYPE


class TestChangeTrackingBaseline:
    """Phase 1 — tracking state and baseline capture.

    Asserts that the scaffolding is present and inert:
    - Tracking attributes are initialised correctly on every construction path.
    - Baseline is captured post-construction / post-transform (0 changes on load).
    - clone() routes through create_from and therefore starts at 0 changes with
      an independent baseline.
    - Tracking state never leaks into to_dict() or to_json().
    """

    # ------------------------------------------------------------------ helpers
    # Class-level dicts are CONSTANTS — never passed directly to create_from.
    # Always obtain a fresh deep copy via the factory methods below so that
    # mutations made by Schema (which aliases its input dict) cannot leak
    # between tests.

    _NEW_FORMAT_DATA = {
        "name": "My Model v1.0",
        "createdDate": "2024-06-01T00:00:00Z",
        "lastModifiedDate": "2024-06-01T00:00:00Z",
        "classes": [
            {
                "type": "Class",
                "name": "Drug",
                "labelProperty": "label",
                "identifierProperty": "id",
                "isAbstract": False,
                "properties": [
                    {
                        "type": "DatatypeProperty",
                        "name": "label",
                        "range": "text",
                        "isOptional": False,
                        "isArray": False,
                        "isLangString": True,
                        "isLabelSynonym": False,
                    }
                ],
            }
        ],
    }

    _LEGACY_FORMAT_DATA = {
        "id": "urn:models:999",
        "guid": "999",
        "type": "DomainModel",
        "name": "Legacy Model",
        "description": "",
        "project": "urn:datagraphs:custom_project",
        "createdDate": "2024-06-01T00:00:00Z",
        "lastModifiedDate": "2024-06-01T00:00:00Z",
        "classes": [
            {
                "label": "Substance",
                "labelProperty": "label",
                "identifierProperty": "id",
                "parentClasses": ["Substance"],
                "objectProperties": [
                    {
                        "propertyName": "label",
                        "isOptional": False,
                        "isArray": False,
                        "propertyDatatype": {
                            "id": "urn:datagraphs:datatypes:text",
                            "type": "PropertyDatatype",
                            "label": "text",
                            "elasticsearchDatatype": "text",
                            "xsdDatatype": "string",
                        },
                        "isNestedObject": False,
                        "guid": "abc",
                        "propertyOrder": 0,
                        "isLangString": True,
                        "id": "urn:models:999:classes:Substance:label",
                    }
                ],
            }
        ],
    }

    def _new_format_data(self) -> dict:
        """Return a fresh deep copy of _NEW_FORMAT_DATA for each test."""
        return copy.deepcopy(TestChangeTrackingBaseline._NEW_FORMAT_DATA)

    def _legacy_format_data(self) -> dict:
        """Return a fresh deep copy of _LEGACY_FORMAT_DATA for each test."""
        return copy.deepcopy(TestChangeTrackingBaseline._LEGACY_FORMAT_DATA)

    # -------------------------------------------------------------- new-format

    def test_empty_schema_has_empty_change_log(self):
        schema = DatagraphsSchema()
        assert schema._tracker.change_log == []

    def test_empty_schema_has_zero_tracking_depth(self):
        schema = DatagraphsSchema()
        assert schema._tracker.depth == 0

    def test_empty_schema_has_baseline(self):
        schema = DatagraphsSchema()
        assert isinstance(schema._tracker.baseline, dict)
        assert "classes" in schema._tracker.baseline

    def test_empty_schema_baseline_is_independent_copy(self):
        """Mutating _schema after construction must not change _baseline."""
        schema = DatagraphsSchema()
        original_baseline_classes = list(schema._tracker.baseline["classes"])
        schema.create_class("NewClass")
        # Baseline should still reflect the empty-classes state.
        assert schema._tracker.baseline["classes"] == original_baseline_classes
        assert len(schema._schema["classes"]) == 1

    def test_create_from_has_empty_change_log(self):
        schema = DatagraphsSchema.create_from(self._new_format_data())
        assert schema._tracker.change_log == []

    def test_create_from_has_zero_tracking_depth(self):
        schema = DatagraphsSchema.create_from(self._new_format_data())
        assert schema._tracker.depth == 0

    def test_create_from_has_baseline_matching_loaded_data(self):
        schema = DatagraphsSchema.create_from(self._new_format_data())
        # Baseline classes should reflect the loaded data, not the empty state.
        assert len(schema._tracker.baseline["classes"]) == 1
        assert schema._tracker.baseline["classes"][0]["name"] == "Drug"

    def test_create_from_baseline_is_independent_copy(self):
        schema = DatagraphsSchema.create_from(self._new_format_data())
        original_count = len(schema._tracker.baseline["classes"])
        schema.create_class("ExtraClass")
        assert len(schema._tracker.baseline["classes"]) == original_count
        assert len(schema._schema["classes"]) == original_count + 1

    # ----------------------------------------------------------- legacy-format

    def test_legacy_load_has_empty_change_log(self):
        """Loading a legacy schema must report 0 changes (baseline is post-transform)."""
        schema = DatagraphsSchema.create_from(self._legacy_format_data())
        assert schema._tracker.change_log == []

    def test_legacy_load_has_zero_tracking_depth(self):
        schema = DatagraphsSchema.create_from(self._legacy_format_data())
        assert schema._tracker.depth == 0

    def test_legacy_load_baseline_reflects_transformed_data(self):
        """Baseline is captured after old_to_new transform, so it matches the
        new-format representation, not the original legacy dict."""
        schema = DatagraphsSchema.create_from(self._legacy_format_data())
        # After transform the class should have been renamed from the legacy
        # 'label' field to 'name'.
        assert len(schema._tracker.baseline["classes"]) == 1
        assert schema._tracker.baseline["classes"][0].get("name") == "Substance"
        assert schema._tracker.baseline["classes"][0].get("type") == "Class"

    # --------------------------------------------------------------- clone

    def test_clone_has_empty_change_log(self):
        """clone() routes through create_from so it must start at 0 changes."""
        schema = DatagraphsSchema.create_from(self._new_format_data())
        schema.create_class("TransientClass")  # would add to change_log once Phase 2 lands
        cloned = schema.clone()
        assert cloned._tracker.change_log == []

    def test_clone_has_zero_tracking_depth(self):
        cloned = DatagraphsSchema.create_from(self._new_format_data()).clone()
        assert cloned._tracker.depth == 0

    def test_clone_baseline_matches_cloned_state(self):
        """The clone's baseline must reflect what was cloned, not the original
        schema's baseline."""
        schema = DatagraphsSchema.create_from(self._new_format_data())
        cloned = schema.clone()
        assert cloned._tracker.baseline["classes"][0]["name"] == "Drug"

    def test_clone_baseline_is_independent_from_original(self):
        """Mutating the clone must not affect the original's baseline."""
        schema = DatagraphsSchema.create_from(self._new_format_data())
        cloned = schema.clone()
        cloned.create_class("CloneOnly")
        assert len(schema._tracker.baseline["classes"]) == 1
        assert len(cloned._tracker.baseline["classes"]) == 1  # cloned baseline unchanged
        assert len(cloned._schema["classes"]) == 2    # live dict has the addition

    # ------------------------------------------------ serialisation exclusion

    def test_baseline_absent_from_to_dict(self):
        schema = DatagraphsSchema()
        d = schema.to_dict()
        assert "_baseline" not in d
        assert "_change_log" not in d
        assert "_tracking_depth" not in d

    def test_baseline_absent_from_to_json(self):
        schema = DatagraphsSchema()
        j = schema.to_json()
        assert "_baseline" not in j
        assert "_change_log" not in j
        assert "_tracking_depth" not in j

    def test_create_from_baseline_absent_from_to_dict(self):
        schema = DatagraphsSchema.create_from(self._new_format_data())
        d = schema.to_dict()
        assert "_baseline" not in d
        assert "_change_log" not in d
        assert "_tracking_depth" not in d

    def test_create_from_baseline_absent_from_to_json(self):
        schema = DatagraphsSchema.create_from(self._new_format_data())
        j = schema.to_json()
        assert "_baseline" not in j
        assert "_change_log" not in j
        assert "_tracking_depth" not in j

    def test_to_dict_output_byte_identical_before_and_after_phase1(self):
        """to_dict() must return the live _schema unchanged — no tracking keys."""
        schema = DatagraphsSchema.create_from(self._new_format_data())
        result = schema.to_dict()
        # The live _schema is exactly what was passed in (minus metadata updates
        # from _set_internal_schema, which update lastModifiedDate in-place).
        assert result is schema._schema  # same object, not a copy


class TestChangeTrackingRecording:
    """Phase 2 — op-log instrumentation of the 14 public mutating methods.

    Verifies that:
    - Each method records exactly one entry with the correct op name and
      intent-bearing args on a successful outermost call.
    - Re-entrant inner calls (create_subclass -> create_class + create_property,
      and apply_to_subclasses recursion) record nothing of their own.
    - A mutation that raises appends nothing.
    - Schema() / create_from / clone append nothing.
    """

    # ------------------------------------------------------------------ helpers
    # Fixtures are constants — always obtain fresh instances via the methods
    # below so that mutations (Schema aliases its input dict) cannot leak
    # between tests.

    _BASE_DATA = {
        "name": "Test Model v1.0",
        "createdDate": "2024-01-01T00:00:00Z",
        "lastModifiedDate": "2024-01-01T00:00:00Z",
        "classes": [
            {
                "type": "Class",
                "name": "Animal",
                "labelProperty": "label",
                "identifierProperty": "id",
                "isAbstract": False,
                "properties": [
                    {
                        "type": "DatatypeProperty",
                        "name": "label",
                        "range": "text",
                        "isOptional": False,
                        "isArray": False,
                        "isLangString": True,
                        "isLabelSynonym": False,
                    },
                    {
                        "type": "DatatypeProperty",
                        "name": "age",
                        "range": "integer",
                        "isOptional": True,
                        "isArray": False,
                        "isLangString": False,
                        "isLabelSynonym": False,
                    },
                ],
            }
        ],
    }

    def _base_data(self) -> dict:
        return copy.deepcopy(TestChangeTrackingRecording._BASE_DATA)

    def _schema(self) -> DatagraphsSchema:
        """Return a fresh Schema pre-populated with one class ('Animal') and
        two properties ('label', 'age') — change_log starts empty."""
        return DatagraphsSchema.create_from(self._base_data())

    def _schema_with_subclass(self) -> DatagraphsSchema:
        """Return a fresh Schema where 'Dog' is a subclass of 'Animal'."""
        s = self._schema()
        s.create_class("Dog", parent_class_name="Animal")
        s._tracker.change_log.clear()
        return s

    # ------------------------------------------- construction records nothing

    def test_construction_schema_init_records_nothing(self):
        schema = DatagraphsSchema()
        assert schema._tracker.change_log == []

    def test_construction_create_from_records_nothing(self):
        schema = DatagraphsSchema.create_from(self._base_data())
        assert schema._tracker.change_log == []

    def test_construction_clone_records_nothing(self):
        schema = DatagraphsSchema.create_from(self._base_data())
        cloned = schema.clone()
        assert cloned._tracker.change_log == []

    # ---------------------------------------------------- create_class

    def test_create_class_records_one_entry(self):
        s = self._schema()
        s.create_class("Plant")
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "create_class"
        assert s._tracker.change_log[0]["args"] == {"class_name": "Plant"}

    def test_create_class_duplicate_records_nothing(self):
        s = self._schema()
        with pytest.raises(SchemaError):
            s.create_class("Animal")
        assert s._tracker.change_log == []

    # ---------------------------------------------------- create_subclass

    def test_create_subclass_records_exactly_one_entry(self):
        """create_subclass internally calls create_class + create_property xN.
        Only one 'create_subclass' entry must appear — not the inner calls."""
        s = self._schema()
        s.create_subclass("Dog", "A dog", "Animal")
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "create_subclass"
        assert s._tracker.change_log[0]["args"] == {
            "class_name": "Dog",
            "parent_class_name": "Animal",
            # Inherited-at-creation property names captured for the report's
            # inherited count + post-creation add surfacing (round-4 B2).
            "inherited_properties": ["label", "age"],
        }

    def test_create_subclass_bad_parent_records_nothing(self):
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.create_subclass("Dog", "A dog", "NonExistent")
        assert s._tracker.change_log == []

    # ---------------------------------------------------- update_class

    def test_update_class_records_one_entry(self):
        s = self._schema()
        s.update_class("Animal", new_name="Creature")
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "update_class"
        assert s._tracker.change_log[0]["args"] == {"class_name": "Animal", "new_name": "Creature"}

    def test_update_class_nonexistent_records_nothing(self):
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.update_class("NoSuchClass", new_name="X")
        assert s._tracker.change_log == []

    # ---------------------------------------------------- delete_class

    def test_delete_class_records_one_entry_with_cascade_flag(self):
        s = self._schema()
        s.delete_class("Animal", cascade_to_subclasses=False)
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "delete_class"
        assert s._tracker.change_log[0]["args"] == {
            "class_name": "Animal",
            "cascade_to_subclasses": False,
        }

    def test_delete_class_default_cascade_flag_is_recorded(self):
        s = self._schema()
        s.delete_class("Animal")
        assert s._tracker.change_log[0]["args"]["cascade_to_subclasses"] is True

    def test_delete_class_nonexistent_records_nothing(self):
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.delete_class("Ghost")
        assert s._tracker.change_log == []

    # ---------------------------------------------------- assign_label_property

    def test_assign_label_property_records_one_entry(self):
        s = self._schema()
        # 'age' exists but we set it as label to test recording
        s.assign_label_property("Animal", "age")
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "assign_label_property"
        assert s._tracker.change_log[0]["args"] == {"class_name": "Animal", "prop_name": "age"}

    def test_assign_label_property_missing_prop_records_nothing(self):
        s = self._schema()
        with pytest.raises(PropertyNotFoundError):
            s.assign_label_property("Animal", "nonExistentProp")
        assert s._tracker.change_log == []

    # ---------------------------------------------------- assign_label_autogen

    def test_assign_label_autogen_records_one_entry(self):
        s = self._schema()
        s.assign_label_autogen("Animal", "{{ name }}")
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "assign_label_autogen"
        assert s._tracker.change_log[0]["args"] == {"class_name": "Animal"}

    def test_assign_label_autogen_nonexistent_class_records_nothing(self):
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.assign_label_autogen("Ghost", "{{ name }}")
        assert s._tracker.change_log == []

    # ---------------------------------------------------- assign_baseclass

    def test_assign_baseclass_records_one_entry(self):
        s = self._schema()
        s.create_class("Mammal")
        s._tracker.change_log.clear()
        s.assign_baseclass("Animal", "Mammal")
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "assign_baseclass"
        assert s._tracker.change_log[0]["args"] == {
            "class_name": "Animal",
            "parent_class_name": "Mammal",
        }

    def test_assign_baseclass_nonexistent_class_records_nothing(self):
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.assign_baseclass("Ghost", "Animal")
        assert s._tracker.change_log == []

    # ---------------------------------------------------- assign_class_description

    def test_assign_class_description_records_one_entry(self):
        s = self._schema()
        s.assign_class_description("Animal", "A living organism")
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "assign_class_description"
        assert s._tracker.change_log[0]["args"] == {"class_name": "Animal"}

    def test_assign_class_description_nonexistent_records_nothing(self):
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.assign_class_description("Ghost", "desc")
        assert s._tracker.change_log == []

    # ---------------------------------------------------- create_property

    def test_create_property_records_one_entry(self):
        s = self._schema()
        s.create_property("Animal", "weight", DATATYPE.INTEGER)
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "create_property"
        args = s._tracker.change_log[0]["args"]
        assert args["class_name"] == "Animal"
        assert args["prop_name"] == "weight"
        assert args["apply_to_subclasses"] is False
        # No apply_to_subclasses intent => the op touched no subclasses.
        assert args["applied_subclasses"] == []

    def test_create_property_apply_to_subclasses_records_one_entry(self):
        """create_property with apply_to_subclasses=True recurses for each
        subclass — the outer call must still produce exactly one entry, and that
        entry records the op-time set of subclasses it applied to (FIX VR-B3)."""
        s = self._schema_with_subclass()
        s.create_property("Animal", "weight", DATATYPE.INTEGER, apply_to_subclasses=True)
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "create_property"
        args = s._tracker.change_log[0]["args"]
        assert args["class_name"] == "Animal"
        assert args["prop_name"] == "weight"
        assert args["apply_to_subclasses"] is True
        assert args["applied_subclasses"] == ["Dog"]

    def test_create_property_nonexistent_class_records_nothing(self):
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.create_property("Ghost", "weight", DATATYPE.INTEGER)
        assert s._tracker.change_log == []

    # ---------------------------------------------------- update_property

    def test_update_property_records_one_entry(self):
        s = self._schema()
        s.update_property("Animal", "age", is_optional=False)
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "update_property"
        args = s._tracker.change_log[0]["args"]
        assert args["class_name"] == "Animal"
        assert args["prop_name"] == "age"
        assert args["apply_to_subclasses"] is False
        assert args["applied_subclasses"] == []

    def test_update_property_apply_to_subclasses_records_one_entry(self):
        """update_property with apply_to_subclasses=True recurses for each
        subclass — the outer call must still produce exactly one entry, and that
        entry records the op-time set of subclasses it applied to (FIX VR-B3)."""
        s = self._schema_with_subclass()
        # First, ensure 'age' exists on the subclass too
        s.create_property("Dog", "age", DATATYPE.INTEGER)
        s._tracker.change_log.clear()
        s.update_property("Animal", "age", is_optional=False, apply_to_subclasses=True)
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "update_property"
        args = s._tracker.change_log[0]["args"]
        assert args["class_name"] == "Animal"
        assert args["prop_name"] == "age"
        assert args["apply_to_subclasses"] is True
        assert args["applied_subclasses"] == ["Dog"]

    def test_update_property_nonexistent_prop_records_nothing(self):
        s = self._schema()
        with pytest.raises(PropertyNotFoundError):
            s.update_property("Animal", "nonExistentProp", is_optional=False)
        assert s._tracker.change_log == []

    # ---------------------------------------------------- rename_property

    def test_rename_property_records_one_entry(self):
        s = self._schema()
        s.rename_property("Animal", "age", "years")
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "rename_property"
        assert s._tracker.change_log[0]["args"] == {
            "class_name": "Animal",
            "old_prop_name": "age",
            "new_prop_name": "years",
        }

    def test_rename_property_conflict_records_nothing(self):
        """Renaming to an already-existing name must raise and record nothing."""
        s = self._schema()
        with pytest.raises(PropertyExistsError):
            s.rename_property("Animal", "age", "label")
        assert s._tracker.change_log == []

    def test_rename_property_nonexistent_prop_records_nothing(self):
        s = self._schema()
        with pytest.raises(PropertyNotFoundError):
            s.rename_property("Animal", "ghost", "anything")
        assert s._tracker.change_log == []

    # ---------------------------------------------------- delete_property

    def test_delete_property_records_one_entry(self):
        s = self._schema()
        s.delete_property("Animal", "age")
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "delete_property"
        assert s._tracker.change_log[0]["args"] == {"class_name": "Animal", "prop_name": "age"}

    def test_delete_property_nonexistent_records_nothing(self):
        s = self._schema()
        with pytest.raises(PropertyNotFoundError):
            s.delete_property("Animal", "ghost")
        assert s._tracker.change_log == []

    # ---------------------------------------------------- assign_property_orders

    def test_assign_property_orders_records_one_entry_with_copy(self):
        s = self._schema()
        orders = {"Animal": ["age", "label"]}
        s.assign_property_orders(orders)
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "assign_property_orders"
        recorded_orders = s._tracker.change_log[0]["args"]["property_orders"]
        assert recorded_orders == {"Animal": ["age", "label"]}

    def test_assign_property_orders_recorded_copy_is_independent(self):
        """The recorded property_orders must be a copy, not an alias of the
        caller's dict, so post-call mutations to the original do not alter the
        op-log entry."""
        s = self._schema()
        orders = {"Animal": ["age", "label"]}
        s.assign_property_orders(orders)
        orders["Animal"].append("extra")  # mutate the INNER list after the call
        recorded_orders = s._tracker.change_log[0]["args"]["property_orders"]
        assert recorded_orders == {"Animal": ["age", "label"]}

    # ---------------------------------------------------- update_schema_metadata

    def test_update_schema_metadata_records_one_entry(self):
        s = self._schema()
        s.update_schema_metadata(name="New Model", version="2.0")
        assert len(s._tracker.change_log) == 1
        assert s._tracker.change_log[0]["op"] == "update_schema_metadata"
        assert s._tracker.change_log[0]["args"] == {"name": "New Model", "version": "2.0"}

    def test_update_schema_metadata_during_construction_records_nothing(self):
        """update_schema_metadata is called inside __init__ before tracking
        state is initialised — the no-op guard must prevent any recording."""
        schema = DatagraphsSchema(name="My Model", version="1.5")
        assert schema._tracker.change_log == []

    def test_update_schema_metadata_during_create_from_records_nothing(self):
        """update_schema_metadata is called inside _set_internal_schema during
        create_from. The subsequent _change_log reset ensures 0 entries."""
        schema = DatagraphsSchema.create_from(self._base_data())
        assert schema._tracker.change_log == []

    # ---------------------------------------------------- tracking depth resets

    def test_tracking_depth_restored_after_exception(self):
        """A raising mutation must leave _tracking_depth at 0."""
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.delete_class("Ghost")
        assert s._tracker.depth == 0

    def test_tracking_depth_restored_after_success(self):
        s = self._schema()
        s.create_class("Plant")
        assert s._tracker.depth == 0

    # ---------------------------------------------------- multiple sequential calls

    def test_multiple_sequential_calls_each_append_one_entry(self):
        s = self._schema()
        s.create_class("Plant")
        s.create_class("Fungus")
        s.delete_class("Fungus")
        assert len(s._tracker.change_log) == 3
        assert s._tracker.change_log[0]["op"] == "create_class"
        assert s._tracker.change_log[1]["op"] == "create_class"
        assert s._tracker.change_log[2]["op"] == "delete_class"


class TestAtomicity:
    """Bug-first regression tests for the property-create/update partial-write.

    The atomic *pre-validation* (round-4 B4) only checked existence / duplicate /
    missing-class up front.  Every error the apply loop raises AFTER that —
    ``InvalidInversePropertyError``, a missing object-range ``ClassNotFoundError``,
    an enum/datatype error — fired *mid-apply*, after earlier cascade targets (and
    the current class's half-built property dict) were already mutated.  Because
    ``_record`` runs only on success, ``change_report`` then OMITTED the failed op
    while the schema was IS half-changed — a partial write with a lying audit trail.

    The fix is snapshot/rollback at the outermost public boundary: on ANY exception
    the schema is restored byte-for-byte, so create/update are genuinely
    all-or-nothing.  These tests therefore assert, after a raise, that:

      * ``to_dict()`` is byte-identical to the pre-call state (no partial write,
        not even a half-built property dict), AND
      * ``change_report`` records NOTHING for the failed op — in BOTH formats
        (the report must not lie about an op the caller saw raise).

    See .sdlc/reviews/schema-change-tracking-uncommitted-5/ (consequences B1 /
    assumptions B2 / maintainability B-HIGH) for the exact reproductions.
    """

    def _empty(self) -> DatagraphsSchema:
        return DatagraphsSchema(name="T", version="1.0")

    def _from(self, s: DatagraphsSchema) -> DatagraphsSchema:
        return DatagraphsSchema.create_from(copy.deepcopy(s.to_dict()))

    def _assert_unchanged_and_silent(self, s: DatagraphsSchema, before: dict) -> None:
        """The schema is byte-identical to *before* and the report is empty in
        BOTH formats — proving no partial write and no lying audit trail."""
        assert s.to_dict() == before, "partial write — schema mutated despite raise"
        assert s.change_report("records") == [], (
            f"report lies: surfaces a change for a raised op:\n"
            f"{s.change_report('records')}"
        )
        assert s.change_report("text") == "", (
            f"text report lies about a raised op:\n{s.change_report('text')}"
        )

    # ------------------------------------------------------------------
    # Fixtures producing a cascade whose inverse_of is valid for the PARENT
    # but invalid for a later subclass (the backref.range == "Parent" only).
    # ------------------------------------------------------------------

    def _cascade_with_subclass(self) -> DatagraphsSchema:
        """Parent -> Child cascade plus a Target class carrying a backref whose
        range is 'Parent', so an inverse_of cascade passes for Parent but raises
        InvalidInversePropertyError for Child."""
        s = self._empty()
        s.create_class("Parent")
        s.create_subclass("Child", "d", "Parent")
        s.create_class("Target")
        s.create_property("Target", "backref", "Parent")  # range == "Parent"
        return self._from(s)

    # ==================================================================
    # CASCADE create_property — mid-apply InvalidInversePropertyError
    # ==================================================================

    def test_create_cascade_inverse_of_invalid_for_subclass_is_atomic(self):
        s = self._cascade_with_subclass()
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(InvalidInversePropertyError):
            s.create_property("Parent", "rel", "Target",
                              inverse_of="backref", apply_to_subclasses=True)

        self._assert_unchanged_and_silent(s, before)
        # No half-built property dict on Parent OR Child.
        for cls in ("Parent", "Child"):
            names = {p["name"] for p in s.find_class(cls)["properties"]}
            assert "rel" not in names, f"{cls} carries a half-built 'rel'"

    # ==================================================================
    # CASCADE update_property — mid-apply InvalidInversePropertyError
    # ==================================================================

    def test_update_cascade_inverse_of_invalid_for_subclass_is_atomic(self):
        s = self._cascade_with_subclass()
        # A valid object property to UPDATE (no inverse yet) on Parent + Child.
        s.create_property("Parent", "rel", "Target", apply_to_subclasses=True)
        s = self._from(s)
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(InvalidInversePropertyError):
            s.update_property("Parent", "rel", description="NEW DESC",
                              inverse_of="backref", apply_to_subclasses=True)

        self._assert_unchanged_and_silent(s, before)
        # Neither Parent.rel nor Child.rel gained the new description or inverseOf.
        for cls in ("Parent", "Child"):
            rel = next(p for p in s.find_class(cls)["properties"]
                       if p["name"] == "rel")
            assert "description" not in rel, f"{cls}.rel got NEW DESC (partial)"
            assert "inverseOf" not in rel, f"{cls}.rel got inverseOf (partial)"

    # ==================================================================
    # CASCADE create_property — mid-apply missing object-range
    # (ClassNotFoundError fires inside _assign_datatype, after append)
    # ==================================================================

    def test_create_cascade_missing_object_range_is_atomic(self):
        s = self._empty()
        s.create_class("Parent")
        s.create_subclass("Child", "d", "Parent")
        s = self._from(s)
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(ClassNotFoundError):
            s.create_property("Parent", "rel", "NoSuchClass",
                              apply_to_subclasses=True)

        self._assert_unchanged_and_silent(s, before)
        for cls in ("Parent", "Child"):
            names = {p["name"] for p in s.find_class(cls)["properties"]}
            assert "rel" not in names, f"{cls} carries a half-built 'rel'"

    # ==================================================================
    # SINGLE-CLASS (no cascade) create_property — mid-apply raise leaves
    # NO half-built property and records nothing.
    # ==================================================================

    def test_single_class_create_inverse_of_raise_is_atomic(self):
        s = self._empty()
        s.create_class("Target")
        s.create_class("Owner")
        # backref.range is NOT "Owner", so inverse_of validation raises.
        s.create_property("Target", "backref", "Owner")
        # Make backref point at the wrong class so the inverse is invalid for Owner.
        s.update_property("Target", "backref", datatype="Target")
        s = self._from(s)
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(InvalidInversePropertyError):
            s.create_property("Owner", "rel", "Target", inverse_of="backref")

        self._assert_unchanged_and_silent(s, before)
        # The half-built property must NOT linger on Owner.
        names = {p["name"] for p in s.find_class("Owner")["properties"]}
        assert "rel" not in names, "single-class partial write: 'rel' lingers"

    def test_single_class_create_missing_object_range_is_atomic(self):
        s = self._empty()
        s.create_class("Owner")
        s = self._from(s)
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(ClassNotFoundError):
            s.create_property("Owner", "rel", "NoSuchClass")

        self._assert_unchanged_and_silent(s, before)
        names = {p["name"] for p in s.find_class("Owner")["properties"]}
        assert "rel" not in names, "single-class partial write: 'rel' lingers"

    # ==================================================================
    # SINGLE-CLASS update_property — mid-apply raise is atomic.
    # ==================================================================

    def test_single_class_update_inverse_of_raise_is_atomic(self):
        s = self._empty()
        s.create_class("Target")
        s.create_class("Owner")
        s.create_property("Target", "backref", "Target")  # range is Target, not Owner
        s.create_property("Owner", "rel", "Target")        # object prop to update
        s = self._from(s)
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(InvalidInversePropertyError):
            s.update_property("Owner", "rel", description="NEW DESC",
                              inverse_of="backref")

        self._assert_unchanged_and_silent(s, before)
        rel = next(p for p in s.find_class("Owner")["properties"]
                   if p["name"] == "rel")
        assert "description" not in rel, "single-class partial write: description set"
        assert "inverseOf" not in rel, "single-class partial write: inverseOf set"

    # ==================================================================
    # create_subclass — a mid-cascade raise (a parent property whose
    # inverse_of is valid for the parent but NOT for the new subclass)
    # must leave NO half-built subclass: the compound create_class +
    # per-property loop is a single all-or-nothing op.
    # ==================================================================

    def _subclass_with_invalid_inherited_inverse(self) -> DatagraphsSchema:
        """Parent carries an object property 'rel' whose inverseOf 'backref'
        (on Target) has range 'Parent'.  Copying 'rel' onto a NEW subclass
        re-validates inverseOf against the subclass name and raises, so
        create_subclass fails mid-loop AFTER create_class already ran."""
        s = self._empty()
        s.create_class("Parent")
        s.create_class("Target")
        s.create_property("Target", "backref", "Parent")  # backref.range == "Parent"
        s.create_property("Parent", "rel", "Target", inverse_of="backref")  # valid for Parent
        return self._from(s)

    def test_create_subclass_midloop_raise_is_atomic(self):
        s = self._subclass_with_invalid_inherited_inverse()
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(InvalidInversePropertyError):
            s.create_subclass("Child", "a child", "Parent")

        self._assert_unchanged_and_silent(s, before)
        # The half-built subclass (created by the inner create_class before the
        # loop raised) must NOT linger — not even its default label property.
        assert s.find_class("Child") is None, "half-built subclass 'Child' lingers"

    # ==================================================================
    # assign_label_property — designating a NON-EXISTENT property must
    # raise WITHOUT first corrupting class.labelProperty (it set the
    # name before validating the property exists).
    # ==================================================================

    def test_assign_label_property_nonexistent_is_atomic(self):
        s = self._empty()
        s.create_class("C")  # default label property is "label"
        s = self._from(s)
        before = copy.deepcopy(s.to_dict())
        original_label = s.find_class("C")["labelProperty"]

        with pytest.raises(PropertyNotFoundError):
            s.assign_label_property("C", "nonexistent")

        self._assert_unchanged_and_silent(s, before)
        # labelProperty must be untouched — not corrupted to the non-existent name
        # (which would be serialisable to the backend via to_dict()/apply_schema).
        assert s.find_class("C")["labelProperty"] == original_label, (
            "labelProperty corrupted to a non-existent property name"
        )

    # ==================================================================
    # In-place rollback restore: an externally-held reference to the
    # classes list (e.g. obtained via the public `classes` view) must
    # remain consistent with the schema after a rolled-back mutation —
    # the snapshot is restored into the SAME list object, not rebound.
    # ==================================================================

    def test_rollback_keeps_external_classes_reference_consistent(self):
        s = self._empty()
        s.create_class("C")
        s = self._from(s)
        external_ref = s.classes  # caller holds the live list
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(PropertyNotFoundError):
            s.assign_label_property("C", "nonexistent")

        # The externally-held reference still IS the schema's class list and
        # reflects the rolled-back (unchanged) state.
        assert external_ref is s.classes, "rollback rebound the classes list object"
        assert s.to_dict()["classes"] == before["classes"]


class TestMutationPerformanceScaling:
    """Regression tests for the atomic-rollback snapshot cost (perf bug).

    The all-or-nothing ``_atomic`` guard originally deep-copied the ENTIRE class
    list on every outermost mutation.  That made two common workloads quadratic:

      * **Narrow construction.**  Building an N-class schema one ``create_class`` /
        ``create_subclass`` at a time deep-copied the whole (growing) class list
        per call — O(N²).  An 8x larger schema took ~64x longer, not ~8x.
      * **Accumulated wide cascades.**  A sequence of L ``create_property`` /
        ``update_property`` calls with ``apply_to_subclasses=True`` at fixed
        subclass-count C deep-copied every class (each carrying the properties
        added by prior calls) on every call — O(L²·C).  8x the calls took ~55x
        longer.

    The fix scopes rollback state to each operation's footprint: a shallow class-
    list snapshot for structure plus a property-granular undo journal for the
    cascade hot path (one O(1) entry per appended/updated property instead of a
    whole-class deep copy).  Construction becomes O(N) and accumulated cascades
    O(L·C).

    These tests time an 8x size step and assert the ratio stays well under the
    quadratic prediction.  Pre-fix they FAIL (~64x / ~55x ≫ the 24x ceiling);
    post-fix they pass comfortably (~8x).  The 24x ceiling sits far from both the
    linear (~8x) and quadratic (~64x) predictions, so the test discriminates the
    asymptotic class while tolerating timing noise.
    """

    def _empty(self) -> DatagraphsSchema:
        return DatagraphsSchema(name="T", version="1.0")

    def test_narrow_construction_is_subquadratic_in_class_count(self):
        """Building N classes one at a time must scale ~linearly in N, not O(N²)
        (the whole-list deep-copy-per-mutation snapshot bug)."""
        import time

        def time_build(n: int) -> float:
            start = time.perf_counter()
            s = self._empty()
            s.create_class("Root")
            for i in range(n):
                s.create_subclass(f"C{i}", "d", "Root")
            return time.perf_counter() - start

        small = time_build(200)
        large = time_build(1600)  # 8x the classes
        assert large < small * 24 + 0.2, (
            f"super-linear (O(N^2)?) construction: small={small:.4f}s "
            f"large={large:.4f}s ratio={large / max(small, 1e-6):.1f}x"
        )

    def _wide_tree(self, n_children: int) -> DatagraphsSchema:
        s = self._empty()
        s.create_class("Animal")
        for i in range(n_children):
            s.create_subclass(f"S{i}", "d", "Animal")
        return s

    def test_repeated_wide_create_cascade_is_subquadratic_in_op_count(self):
        """A sequence of L wide create-cascades at fixed C must scale ~linearly in
        L, not O(L²·C) (per-call whole-class deep copy of property-heavy classes)."""
        import time

        def time_cascades(n_ops: int) -> float:
            s = self._wide_tree(200)  # fixed C
            start = time.perf_counter()
            for k in range(n_ops):
                s.create_property("Animal", f"p{k}", DATATYPE.TEXT,
                                  apply_to_subclasses=True)
            return time.perf_counter() - start

        small = time_cascades(15)
        large = time_cascades(120)  # 8x the ops, C fixed
        assert large < small * 24 + 0.2, (
            f"super-linear (O(L^2*C)?) accumulated create-cascade: small={small:.4f}s "
            f"large={large:.4f}s ratio={large / max(small, 1e-6):.1f}x"
        )

    def test_repeated_wide_update_cascade_is_subquadratic_in_op_count(self):
        """A sequence of L wide update-cascades at fixed C must scale ~linearly in
        L, not O(L²·C) — the update hot path journals one property per target,
        not the whole class dict."""
        import time

        def time_updates(n_ops: int) -> float:
            s = self._wide_tree(200)  # fixed C
            # Seed properties to update, OUTSIDE the timed region.
            for k in range(n_ops):
                s.create_property("Animal", f"p{k}", DATATYPE.TEXT,
                                  apply_to_subclasses=True)
            start = time.perf_counter()
            for k in range(n_ops):
                s.update_property("Animal", f"p{k}", is_optional=False,
                                  apply_to_subclasses=True)
            return time.perf_counter() - start

        small = time_updates(15)
        large = time_updates(120)  # 8x the ops, C fixed
        assert large < small * 24 + 0.2, (
            f"super-linear (O(L^2*C)?) accumulated update-cascade: small={small:.4f}s "
            f"large={large:.4f}s ratio={large / max(small, 1e-6):.1f}x"
        )
