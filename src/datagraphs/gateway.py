"""Gateway for synchronising data between the filesystem and a DataGraphs project."""

import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from datagraphs.client import AuthenticationError, FailedWrite, PutResult, TimedOutWrite, TokenRequestError
from datagraphs.client import Client as DatagraphsClient
from datagraphs.dataset import Dataset
from datagraphs.enums import ISOLATION_MODE, SCHEMA_APPLY_MODE, VALIDATION_MODE
from datagraphs.schema import Schema
from datagraphs.utils import rewrite_urns_for_project

_logger = logging.getLogger(__name__)

# The process umask, read once at import (reading it means briefly setting it),
# so that dumped files get the permissions a plain open() would give them.
_UMASK = os.umask(0)
os.umask(_UMASK)

class SkipReason(Enum):
    """Why `Gateway.load_data` did not load a class."""

    NO_FILE = 'no data file'
    EMPTY_FILE = 'data file holds no entities'
    NOT_IN_DATASET = 'class not found in any dataset'

@dataclass(frozen=True)
class SkippedClass:
    """A class that `Gateway.load_data` did not load, and why.

    :ivar reason: Why the class was skipped.
    :ivar path: The data file looked for or read, or ``None`` when the class
        was skipped before a file was involved (`SkipReason.NOT_IN_DATASET`).
    """

    reason: SkipReason
    path: Path | None = None

@dataclass
class LoadResult:
    """The outcome of `Gateway.load_data`.

    Each reported entity's ``index`` is its position in its class's data file,
    so entities without an ``id`` can still be located.

    :ivar loaded: The number of entities confirmed as written.
    :ivar skipped: Per class, why it was not loaded: no data file, an empty
        one, or (for a single requested class) no dataset. Base classes, whose
        data loads via their subclasses, are not included.
    :ivar failed: Per class, the entities that were not written and why.
    :ivar timed_out: Per class, the entities whose write timed out and may or
        may not have completed (see `PutResult.timed_out` for how to verify
        them before reloading).
    :ivar class_errors: Per class, the exception that stopped its file being
        loaded at all (e.g. a `json.JSONDecodeError` for invalid JSON, or a
        `ValueError` for a malformed entity ``id``).
    """

    loaded: int = 0
    skipped: dict[str, SkippedClass] = field(default_factory=dict)
    failed: dict[str, list[FailedWrite]] = field(default_factory=dict)
    timed_out: dict[str, list[TimedOutWrite]] = field(default_factory=dict)
    class_errors: dict[str, Exception] = field(default_factory=dict)

    @property
    def failed_count(self) -> int:
        """The total number of entities that were not written."""
        return sum(len(failures) for failures in self.failed.values())

    @property
    def timed_out_count(self) -> int:
        """The total number of entities whose write timed out."""
        return sum(len(timeouts) for timeouts in self.timed_out.values())

    @property
    def ok(self) -> bool:
        """True when every entity found was confirmed as written.

        Skipped classes do not affect this: a class with no data file has
        nothing to load.
        """
        return not self.failed and not self.timed_out and not self.class_errors

@dataclass
class DumpResult:
    """The outcome of `Gateway.dump_data`.

    :ivar exported: The number of entities written to files.
    :ivar files: Per class, the file its entities were written to.
    :ivar failed: Per class, the exception that stopped it being exported.
        No file is written for a failed class, so a file from an earlier
        export is left as it was.
    """

    exported: int = 0
    files: dict[str, Path] = field(default_factory=dict)
    failed: dict[str, Exception] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        """True when every class requested was exported in full."""
        return not self.failed

class Gateway:
    """Higher-level wrapper for deploying projects and bulk import/export of data."""

    def __init__(self, client: DatagraphsClient, wait_time_ms: int = DatagraphsClient.DEFAULT_WAIT_TIME_MS) -> None:
        """Initialise the Gateway.

        :param client: A DataGraphs API client.
        :param wait_time_ms: Delay in milliseconds between successive API calls.
        """
        self._client = client
        self._wait_time_ms = wait_time_ms
        self._client.set_wait_time(wait_time_ms)
        self._schema = None

    @property
    def client(self) -> DatagraphsClient:
        """The underlying `Client` instance."""
        return self._client

    def _get_schema(self) -> Schema:
        if self._schema is None:
            self._schema = self._client.get_schema()
        return self._schema

    def load_project(self, schema: Schema, datasets: list[Dataset], validation_mode: VALIDATION_MODE = VALIDATION_MODE.PROMPT) -> None:
        """Deploy the project schema and datasets to the API.

        N.B. This deletes and/or removes all blocking data dependencies before applying the new schema and datasets.

        :param schema: The schema to deploy.
        :param datasets: The datasets to deploy.
        :param validation_mode: How to handle mismatches between deployment and
            existing datasets. See `VALIDATION_MODE`.
        :raises ValueError: If *validation_mode* is not a `VALIDATION_MODE`
            value, or duplicate class names are found across datasets.
        """
        validation_mode = VALIDATION_MODE(validation_mode)
        if self._validate_datasets(datasets, self._client.get_datasets(), validation_mode):
            try:
                self._client.apply_schema(schema, mode=SCHEMA_APPLY_MODE.FORCE)
                self._client.apply_datasets(datasets)
            except Exception as e:  # noqa: BLE001 - intentional: surface any deploy failure via logging rather than propagating
                _logger.error('Error loading project: %s', str(e))

    def _validate_datasets(self, deployment_datasets: list[Dataset], existing_datasets: list[Dataset], validation_mode: VALIDATION_MODE) -> None:
        if self._is_empty_project(existing_datasets):
            _logger.info('The target project is empty - skipping dataset validation')
            return True
        """Validate that the local and API datasets match in terms of class names."""
        if validation_mode == VALIDATION_MODE.BYPASS:
            _logger.warning('Validation mode set to BYPASS - skipping all dataset validations')
            return True 
        deployment_classes = self._ensure_no_duplicate_classes(deployment_datasets)
        existing_results = self._verify_datasets_against_classlist(existing_datasets, deployment_classes)
        deployment_results = self._verify_datasets_against_classlist(deployment_datasets, existing_results[0])
        if len(existing_results[1]) > 0 or len(deployment_results[1]) > 0:
            warning_lines = [f'The class {m["class_name"]} was found in existing dataset {m["dataset_slug"]} but not in any deployment dataset.' for m in existing_results[1]]
            warning_lines += [f'The class {m["class_name"]} was found in deployment dataset {m["dataset_slug"]} but not in any existing dataset.' for m in deployment_results[1]]
            warning_message = 'Dataset validation found mismatches between deployment and existing datasets:\n' + '\n'.join(warning_lines)
            _logger.warning(warning_message)
            if validation_mode == VALIDATION_MODE.PROMPT:
                response = input(f'{warning_message}\nDo you wish to continue? (y/n): ').strip().lower()
                return response == 'y'
            else:
                return True
        return True

    def _is_empty_project(self, datasets: list[Dataset]) -> bool:
        return len([cls for d in datasets for cls in d.classes]) == 0

    def _ensure_no_duplicate_classes(self, datasets: list[Dataset]) -> None:
        """Validate that no duplicate class names are found across the provided datasets."""
        seen_classes: dict[str, str] = {}
        dataset_classes = [(cls, d.slug) for d in datasets for cls in d.classes]
        for class_name, dataset_slug in dataset_classes:
            if class_name in seen_classes:
                raise ValueError(
                    f'Duplicate class {class_name} found in dataset {dataset_slug} '
                    f'- already defined in dataset {seen_classes[class_name]}'
                    )
            seen_classes[class_name] = dataset_slug
        return seen_classes.keys()

    def _verify_datasets_against_classlist(self, datasets: list[Dataset], classes: list[str]) -> None:
        missing_classes = []
        dataset_classes = set()
        for dataset in datasets:
            dataset_classes.update(dataset.classes)
            for class_name in dataset.classes:
                if class_name not in classes:
                    missing_classes.append({'class_name': class_name, 'dataset_slug': dataset.slug})
        return dataset_classes, missing_classes

    def dump_project(self, schema_path: str | Path, datasets_path: str | Path) -> None:
        """Export the project schema and dataset configurations to JSON files.

        Files are named ``{project_name}-v{version}-schema.json`` and
        ``{project_name}-v{version}-datasets.json``.

        :param schema_path: Directory to write the schema JSON file.
        :param datasets_path: Directory to write the datasets JSON file.
        """
        name_prefix = f'{self._client.project_name}-v{self._get_schema().version}'
        self._dump_schema(schema_path, name_prefix)
        self._dump_datasets(datasets_path, name_prefix)

    def _dump_schema(self, schema_path: str | Path, name_prefix: str) -> None:
        """Dump the project schema to a JSON file."""
        schema_path = Path(schema_path) / f'{name_prefix}-schema.json'
        schema = self._client.get_schema()  
        with open(schema_path, 'w', encoding='utf-8') as schema_file:
            json.dump(schema.to_dict(), schema_file, indent=2)
    
    def _dump_datasets(self, datasets_path: str | Path, name_prefix: str) -> None:
        """Dump the project datasets to JSON files."""
        datasets_path = Path(datasets_path) / f'{name_prefix}-datasets.json'
        datasets = self._client.get_datasets()
        json_data = [dataset.to_dict() for dataset in datasets]
        with open(datasets_path, 'w', encoding='utf-8') as data_file:
            json.dump(json_data, data_file, indent=2)

    def load_data(
        self,
        class_name: str = Schema.ALL_CLASSES,
        from_dir_path: str | Path = "",
        file_path: str | Path = "",
        isolation: ISOLATION_MODE = ISOLATION_MODE.BISECT,
    ) -> LoadResult:
        """Load data from JSON files into the DataGraphs project.

        Base classes (those with subclasses) are automatically skipped when
        loading all classes. Check `LoadResult.ok` on the result: a load that
        returns normally may still have failed or timed-out entities.

        :param class_name: The class name to load, or ``Schema.ALL_CLASSES`` to
            load every non-base class found across all datasets.
        :param from_dir_path: Directory containing ``<ClassName>.json`` files.
        :param file_path: Explicit path to a single JSON file (used when loading
            a specific *class_name*).
        :param isolation: How to find the rejected entities of a batch the API
            rejects (see `Client.put`).
        :returns: The outcome of the load, per class.
        :raises ValueError: If *isolation* is not an `ISOLATION_MODE` value.
        :raises AuthenticationError: If authentication fails; no further
            classes are attempted.
        :raises TokenRequestError: If an auth token cannot be obtained; no
            further classes are attempted.
        """
        isolation = ISOLATION_MODE(isolation)
        from_dir_path = Path(from_dir_path) if from_dir_path else Path()
        result = LoadResult()
        datasets = self._client.get_datasets()
        if class_name == Schema.ALL_CLASSES:
            for dataset in datasets:
                self._load_data_for_dataset(result, dataset, from_dir_path, isolation)
        else:
            dataset = next((d for d in datasets if class_name in d.classes), None)
            if dataset is not None:
                self._load_data_for_class(result, class_name, dataset.slug, isolation, from_dir_path, file_path)
            else:
                _logger.error('The class %s was not found in any dataset - cannot load data.', class_name)
                result.skipped[class_name] = SkippedClass(SkipReason.NOT_IN_DATASET)
        self._log_load_result(result)
        return result

    def _load_data_for_dataset(self, result: LoadResult, dataset: Dataset, from_dir_path: str | Path, isolation: ISOLATION_MODE) -> None:
        """Load data for all non-base classes in a dataset."""
        for dataset_class in dataset.classes:
            if len(self._get_schema().find_subclasses(dataset_class)) == 0:
                self._load_data_for_class(result, dataset_class, dataset.slug, isolation, from_dir_path)
            else:
                _logger.info('%s is a baseclass - not loading as data will be loaded via subclasses', dataset_class)

    def _load_data_for_class(self, result: LoadResult, class_name: str, dataset_slug: str, isolation: ISOLATION_MODE, from_dir_path: str | Path = "", file_path: str | Path = "") -> None:
        """Load data for a specific class from a JSON file, accounting for it in *result*."""
        try:
            outcome = self._load_from_file(class_name, dataset_slug, isolation, from_dir_path, file_path)
        except (AuthenticationError, TokenRequestError):
            raise
        except Exception as e:  # noqa: BLE001 - intentional: log the failing class, record the error, keep loading others
            _logger.error('Error loading data for %s: %s', class_name, str(e))
            result.class_errors[class_name] = e
            return
        if isinstance(outcome, SkippedClass):
            result.skipped[class_name] = outcome
            return
        put_result = outcome
        result.loaded += put_result.loaded
        if put_result.failed:
            result.failed[class_name] = put_result.failed
        if put_result.timed_out:
            result.timed_out[class_name] = put_result.timed_out

    def _load_from_file(
        self,
        class_name: str,
        dataset_slug: str,
        isolation: ISOLATION_MODE,
        from_dir_path: str | Path = "",
        file_path: str | Path = "",
    ) -> PutResult | SkippedClass:
        """Read a JSON file and PUT its contents into the project.

        Returns:
            The outcome of the write, or why the class was skipped if there
            was no file or it held no entities.
        """
        json_file_path = Path(from_dir_path).joinpath(f'{class_name}.json') if not file_path else Path(file_path)
        if not json_file_path.is_file():
            _logger.error('No file found at %s...', json_file_path)
            return SkippedClass(SkipReason.NO_FILE, json_file_path)
        _logger.info('Reading data from %s...', json_file_path)
        with open(json_file_path, 'r', encoding='utf-8') as dataFile:
            data = json.load(dataFile)
        if len(data) == 0:
            _logger.warning('No entities found in file %s...', json_file_path)
            return SkippedClass(SkipReason.EMPTY_FILE, json_file_path)
        data = self._map_data_project_urns(data)
        _logger.info('Writing data for %s...', class_name)
        return self._client.put(dataset_slug, data, isolation=isolation)

    @staticmethod
    def _log_load_result(result: LoadResult) -> None:
        if result.ok:
            _logger.info('Loaded %d entities (%d classes skipped)', result.loaded, len(result.skipped))
            return
        _logger.error(
            'Data load incomplete: %d entities loaded, %d failed, %d timed out, %d classes errored, %d classes skipped',
            result.loaded, result.failed_count, result.timed_out_count, len(result.class_errors), len(result.skipped),
        )
        for class_name, failures in result.failed.items():
            _logger.error('   %s: %d failed: %s', class_name, len(failures), [f.describe() for f in failures])
        for class_name, timeouts in result.timed_out.items():
            _logger.error('   %s: %d timed out (may have been written - verify before reloading): %s',
                          class_name, len(timeouts), [t.describe() for t in timeouts])
        for class_name, error in result.class_errors.items():
            _logger.error('   %s: not loaded: %s', class_name, error)

    def _map_data_project_urns(self, data: list[dict]) -> list[dict]:
        """Re-map URN project segments to match the current client project.

        Args:
            data: A list of entity dicts, each containing an ``id`` URN.

        Returns:
            The list with URNs re-mapped where necessary.

        Raises:
            ValueError: If an entity is not a dict, or has an ``id`` that is not a string.
        """
        entities = []
        for entity in data:
            if not isinstance(entity, dict):
                raise ValueError(f'Invalid format - could not read data: {entity!s}')  # noqa: TRY004 - ValueError kept for consistency with the sibling raise below and the documented contract
            if 'id' in entity and not isinstance(entity['id'], str):
                raise ValueError(f'Expected id property to be string - found type {type(entity["id"])}')
            entities.append(rewrite_urns_for_project(entity, self._client.project_name))
        return entities

    def dump_data(self, to_dir_path: str | Path, class_name: str = Schema.ALL_CLASSES, excluded_class_names: str = "", include_date_fields: bool = False) -> DumpResult:
        """Export entity data from the project to JSON files on disk.

        Each class is written to a separate ``<ClassName>.json`` file, and
        only once every entity of the class has been read, so a file never
        holds a partial export. A class that cannot be read is reported in
        `DumpResult.failed` and the remaining classes are still exported:
        check `DumpResult.ok` on the result. Base classes are automatically
        skipped.

        :param to_dir_path: Target directory. Created automatically if it does
            not exist.
        :param class_name: The class name to dump, or ``Schema.ALL_CLASSES``
            for all.
        :param excluded_class_names: Comma-separated list of classes to exclude from the export if class_name is ``Schema.ALL_CLASSES``.
        :param include_date_fields: Whether to include system date metadata.
        :returns: The outcome of the export, per class.
        :raises AuthenticationError: If authentication fails; no further
            classes are attempted.
        :raises TokenRequestError: If an auth token cannot be obtained; no
            further classes are attempted.
        """
        to_dir_path = Path(to_dir_path)
        to_dir_path.mkdir(parents=True, exist_ok=True)
        result = DumpResult()

        if class_name == Schema.ALL_CLASSES:
            datasets = self._client.get_datasets()
            dataset_classes = [cls for d in datasets for cls in d.classes]
            excluded_classes = [cls.strip() for cls in excluded_class_names.split(',')] if excluded_class_names else []
            for dataset_class in dataset_classes:
                if dataset_class in excluded_classes:
                    continue
                if len(self._get_schema().find_subclasses(dataset_class)) == 0:   
                    self._dump_class(result, dataset_class, to_dir_path, include_date_fields)
                    time.sleep(self._wait_time_ms / 1000)
        else:
            self._dump_class(result, class_name, to_dir_path, include_date_fields)
        if result.ok:
            _logger.info('Exported %d entities from %d classes', result.exported, len(result.files))
        else:
            _logger.error('Data export incomplete: %d entities exported, %d classes failed: %s',
                          result.exported, len(result.failed), sorted(result.failed))
        return result

    def _dump_class(self, result: DumpResult, class_name: str, to_dir_path: Path, include_date_fields: bool) -> None:
        """Export one class to its file, accounting for it in *result*."""
        try:
            file_path, count = self._persist_to_file(class_name, to_dir_path, include_date_fields)
        except (AuthenticationError, TokenRequestError):
            raise
        except Exception as e:  # noqa: BLE001 - intentional: record the failing class and keep exporting the rest
            _logger.error('Error exporting data for %s: %s', class_name, str(e))
            result.failed[class_name] = e
            return
        result.files[class_name] = file_path
        result.exported += count

    def _persist_to_file(self, class_name: str, to_dir_path: Path, include_date_fields: bool) -> tuple[Path, int]:
        """Fetch entities of *class_name* from the API and write them to a JSON file.

        The file is written to a temporary name and then renamed into place, so
        it is either complete or left as it was. It keeps the permissions of the
        file it replaces, or, for a new file, gets those the umask allows.

        :returns: The file written, and the number of entities in it.
        """
        _logger.info('Fetching data for %s...', class_name)
        data = self._client.get(class_name=class_name, include_date_fields=include_date_fields)
        file_path = to_dir_path.joinpath(f'{class_name}.json')
        mode = file_path.stat().st_mode & 0o7777 if file_path.exists() else 0o666 & ~_UMASK
        fd, tmp_path = tempfile.mkstemp(dir=to_dir_path, prefix=f'.{class_name}.', suffix='.json.tmp')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as data_file:
                json.dump(data, data_file, indent=2)
            # mkstemp creates the file owner-only.
            os.chmod(tmp_path, mode)
            os.replace(tmp_path, file_path)
        except BaseException:
            Path(tmp_path).unlink(missing_ok=True)
            raise
        return file_path, len(data)

    def clear_down(self) -> None:
        """Clear all data from all datasets in the project.

        The datasets themselves are preserved.
        """
        datasets = self._client.get_datasets()
        for dataset in datasets:
            self._client.clear_dataset(dataset.slug)