"""Gateway for synchronising data between the filesystem and a DataGraphs project."""

import time
import json
import logging
from pathlib import Path
from typing import Union
from datagraphs.client import Client as DatagraphsClient
from datagraphs.schema import Schema
from datagraphs.dataset import Dataset
from datagraphs.utils import get_project_from_urn, map_project_name
from datagraphs.enums import VALIDATION_MODE

logger = logging.getLogger(__name__)

class Gateway:
    """Gateway for loading and dumping data between the filesystem and a Datagraphs project."""

    DEFAULT_WAIT_TIME_MS = 200
    UNKNOWN_PROJECT_NAME = '__unknown__'

    def __init__(self, client: DatagraphsClient, wait_time_ms: int = DEFAULT_WAIT_TIME_MS) -> None:
        """Initialise the Gateway.

        Args:
            client: A Datagraphs API client.
            wait_time_ms: Delay in milliseconds between successive API calls.
        """
        self._client = client
        self._wait_time_ms = wait_time_ms
        self._schema = None

    @property
    def client(self) -> DatagraphsClient:
        return self._client

    def _get_schema(self) -> Schema:
        if self._schema is None:
            self._schema = self._client.get_schema()
        return self._schema

    def load_project(self, schema: Schema, datasets: list[Dataset], validation_mode: VALIDATION_MODE = VALIDATION_MODE.PROMPT) -> None:
        """Deploy the project schema and datasets to the API."""
        if self._validate_datasets(datasets, self._client.get_datasets(), validation_mode):
            self._client.tear_down()
            self._client.apply_schema(schema)
            self._client.apply_datasets(datasets)        

    def _validate_datasets(self, deployment_datasets: list[Dataset], existing_datasets: list[Dataset], validation_mode: VALIDATION_MODE) -> None:
        """Validate that the local and API datasets match in terms of class names."""
        if validation_mode == VALIDATION_MODE.BYPASS:
            logger.warning('Validation mode set to BYPASS - skipping all dataset validations')
            return True 
        deployment_classes = self._ensure_no_duplicate_classes(deployment_datasets)
        existing_results = self._verify_datasets_against_classlist(existing_datasets, deployment_classes)
        deployment_results = self._verify_datasets_against_classlist(deployment_datasets, existing_results[0])
        if len(existing_results[1]) > 0 or len(deployment_results[1]) > 0:
            warning_lines = [f'The class {m["class_name"]} was found in existing dataset {m["dataset_slug"]} but not in any deployment dataset.' for m in existing_results[1]]
            warning_lines += [f'The class {m["class_name"]} was found in deployment dataset {m["dataset_slug"]} but not in any existing dataset.' for m in deployment_results[1]]
            warning_message = 'Dataset validation found mismatches between deployment and existing datasets:\n' + '\n'.join(warning_lines)
            logger.warning(warning_message)
            if validation_mode == VALIDATION_MODE.PROMPT:
                response = input(f'{warning_message}\nDo you wish to continue? (y/n): ').strip().lower()
                return response == 'y'
            else:
                return True
        return True

    def _ensure_no_duplicate_classes(self, datasets: list[Dataset]) -> None:
        """Validate that no duplicate class names are found across the provided datasets."""
        seen_classes: dict[str, str] = {}
        for dataset in datasets:
            for class_name in dataset.classes:
                if class_name in seen_classes:
                    raise ValueError(
                        f'Duplicate class {class_name} found in dataset {dataset.slug} '
                        f'- already defined in dataset {seen_classes[class_name]}'
                    )
                seen_classes[class_name] = dataset.slug
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

    def dump_project(self, schema_path: Union[str, Path], datasets_path: Union[str, Path]) -> None:
        """Dump the project schema and datasets to the filesystem."""
        name_prefix = f'{self._client.project_name}-v{self._get_schema().version}'
        self._dump_schema(schema_path, name_prefix)
        self._dump_datasets(datasets_path, name_prefix)

    def _dump_schema(self, schema_path: Union[str, Path], name_prefix: str) -> None:
        """Dump the project schema to a JSON file."""
        schema_path = Path(schema_path) / f'{name_prefix}-schema.json'
        schema = self._client.get_schema()  
        with open(schema_path, 'w', encoding='utf-8') as schema_file:
            json.dump(schema.to_dict(), schema_file, indent=2)
    
    def _dump_datasets(self, datasets_path: Union[str, Path], name_prefix: str) -> None:
        """Dump the project datasets to JSON files."""
        datasets_path = Path(datasets_path) / f'{name_prefix}-datasets.json'
        datasets = self._client.get_datasets()
        json_data = [dataset.to_dict() for dataset in datasets]
        with open(datasets_path, 'w', encoding='utf-8') as data_file:
            json.dump(json_data, data_file, indent=2)

    def load_data(
        self,
        class_name: str = Schema.ALL_CLASSES,
        from_dir_path: Union[str, Path] = "",
        file_path: Union[str, Path] = "",
    ) -> dict:
        """Load data from JSON files into the Datagraphs project.

        Args:
            class_name: The class name to load, or ``Schema.ALL_CLASSES`` to load every
                non-base class found across all datasets.
            from_dir_path: Directory containing ``<ClassName>.json`` files.
            file_path: Explicit path to a single JSON file (used when loading a
                specific *class_name*).

        Returns:
            A dict with ``loaded`` and ``skipped`` counts.

        Raises:
            FileNotFoundError: If *file_path* is supplied but does not exist.
            ValueError: If the requested *class_name* is not found in any dataset.
        """
        from_dir_path = Path(from_dir_path) if from_dir_path else Path()
        stats = {"loaded": 0, "skipped": 0}

        datasets = self._client.get_datasets()
        for dataset in datasets:
            for dataset_class in dataset.classes:
                if len(self._get_schema().find_subclasses(dataset_class)) == 0:
                    if dataset_class == class_name:
                        try:
                            result = self._load_from_file(dataset_class, dataset.slug, from_dir_path, file_path)
                            stats["loaded"] += result["loaded"]
                            stats["skipped"] += result["skipped"]
                        except Exception as e:
                            logger.error('Error loading data for %s: %s', dataset_class, str(e))
                            stats["skipped"] += 1
                        return stats
                    elif class_name == Schema.ALL_CLASSES:
                        try:
                            result = self._load_from_file(dataset_class, dataset.slug, from_dir_path)
                            stats["loaded"] += result["loaded"]
                            stats["skipped"] += result["skipped"]
                            time.sleep(self._wait_time_ms / 1000)
                        except Exception as e:
                            logger.error('Error loading data for %s: %s', dataset_class, str(e))
                            stats["skipped"] += 1
                else:
                    logger.info('%s is a baseclass - not loading as data will be loaded via subclasses', dataset_class)
        if class_name != Schema.ALL_CLASSES:
            logger.error('The class %s was not found in any dataset - cannot load data for this class.', class_name)
            stats["skipped"] += 1
        return stats

    def _load_from_file(
        self,
        class_name: str,
        dataset_slug: str,
        from_dir_path: Union[str, Path] = "",
        file_path: Union[str, Path] = "",
    ) -> dict:
        """Read a JSON file and PUT its contents into the project.

        Returns:
            A dict with ``loaded`` and ``skipped`` counts.

        Raises:
            FileNotFoundError: If *file_path* was explicitly provided but does not exist.
        """
        json_file_path = Path(from_dir_path).joinpath(f'{class_name}.json') if not file_path else Path(file_path)
        if json_file_path.is_file():
            logger.info('Reading data from %s...', json_file_path)
            with open(json_file_path, 'r', encoding='utf-8') as dataFile:
                data = json.load(dataFile)
                if len(data) > 0:
                    data = self._map_data_project_urns(data)
                    logger.info('Writing data for %s...', class_name)
                    self._client.put(dataset_slug, data)
                    return {"loaded": len(data), "skipped": 0}
                else:
                    logger.warning('No entities found in file %s...', json_file_path)
                    return {"loaded": 0, "skipped": 1}
        else:
            logger.error('No file found at %s...', json_file_path)
            return {"loaded": 0, "skipped": 1}

    def _map_data_project_urns(self, data: list[dict]) -> list[dict]:
        """Re-map URN project segments to match the current client project.

        Args:
            data: A list of entity dicts, each containing an ``id`` URN.

        Returns:
            The list with URNs re-mapped where necessary.

        Raises:
            ValueError: If an entity is missing a valid string ``id``.
        """
        entities = []
        for entity in data:
            if isinstance(entity, dict):
                source_project_name = self._get_project_name_from_entity(entity)
                if source_project_name != self._client.project_name:
                    entity = map_project_name(entity, from_urn=f'urn:{source_project_name}:', to_urn=f'urn:{self._client.project_name}:')
                entities.append(entity)
            else:
                raise ValueError(f'Invalid format - could not read data: {str(entity)}')
        return entities

    def _get_project_name_from_entity(self, entity: dict) -> str:
        project_name = self.UNKNOWN_PROJECT_NAME
        if 'id' in entity:
            if isinstance(entity['id'], str):
                project_name = get_project_from_urn(entity['id'])
            else:
                raise ValueError(f'Expected id property to be string - found type {type(entity['id'])}')
        return project_name

    def dump_data(self, to_dir_path: Union[str, Path], class_name: str = Schema.ALL_CLASSES, include_date_fields: bool = False) -> dict:
        """Dump data from the Datagraphs project to JSON files on disk.

        Args:
            to_dir_path: Directory to write ``<ClassName>.json`` files into.
                Created automatically if it does not exist.
            class_name: The class name to dump, or ``Schema.ALL_CLASSES`` for all.
            include_date_fields: Whether to include date fields in the dumped data.

        Returns:
            A dict with ``exported`` count.
        """
        to_dir_path = Path(to_dir_path)
        to_dir_path.mkdir(parents=True, exist_ok=True)
        stats = {"exported": 0}

        if class_name == Schema.ALL_CLASSES:
            datasets = self._client.get_datasets()
            for dataset in datasets:
                for dataset_class in dataset.classes:
                    if len(self._get_schema().find_subclasses(dataset_class)) == 0:   
                        try:
                            result = self._persist_to_file(dataset_class, to_dir_path, include_date_fields)
                            stats["exported"] += result
                            time.sleep(self._wait_time_ms / 1000)
                        except Exception as e:
                            logger.error('Error exporting data for %s: %s', dataset_class, str(e))                     
        else:
            try:
                result = self._persist_to_file(class_name, to_dir_path, include_date_fields)
                stats["exported"] += result
            except Exception as e:
                logger.error('Error exporting data for %s: %s', class_name, str(e))
        return stats

    def _persist_to_file(self, class_name: str, to_dir_path: Union[str, Path], include_date_fields: bool) -> int:
        """Fetch entities of *class_name* from the API and write them to a JSON file.

        Args:
            class_name: The class name to fetch.
            to_dir_path: Target directory (must already exist).
        """
        logger.info('Fetching data for %s...', class_name)
        data = self._client.get(class_name=class_name, include_date_fields=include_date_fields)
        file_path = Path(to_dir_path).joinpath(f'{class_name}.json')
        with open(file_path, 'w', encoding='utf-8') as dataFile:
            json.dump(data, dataFile, indent=2)
        return len(data)

    def clear_down(self) -> None:
        """Clear data out of all datasets."""
        datasets = self._client.get_datasets()
        for dataset in datasets:
            self._client.clear_dataset(dataset.slug)