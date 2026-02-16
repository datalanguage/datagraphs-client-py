import pytest
from datagraphs.dataset import Dataset

class TestDataset:

    def test_should_create_dataset_from_dict(self):
        data = {
            "name": "Agents",
            "account": "test_account",
            "project": "test_project",
            "isPrivate": True,
            "isRestricted": False,
            "classes": ["Agent", "Formulation"]
        }
        dataset = Dataset.create_from(data)
        assert dataset.name == "Agents"
        assert dataset.account == "test_account"
        assert dataset.project == "test_project"
        assert dataset.is_private
        assert not dataset.is_restricted
        assert dataset.classes == ["Agent", "Formulation"]

    def test_should_set_is_private(self):
        dataset = Dataset(name="Test Dataset", account="test_account", project="test_project")
        assert dataset.is_private
        dataset.is_private = False
        assert not dataset.is_private

    def test_should_set_is_restricted(self):
        dataset = Dataset(name="Test Dataset", account="test_account", project="test_project")
        assert not dataset.is_restricted
        dataset.is_restricted = True
        assert dataset.is_restricted

    def test_should_set_classes(self):
        dataset = Dataset(name="Test Dataset", account="test_account", project="test_project")
        assert dataset.classes == []
        dataset.classes = ["Class1", "Class2"]
        assert dataset.classes == ["Class1", "Class2"]

    def test_should_generate_id(self):
        dataset = Dataset(name="Test Dataset", account="test_account", project="test_project")
        assert dataset.id == "urn:test_project:test-dataset"

    def test_should_convert_to_dict(self):
        dataset = Dataset(name="Test Dataset", account="test_account", project="test_project", is_private=False, is_restricted=True, classes=["Class1", "Class2"])
        expected_dict = {
            "name": "Test Dataset",
            "account": "test_account",
            "project": "test_project",
            "isPrivate": False,
            "isRestricted": True,
            "classes": ["Class1", "Class2"],
            "type": "DataSet",
            "namespace": "urn:test_project:test-dataset",
            "id": "urn:test_project:test-dataset"
        }
        assert dataset.to_dict() == expected_dict