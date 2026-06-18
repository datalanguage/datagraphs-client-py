"""Dataset representation for DataGraphs projects."""


class Dataset:
    """Represents a dataset within a DataGraphs project."""

    def __init__(self, name: str, project: str, account: str = '', is_private: bool = True, is_restricted: bool = False, classes: list[str] | None = None) -> None:
        """Create a new dataset.

        :param name: Dataset name.
        :param project: Project name.
        :param account: Account name.
        :param is_private: Whether the dataset is private.
        :param is_restricted: Whether the dataset has restricted access.
        :param classes: List of class names belonging to this dataset.
        """
        self._name = name
        self._account = account
        self._project = project
        self._is_private = is_private
        self._is_restricted = is_restricted
        self._classes = classes if classes is not None else []

    @property
    def name(self) -> str:
        """The dataset name."""
        return self._name

    @property
    def account(self) -> str:
        """The account name."""
        return self._account

    @property
    def project(self) -> str:
        """The project name."""
        return self._project

    @property
    def is_private(self) -> bool:
        """Whether the dataset is private."""
        return self._is_private

    @is_private.setter
    def is_private(self, value: bool) -> None:
        self._is_private = value

    @property
    def is_restricted(self) -> bool:
        """Whether the dataset has restricted access."""
        return self._is_restricted

    @is_restricted.setter
    def is_restricted(self, value: bool) -> None:
        self._is_restricted = value

    @property
    def classes(self) -> list[str]:
        """The class names belonging to this dataset."""
        return self._classes

    @classes.setter
    def classes(self, value: list[str]) -> None:
        self._classes = value

    @property
    def id(self) -> str:
        """The auto-generated URN identifier (e.g. ``'urn:project:dataset-slug'``)."""
        return f"urn:{self.project}:{self._sanitise_name(self.name)}"

    @property
    def slug(self) -> str:
        """The slug portion of the dataset ID."""
        return self.id[self.id.rfind(':') + 1:]

    def _sanitise_name(self, value: str) -> str:
        return value.lower().replace(" ", "-")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Dataset):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return NotImplemented
        return not result

    def to_dict(self) -> dict:
        """Convert the dataset to a dictionary suitable for API requests.

        :returns: A dict representation of the dataset.
        """
        return {
            "name": self.name,
            "account": self.account,
            "project": self.project,
            "isPrivate": self.is_private,
            "isRestricted": self.is_restricted,
            "classes": self.classes,
            "type": "DataSet",
            "namespace": self.id,
            "id": self.id,
        }

    @staticmethod
    def create_from(data: dict) -> "Dataset":
        """Create a `Dataset` instance from a dictionary.

        :param data: Dictionary with dataset fields.
        :returns: A new `Dataset` instance.
        """
        return Dataset(
            name=data.get("name"),
            account=data.get("account"),
            project=data.get("project"),
            is_private=data.get("isPrivate", True),
            is_restricted=data.get("isRestricted", False),
            classes=data.get("classes", data.get("conceptTypes", [])),
        )
