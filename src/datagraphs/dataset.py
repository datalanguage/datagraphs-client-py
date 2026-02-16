# import uuid
# import json
# import datetime
# from enum import Enum
# from typing import Self

class Dataset:

  def __init__(self, name:str, project:str, account:str='', is_private:bool=True, is_restricted:bool=False, classes:list[str]=[]) -> None:
    self._name = name
    self._account = account
    self._project = project
    self._is_private = is_private
    self._is_restricted = is_restricted
    self._classes = classes

  @property
  def name(self) -> str:
    return self._name

  @property
  def account(self) -> str:
    return self._account

  @property
  def project(self) -> str:
    return self._project

  @property
  def is_private(self) -> bool:
    return self._is_private

  @is_private.setter
  def is_private(self, value: bool) -> None:
    self._is_private = value

  @property
  def is_restricted(self) -> bool:
    return self._is_restricted

  @is_restricted.setter
  def is_restricted(self, value: bool) -> None:
    self._is_restricted = value

  @property
  def classes(self) -> list[str]:
    return self._classes

  @classes.setter
  def classes(self, value: list[str] = []) -> None:
    self._classes = value

  @property
  def id(self) -> str:
    return f"urn:{self.project}:{self.sanitise_name(self.name)}"

  def sanitise_name(self, value:str) -> str:
    return value.lower().replace(" ", "-")

  def to_dict(self) -> dict:
    return {
      "name": self.name,
      "account": self.account,
      "project": self.project,
      "isPrivate": self.is_private,
      "isRestricted": self.is_restricted,
      "classes": self.classes,
      "type": "DataSet",
      "namespace": self.id,
      "id": self.id
    }

  @staticmethod
  def create_from(data:dict) -> "Dataset":   
    return Dataset(
        name=data.get("name"),
        account=data.get("account"),
        project=data.get("project"),
        is_private=data.get("isPrivate", True),
        is_restricted=data.get("isRestricted", False),
        classes=data.get("classes", [])
    ) 
