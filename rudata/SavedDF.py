from typing import List

import pandas as pd


class SavedDF:

    _instance: pd.DataFrame = None

    @property
    def instance(self):
        return self._instance

    @instance.setter
    def instance(self, df):
        self._instance = df


class Emitents(SavedDF):

    def get_fininst(self) -> List[int]:
        return self.instance.fininstid.unique().tolist()


class FintoolReferenceData(SavedDF):

    def get_fintool(self) -> List[int]:
        return self.instance.fintoolid.unique().tolist()

    def get_isin(self) -> List[str]:
        return self.instance.isincode.unique().tolist()


class Calendar(SavedDF):
    pass
