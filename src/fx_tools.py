import pandas as pd

from src.overall_commands import *
from src.utils import read_pandas_data
from src.config import FX_VARIATIONS, DFT_FX_FILE, DFT_FX_EXT

fx_types = ["fx", "digital"]


def load_fx_data(fx_folder, category="fx", period="daily"):
    if period not in FX_VARIATIONS:
        raise ValueError(f"The period {period} is not supported. Please select one among {FX_VARIATIONS}")
    if category not in fx_types:
        raise ValueError(f"The category must be within the values {fx_types}")
    if not fx_folder.exists():
        raise ValueError(f"The fx folder {fx_folder} was NOT found")

    file_ref = fx_folder.joinpath(DFT_FX_FILE + "_" + category + "_" + period + DFT_FX_EXT)
    data = read_pandas_data(file_ref)
    if data is None:
        raise ValueError(f"It was not possible to load the data from {file_ref}")

    if "open" in data.columns:
        data.open = data.open.astype(float)
    if "close" in data.columns:
        data.close = data.close.astype(float)
    if "high" in data.columns:
        data.high = data.high.astype(float)
    if "low" in data.columns:
        data.low = data.low.astype(float)
    return data


class FxManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not FxManager._instance:
            FxManager._instance = super(FxManager, cls).__new__(cls, *args, **kwargs)
        return FxManager._instance

    def __init__(self):
        self.fx_data = {}
        self.crypto_data = {}
        self.load_fx_data()

    def load_fx_data(self):
        """
        Load all Fx and Digital data into memory for later calls
        """
        fx_refs, fx_folders = get_fx_references()
        crypto_refs, crypto_folders = get_crypto_references()

        for fxref, fxf in zip(fx_refs, fx_folders):
            data = load_fx_data(fxf, category="fx")
            # Direct references
            if fxref[0] in self.fx_data:
                self.fx_data[fxref[0]][fxref[1]] = data
            else:
                self.fx_data[fxref[0]] = {fxref[1]: data}

        for cryref, crf in zip(crypto_refs, crypto_folders):
            data = load_fx_data(crf, category="digital")
            # Direct references
            if cryref[0] in self.crypto_data:
                self.crypto_data[cryref[0]][cryref[1]] = data
            else:
                self.crypto_data[cryref[0]] = {cryref[1]: data}

    def query(self, fxfrom, fxto, date_ini=None, date_end=None):
        """
        Query FX/Digital data
        :param fxfrom:   from what currency
        :param fxto:     to what currency/market
        :param date_ini: (optional) select from this date on
        :param date_end: (optional) select up to this date
        :return:         data with applied filters
        """
        if fxfrom in self.crypto_data.keys():
            crypto = fxfrom
            reversed = False
        elif fxto in self.crypto_data.keys():
            crypto = fxto
            reversed = True
        else:
            crypto = None

        if crypto:
            # Digital currencies selection
            if reversed:
                data = 1 / self.crypto_data[fxto][fxfrom]
            else:
                data = self.crypto_data[fxfrom][fxto]

        else:
            # Currencies selection
            if fxfrom in self.fx_data.keys():
                if fxto in self.fx_data[fxfrom].keys():
                    data = self.fx_data[fxfrom][fxto]
                else:
                    LOG.warning(f"Fx data to {fxto} not available from {fxfrom}")
                    return None
            if fxto in self.fx_data.keys():
                if fxfrom in self.fx_data[fxto].keys():
                    data = 1 / self.fx_data[fxfrom][fxto]
        # Apply date filters
        if date_ini:
            data = data[data.index >= date_ini]
        if date_end:
            data = data[data.index <= date_end]
        return data


if __name__ == "__main__":
    fx_mng = FxManager()
    rst = fx_mng.query("GBP", "CNY")
