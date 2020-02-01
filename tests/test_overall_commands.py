from pathlib import PosixPath
import pytest
from hamcrest import *
from src.overall_commands import *
from src.utils import DelayedAssert


GET_FOLDERS_DATA = [get_stock_folders, get_fx_folders, get_crypto_folders]


@pytest.mark.parametrize("func", GET_FOLDERS_DATA)
def test_get_stock_folders(func):
    data_folder = DATA_FOLDER.as_posix()
    folders = func()

    assert_that(folders, instance_of(list))
    [assert_that(kf, instance_of(PosixPath)) for kf in folders]
    [assert_that(kf.as_posix(), contains_string(data_folder)) for kf in folders]


def test_get_stocks_references():
    stocks_refs = get_stocks_references()

    assert_that(stocks_refs, instance_of(tuple))
    assert_that(stocks_refs, has_length(2))
    assert_that(stocks_refs[0], instance_of(list))
    assert_that(stocks_refs[1], instance_of(list))
    assert_that(len(stocks_refs[0]) == len(stocks_refs[1]),
                "Stock string references and paths do NOT have the same length")
    [assert_that(kf, instance_of(str)) for kf in stocks_refs[0]]
    [assert_that(kf, instance_of(PosixPath)) for kf in stocks_refs[1]]


if __name__ == "__main__":
    test_get_stocks_references()
