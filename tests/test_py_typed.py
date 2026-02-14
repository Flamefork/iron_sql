import importlib.resources


def test_package_is_marked_as_typed() -> None:
    assert importlib.resources.files("iron_sql").joinpath("py.typed").is_file()
