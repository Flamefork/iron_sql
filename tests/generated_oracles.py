import inspect
import symtable
import typing
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import cast

from iron_sql.codegen.generator import is_query_method_external_read
from iron_sql.codegen.generator import query_method_required_external_reads
from iron_sql.runtime import Query

_QUERY_METHODS = {
    "execute",
    "query_all_rows",
    "query_optional_row",
    "query_single_row",
    "query_stream",
}
_CURSOR_METHODS = {
    "query_all_rows",
    "query_optional_row",
    "query_single_row",
}
_QUERY_CLASS_BINDINGS = (
    {"_locations", "_row_factory", "_stmt", "execute"},
    {
        "_locations",
        "_row_factory",
        "_stmt",
        "query_all_rows",
        "query_optional_row",
        "query_single_row",
        "query_stream",
    },
)


def assert_generated_module_contract(module: ModuleType) -> None:
    assert_generated_type_hints_resolve(module)
    query_classes = generated_query_classes(module)
    source = Path(str(module.__file__)).read_text(encoding="utf-8")
    assert_query_source_namespaces(
        source,
        tuple(query_class.__name__ for query_class in query_classes),
    )
    for query_class in query_classes:
        for method_name in _QUERY_METHODS & vars(query_class).keys():
            method = vars(query_class)[method_name]
            parameters = tuple(inspect.signature(method).parameters)
            if not parameters or parameters[0] != "self":
                label = f"{query_class.__name__}.{method_name}"
                msg = f"{label} has parameters {parameters!r}"
                raise AssertionError(msg)


def assert_generated_type_hints_resolve(module: ModuleType) -> None:
    namespace = cast("dict[str, object]", vars(module))
    problems: list[str] = []
    for name, value in list(namespace.items()):
        if getattr(value, "__module__", None) != module.__name__:
            continue
        for owner, label, local_namespace in annotated_targets(name, value):
            try:
                typing.get_type_hints(owner, namespace, local_namespace)
            except NameError as exc:
                problems.append(f"{label}: {exc}")
    if problems:
        details = "\n  ".join(problems)
        module_label = f"generated module {module.__name__}"
        msg = f"{module_label} has unresolved type hints:\n  {details}"
        raise AssertionError(msg)


type AnnotatedTarget = tuple[object, str, dict[str, object]]


def annotated_targets(name: str, value: object) -> list[AnnotatedTarget]:
    if inspect.isclass(value):
        methods = inspect.getmembers(value, inspect.isfunction)
        return [
            (value, name, type_parameter_namespace(value)),
            *(
                (
                    method,
                    f"{name}.{method_name}",
                    type_parameter_namespace(value, method),
                )
                for method_name, method in methods
                if method.__qualname__.startswith(f"{name}.")
            ),
        ]
    if inspect.isfunction(value):
        return [(value, name, type_parameter_namespace(value))]
    return []


def type_parameter_namespace(*owners: object) -> dict[str, object]:
    return {
        parameter.__name__: parameter
        for owner in owners
        for parameter in cast(
            "tuple[typing.TypeVar, ...]",
            getattr(owner, "__type_params__", ()),
        )
    }


def generated_query_classes(module: ModuleType) -> tuple[type[Query[object]], ...]:
    module_query = vars(module)["Query"]
    if not inspect.isclass(module_query) or not issubclass(module_query, Query):
        msg = f"generated module {module.__name__} has no runtime Query base"
        raise AssertionError(msg)
    return tuple(
        cast("type[Query[object]]", value)
        for value in vars(module).values()
        if inspect.isclass(value)
        and value is not module_query
        and issubclass(value, module_query)
    )


def assert_query_source_namespaces(
    source: str,
    query_class_names: tuple[str, ...],
) -> None:
    module_table = symtable.symtable(source, "generated.py", "exec")
    class_tables = {
        table.get_name(): table
        for table in symbol_tables(module_table)
        if isinstance(table, symtable.Class) and table.get_name() in query_class_names
    }
    missing = sorted(set(query_class_names) - class_tables.keys())
    if missing:
        msg = f"generated query class scopes not found: {', '.join(missing)}"
        raise AssertionError(msg)
    problems: list[str] = []
    for class_name in query_class_names:
        class_table = class_tables[class_name]
        bindings = symbol_table_bindings(class_table)
        if bindings not in _QUERY_CLASS_BINDINGS:
            problems.append(f"{class_name} binds {sorted(bindings)!r}")
        method_tables = [
            table
            for table in class_table.get_children()
            if isinstance(table, symtable.Function)
        ]
        actual_methods = {table.get_name() for table in method_tables}
        expected_methods = bindings & _QUERY_METHODS
        if actual_methods != expected_methods:
            actual_label = f"{class_name} method scopes {sorted(actual_methods)!r}"
            problems.append(f"{actual_label}, bindings {sorted(expected_methods)!r}")
        for method_table in method_tables:
            problems.extend(method_namespace_problems(class_name, method_table))
    if problems:
        details = "\n  ".join(problems)
        msg = f"generated query namespaces violate the render contract:\n  {details}"
        raise AssertionError(msg)


def method_namespace_problems(
    class_name: str,
    method: symtable.Function,
) -> list[str]:
    label = f"{class_name}.{method.get_name()}"
    parameters = set(method.get_parameters())
    expected_locals = set(parameters)
    if method.get_name() in _CURSOR_METHODS:
        expected_locals.add("cur")
    actual_locals = set(method.get_locals())
    problems: list[str] = []
    if actual_locals != expected_locals:
        actual_label = f"{label} locals {sorted(actual_locals)!r}"
        problems.append(f"{actual_label}, expected {sorted(expected_locals)!r}")
    reads = set(method.get_globals()) | set(method.get_frees())
    required_reads = set(query_method_required_external_reads(method.get_name()))
    missing_reads = sorted(required_reads - reads)
    if missing_reads:
        problems.append(f"{label} does not read required {', '.join(missing_reads)}")
    unclaimed_reads = sorted(
        name for name in reads if not is_query_method_external_read(name)
    )
    if unclaimed_reads:
        problems.append(f"{label} reads unclaimed {', '.join(unclaimed_reads)}")
    nested = list(method.get_children())
    if nested:
        nested_names = ", ".join(table.get_name() for table in nested)
        problems.append(f"{label} has unknown nested scopes {nested_names}")
    return problems


def symbol_tables(table: symtable.SymbolTable) -> Iterator[symtable.SymbolTable]:
    for child in table.get_children():
        yield child
        yield from symbol_tables(child)


def symbol_table_bindings(table: symtable.SymbolTable) -> set[str]:
    return {
        symbol.get_name()
        for symbol in table.get_symbols()
        if symbol.is_annotated()
        or symbol.is_assigned()
        or symbol.is_imported()
        or symbol.is_namespace()
    }
