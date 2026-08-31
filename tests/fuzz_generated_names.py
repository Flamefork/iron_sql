import secrets
import sys
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from operator import add

from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import seed
from hypothesis import settings
from hypothesis import strategies as st

from iron_sql.codegen.generator import NameOrigin
from iron_sql.codegen.generator import name_claim_issues
from tests.generated_oracles import GeneratedNameContext
from tests.generated_oracles import observe_cpython_name

_RENDERER_NAMES: dict[GeneratedNameContext, tuple[str, ...]] = {
    "class_binding": (
        "_locations",
        "_row_factory",
        "_stmt",
        "query_single_row",
    ),
    "class_parameter": ("self", "cur", "runtime", "psycopg"),
    "module_expression_binding": ("runtime", "TESTDB_POOL", "Query"),
    "module_expression_read": ("runtime", "psycopg", "Query"),
}


@dataclass(kw_only=True, frozen=True)
class GeneratedNameExample:
    context: GeneratedNameContext
    spelling: str
    group: str


_IDENTIFIER_START = st.characters(
    categories=("Lu", "Ll", "Lt", "Lm", "Lo", "Nl"),
    include_characters="_",
)
_IDENTIFIER_CONTINUE = st.characters(
    categories=("Lu", "Ll", "Lt", "Lm", "Lo", "Nl", "Mn", "Mc", "Nd", "Pc"),
    include_characters="_",
)


join_identifier: Callable[[str, str], str] = add
_UNICODE_SPELLINGS: st.SearchStrategy[str] = st.builds(
    join_identifier,
    _IDENTIFIER_START,
    st.text(_IDENTIFIER_CONTINUE, max_size=12),
)
_PLAIN_SPELLINGS = st.from_regex(r"plain_[A-Za-z][A-Za-z0-9_]{0,10}", fullmatch=True)


@st.composite
def generated_name_examples(
    draw: st.DrawFn,
) -> GeneratedNameExample:
    context = draw(st.sampled_from(tuple(_RENDERER_NAMES)))
    group = draw(
        st.sampled_from((
            "ordinary",
            "keyword",
            "invalid",
            "nfkc",
            "private_dunder",
            "renderer_owned",
            "unicode",
        ))
    )
    match group:
        case "ordinary":
            spelling = draw(_PLAIN_SPELLINGS)
        case "keyword":
            spelling = draw(st.sampled_from(("class", "match", "return", "with")))
        case "invalid":
            spelling = draw(st.sampled_from(("bad-name", "two words", "1value", "")))
        case "nfkc":
            spelling = draw(st.sampled_from(("K", "ｃｌａｓｓ", "Ⅳ", "ª")))
        case "private_dunder":
            spelling = draw(st.sampled_from(("__value", "__private", "___")))
        case "renderer_owned":
            spelling = draw(st.sampled_from(_RENDERER_NAMES[context]))
        case "unicode":
            spelling = draw(_UNICODE_SPELLINGS)
    return GeneratedNameExample(context=context, spelling=spelling, group=group)


def production_accepts(example: GeneratedNameExample) -> bool:
    claims = [
        NameOrigin(name=name, origin="renderer", locations=())
        for name in _RENDERER_NAMES[example.context]
    ]
    claims.append(
        NameOrigin(name=example.spelling, origin="generated input", locations=())
    )
    class_name = "Generated" if example.context.startswith("class_") else None
    return not name_claim_issues("generated scope", claims, class_name=class_name)


def renderer_bindings(context: GeneratedNameContext) -> set[str]:
    observations = (
        observe_cpython_name(name, context) for name in _RENDERER_NAMES[context]
    )
    return {
        observation.binding
        for observation in observations
        if observation.accepted and observation.binding is not None
    }


def run(campaign_seed: int | None = None) -> int:
    max_examples = 5000
    selected_seed = campaign_seed if campaign_seed is not None else secrets.randbits(64)
    outcomes: Counter[str] = Counter()
    sys.stdout.write(f"seed: {selected_seed}\nexamples: {max_examples}\n")
    sys.stdout.flush()

    @seed(selected_seed)
    @given(generated_name_examples())
    @settings(
        max_examples=max_examples,
        deadline=None,
        database=None,
        suppress_health_check=(HealthCheck.data_too_large,),
    )
    def campaign(example: GeneratedNameExample) -> None:
        observation = observe_cpython_name(example.spelling, example.context)
        accepted = production_accepts(example)
        owned = renderer_bindings(example.context)
        safe = (
            observation.accepted
            and observation.binding == example.spelling
            and observation.binding not in owned
        )
        if observation.accepted:
            outcomes["valid-cpython"] += 1
        if accepted and safe:
            outcomes["accepted"] += 1
            return
        if not accepted and example.group == "ordinary":
            msg = (
                f"plain identifier refused: seed={selected_seed}, "
                f"context={example.context}, spelling={example.spelling!r}"
            )
            raise AssertionError(msg)
        if not accepted:
            outcomes["loud-refusal"] += 1
            return
        outcomes["divergence"] += 1
        msg = (
            f"namespace divergence: seed={selected_seed}, context={example.context}, "
            f"spelling={example.spelling!r}, binding={observation.binding!r}, "
            f"namespace={observation.namespace}, "
            f"syntax_error={observation.syntax_error!r}"
        )
        raise AssertionError(msg)

    campaign()
    if outcomes["valid-cpython"] == 0:
        msg = f"no valid CPython names checked with seed {selected_seed}"
        raise AssertionError(msg)
    sys.stdout.write(f"outcomes: {dict(outcomes)}\n")
    return 0


if __name__ == "__main__":
    sys.exit(run())
