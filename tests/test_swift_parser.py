from pathlib import Path

from graphrag.indexer.swift_parser import SwiftParser


def load_fixture(name: str) -> str:
    fixture_path = Path(__file__).parent / "fixtures" / "swift" / name
    return fixture_path.read_text()


def test_swift_parser_extracts_entities_and_members():
    parser = SwiftParser()
    source = load_fixture("Sample.swift")
    records = list(parser.parse(source, Path("Sources/Greeter.swift")))

    greeter = next(
        (r for r in records if r.name == "Greeter" and r.kind != "extension"), None
    )
    assert greeter is not None
    assert greeter.kind == "struct"
    assert greeter.module == "Sources"
    assert len(greeter.members) == 2
    member_names = {m.name for m in greeter.members}
    assert member_names == {"name", "greet"}

    extension = next((r for r in records if r.kind == "extension"), None)
    assert extension is not None
    assert extension.extended_type == "Greeter"
    assert len(extension.members) == 1
    assert extension.members[0].name == "excitedGreeting"

