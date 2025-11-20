from pathlib import Path

from graphrag.indexer.dependencies import TuistDependenciesWorker
from graphrag.indexer.swift_parser import SwiftParser


def load_fixture(name: str) -> str:
    fixture_path = Path(__file__).parent / "fixtures" / "swift" / name
    return fixture_path.read_text()


def test_swift_parser_extracts_entities_and_members():
    parser = SwiftParser()
    source = load_fixture("Sample.swift")
    parsed = parser.parse(source, Path("Sources/Greeter.swift"))
    records = parsed.entities

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


def test_swift_parser_relationships_and_module_resolution(tmp_path):
    project_dir = tmp_path / "Features" / "MyModule"
    project_dir.mkdir(parents=True)
    (project_dir / "Project.swift").write_text(
        """
        import ProjectDescription

        let project = Project(
            name: "MyModule",
            targets: [
                Target(
                    name: "MyModule",
                    platform: .iOS,
                    product: .app,
                    sources: ["Sources/**"]
                ),
                Target(
                    name: "MyModuleTests",
                    platform: .iOS,
                    product: .unitTests,
                    sources: ["Tests/**"]
                )
            ]
        )
        """
    )
    source_dir = project_dir / "Sources"
    source_dir.mkdir()
    source_code = """
    protocol ISomePresenter {}
    protocol ISomeViewController {}
    protocol IInternetWorker {}
    protocol HasLogger {}
    class BasePresenter {}
    class DummyPresenter: ISomePresenter {}
    class InternetWorker: IInternetWorker {}
    class Dependency {}
    class WorkerBuilder {
        init(dependency: Dependency) {}
    }

    class MyModuleAssembly {
        func makePresenter() -> MyModulePresenter {
            let worker = InternetWorker()
            let presenter = MyModulePresenter(view: makeViewController(), worker: worker)
            return presenter
        }

        func makeViewController() -> MyModuleViewController {
            return MyModuleViewController(presenter: makePresenterStub())
        }

        func makePresenterStub() -> ISomePresenter {
            return DummyPresenter()
        }

        func makeBuilder() -> WorkerBuilder {
            let builder = WorkerBuilder(dependency: Dependency())
            return builder
        }

        func makeImmediate() -> DummyPresenter {
            DummyPresenter()
        }
    }

    class MyModulePresenter: BasePresenter, ISomePresenter {
        weak var viewController: ISomeViewController?
        var worker: IInternetWorker

        init(view: ISomeViewController?, worker: IInternetWorker) {
            self.viewController = view
            self.worker = worker
        }

        func preparedBuilder() -> WorkerBuilder {
            let prepared = WorkerBuilder(dependency: Dependency())
            return prepared
        }
    }

    class MyModuleViewController {
        var presenter: ISomePresenter

        init(presenter: ISomePresenter) {
            self.presenter = presenter
        }
    }

    extension MyModulePresenter: HasLogger {}
    """
    file_rel_path = Path("Features/MyModule/Sources/Module.swift")
    deps_worker = TuistDependenciesWorker(tmp_path)
    parser = SwiftParser(project_root=tmp_path, dependencies=deps_worker)
    parsed = parser.parse(source_code, file_rel_path)

    assembly = next(r for r in parsed.entities if r.name == "MyModuleAssembly")
    assert assembly.module == "MyModule"
    assert assembly.target_type == "app"
    rel_types = {(rel.edge_type, rel.target_name) for rel in parsed.relationships}
    assert ("creates", "MyModulePresenter") in rel_types
    assert ("strongReference", "ISomePresenter") in rel_types
    assert ("weakReference", "ISomeViewController") in rel_types
    assert ("creates", "WorkerBuilder") in rel_types
    assert ("creates", "Dependency") in rel_types
    assert ("creates", "DummyPresenter") in rel_types
    assert ("superclass", "BasePresenter") in rel_types
    assert ("conforms", "ISomePresenter") in rel_types
    assert ("conforms", "HasLogger") in rel_types

