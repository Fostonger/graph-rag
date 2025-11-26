"""Tests for Project.swift parsing and file-to-target matching."""
from pathlib import Path

import pytest

from graphrag.indexer.dependencies import TuistDependenciesWorker
from graphrag.indexer.project_parsers import SwiftGekoProjectParser
from graphrag.indexer.swift_parser import SwiftParser


REAL_LIFE_PROJECT_SWIFT = """
import ProjectDescription

let project = MyProject.Module(
    name: "ActualTargetName",
    targets: [
        MyProject.Module.Target(
            name: "ActualTargetName",
            sources: [
                "ActualTargetName/ActualTargetName/Classes/**/*.{swift}",
            ],
            additionalFiles: [
                "ActualTargetName/ActualTargetName/Accessibility/**/*.yaml",
                "ActualTargetName/ActualTargetName/Yamls/**/*.yml",
            ],
            dependencies: [
                .external(name: "ExternalDependency"),
                .local(name: "LocalDependencyIO"),
                .local(name: "AnotherLocalDependencyInterfaces"),
            ],
            resources: MyProject.Module.Target.ResourcesBundle(
                resources: [
                    "ActualTargetName/ActualTargetName/Localization/**/*.json",
                ]
            ),
            swiftGen: MyProject.Module.Target.SwiftGen(
                resources: [
                    "ActualTargetName/ActualTargetName/Accessibility/Accessibility.yaml",
                    "ActualTargetName/ActualTargetName/Yamls/StringLiterals.yml",
                ],
                generatedFiles: [
                    "ActualTargetName/ActualTargetName/Classes/Generated/GeneratedAccessibility.swift",
                    "ActualTargetName/ActualTargetName/Classes/Generated/GeneratedYamls.swift",
                ],
                customBundleName: "selfEmployment"
            ),
            tests: [
                MyProject.Module.Target.Tests(
                    testsType: .unit,
                    sources: [
                        "ActualTargetName/ActualTargetName/Tests/UnitTests/**/*.{swift}",
                        "ActualTargetName/ActualTargetName/TestsCommon/Mocks/**/*.{swift}",
                        "ActualTargetName/ActualTargetName/TestsCommon/Generated/**/*.{swift}",
                        "ActualTargetName/ActualTargetName/TestsCommon/Utils/UnitTests/**/*.{swift}",
                    ],
                    additionalFiles: [
                        "ActualTargetName/ActualTargetName/TestsCommon/**/**/*.yml",
                    ],
                    dependencies: [
                        .target(name: "ActualTargetName"),
                        .external(name: "ExternalDependencyMock"),
                        .local(name: "LocalDependencyMock"),
                        .local(name: "AnotherLocalDependencyMock"),
                        .local(name: "ActualTargetNameMock"),
                    ],
                    resources: [
                    ]
                ),
                MyProject.Module.Target.Tests(
                    testsType: .snapshot,
                    sources: [
                        "ActualTargetName/ActualTargetName/Tests/SnapshotTests/**/*.{swift}",
                        "ActualTargetName/ActualTargetName/TestsCommon/Mocks/**/*.{swift}",
                        "ActualTargetName/ActualTargetName/TestsCommon/Generated/**/*.{swift}",
                    ],
                    additionalFiles: [
                        "ActualTargetName/ActualTargetName/TestsCommon/**/**/*.yml",
                    ],
                    dependencies: [
                        .target(name: "ActualTargetName"),
                        .external(name: "ExternalDependencyMock"),
                        .local(name: "ActualTargetNameMock"),
                    ],
                    resources: [
                    ]
                ),
                MyProject.Module.Target.Tests(
                    testsType: .kif,
                    sources: [
                        "ActualTargetName/ActualTargetName/Tests/KifTests/**/*.{swift}",
                        "ActualTargetName/ActualTargetName/TestsCommon/Mocks/**/*.{swift}",
                        "ActualTargetName/ActualTargetName/TestsCommon/Generated/**/*.{swift}",
                        "ActualTargetName/ActualTargetName/TestsCommon/Utils/KifTests/*.{swift}",
                    ],
                    additionalFiles: [
                        "ActualTargetName/ActualTargetName/TestsCommon/**/**/*.yml",
                    ],
                    dependencies: [
                        .target(name: "ActualTargetName"),
                        .external(name: "KIF"),
                        .external(name: "ExternalDependencyMock"),
                        .local(name: "ActualTargetNameMock"),
                    ],
                    resources: [
                    ]
                ),
            ]
        ),
        MyProject.Module.Target(
            name: "ActualTargetNameIO",
            sources: [
                "ActualTargetNameIO/ActualTargetNameIO/Classes/**/*.{swift}",
            ],
            additionalFiles: [
            ],
            dependencies: [
                .local(name: "FeatureTogglesInterfaces"),
                .local(name: "MobileBankIO"),
            ],
            resources: nil,
            swiftGen: MyProject.Module.Target.SwiftGen(
                resources: [
                    "ActualTargetNameIO/ActualTargetNameIO/Yamls/StringLiterals.yml",
                ],
                generatedFiles: [
                    "ActualTargetNameIO/ActualTargetNameIO/Classes/Generated/GeneratedYamls.swift",
                ],
                customBundleName: "ActualTargetNameIO"
            ),
            tests: [
            ]
        ),
        MyProject.Module.Target(
            name: "ActualTargetNameMock",
            sources: [
                "ActualTargetNameMock/ActualTargetNameMock/Classes/**/*.{swift}",
            ],
            additionalFiles: [
            ],
            dependencies: [
                .external(name: "MockUtils"),
                .local(name: "ActualTargetNameIO"),
            ],
            resources: nil,
            swiftGen: nil,
            tests: [
            ]
        ),
    ]
).makeProject()
"""


class TestSwiftGekoProjectParser:
    """Unit tests for SwiftGekoProjectParser."""

    def test_parses_project_name(self, tmp_path: Path):
        project_file = tmp_path / "Project.swift"
        project_file.write_text(REAL_LIFE_PROJECT_SWIFT)
        
        parser = SwiftGekoProjectParser()
        metadata = parser.parse(project_file)
        
        assert metadata.name == "ActualTargetName"

    def test_parses_all_targets(self, tmp_path: Path):
        project_file = tmp_path / "Project.swift"
        project_file.write_text(REAL_LIFE_PROJECT_SWIFT)
        
        parser = SwiftGekoProjectParser()
        metadata = parser.parse(project_file)
        
        target_names = [t.name for t in metadata.targets]
        assert target_names == ["ActualTargetName", "ActualTargetNameIO", "ActualTargetNameMock"]

    def test_parses_target_sources(self, tmp_path: Path):
        project_file = tmp_path / "Project.swift"
        project_file.write_text(REAL_LIFE_PROJECT_SWIFT)
        
        parser = SwiftGekoProjectParser()
        metadata = parser.parse(project_file)
        
        main_target = next(t for t in metadata.targets if t.name == "ActualTargetName")
        assert "ActualTargetName/ActualTargetName/Classes/**/*.{swift}" in main_target.sources

    def test_parses_tests_with_types(self, tmp_path: Path):
        project_file = tmp_path / "Project.swift"
        project_file.write_text(REAL_LIFE_PROJECT_SWIFT)
        
        parser = SwiftGekoProjectParser()
        metadata = parser.parse(project_file)
        
        main_target = next(t for t in metadata.targets if t.name == "ActualTargetName")
        assert len(main_target.tests) == 3
        
        test_types = [t.tests_type for t in main_target.tests]
        assert test_types == ["unit", "snapshot", "kif"]

    def test_parses_test_sources(self, tmp_path: Path):
        project_file = tmp_path / "Project.swift"
        project_file.write_text(REAL_LIFE_PROJECT_SWIFT)
        
        parser = SwiftGekoProjectParser()
        metadata = parser.parse(project_file)
        
        main_target = next(t for t in metadata.targets if t.name == "ActualTargetName")
        unit_tests = next(t for t in main_target.tests if t.tests_type == "unit")
        
        assert "ActualTargetName/ActualTargetName/Tests/UnitTests/**/*.{swift}" in unit_tests.sources
        assert "ActualTargetName/ActualTargetName/TestsCommon/Mocks/**/*.{swift}" in unit_tests.sources

    def test_parses_test_dependencies(self, tmp_path: Path):
        project_file = tmp_path / "Project.swift"
        project_file.write_text(REAL_LIFE_PROJECT_SWIFT)
        
        parser = SwiftGekoProjectParser()
        metadata = parser.parse(project_file)
        
        main_target = next(t for t in metadata.targets if t.name == "ActualTargetName")
        unit_tests = next(t for t in main_target.tests if t.tests_type == "unit")
        
        assert "target:ActualTargetName" in unit_tests.dependencies
        assert "external:ExternalDependencyMock" in unit_tests.dependencies
        assert "local:LocalDependencyMock" in unit_tests.dependencies

    def test_classifies_io_target_as_interface(self, tmp_path: Path):
        project_file = tmp_path / "Project.swift"
        project_file.write_text(REAL_LIFE_PROJECT_SWIFT)
        
        parser = SwiftGekoProjectParser()
        metadata = parser.parse(project_file)
        
        io_target = next(t for t in metadata.targets if t.name == "ActualTargetNameIO")
        assert io_target.target_type == "interface"

    def test_classifies_mock_target_as_mock(self, tmp_path: Path):
        project_file = tmp_path / "Project.swift"
        project_file.write_text(REAL_LIFE_PROJECT_SWIFT)
        
        parser = SwiftGekoProjectParser()
        metadata = parser.parse(project_file)
        
        mock_target = next(t for t in metadata.targets if t.name == "ActualTargetNameMock")
        assert mock_target.target_type == "mock"

    def test_targets_with_empty_tests_array(self, tmp_path: Path):
        project_file = tmp_path / "Project.swift"
        project_file.write_text(REAL_LIFE_PROJECT_SWIFT)
        
        parser = SwiftGekoProjectParser()
        metadata = parser.parse(project_file)
        
        io_target = next(t for t in metadata.targets if t.name == "ActualTargetNameIO")
        mock_target = next(t for t in metadata.targets if t.name == "ActualTargetNameMock")
        
        assert io_target.tests == []
        assert mock_target.tests == []


class TestTuistDependenciesWorkerWithTests:
    """Tests for TuistDependenciesWorker handling test sources."""

    def test_matches_test_file_to_test_target(self, tmp_path: Path):
        project_dir = tmp_path / "Features" / "MyModule"
        project_dir.mkdir(parents=True)
        (project_dir / "Project.swift").write_text(REAL_LIFE_PROJECT_SWIFT)
        
        # Create the directory structure
        test_dir = project_dir / "ActualTargetName" / "ActualTargetName" / "Tests" / "UnitTests"
        test_dir.mkdir(parents=True)
        
        deps_worker = TuistDependenciesWorker(tmp_path)
        test_file_path = Path("Features/MyModule/ActualTargetName/ActualTargetName/Tests/UnitTests/SomeTest.swift")
        
        target = deps_worker.target_for_file(test_file_path)
        
        assert target is not None
        assert target.target_type == "test"
        assert "UnitTests" in target.name or "Unit" in target.name

    def test_matches_snapshot_test_file_to_test_target(self, tmp_path: Path):
        project_dir = tmp_path / "Features" / "MyModule"
        project_dir.mkdir(parents=True)
        (project_dir / "Project.swift").write_text(REAL_LIFE_PROJECT_SWIFT)
        
        # Create the directory structure
        test_dir = project_dir / "ActualTargetName" / "ActualTargetName" / "Tests" / "SnapshotTests"
        test_dir.mkdir(parents=True)
        
        deps_worker = TuistDependenciesWorker(tmp_path)
        test_file_path = Path("Features/MyModule/ActualTargetName/ActualTargetName/Tests/SnapshotTests/MySnapshotTest.swift")
        
        target = deps_worker.target_for_file(test_file_path)
        
        assert target is not None
        assert target.target_type == "test"

    def test_matches_app_file_to_app_target(self, tmp_path: Path):
        project_dir = tmp_path / "Features" / "MyModule"
        project_dir.mkdir(parents=True)
        (project_dir / "Project.swift").write_text(REAL_LIFE_PROJECT_SWIFT)
        
        # Create the directory structure
        source_dir = project_dir / "ActualTargetName" / "ActualTargetName" / "Classes"
        source_dir.mkdir(parents=True)
        
        deps_worker = TuistDependenciesWorker(tmp_path)
        source_file_path = Path("Features/MyModule/ActualTargetName/ActualTargetName/Classes/MyClass.swift")
        
        target = deps_worker.target_for_file(source_file_path)
        
        assert target is not None
        assert target.target_type == "app"
        assert target.name == "ActualTargetName"

    def test_matches_io_file_to_interface_target(self, tmp_path: Path):
        project_dir = tmp_path / "Features" / "MyModule"
        project_dir.mkdir(parents=True)
        (project_dir / "Project.swift").write_text(REAL_LIFE_PROJECT_SWIFT)
        
        # Create the directory structure
        io_dir = project_dir / "ActualTargetNameIO" / "ActualTargetNameIO" / "Classes"
        io_dir.mkdir(parents=True)
        
        deps_worker = TuistDependenciesWorker(tmp_path)
        io_file_path = Path("Features/MyModule/ActualTargetNameIO/ActualTargetNameIO/Classes/Protocol.swift")
        
        target = deps_worker.target_for_file(io_file_path)
        
        assert target is not None
        assert target.target_type == "interface"
        assert target.name == "ActualTargetNameIO"

    def test_matches_mock_file_to_mock_target(self, tmp_path: Path):
        project_dir = tmp_path / "Features" / "MyModule"
        project_dir.mkdir(parents=True)
        (project_dir / "Project.swift").write_text(REAL_LIFE_PROJECT_SWIFT)
        
        # Create the directory structure
        mock_dir = project_dir / "ActualTargetNameMock" / "ActualTargetNameMock" / "Classes"
        mock_dir.mkdir(parents=True)
        
        deps_worker = TuistDependenciesWorker(tmp_path)
        mock_file_path = Path("Features/MyModule/ActualTargetNameMock/ActualTargetNameMock/Classes/MockService.swift")
        
        target = deps_worker.target_for_file(mock_file_path)
        
        assert target is not None
        assert target.target_type == "mock"
        assert target.name == "ActualTargetNameMock"

    def test_tests_common_files_matched_as_test(self, tmp_path: Path):
        """Files in TestsCommon should be matched as test files."""
        project_dir = tmp_path / "Features" / "MyModule"
        project_dir.mkdir(parents=True)
        (project_dir / "Project.swift").write_text(REAL_LIFE_PROJECT_SWIFT)
        
        # Create the directory structure
        mocks_dir = project_dir / "ActualTargetName" / "ActualTargetName" / "TestsCommon" / "Mocks"
        mocks_dir.mkdir(parents=True)
        
        deps_worker = TuistDependenciesWorker(tmp_path)
        mock_file_path = Path("Features/MyModule/ActualTargetName/ActualTargetName/TestsCommon/Mocks/SomeMock.swift")
        
        target = deps_worker.target_for_file(mock_file_path)
        
        assert target is not None
        assert target.target_type == "test"


class TestSwiftParserIntegration:
    """Integration tests for SwiftParser with test file detection."""

    def test_parses_test_file_with_correct_target_type(self, tmp_path: Path):
        project_dir = tmp_path / "Features" / "MyModule"
        project_dir.mkdir(parents=True)
        (project_dir / "Project.swift").write_text(REAL_LIFE_PROJECT_SWIFT)
        
        # Create the directory structure
        test_dir = project_dir / "ActualTargetName" / "ActualTargetName" / "Tests" / "UnitTests"
        test_dir.mkdir(parents=True)
        
        test_code = """
import XCTest

class MyServiceTests: XCTestCase {
    func testExample() {
        XCTAssertTrue(true)
    }
}
"""
        test_file = test_dir / "MyServiceTests.swift"
        test_file.write_text(test_code)
        
        deps_worker = TuistDependenciesWorker(tmp_path)
        parser = SwiftParser(project_root=tmp_path, dependencies=deps_worker)
        
        relative_path = test_file.relative_to(tmp_path)
        parsed = parser.parse(test_code, relative_path)
        
        entity = next(r for r in parsed.entities if r.name == "MyServiceTests")
        assert entity.target_type == "test"

    def test_parses_app_file_with_correct_target_type(self, tmp_path: Path):
        project_dir = tmp_path / "Features" / "MyModule"
        project_dir.mkdir(parents=True)
        (project_dir / "Project.swift").write_text(REAL_LIFE_PROJECT_SWIFT)
        
        # Create the directory structure
        source_dir = project_dir / "ActualTargetName" / "ActualTargetName" / "Classes"
        source_dir.mkdir(parents=True)
        
        source_code = """
class MyService {
    func doSomething() {}
}
"""
        source_file = source_dir / "MyService.swift"
        source_file.write_text(source_code)
        
        deps_worker = TuistDependenciesWorker(tmp_path)
        parser = SwiftParser(project_root=tmp_path, dependencies=deps_worker)
        
        relative_path = source_file.relative_to(tmp_path)
        parsed = parser.parse(source_code, relative_path)
        
        entity = next(r for r in parsed.entities if r.name == "MyService")
        assert entity.target_type == "app"

    def test_parses_interface_file_with_correct_target_type(self, tmp_path: Path):
        project_dir = tmp_path / "Features" / "MyModule"
        project_dir.mkdir(parents=True)
        (project_dir / "Project.swift").write_text(REAL_LIFE_PROJECT_SWIFT)
        
        # Create the directory structure
        io_dir = project_dir / "ActualTargetNameIO" / "ActualTargetNameIO" / "Classes"
        io_dir.mkdir(parents=True)
        
        source_code = """
protocol MyServiceProtocol {
    func doSomething()
}
"""
        source_file = io_dir / "MyServiceProtocol.swift"
        source_file.write_text(source_code)
        
        deps_worker = TuistDependenciesWorker(tmp_path)
        parser = SwiftParser(project_root=tmp_path, dependencies=deps_worker)
        
        relative_path = source_file.relative_to(tmp_path)
        parsed = parser.parse(source_code, relative_path)
        
        entity = next(r for r in parsed.entities if r.name == "MyServiceProtocol")
        assert entity.target_type == "interface"


class TestMultipleTestTypesInSameTarget:
    """Tests for targets with multiple test types (unit, snapshot, kif)."""

    def test_different_test_types_get_different_target_names(self, tmp_path: Path):
        project_dir = tmp_path / "Features" / "MyModule"
        project_dir.mkdir(parents=True)
        (project_dir / "Project.swift").write_text(REAL_LIFE_PROJECT_SWIFT)
        
        deps_worker = TuistDependenciesWorker(tmp_path)
        
        # Find all test targets
        test_targets = [t for t in deps_worker._targets if t.target_type == "test"]
        test_target_names = [t.name for t in test_targets]
        
        # Should have separate entries for unit, snapshot, and kif tests
        assert len(test_targets) >= 3
        assert any("Unit" in name for name in test_target_names)
        assert any("Snapshot" in name for name in test_target_names)
        assert any("Kif" in name for name in test_target_names)

    def test_kif_test_file_matched_correctly(self, tmp_path: Path):
        project_dir = tmp_path / "Features" / "MyModule"
        project_dir.mkdir(parents=True)
        (project_dir / "Project.swift").write_text(REAL_LIFE_PROJECT_SWIFT)
        
        # Create the directory structure
        kif_dir = project_dir / "ActualTargetName" / "ActualTargetName" / "Tests" / "KifTests"
        kif_dir.mkdir(parents=True)
        
        deps_worker = TuistDependenciesWorker(tmp_path)
        kif_file_path = Path("Features/MyModule/ActualTargetName/ActualTargetName/Tests/KifTests/UITest.swift")
        
        target = deps_worker.target_for_file(kif_file_path)
        
        assert target is not None
        assert target.target_type == "test"
        assert "Kif" in target.name

