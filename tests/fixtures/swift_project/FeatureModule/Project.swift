import ProjectDescription

let project = MyProject.Module(
    name: "FeatureModule",
    targets: [
        MyProject.Module.Target(
            name: "FeatureModule",
            sources: [
                "Targets/FeatureModule/Sources/**/*.swift",
            ],
            dependencies: [
                .local(name: "SimpleCoreIO"),
            ],
            tests: [
                MyProject.Module.Target.Tests(
                    testsType: .unit,
                    sources: [
                        "Targets/FeatureModule/Tests/UnitTests/**/*.swift",
                    ],
                    dependencies: [
                        .target(name: "FeatureModule"),
                    ],
                    resources: []
                ),
            ]
        ),
    ]
).makeProject()

