import ProjectDescription

let project = MyProject.Module(
    name: "SimpleCore",
    targets: [
        MyProject.Module.Target(
            name: "SimpleCore",
            sources: [
                "Targets/SimpleCore/Sources/**/*.swift",
            ],
            dependencies: [
                .local(name: "SimpleCoreIO"),
            ],
            tests: []
        ),
        MyProject.Module.Target(
            name: "SimpleCoreIO",
            sources: [
                "Targets/SimpleCoreIO/Sources/**/*.swift",
            ],
            dependencies: [],
            tests: []
        ),
    ]
).makeProject()

