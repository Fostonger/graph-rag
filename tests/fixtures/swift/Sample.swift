import Foundation

struct Greeter {
    let name: String

    func greet() -> String {
        return "Hello, \\(name)"
    }
}

extension Greeter {
    func excitedGreeting() -> String {
        return greet() + "!"
    }
}

