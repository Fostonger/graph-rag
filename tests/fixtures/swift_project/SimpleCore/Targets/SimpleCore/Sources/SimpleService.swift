import Foundation
import SimpleCoreIO

/// Internal implementation of ISimpleService
class SimpleService {
    private let baseURL: String
    
    init(baseURL: String = "https://api.example.com") {
        self.baseURL = baseURL
    }
}

extension SimpleService: ISimpleService {
    func fetchData() -> String {
        return "Data from \(baseURL)"
    }
}

