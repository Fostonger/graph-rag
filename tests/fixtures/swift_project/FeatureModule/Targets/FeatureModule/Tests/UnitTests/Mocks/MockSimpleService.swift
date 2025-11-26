import Foundation
import SimpleCoreIO

/// Mock implementation of ISimpleService for testing
final class MockSimpleService: ISimpleService {
    var fetchDataCalled = false
    var fetchDataResult: String = "mock data"
    
    func fetchData() -> String {
        fetchDataCalled = true
        return fetchDataResult
    }
}

