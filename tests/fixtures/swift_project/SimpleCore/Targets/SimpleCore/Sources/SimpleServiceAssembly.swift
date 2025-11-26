import Foundation
import SimpleCoreIO

/// Public assembly for creating SimpleService instances
public class SimpleServiceAssembly: ISimpleServiceAssembly {
    private let service: SimpleService
    
    public init() {
        self.service = SimpleService()
    }
    
    public func buildService() -> ISimpleService {
        return service
    }
}

