import Foundation
@testable import FeatureModule

/// Mock implementation of IFeatureViewModelBuilder
final class MockFeatureViewModelBuilder: IFeatureViewModelBuilder {
    var buildCalled = false
    var lastBuildInput: String?
    var buildResult: FeatureViewModel = FakeFeatureViewModel.make()
    
    func build(from data: String) -> FeatureViewModel {
        buildCalled = true
        lastBuildInput = data
        return buildResult
    }
}

