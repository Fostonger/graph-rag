import Foundation
@testable import FeatureModule

/// Mock implementation of IFeatureView
final class MockFeatureViewController: IFeatureView {
    var displayCalled = false
    var lastDisplayedViewModel: FeatureViewModel?
    
    func display(viewModel: FeatureViewModel) {
        displayCalled = true
        lastDisplayedViewModel = viewModel
    }
}

