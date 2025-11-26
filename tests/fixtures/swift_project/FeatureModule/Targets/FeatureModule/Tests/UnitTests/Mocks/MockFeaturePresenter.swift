import Foundation
@testable import FeatureModule

/// Mock implementation of IFeaturePresenter
final class MockFeaturePresenter: IFeaturePresenter {
    var viewDidLoadCalled = false
    var refreshDataCalled = false
    
    func viewDidLoad() {
        viewDidLoadCalled = true
    }
    
    func refreshData() {
        refreshDataCalled = true
    }
}

