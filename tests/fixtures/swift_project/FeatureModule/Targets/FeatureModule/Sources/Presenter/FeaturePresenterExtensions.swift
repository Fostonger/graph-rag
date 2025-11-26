import Foundation

/// Extension adding IFeaturePresenter conformance
extension FeaturePresenter: IFeaturePresenter {
    public func viewDidLoad() {
        updateView()
    }
    
    public func refreshData() {
        updateView()
    }
}

