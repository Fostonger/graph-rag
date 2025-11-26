import Foundation

/// Protocol for building view models
public protocol IFeatureViewModelBuilder {
    func build(from data: String) -> FeatureViewModel
}

/// Builder for creating FeatureViewModel instances
public class FeatureViewModelBuilder: IFeatureViewModelBuilder {
    public init() {}
    
    public func build(from data: String) -> FeatureViewModel {
        return FeatureViewModel(
            title: "Feature",
            description: data,
            isLoading: false
        )
    }
}

