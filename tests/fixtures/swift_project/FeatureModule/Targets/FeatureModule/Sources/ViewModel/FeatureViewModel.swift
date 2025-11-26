import Foundation

/// Data container for the feature view
public struct FeatureViewModel {
    public let title: String
    public let description: String
    public let isLoading: Bool
    
    public init(title: String, description: String, isLoading: Bool = false) {
        self.title = title
        self.description = description
        self.isLoading = isLoading
    }
}

