import Foundation
@testable import FeatureModule

/// Fake FeatureViewModel for testing
struct FakeFeatureViewModel {
    static func make(
        title: String = "Fake Title",
        description: String = "Fake Description",
        isLoading: Bool = false
    ) -> FeatureViewModel {
        return FeatureViewModel(
            title: title,
            description: description,
            isLoading: isLoading
        )
    }
}

