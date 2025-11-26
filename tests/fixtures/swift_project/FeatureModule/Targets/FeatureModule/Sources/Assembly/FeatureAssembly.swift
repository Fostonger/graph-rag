import Foundation
import SimpleCoreIO

/// Assembly that creates and wires all Feature module components
public class FeatureAssembly {
    private let serviceAssembly: ISimpleServiceAssembly
    
    public init(serviceAssembly: ISimpleServiceAssembly) {
        self.serviceAssembly = serviceAssembly
    }
    
    public func build() -> FeatureViewController {
        let service = serviceAssembly.buildService()
        let viewModelBuilder = FeatureViewModelBuilder()
        let presenter = FeaturePresenter(
            service: service,
            viewModelBuilder: viewModelBuilder
        )
        let viewController = FeatureViewController(presenter: presenter)
        presenter.view = viewController
        return viewController
    }
}

