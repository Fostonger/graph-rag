import Foundation
import SimpleCoreIO

/// Protocol for the presenter's view
public protocol IFeatureView: AnyObject {
    func display(viewModel: FeatureViewModel)
}

/// Protocol for the presenter
public protocol IFeaturePresenter {
    func viewDidLoad()
    func refreshData()
}

/// Presenter handling the feature's business logic
public class FeaturePresenter {
    private let service: ISimpleService
    private let viewModelBuilder: IFeatureViewModelBuilder
    public weak var view: IFeatureView?
    
    public init(service: ISimpleService, viewModelBuilder: IFeatureViewModelBuilder) {
        self.service = service
        self.viewModelBuilder = viewModelBuilder
    }
    
    private func updateView() {
        let data = service.fetchData()
        let viewModel = viewModelBuilder.build(from: data)
        view?.display(viewModel: viewModel)
    }
}

