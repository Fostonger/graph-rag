import Foundation

/// Protocol for the view controller
public protocol IFeatureViewController: IFeatureView {
    var presenter: IFeaturePresenter { get }
}

/// View controller for the Feature module
public class FeatureViewController: IFeatureViewController {
    public let presenter: IFeaturePresenter
    private var currentViewModel: FeatureViewModel?
    
    public init(presenter: IFeaturePresenter) {
        self.presenter = presenter
    }
    
    public func viewDidLoad() {
        presenter.viewDidLoad()
    }
    
    public func display(viewModel: FeatureViewModel) {
        self.currentViewModel = viewModel
    }
}

