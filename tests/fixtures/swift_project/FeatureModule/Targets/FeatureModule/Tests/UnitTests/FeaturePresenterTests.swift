import XCTest
@testable import FeatureModule

final class FeaturePresenterTests: XCTestCase {
    private var sut: FeaturePresenter!
    private var mockService: MockSimpleService!
    private var mockViewModelBuilder: MockFeatureViewModelBuilder!
    private var mockView: MockFeatureViewController!
    
    override func setUp() {
        super.setUp()
        mockService = MockSimpleService()
        mockViewModelBuilder = MockFeatureViewModelBuilder()
        sut = FeaturePresenter(
            service: mockService,
            viewModelBuilder: mockViewModelBuilder
        )
        mockView = MockFeatureViewController()
        sut.view = mockView
    }
    
    override func tearDown() {
        sut = nil
        mockService = nil
        mockViewModelBuilder = nil
        mockView = nil
        super.tearDown()
    }
    
    func testViewDidLoadCallsService() {
        sut.viewDidLoad()
        
        XCTAssertTrue(mockService.fetchDataCalled)
    }
    
    func testViewDidLoadBuildsViewModel() {
        mockService.fetchDataResult = "test data"
        
        sut.viewDidLoad()
        
        XCTAssertEqual(mockViewModelBuilder.lastBuildInput, "test data")
    }
    
    func testViewDidLoadDisplaysViewModel() {
        let expectedViewModel = FakeFeatureViewModel.make()
        mockViewModelBuilder.buildResult = expectedViewModel
        
        sut.viewDidLoad()
        
        XCTAssertEqual(mockView.lastDisplayedViewModel?.title, expectedViewModel.title)
    }
    
    func testRefreshDataUpdatesView() {
        sut.refreshData()
        
        XCTAssertTrue(mockView.displayCalled)
    }
}

