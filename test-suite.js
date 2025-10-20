/**
 * Test Suite for Impact.com Script Optimization
 * 
 * Comprehensive test suite covering all major functionality
 * Run these tests to verify the optimized script works correctly
 */

// ============================================================================
// TEST CONFIGURATION
// ============================================================================

const TEST_CONFIG = {
  // Test data
  MOCK_SID: 'test_sid_12345',
  MOCK_TOKEN: 'test_token_67890',
  MOCK_SPREADSHEET_ID: 'test_spreadsheet_id',
  
  // Test settings
  ENABLE_MOCKING: true,
  VERBOSE_LOGGING: true,
  TEST_TIMEOUT: 30000
};

// ============================================================================
// MOCK DATA
// ============================================================================

const MOCK_REPORTS = [
  {
    Id: 'report_001',
    Name: 'Test Report 1',
    ApiAccessible: true,
    Description: 'Test report for unit testing'
  },
  {
    Id: 'report_002', 
    Name: 'Test Report 2',
    ApiAccessible: true,
    Description: 'Another test report'
  },
  {
    Id: 'report_003',
    Name: 'Restricted Report',
    ApiAccessible: false,
    Description: 'This report is not accessible'
  }
];

const MOCK_CSV_DATA = `Report ID,Date,Impressions,Clicks,Conversions
report_001,2024-01-01,1000,50,5
report_001,2024-01-02,1200,60,6
report_002,2024-01-01,800,40,4`;

const MOCK_JOB_RESPONSE = {
  QueuedUri: '/Mediapartners/test_sid_12345/Jobs/job_12345',
  Status: 'Queued'
};

const MOCK_JOB_STATUS = {
  Status: 'Completed',
  ResultUri: '/Mediapartners/test_sid_12345/Jobs/job_12345/Result'
};

// ============================================================================
// MOCK IMPLEMENTATIONS
// ============================================================================

/**
 * Mock PropertiesService for testing
 */
class MockPropertiesService {
  constructor() {
    this.properties = new Map();
  }
  
  getScriptProperties() {
    return {
      getProperty: (key) => this.properties.get(key),
      setProperty: (key, value) => this.properties.set(key, value),
      deleteProperty: (key) => this.properties.delete(key)
    };
  }
}

/**
 * Mock UrlFetchApp for testing
 */
class MockUrlFetchApp {
  constructor() {
    this.responses = new Map();
    this.requests = [];
  }
  
  setMockResponse(url, response) {
    this.responses.set(url, response);
  }
  
  fetch(url, options = {}) {
    this.requests.push({ url, options, timestamp: new Date() });
    
    const response = this.responses.get(url);
    if (!response) {
      throw new Error(`No mock response for URL: ${url}`);
    }
    
    return {
      getResponseCode: () => response.statusCode || 200,
      getContentText: () => response.content || '{}'
    };
  }
}

/**
 * Mock SpreadsheetApp for testing
 */
class MockSpreadsheetApp {
  constructor() {
    this.spreadsheets = new Map();
    this.sheets = new Map();
  }
  
  openById(id) {
    if (!this.spreadsheets.has(id)) {
      this.spreadsheets.set(id, new MockSpreadsheet(id));
    }
    return this.spreadsheets.get(id);
  }
}

class MockSpreadsheet {
  constructor(id) {
    this.id = id;
    this.sheets = [];
  }
  
  insertSheet(name) {
    const sheet = new MockSheet(name);
    this.sheets.push(sheet);
    return sheet;
  }
  
  getSheets() {
    return this.sheets;
  }
  
  deleteSheet(sheet) {
    const index = this.sheets.indexOf(sheet);
    if (index > -1) {
      this.sheets.splice(index, 1);
    }
  }
  
  getUrl() {
    return `https://docs.google.com/spreadsheets/d/${this.id}`;
  }
}

class MockSheet {
  constructor(name) {
    this.name = name;
    this.data = [];
    this.notes = new Map();
  }
  
  setName(name) {
    this.name = name;
  }
  
  getName() {
    return this.name;
  }
  
  getRange(row, col, numRows = 1, numCols = 1) {
    return new MockRange(this, row, col, numRows, numCols);
  }
  
  setFrozenRows(rows) {
    this.frozenRows = rows;
  }
  
  setFrozenColumns(cols) {
    this.frozenColumns = cols;
  }
  
  autoResizeColumns(startCol, numCols) {
    this.autoResized = { startCol, numCols };
  }
  
  clear() {
    this.data = [];
  }
}

class MockRange {
  constructor(sheet, row, col, numRows, numCols) {
    this.sheet = sheet;
    this.row = row;
    this.col = col;
    this.numRows = numRows;
    this.numCols = numCols;
  }
  
  setValues(values) {
    this.values = values;
  }
  
  setFontWeight(weight) {
    this.fontWeight = weight;
  }
  
  setBackground(color) {
    this.backgroundColor = color;
  }
  
  setFontColor(color) {
    this.fontColor = color;
  }
  
  setBorder(top, left, bottom, right, vertical, horizontal) {
    this.border = { top, left, bottom, right, vertical, horizontal };
  }
  
  setNote(note) {
    this.sheet.notes.set(`${this.row},${this.col}`, note);
  }
}

// ============================================================================
// TEST SETUP
// ============================================================================

/**
 * Setup test environment
 */
function setupTestEnvironment() {
  console.log('Setting up test environment...');
  
  // Mock global objects
  if (TEST_CONFIG.ENABLE_MOCKING) {
    global.PropertiesService = new MockPropertiesService();
    global.UrlFetchApp = new MockUrlFetchApp();
    global.SpreadsheetApp = new MockSpreadsheetApp();
    global.Utilities = {
      base64Encode: (str) => btoa(str),
      parseCsv: (csv) => csv.split('\n').map(row => row.split(',')),
      sleep: (ms) => new Promise(resolve => setTimeout(resolve, ms))
    };
    global.Logger = {
      log: (msg) => console.log(`[Logger] ${msg}`)
    };
  }
  
  // Set test credentials
  const props = PropertiesService.getScriptProperties();
  props.setProperty('IMPACT_SID', TEST_CONFIG.MOCK_SID);
  props.setProperty('IMPACT_TOKEN', TEST_CONFIG.MOCK_TOKEN);
  props.setProperty('IMPACT_SPREADSHEET_ID', TEST_CONFIG.MOCK_SPREADSHEET_ID);
  
  console.log('Test environment setup complete');
}

/**
 * Setup mock API responses
 */
function setupMockResponses() {
  const urlFetch = UrlFetchApp;
  
  // Mock reports discovery
  urlFetch.setMockResponse(
    `https://api.impact.com/Mediapartners/${TEST_CONFIG.MOCK_SID}/Reports`,
    {
      statusCode: 200,
      content: JSON.stringify({ Reports: MOCK_REPORTS })
    }
  );
  
  // Mock export scheduling
  MOCK_REPORTS.filter(r => r.ApiAccessible).forEach(report => {
    urlFetch.setMockResponse(
      `https://api.impact.com/Mediapartners/${TEST_CONFIG.MOCK_SID}/ReportExport/${report.Id}?subid=mula`,
      {
        statusCode: 200,
        content: JSON.stringify(MOCK_JOB_RESPONSE)
      }
    );
  });
  
  // Mock job status checking
  urlFetch.setMockResponse(
    `https://api.impact.com/Mediapartners/${TEST_CONFIG.MOCK_SID}/Jobs/job_12345`,
    {
      statusCode: 200,
      content: JSON.stringify(MOCK_JOB_STATUS)
    }
  );
  
  // Mock result download
  urlFetch.setMockResponse(
    'https://api.impact.com/Mediapartners/test_sid_12345/Jobs/job_12345/Result',
    {
      statusCode: 200,
      content: MOCK_CSV_DATA
    }
  );
  
  console.log('Mock responses setup complete');
}

// ============================================================================
// UNIT TESTS
// ============================================================================

/**
 * Test configuration management
 */
function testConfigurationManagement() {
  console.log('\n=== Testing Configuration Management ===');
  
  try {
    const config = new ImpactConfig();
    
    // Test default values
    assert(config.get('apiBaseUrl') === 'https://api.impact.com', 'Default API base URL');
    assert(config.get('timeout') === 30000, 'Default timeout');
    assert(config.get('maxRetries') === 3, 'Default max retries');
    
    // Test setting values
    config.set('testKey', 'testValue');
    assert(config.get('testKey') === 'testValue', 'Set and get configuration value');
    
    // Test validation
    const validation = config.validate();
    assert(validation.isValid === true, 'Configuration validation should pass');
    
    console.log('✅ Configuration management tests passed');
    return true;
  } catch (error) {
    console.error('❌ Configuration management tests failed:', error.message);
    return false;
  }
}

/**
 * Test logging system
 */
function testLoggingSystem() {
  console.log('\n=== Testing Logging System ===');
  
  try {
    const config = new ImpactConfig();
    const logger = new ImpactLogger(config);
    
    // Test different log levels
    logger.debug('Debug message', { test: true });
    logger.info('Info message', { test: true });
    logger.warn('Warning message', { test: true });
    logger.error('Error message', { test: true });
    
    // Test log storage
    const logs = JSON.parse(config.props.getProperty('IMPACT_LOGS') || '[]');
    assert(logs.length >= 4, 'Logs should be stored');
    
    console.log('✅ Logging system tests passed');
    return true;
  } catch (error) {
    console.error('❌ Logging system tests failed:', error.message);
    return false;
  }
}

/**
 * Test API client
 */
async function testAPIClient() {
  console.log('\n=== Testing API Client ===');
  
  try {
    const config = new ImpactConfig();
    const logger = new ImpactLogger(config);
    const retryManager = new RetryManager(config, logger);
    const apiClient = new ImpactAPIClient(config, logger, retryManager);
    
    // Test report discovery
    const reports = await apiClient.discoverReports();
    assert(Array.isArray(reports), 'Reports should be an array');
    assert(reports.length > 0, 'Should have reports');
    assert(reports.every(r => r.ApiAccessible), 'All returned reports should be accessible');
    
    // Test export scheduling
    const job = await apiClient.scheduleExport('report_001');
    assert(job.reportId === 'report_001', 'Job should have correct report ID');
    assert(job.jobId === 'job_12345', 'Job should have correct job ID');
    
    // Test job status checking
    const status = await apiClient.checkJobStatus('job_12345');
    assert(status.status === 'completed', 'Job should be completed');
    
    console.log('✅ API client tests passed');
    return true;
  } catch (error) {
    console.error('❌ API client tests failed:', error.message);
    return false;
  }
}

/**
 * Test data processor
 */
function testDataProcessor() {
  console.log('\n=== Testing Data Processor ===');
  
  try {
    const config = new ImpactConfig();
    const logger = new ImpactLogger(config);
    const processor = new DataProcessor(config, logger);
    
    // Test CSV processing
    const result = processor.processCSVData(MOCK_CSV_DATA);
    assert(result.headers.length > 0, 'Should have headers');
    assert(result.data.length > 0, 'Should have data');
    assert(result.rowCount > 0, 'Should have row count');
    assert(result.columnCount > 0, 'Should have column count');
    
    // Test data validation
    assert(result.validation.isValid !== undefined, 'Should have validation result');
    
    console.log('✅ Data processor tests passed');
    return true;
  } catch (error) {
    console.error('❌ Data processor tests failed:', error.message);
    return false;
  }
}

/**
 * Test spreadsheet manager
 */
function testSpreadsheetManager() {
  console.log('\n=== Testing Spreadsheet Manager ===');
  
  try {
    const config = new ImpactConfig();
    const logger = new ImpactLogger(config);
    const manager = new SpreadsheetManager(config, logger);
    
    // Test spreadsheet access
    const spreadsheet = manager.getSpreadsheet();
    assert(spreadsheet !== null, 'Should get spreadsheet');
    
    // Test sheet creation
    const processedData = {
      headers: ['Report ID', 'Date', 'Impressions'],
      data: [['report_001', '2024-01-01', '1000']],
      rowCount: 1,
      columnCount: 3,
      validation: { isValid: true, issues: [] }
    };
    
    const sheetInfo = manager.createReportSheet('test_report', processedData, { name: 'Test Report' });
    assert(sheetInfo.sheetName !== null, 'Should create sheet');
    assert(sheetInfo.rowCount === 1, 'Should have correct row count');
    assert(sheetInfo.columnCount === 3, 'Should have correct column count');
    
    console.log('✅ Spreadsheet manager tests passed');
    return true;
  } catch (error) {
    console.error('❌ Spreadsheet manager tests failed:', error.message);
    return false;
  }
}

/**
 * Test retry manager
 */
async function testRetryManager() {
  console.log('\n=== Testing Retry Manager ===');
  
  try {
    const config = new ImpactConfig();
    const logger = new ImpactLogger(config);
    const retryManager = new RetryManager(config, logger);
    
    let attemptCount = 0;
    const failingFunction = async () => {
      attemptCount++;
      if (attemptCount < 3) {
        throw new Error('Simulated failure');
      }
      return 'success';
    };
    
    // Test retry with eventual success
    const result = await retryManager.executeWithRetry(failingFunction, [], { maxRetries: 3 });
    assert(result === 'success', 'Should eventually succeed');
    assert(attemptCount === 3, 'Should retry correct number of times');
    
    console.log('✅ Retry manager tests passed');
    return true;
  } catch (error) {
    console.error('❌ Retry manager tests failed:', error.message);
    return false;
  }
}

// ============================================================================
// INTEGRATION TESTS
// ============================================================================

/**
 * Test complete discovery process
 */
async function testCompleteDiscovery() {
  console.log('\n=== Testing Complete Discovery Process ===');
  
  try {
    const orchestrator = new ImpactDiscoveryOrchestrator();
    
    // Run discovery
    const results = await orchestrator.runCompleteDiscovery();
    
    assert(results !== null, 'Should return results');
    assert(results.successful !== undefined, 'Should have successful results');
    assert(results.failed !== undefined, 'Should have failed results');
    assert(Array.isArray(results.successful), 'Successful should be array');
    assert(Array.isArray(results.failed), 'Failed should be array');
    
    console.log(`✅ Complete discovery test passed - ${results.successful.length} successful, ${results.failed.length} failed`);
    return true;
  } catch (error) {
    console.error('❌ Complete discovery test failed:', error.message);
    return false;
  }
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Assert function for testing
 */
function assert(condition, message) {
  if (!condition) {
    throw new Error(`Assertion failed: ${message}`);
  }
}

/**
 * Run all tests
 */
async function runAllTests() {
  console.log('🧪 Starting Impact.com Script Test Suite');
  console.log('==========================================');
  
  setupTestEnvironment();
  setupMockResponses();
  
  const tests = [
    { name: 'Configuration Management', fn: testConfigurationManagement },
    { name: 'Logging System', fn: testLoggingSystem },
    { name: 'API Client', fn: testAPIClient },
    { name: 'Data Processor', fn: testDataProcessor },
    { name: 'Spreadsheet Manager', fn: testSpreadsheetManager },
    { name: 'Retry Manager', fn: testRetryManager },
    { name: 'Complete Discovery', fn: testCompleteDiscovery }
  ];
  
  let passed = 0;
  let failed = 0;
  
  for (const test of tests) {
    try {
      const result = await test.fn();
      if (result) {
        passed++;
      } else {
        failed++;
      }
    } catch (error) {
      console.error(`❌ ${test.name} failed with error:`, error.message);
      failed++;
    }
  }
  
  console.log('\n==========================================');
  console.log(`🏁 Test Suite Complete: ${passed} passed, ${failed} failed`);
  
  if (failed === 0) {
    console.log('🎉 All tests passed! The optimized script is working correctly.');
  } else {
    console.log('⚠️  Some tests failed. Please review the errors above.');
  }
  
  return { passed, failed };
}

/**
 * Run specific test category
 */
async function runUnitTests() {
  console.log('🧪 Running Unit Tests');
  setupTestEnvironment();
  setupMockResponses();
  
  const unitTests = [
    testConfigurationManagement,
    testLoggingSystem,
    testDataProcessor,
    testSpreadsheetManager,
    testRetryManager
  ];
  
  let passed = 0;
  for (const test of unitTests) {
    try {
      if (await test()) passed++;
    } catch (error) {
      console.error('Test failed:', error.message);
    }
  }
  
  console.log(`Unit tests complete: ${passed}/${unitTests.length} passed`);
  return passed === unitTests.length;
}

/**
 * Run integration tests
 */
async function runIntegrationTests() {
  console.log('🧪 Running Integration Tests');
  setupTestEnvironment();
  setupMockResponses();
  
  try {
    await testAPIClient();
    await testCompleteDiscovery();
    console.log('✅ Integration tests passed');
    return true;
  } catch (error) {
    console.error('❌ Integration tests failed:', error.message);
    return false;
  }
}

// ============================================================================
// PERFORMANCE TESTS
// ============================================================================

/**
 * Test performance with large datasets
 */
async function testPerformance() {
  console.log('\n=== Performance Testing ===');
  
  try {
    const config = new ImpactConfig();
    const logger = new ImpactLogger(config);
    const processor = new DataProcessor(config, logger);
    
    // Generate large CSV data
    const largeCSV = generateLargeCSV(10000);
    
    const startTime = Date.now();
    const result = processor.processCSVData(largeCSV);
    const endTime = Date.now();
    
    const processingTime = endTime - startTime;
    console.log(`Processed ${result.rowCount} rows in ${processingTime}ms`);
    
    assert(processingTime < 5000, 'Should process large dataset quickly');
    assert(result.rowCount === 10000, 'Should process all rows');
    
    console.log('✅ Performance test passed');
    return true;
  } catch (error) {
    console.error('❌ Performance test failed:', error.message);
    return false;
  }
}

/**
 * Generate large CSV data for testing
 */
function generateLargeCSV(rowCount) {
  const headers = ['ID', 'Date', 'Value1', 'Value2', 'Value3'];
  const rows = [headers.join(',')];
  
  for (let i = 0; i < rowCount; i++) {
    const row = [
      `id_${i}`,
      `2024-01-${String(Math.floor(Math.random() * 28) + 1).padStart(2, '0')}`,
      Math.floor(Math.random() * 1000),
      Math.floor(Math.random() * 1000),
      Math.floor(Math.random() * 1000)
    ];
    rows.push(row.join(','));
  }
  
  return rows.join('\n');
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

/**
 * Main test execution function
 */
async function main() {
  try {
    await runAllTests();
  } catch (error) {
    console.error('Test suite failed:', error.message);
  }
}

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    runAllTests,
    runUnitTests,
    runIntegrationTests,
    testPerformance,
    setupTestEnvironment,
    setupMockResponses
  };
}
