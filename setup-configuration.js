/**
 * Configuration Setup Script for Impact.com Script
 * 
 * This script helps you set up the configuration for the Impact.com script
 * without hitting Google Apps Script's 50 property limit.
 * 
 * Run this script first to set up your configuration, then use the main script.
 */

/**
 * Set up basic configuration with required credentials
 * @param {string} sid - Impact.com SID
 * @param {string} token - Impact.com API Token
 * @param {string} spreadsheetId - Target Google Spreadsheet ID (optional)
 */
function setupBasicConfiguration(sid, token, spreadsheetId = null) {
  console.log('Setting up basic configuration...');
  console.log('Received SID:', sid, 'Type:', typeof sid);
  console.log('Received Token:', token ? token.substring(0, 8) + '...' : 'undefined', 'Type:', typeof token);
  console.log('Received SpreadsheetId:', spreadsheetId, 'Type:', typeof spreadsheetId);
  
  // Validate inputs with better error messages
  if (!sid) {
    throw new Error('SID parameter is missing. Call: setupBasicConfiguration("your_sid", "your_token")');
  }
  if (typeof sid !== 'string') {
    throw new Error('SID must be a string. You passed: ' + typeof sid);
  }
  if (sid.trim() === '') {
    throw new Error('SID cannot be empty');
  }
  
  if (!token) {
    throw new Error('Token parameter is missing. Call: setupBasicConfiguration("your_sid", "your_token")');
  }
  if (typeof token !== 'string') {
    throw new Error('Token must be a string. You passed: ' + typeof token);
  }
  if (token.trim() === '') {
    throw new Error('Token cannot be empty');
  }
  
  const props = PropertiesService.getScriptProperties();
  
  // Set required credentials
  props.setProperty('IMPACT_SID', String(sid));
  props.setProperty('IMPACT_TOKEN', String(token));
  
  if (spreadsheetId && spreadsheetId.trim() !== '') {
    props.setProperty('IMPACT_SPREADSHEET_ID', String(spreadsheetId));
  }
  
  // Set up default configuration
  const defaultConfig = {
    // API Configuration
    apiBaseUrl: 'https://api.impact.com',
    timeout: 30000,
    maxRetries: 3,
    retryDelay: 1000,
    
    // Polling Configuration
    maxPollingAttempts: 12,
    initialPollingDelay: 10000,
    maxPollingDelay: 60000,
    pollingMultiplier: 1.5,
    
    // Rate Limiting
    requestDelay: 2000,
    burstLimit: 5,
    
    // Data Processing
    maxRowsPerSheet: 100000,
    enableDataValidation: true,
    enableDataSanitization: true,
    
    // Logging
    logLevel: 'INFO',
    enableDetailedLogging: true,
    logRetentionDays: 30,
    
    // Error Handling
    enableCircuitBreaker: true,
    circuitBreakerThreshold: 5,
    circuitBreakerTimeout: 300000,
    
    // Output
    defaultSheetName: 'Impact Data',
    enableAutoFormatting: true,
    enableDataNotes: true
  };
  
  props.setProperty('IMPACT_CONFIG', JSON.stringify(defaultConfig));
  
  console.log('✅ Basic configuration setup complete!');
  console.log('SID:', sid);
  console.log('Token:', token.substring(0, 8) + '...');
  console.log('Spreadsheet ID:', spreadsheetId || 'Using default');
  
  return defaultConfig;
}

/**
 * Set up development configuration
 * @param {string} sid - Impact.com SID
 * @param {string} token - Impact.com API Token
 * @param {string} spreadsheetId - Target Google Spreadsheet ID (optional)
 */
function setupDevelopmentConfiguration(sid, token, spreadsheetId = null) {
  console.log('Setting up development configuration...');
  
  // Validate inputs
  if (!sid || typeof sid !== 'string') {
    throw new Error('SID must be a non-empty string');
  }
  if (!token || typeof token !== 'string') {
    throw new Error('Token must be a non-empty string');
  }
  
  const props = PropertiesService.getScriptProperties();
  
  // Set required credentials
  props.setProperty('IMPACT_SID', String(sid));
  props.setProperty('IMPACT_TOKEN', String(token));
  
  if (spreadsheetId && spreadsheetId.trim() !== '') {
    props.setProperty('IMPACT_SPREADSHEET_ID', String(spreadsheetId));
  }
  
  // Development-optimized configuration
  const devConfig = {
    // API Configuration
    apiBaseUrl: 'https://api.impact.com',
    timeout: 10000,  // Shorter timeout for faster feedback
    maxRetries: 2,   // Fewer retries for faster testing
    retryDelay: 500,
    
    // Polling Configuration
    maxPollingAttempts: 8,
    initialPollingDelay: 5000,
    maxPollingDelay: 30000,
    pollingMultiplier: 1.5,
    
    // Rate Limiting
    requestDelay: 500,  // Faster requests for testing
    burstLimit: 3,
    
    // Data Processing
    maxRowsPerSheet: 1000,  // Smaller datasets for testing
    enableDataValidation: true,
    enableDataSanitization: true,
    
    // Logging
    logLevel: 'DEBUG',  // More verbose logging
    enableDetailedLogging: true,
    logRetentionDays: 7,  // Shorter retention for dev
    
    // Error Handling
    enableCircuitBreaker: true,
    circuitBreakerThreshold: 3,
    circuitBreakerTimeout: 60000,
    
    // Output
    defaultSheetName: 'Impact Data (Dev)',
    enableAutoFormatting: true,
    enableDataNotes: true
  };
  
  props.setProperty('IMPACT_CONFIG', JSON.stringify(devConfig));
  
  console.log('✅ Development configuration setup complete!');
  console.log('SID:', sid);
  console.log('Token:', token.substring(0, 8) + '...');
  console.log('Spreadsheet ID:', spreadsheetId || 'Using default');
  console.log('Log Level: DEBUG');
  console.log('Max Rows per Sheet: 1,000');
  
  return devConfig;
}

/**
 * Set up production configuration
 * @param {string} sid - Impact.com SID
 * @param {string} token - Impact.com API Token
 * @param {string} spreadsheetId - Target Google Spreadsheet ID (optional)
 */
function setupProductionConfiguration(sid, token, spreadsheetId = null) {
  console.log('Setting up production configuration...');
  
  // Validate inputs
  if (!sid || typeof sid !== 'string') {
    throw new Error('SID must be a non-empty string');
  }
  if (!token || typeof token !== 'string') {
    throw new Error('Token must be a non-empty string');
  }
  
  const props = PropertiesService.getScriptProperties();
  
  // Set required credentials
  props.setProperty('IMPACT_SID', String(sid));
  props.setProperty('IMPACT_TOKEN', String(token));
  
  if (spreadsheetId && spreadsheetId.trim() !== '') {
    props.setProperty('IMPACT_SPREADSHEET_ID', String(spreadsheetId));
  }
  
  // Production-optimized configuration
  const prodConfig = {
    // API Configuration
    apiBaseUrl: 'https://api.impact.com',
    timeout: 60000,  // Longer timeout for reliability
    maxRetries: 5,   // More retries for reliability
    retryDelay: 2000,
    
    // Polling Configuration
    maxPollingAttempts: 20,
    initialPollingDelay: 15000,
    maxPollingDelay: 120000,
    pollingMultiplier: 1.3,
    
    // Rate Limiting
    requestDelay: 3000,  // Conservative rate limiting
    burstLimit: 2,
    
    // Data Processing
    maxRowsPerSheet: 200000,  // Larger datasets
    enableDataValidation: true,
    enableDataSanitization: true,
    
    // Logging
    logLevel: 'INFO',  // Production logging
    enableDetailedLogging: false,  // Less verbose for production
    logRetentionDays: 30,
    
    // Error Handling
    enableCircuitBreaker: true,
    circuitBreakerThreshold: 5,
    circuitBreakerTimeout: 600000,  // 10 minutes
    
    // Output
    defaultSheetName: 'Impact Data',
    enableAutoFormatting: true,
    enableDataNotes: true
  };
  
  props.setProperty('IMPACT_CONFIG', JSON.stringify(prodConfig));
  
  console.log('✅ Production configuration setup complete!');
  console.log('SID:', sid);
  console.log('Token:', token.substring(0, 8) + '...');
  console.log('Spreadsheet ID:', spreadsheetId || 'Using default');
  console.log('Log Level: INFO');
  console.log('Max Rows per Sheet: 200,000');
  
  return prodConfig;
}

/**
 * View current configuration
 */
function viewCurrentConfiguration() {
  console.log('Current Configuration:');
  console.log('=====================');
  
  const props = PropertiesService.getScriptProperties();
  
  // Show credentials (masked)
  const sid = props.getProperty('IMPACT_SID');
  const token = props.getProperty('IMPACT_TOKEN');
  const spreadsheetId = props.getProperty('IMPACT_SPREADSHEET_ID');
  
  console.log('SID:', sid || 'Not set');
  console.log('Token:', token ? token.substring(0, 8) + '...' : 'Not set');
  console.log('Spreadsheet ID:', spreadsheetId || 'Not set (will use default)');
  
  // Show configuration
  const configJson = props.getProperty('IMPACT_CONFIG');
  if (configJson) {
    try {
      const config = JSON.parse(configJson);
      console.log('\nConfiguration Settings:');
      console.log('----------------------');
      Object.entries(config).forEach(([key, value]) => {
        console.log(`${key}: ${value}`);
      });
    } catch (error) {
      console.error('Failed to parse configuration:', error.message);
    }
  } else {
    console.log('\nNo configuration found. Run setupBasicConfiguration() first.');
  }
}

/**
 * Clear all configuration
 */
function clearConfiguration() {
  console.log('Clearing all configuration...');
  
  const props = PropertiesService.getScriptProperties();
  
  // Clear all Impact-related properties
  const propertiesToDelete = [
    'IMPACT_SID',
    'IMPACT_TOKEN', 
    'IMPACT_SPREADSHEET_ID',
    'IMPACT_CONFIG',
    'IMPACT_LOGS'
  ];
  
  let clearedCount = 0;
  propertiesToDelete.forEach(prop => {
    if (props.getProperty(prop)) {
      props.deleteProperty(prop);
      clearedCount++;
      console.log(`Cleared: ${prop}`);
    }
  });
  
  console.log(`✅ Cleared ${clearedCount} properties!`);
}

/**
 * Clear ALL properties (use with caution)
 */
function clearAllProperties() {
  console.log('⚠️  Clearing ALL script properties...');
  
  const props = PropertiesService.getScriptProperties();
  const allProps = props.getProperties();
  
  console.log(`Found ${Object.keys(allProps).length} properties to clear`);
  
  // Clear all properties
  props.deleteAllProperties();
  
  console.log('✅ All properties cleared!');
}

/**
 * Test configuration
 */
function testConfiguration() {
  console.log('Testing configuration...');
  
  const props = PropertiesService.getScriptProperties();
  
  // Check required properties
  const sid = props.getProperty('IMPACT_SID');
  const token = props.getProperty('IMPACT_TOKEN');
  const configJson = props.getProperty('IMPACT_CONFIG');
  
  if (!sid) {
    console.error('❌ IMPACT_SID is not set');
    return false;
  }
  
  if (!token) {
    console.error('❌ IMPACT_TOKEN is not set');
    return false;
  }
  
  if (!configJson) {
    console.error('❌ IMPACT_CONFIG is not set');
    return false;
  }
  
  try {
    const config = JSON.parse(configJson);
    console.log('✅ Configuration is valid');
    console.log('Configuration keys:', Object.keys(config).length);
    return true;
  } catch (error) {
    console.error('❌ Configuration is invalid:', error.message);
    return false;
  }
}

/**
 * Quick setup for testing
 * Replace with your actual credentials
 */
function quickSetup() {
  // Replace these with your actual credentials
  const SID = 'YOUR_IMPACT_SID_HERE';
  const TOKEN = 'YOUR_IMPACT_TOKEN_HERE';
  const SPREADSHEET_ID = 'YOUR_SPREADSHEET_ID_HERE'; // Optional
  
  if (SID === 'YOUR_IMPACT_SID_HERE' || TOKEN === 'YOUR_IMPACT_TOKEN_HERE') {
    console.log('❌ Please update the credentials in quickSetup() function first');
    console.log('Edit the SID, TOKEN, and SPREADSHEET_ID variables with your actual values');
    return;
  }
  
  setupBasicConfiguration(SID, TOKEN, SPREADSHEET_ID);
  testConfiguration();
}

/**
 * Simple test function to debug the setup
 */
function testSetup() {
  console.log('Testing setup with sample data...');
  
  try {
    const props = PropertiesService.getScriptProperties();
    
    // Test setting a simple property
    props.setProperty('TEST_PROPERTY', 'test_value');
    console.log('✅ Simple property set successfully');
    
    // Test setting credentials
    props.setProperty('IMPACT_SID', 'test_sid_12345');
    props.setProperty('IMPACT_TOKEN', 'test_token_67890');
    console.log('✅ Credentials set successfully');
    
    // Test setting JSON config
    const testConfig = { test: true, value: 123 };
    props.setProperty('IMPACT_CONFIG', JSON.stringify(testConfig));
    console.log('✅ JSON config set successfully');
    
    // Test reading back
    const readSid = props.getProperty('IMPACT_SID');
    const readToken = props.getProperty('IMPACT_TOKEN');
    const readConfig = props.getProperty('IMPACT_CONFIG');
    
    console.log('Read SID:', readSid);
    console.log('Read Token:', readToken);
    console.log('Read Config:', readConfig);
    
    // Clean up test properties
    props.deleteProperty('TEST_PROPERTY');
    props.deleteProperty('IMPACT_SID');
    props.deleteProperty('IMPACT_TOKEN');
    props.deleteProperty('IMPACT_CONFIG');
    
    console.log('✅ All tests passed!');
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    console.error('Error details:', error);
  }
}

/**
 * Simple setup with hardcoded test values - no parameters needed
 * Edit the values below with your actual credentials
 */
function setupWithTestValues() {
  console.log('Setting up with test values...');
  
  // EDIT THESE VALUES WITH YOUR ACTUAL CREDENTIALS
  const SID = 'YOUR_ACTUAL_SID_HERE';
  const TOKEN = 'YOUR_ACTUAL_TOKEN_HERE';
  const SPREADSHEET_ID = 'YOUR_SPREADSHEET_ID_HERE'; // Optional
  
  if (SID === 'YOUR_ACTUAL_SID_HERE' || TOKEN === 'YOUR_ACTUAL_TOKEN_HERE') {
    console.log('❌ Please edit the SID and TOKEN values in the setupWithTestValues() function first');
    console.log('Replace YOUR_ACTUAL_SID_HERE and YOUR_ACTUAL_TOKEN_HERE with your real credentials');
    return;
  }
  
  try {
    setupBasicConfiguration(SID, TOKEN, SPREADSHEET_ID);
    console.log('✅ Setup completed successfully!');
  } catch (error) {
    console.error('❌ Setup failed:', error.message);
  }
}

// Example usage:
// setupBasicConfiguration('your_sid', 'your_token', 'your_spreadsheet_id');
// setupDevelopmentConfiguration('your_sid', 'your_token', 'your_spreadsheet_id');
// setupProductionConfiguration('your_sid', 'your_token', 'your_spreadsheet_id');
// viewCurrentConfiguration();
// testConfiguration();
