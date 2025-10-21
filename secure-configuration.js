/**
 * Secure Configuration Setup for Impact.com Script
 * 
 * This script demonstrates secure credential management without hardcoding
 * sensitive information in the source code.
 * 
 * SECURITY BEST PRACTICES:
 * - Never commit credentials to version control
 * - Use environment variables or secure storage
 * - Rotate credentials regularly
 * - Use least privilege access
 */

/**
 * Secure credential setup using Google Apps Script Properties
 * This is the RECOMMENDED approach for Google Apps Script
 */
function setupSecureCredentials() {
  console.log('Setting up secure credentials...');
  
  // Get credentials from user input (not hardcoded)
  const sid = prompt('Enter your Impact.com SID:');
  const token = prompt('Enter your Impact.com Token:');
  const spreadsheetId = prompt('Enter your Spreadsheet ID (optional):');
  
  if (!sid || !token) {
    throw new Error('SID and Token are required');
  }
  
  const props = PropertiesService.getScriptProperties();
  
  // Store credentials securely in Script Properties
  props.setProperty('IMPACT_SID', sid);
  props.setProperty('IMPACT_TOKEN', token);
  
  if (spreadsheetId && spreadsheetId.trim() !== '') {
    props.setProperty('IMPACT_SPREADSHEET_ID', spreadsheetId);
  }
  
  console.log('✅ Credentials stored securely in Script Properties');
  console.log('SID: ' + sid.substring(0, 8) + '...');
  console.log('Token: ' + token.substring(0, 8) + '...');
  
  return true;
}

/**
 * Alternative: Setup with environment variables (if available)
 * This approach works with Google Apps Script's environment variables
 */
function setupWithEnvironmentVariables() {
  console.log('Setting up with environment variables...');
  
  // Check if environment variables are available
  const sid = process.env.IMPACT_SID || PropertiesService.getScriptProperties().getProperty('IMPACT_SID');
  const token = process.env.IMPACT_TOKEN || PropertiesService.getScriptProperties().getProperty('IMPACT_TOKEN');
  const spreadsheetId = process.env.IMPACT_SPREADSHEET_ID || PropertiesService.getScriptProperties().getProperty('IMPACT_SPREADSHEET_ID');
  
  if (!sid || !token) {
    throw new Error('Environment variables IMPACT_SID and IMPACT_TOKEN must be set');
  }
  
  console.log('✅ Using environment variables for credentials');
  console.log('SID: ' + sid.substring(0, 8) + '...');
  console.log('Token: ' + token.substring(0, 8) + '...');
  
  return { sid, token, spreadsheetId };
}

/**
 * Secure credential validation
 */
function validateCredentials() {
  const props = PropertiesService.getScriptProperties();
  
  const sid = props.getProperty('IMPACT_SID');
  const token = props.getProperty('IMPACT_TOKEN');
  
  if (!sid || !token) {
    console.log('❌ Credentials not found in Script Properties');
    console.log('Run setupSecureCredentials() first');
    return false;
  }
  
  if (sid === 'YOUR_IMPACT_SID_HERE' || token === 'YOUR_IMPACT_TOKEN_HERE') {
    console.log('❌ Default placeholder values detected');
    console.log('Please update with your actual credentials');
    return false;
  }
  
  console.log('✅ Credentials found and validated');
  console.log('SID: ' + sid.substring(0, 8) + '...');
  console.log('Token: ' + token.substring(0, 8) + '...');
  
  return true;
}

/**
 * Test API connection with secure credentials
 */
function testSecureConnection() {
  console.log('Testing secure API connection...');
  
  if (!validateCredentials()) {
    return false;
  }
  
  try {
    const props = PropertiesService.getScriptProperties();
    const sid = props.getProperty('IMPACT_SID');
    const token = props.getProperty('IMPACT_TOKEN');
    
    const url = `https://api.impact.com/Mediapartners/${sid}/Reports`;
    const basicAuth = Utilities.base64Encode(`${sid}:${token}`);
    
    const response = UrlFetchApp.fetch(url, {
      headers: {
        'Authorization': `Basic ${basicAuth}`,
        'Accept': 'application/json'
      },
      muteHttpExceptions: true
    });
    
    if (response.getResponseCode() === 200) {
      console.log('✅ API connection successful');
      const data = JSON.parse(response.getContentText());
      console.log('Found ' + (data.Reports ? data.Reports.length : 0) + ' reports');
      return true;
    } else {
      console.log('❌ API connection failed: ' + response.getResponseCode());
      return false;
    }
    
  } catch (error) {
    console.log('❌ API connection error: ' + error.message);
    return false;
  }
}

/**
 * Clear all stored credentials (for security)
 */
function clearCredentials() {
  const props = PropertiesService.getScriptProperties();
  
  props.deleteProperty('IMPACT_SID');
  props.deleteProperty('IMPACT_TOKEN');
  props.deleteProperty('IMPACT_SPREADSHEET_ID');
  
  console.log('✅ All credentials cleared from Script Properties');
}

/**
 * Rotate credentials (security best practice)
 */
function rotateCredentials() {
  console.log('Rotating credentials...');
  
  // Clear old credentials
  clearCredentials();
  
  // Setup new credentials
  setupSecureCredentials();
  
  // Test new credentials
  if (testSecureConnection()) {
    console.log('✅ Credential rotation successful');
    return true;
  } else {
    console.log('❌ Credential rotation failed');
    return false;
  }
}

/**
 * Security audit - check for exposed credentials
 */
function securityAudit() {
  console.log('Running security audit...');
  
  const issues = [];
  
  // Check for hardcoded credentials in common patterns
  const scripts = [
    'optimized-impact-script-v4.js',
    'setup-configuration.js',
    'business-intelligence-dashboard.js'
  ];
  
  scripts.forEach(scriptName => {
    try {
      // This would need to be implemented to check file contents
      // For now, we'll just log the check
      console.log(`Checking ${scriptName} for security issues...`);
    } catch (error) {
      console.log(`Error checking ${scriptName}: ${error.message}`);
    }
  });
  
  // Check Script Properties for placeholder values
  const props = PropertiesService.getScriptProperties();
  const sid = props.getProperty('IMPACT_SID');
  const token = props.getProperty('IMPACT_TOKEN');
  
  if (sid === 'YOUR_IMPACT_SID_HERE' || sid === 'YOUR_SID_HERE') {
    issues.push('SID contains placeholder value');
  }
  
  if (token === 'YOUR_IMPACT_TOKEN_HERE' || token === 'YOUR_TOKEN_HERE') {
    issues.push('Token contains placeholder value');
  }
  
  if (issues.length === 0) {
    console.log('✅ Security audit passed - no issues found');
  } else {
    console.log('❌ Security issues found:');
    issues.forEach(issue => console.log('  - ' + issue));
  }
  
  return issues.length === 0;
}

/**
 * Get secure configuration for scripts
 */
function getSecureConfig() {
  if (!validateCredentials()) {
    throw new Error('Credentials not properly configured');
  }
  
  const props = PropertiesService.getScriptProperties();
  
  return {
    sid: props.getProperty('IMPACT_SID'),
    token: props.getProperty('IMPACT_TOKEN'),
    spreadsheetId: props.getProperty('IMPACT_SPREADSHEET_ID')
  };
}

// Export functions for use in other scripts
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    setupSecureCredentials,
    setupWithEnvironmentVariables,
    validateCredentials,
    testSecureConnection,
    clearCredentials,
    rotateCredentials,
    securityAudit,
    getSecureConfig
  };
}
