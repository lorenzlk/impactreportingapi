# Impact.com Script Configuration Guide

This guide provides detailed information on configuring the optimized Impact.com script for different environments and use cases.

## 🔧 Configuration Overview

The script uses a comprehensive configuration management system that allows you to customize behavior without modifying code. All configuration is stored in Google Apps Script Properties and can be updated at runtime.

## 📋 Required Configuration

### Essential Properties

These properties must be set for the script to function:

```javascript
// Impact.com API Credentials
IMPACT_SID: "your_impact_sid_here"
IMPACT_TOKEN: "your_impact_token_here"

// Target Spreadsheet (optional - will use default if not set)
IMPACT_SPREADSHEET_ID: "your_google_sheet_id_here"
```

## ⚙️ Advanced Configuration

### API Configuration

Control API behavior and timeouts:

```javascript
// Base API URL
IMPACT_CONFIG_APIBASEURL: "https://api.impact.com"

// Request timeout in milliseconds
IMPACT_CONFIG_TIMEOUT: 30000

// Maximum number of retry attempts
IMPACT_CONFIG_MAXRETRIES: 3

// Base delay between retries in milliseconds
IMPACT_CONFIG_RETRYDELAY: 1000
```

### Polling Configuration

Control how the script polls for job completion:

```javascript
// Maximum polling attempts
IMPACT_CONFIG_MAXPOLLINGATTEMPTS: 12

// Initial delay before first poll (milliseconds)
IMPACT_CONFIG_INITIALPOLLINGDELAY: 10000

// Maximum delay between polls (milliseconds)
IMPACT_CONFIG_MAXPOLLINGDELAY: 60000

// Multiplier for exponential backoff
IMPACT_CONFIG_POLLINGMULTIPLIER: 1.5
```

### Rate Limiting

Control request frequency to avoid API limits:

```javascript
// Delay between requests in milliseconds
IMPACT_CONFIG_REQUESTDELAY: 2000

// Maximum requests in burst
IMPACT_CONFIG_BURSTLIMIT: 5
```

### Data Processing

Control data handling and validation:

```javascript
// Maximum rows per spreadsheet sheet
IMPACT_CONFIG_MAXROWSPERSHEET: 100000

// Enable data validation
IMPACT_CONFIG_ENABLEDATAVALIDATION: true

// Enable data sanitization
IMPACT_CONFIG_ENABLEDATASANITIZATION: true
```

### Logging Configuration

Control logging behavior and retention:

```javascript
// Log level (DEBUG, INFO, WARN, ERROR, FATAL)
IMPACT_CONFIG_LOGLEVEL: "INFO"

// Enable detailed logging
IMPACT_CONFIG_ENABLEDETAILEDLOGGING: true

// Log retention in days
IMPACT_CONFIG_LOGRETENTIONDAYS: 30
```

### Error Handling

Control error handling and recovery:

```javascript
// Enable circuit breaker pattern
IMPACT_CONFIG_ENABLECIRCUITBREAKER: true

// Circuit breaker failure threshold
IMPACT_CONFIG_CIRCUITBREAKERTHRESHOLD: 5

// Circuit breaker timeout in milliseconds
IMPACT_CONFIG_CIRCUITBREAKERTIMEOUT: 300000
```

### Output Configuration

Control spreadsheet output formatting:

```javascript
// Default sheet name
IMPACT_CONFIG_DEFAULTSHEETNAME: "Impact Data"

// Enable automatic formatting
IMPACT_CONFIG_ENABLEAUTOFORMATTING: true

// Enable data notes and metadata
IMPACT_CONFIG_ENABLEDATANOTES: true
```

## 🏗️ Environment-Specific Configurations

### Development Environment

For development and testing:

```javascript
// Lower timeouts for faster feedback
IMPACT_CONFIG_TIMEOUT: 10000
IMPACT_CONFIG_MAXRETRIES: 2
IMPACT_CONFIG_REQUESTDELAY: 500

// Enable debug logging
IMPACT_CONFIG_LOGLEVEL: "DEBUG"
IMPACT_CONFIG_ENABLEDETAILEDLOGGING: true

// Smaller data limits for testing
IMPACT_CONFIG_MAXROWSPERSHEET: 1000
```

### Production Environment

For production use:

```javascript
// Robust timeouts and retries
IMPACT_CONFIG_TIMEOUT: 60000
IMPACT_CONFIG_MAXRETRIES: 5
IMPACT_CONFIG_REQUESTDELAY: 2000

// Production logging
IMPACT_CONFIG_LOGLEVEL: "INFO"
IMPACT_CONFIG_ENABLEDETAILEDLOGGING: false

// Full data processing
IMPACT_CONFIG_MAXROWSPERSHEET: 100000
IMPACT_CONFIG_ENABLEDATAVALIDATION: true
IMPACT_CONFIG_ENABLEDATASANITIZATION: true
```

### High-Volume Environment

For processing large amounts of data:

```javascript
// Extended timeouts
IMPACT_CONFIG_TIMEOUT: 120000
IMPACT_CONFIG_MAXPOLLINGATTEMPTS: 20
IMPACT_CONFIG_MAXPOLLINGDELAY: 120000

// Conservative rate limiting
IMPACT_CONFIG_REQUESTDELAY: 5000
IMPACT_CONFIG_BURSTLIMIT: 2

// Large data limits
IMPACT_CONFIG_MAXROWSPERSHEET: 500000
```

## 🔄 Configuration Management

### Setting Configuration Values

Use the provided utility functions to manage configuration:

```javascript
// Get current configuration
const config = getConfiguration();

// Update specific values
updateConfiguration({
  logLevel: 'DEBUG',
  maxRetries: 5,
  requestDelay: 1000
});

// Update multiple values
updateConfiguration({
  timeout: 45000,
  enableDataValidation: true,
  enableCircuitBreaker: true
});
```

### Configuration Validation

The system automatically validates configuration values:

```javascript
// Check system health (includes config validation)
const health = getSystemHealth();
if (!health.configValid) {
  console.error('Configuration errors:', health.configErrors);
}
```

### Environment Variables

For different environments, you can set different property names:

```javascript
// Development
IMPACT_CONFIG_DEV_TIMEOUT: 10000

// Production  
IMPACT_CONFIG_PROD_TIMEOUT: 60000

// Staging
IMPACT_CONFIG_STAGING_TIMEOUT: 30000
```

## 📊 Performance Tuning

### Memory Optimization

For memory-constrained environments:

```javascript
// Reduce data processing limits
IMPACT_CONFIG_MAXROWSPERSHEET: 50000
IMPACT_CONFIG_ENABLEDATAVALIDATION: false
IMPACT_CONFIG_ENABLEDATASANITIZATION: false

// Reduce logging
IMPACT_CONFIG_LOGRETENTIONDAYS: 7
IMPACT_CONFIG_ENABLEDETAILEDLOGGING: false
```

### Speed Optimization

For faster processing:

```javascript
// Reduce delays
IMPACT_CONFIG_REQUESTDELAY: 500
IMPACT_CONFIG_INITIALPOLLINGDELAY: 5000

// Reduce retries
IMPACT_CONFIG_MAXRETRIES: 2

// Disable validation for speed
IMPACT_CONFIG_ENABLEDATAVALIDATION: false
```

### Reliability Optimization

For maximum reliability:

```javascript
// Increase timeouts and retries
IMPACT_CONFIG_TIMEOUT: 120000
IMPACT_CONFIG_MAXRETRIES: 10
IMPACT_CONFIG_MAXPOLLINGATTEMPTS: 30

// Enable all safety features
IMPACT_CONFIG_ENABLECIRCUITBREAKER: true
IMPACT_CONFIG_ENABLEDATAVALIDATION: true
IMPACT_CONFIG_ENABLEDATASANITIZATION: true
```

## 🔍 Monitoring Configuration

### Health Check Settings

Configure health monitoring:

```javascript
// Enable detailed health reporting
IMPACT_CONFIG_ENABLEHEALTHCHECKS: true

// Health check interval (milliseconds)
IMPACT_CONFIG_HEALTHCHECKINTERVAL: 300000

// Alert thresholds
IMPACT_CONFIG_ERRORTHRESHOLD: 5
IMPACT_CONFIG_PERFORMANCETHRESHOLD: 30000
```

### Logging Levels

Choose appropriate logging levels:

- **DEBUG**: Detailed execution information (development only)
- **INFO**: General process information (recommended for production)
- **WARN**: Warning conditions (minimal logging)
- **ERROR**: Error conditions only
- **FATAL**: Critical failures only

## 🛠️ Troubleshooting Configuration

### Common Configuration Issues

1. **Timeout Errors**
   ```javascript
   // Increase timeout
   IMPACT_CONFIG_TIMEOUT: 60000
   IMPACT_CONFIG_MAXPOLLINGATTEMPTS: 20
   ```

2. **Rate Limiting Errors**
   ```javascript
   // Increase delays
   IMPACT_CONFIG_REQUESTDELAY: 5000
   IMPACT_CONFIG_BURSTLIMIT: 1
   ```

3. **Memory Issues**
   ```javascript
   // Reduce data limits
   IMPACT_CONFIG_MAXROWSPERSHEET: 25000
   IMPACT_CONFIG_ENABLEDATAVALIDATION: false
   ```

4. **Authentication Errors**
   ```javascript
   // Check credentials are set
   IMPACT_SID: "correct_sid"
   IMPACT_TOKEN: "correct_token"
   ```

### Configuration Validation

The system provides built-in validation:

```javascript
// Get validation results
const health = getSystemHealth();
console.log('Config valid:', health.configValid);
console.log('Errors:', health.configErrors);
console.log('Warnings:', health.configWarnings);
```

## 📝 Configuration Examples

### Complete Development Setup

```javascript
// Essential credentials
IMPACT_SID: "dev_sid_12345"
IMPACT_TOKEN: "dev_token_67890"
IMPACT_SPREADSHEET_ID: "dev_spreadsheet_id"

// Development settings
IMPACT_CONFIG_LOGLEVEL: "DEBUG"
IMPACT_CONFIG_ENABLEDETAILEDLOGGING: true
IMPACT_CONFIG_TIMEOUT: 10000
IMPACT_CONFIG_MAXRETRIES: 2
IMPACT_CONFIG_REQUESTDELAY: 500
IMPACT_CONFIG_MAXROWSPERSHEET: 1000
```

### Complete Production Setup

```javascript
// Essential credentials
IMPACT_SID: "prod_sid_12345"
IMPACT_TOKEN: "prod_token_67890"
IMPACT_SPREADSHEET_ID: "prod_spreadsheet_id"

// Production settings
IMPACT_CONFIG_LOGLEVEL: "INFO"
IMPACT_CONFIG_ENABLEDETAILEDLOGGING: false
IMPACT_CONFIG_TIMEOUT: 60000
IMPACT_CONFIG_MAXRETRIES: 5
IMPACT_CONFIG_REQUESTDELAY: 2000
IMPACT_CONFIG_MAXROWSPERSHEET: 100000
IMPACT_CONFIG_ENABLEDATAVALIDATION: true
IMPACT_CONFIG_ENABLEDATASANITIZATION: true
IMPACT_CONFIG_ENABLECIRCUITBREAKER: true
```

## 🔄 Configuration Updates

### Runtime Updates

Configuration can be updated at runtime:

```javascript
// Update configuration during execution
updateConfiguration({
  logLevel: 'DEBUG',
  maxRetries: 3
});

// Run discovery with new settings
const results = runCompleteDiscovery();
```

### Batch Updates

Update multiple settings at once:

```javascript
const newConfig = {
  timeout: 45000,
  maxRetries: 4,
  requestDelay: 1500,
  enableDataValidation: true,
  enableCircuitBreaker: true
};

updateConfiguration(newConfig);
```

## 📚 Best Practices

1. **Start with defaults** and adjust based on your needs
2. **Test configuration changes** in development first
3. **Monitor performance** after configuration changes
4. **Use environment-specific** configurations
5. **Document custom configurations** for team members
6. **Validate configuration** before running production jobs
7. **Keep credentials secure** and rotate regularly
8. **Monitor logs** for configuration-related issues

---

*For more information, see the main README.md file or contact support.*
