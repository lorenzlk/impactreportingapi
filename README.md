# Impact.com Data Discovery and Export System

A robust, enterprise-grade solution for discovering and exporting data from the Impact.com API with comprehensive error handling, monitoring, and configuration management.

## 🚀 Features

### Core Functionality
- **Complete Data Discovery**: Automatically discover all available reports from Impact.com API
- **Intelligent Export Scheduling**: Schedule and manage bulk data exports with rate limiting
- **Advanced Data Processing**: Validate, sanitize, and process CSV data with quality checks
- **Automated Spreadsheet Generation**: Create formatted Google Sheets with metadata and notes

### Enterprise Features
- **Configuration Management**: Externalized configuration with validation and environment support
- **Error Handling & Recovery**: Circuit breaker pattern, exponential backoff, and comprehensive retry logic
- **Monitoring & Logging**: Structured logging with multiple levels and persistence
- **Data Validation**: Quality checks, duplicate detection, and security sanitization
- **Performance Optimization**: Asynchronous processing, intelligent polling, and memory management

## 📋 Requirements

- Google Apps Script environment
- Impact.com API credentials (SID and Token)
- Google Sheets API access
- Internet connectivity for API calls

## 🛠️ Installation

1. **Create a new Google Apps Script project**
2. **Copy the optimized script** (`optimized-impact-script.js`) into your project
3. **Copy the setup script** (`setup-configuration.js`) into your project
4. **Run the setup script** to configure your environment:
   ```javascript
   // Quick setup (replace with your credentials)
   setupBasicConfiguration('your_sid', 'your_token', 'your_spreadsheet_id');
   
   // Or choose environment-specific setup:
   setupDevelopmentConfiguration('your_sid', 'your_token', 'your_spreadsheet_id');
   setupProductionConfiguration('your_sid', 'your_token', 'your_spreadsheet_id');
   ```
5. **Test your configuration**:
   ```javascript
   testConfiguration(); // Should return true
   ```

## ⚙️ Configuration

The system uses a comprehensive configuration management system that stores all settings in a single JSON property to avoid Google Apps Script's 50 property limit.

### Quick Setup

Use the provided setup script for easy configuration:

```javascript
// Basic setup
setupBasicConfiguration('your_sid', 'your_token', 'your_spreadsheet_id');

// Development setup (faster, more verbose)
setupDevelopmentConfiguration('your_sid', 'your_token', 'your_spreadsheet_id');

// Production setup (robust, optimized)
setupProductionConfiguration('your_sid', 'your_token', 'your_spreadsheet_id');
```

### Manual Configuration

All settings are stored in a single `IMPACT_CONFIG` property as JSON:

```javascript
{
  "apiBaseUrl": "https://api.impact.com",
  "timeout": 30000,
  "maxRetries": 3,
  "retryDelay": 1000,
  "maxPollingAttempts": 12,
  "initialPollingDelay": 10000,
  "maxPollingDelay": 60000,
  "pollingMultiplier": 1.5,
  "requestDelay": 2000,
  "burstLimit": 5,
  "maxRowsPerSheet": 100000,
  "enableDataValidation": true,
  "enableDataSanitization": true,
  "logLevel": "INFO",
  "enableDetailedLogging": true,
  "logRetentionDays": 30,
  "enableCircuitBreaker": true,
  "circuitBreakerThreshold": 5,
  "circuitBreakerTimeout": 300000,
  "defaultSheetName": "Impact Data",
  "enableAutoFormatting": true,
  "enableDataNotes": true
}
```

## 🚀 Usage

### Basic Usage

```javascript
// Run complete discovery process
function main() {
  const results = runCompleteDiscovery();
  console.log(`Discovery complete: ${results.successful.length} successful, ${results.failed.length} failed`);
}
```

### Advanced Usage

```javascript
// Custom configuration
function customDiscovery() {
  const orchestrator = new ImpactDiscoveryOrchestrator();
  
  // Update configuration
  orchestrator.config.set('maxRetries', 5);
  orchestrator.config.set('requestDelay', 1000);
  
  // Run discovery
  return orchestrator.runCompleteDiscovery();
}
```

### Individual Functions

```javascript
// Discover reports only
const reports = await discoverAllReports();

// Schedule exports only
const exportResults = await exportAllReportsRaw();

// Check system health
const health = getSystemHealth();
```

## 📊 Monitoring

### Health Checks
```javascript
const health = getSystemHealth();
console.log(`System Status: ${health.status}`);
console.log(`Recent Errors: ${health.recentErrors}`);
```

### Logging
The system provides comprehensive logging at multiple levels:
- **DEBUG**: Detailed execution information
- **INFO**: General process information
- **WARN**: Warning conditions
- **ERROR**: Error conditions
- **FATAL**: Critical failures

### Metrics
- Execution time tracking
- Success/failure rates
- Data quality metrics
- Performance statistics

## 🔧 Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify SID and TOKEN are correct
   - Check API credentials in Script Properties

2. **Timeout Errors**
   - Increase timeout configuration
   - Check network connectivity
   - Reduce batch sizes

3. **Memory Issues**
   - Reduce maxRowsPerSheet
   - Enable data validation
   - Process reports in smaller batches

4. **Rate Limiting**
   - Increase requestDelay
   - Reduce concurrent operations
   - Check API quotas

### Debug Mode

Enable debug logging for detailed troubleshooting:

```javascript
updateConfiguration({
  logLevel: 'DEBUG',
  enableDetailedLogging: true
});
```

## 📈 Performance Optimization

### Recommended Settings

For large-scale operations:
```javascript
updateConfiguration({
  maxRetries: 5,
  requestDelay: 1000,
  maxRowsPerSheet: 50000,
  enableDataValidation: true,
  enableCircuitBreaker: true
});
```

For development/testing:
```javascript
updateConfiguration({
  logLevel: 'DEBUG',
  maxRetries: 2,
  requestDelay: 500,
  enableDetailedLogging: true
});
```

## 🔒 Security

- All credentials are stored securely in Script Properties
- Data sanitization removes potentially malicious content
- Input validation prevents injection attacks
- Audit logging tracks all operations

## 📝 API Reference

### Main Functions

- `runCompleteDiscovery()`: Complete discovery process
- `discoverAllReports()`: Discover available reports
- `exportAllReportsRaw()`: Schedule all exports
- `fetchAllDiscoveryData()`: Process all exports

### Utility Functions

- `getSystemHealth()`: Get system status
- `getConfiguration()`: Get current configuration
- `updateConfiguration(updates)`: Update configuration
- `clearLogs()`: Clear all logs

### Classes

- `ImpactConfig`: Configuration management
- `ImpactLogger`: Logging system
- `ImpactAPIClient`: API client
- `DataProcessor`: Data processing
- `SpreadsheetManager`: Spreadsheet operations
- `ImpactDiscoveryOrchestrator`: Main orchestrator

## 🤝 Contributing

1. Follow the coding standards in `.cursorrules`
2. Add comprehensive tests for new features
3. Update documentation for API changes
4. Ensure backward compatibility

## 📄 License

MIT License - see LICENSE file for details

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section
2. Review logs for error details
3. Check system health status
4. Contact support with detailed error information

## 🔄 Version History

### v2.0.0 (Current)
- Complete rewrite with enterprise features
- Configuration management system
- Advanced error handling and recovery
- Comprehensive logging and monitoring
- Data validation and sanitization
- Performance optimizations

### v1.0.0 (Legacy)
- Basic discovery and export functionality
- Simple error handling
- Fixed configuration
- Limited monitoring

---

*Last updated: [Current Date]*
*Version: 2.0.0*
