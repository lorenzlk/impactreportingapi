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
2. **Copy the main script** (`optimized-impact-script-v4.js`) into your project
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
   testConnection(); // Should return success with report count
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

All settings are stored in a single `IMPACT_OPTIMIZED_CONFIG` property as JSON:

```javascript
{
  "apiBaseUrl": "https://api.impact.com",
  "maxRetries": 5,
  "retryDelay": 1000,
  "maxPollingAttempts": 30,
  "initialPollingDelay": 3000,
  "maxPollingDelay": 60000,
  "requestDelay": 800,
  "parallelRequestLimit": 3,
  "maxRowsPerSheet": 50000,
  "enableParallelProcessing": true,
  "enableSmartChunking": true,
  "enableMemoryOptimization": true,
  "enableResume": true,
  "enableAutoRecovery": true
}
```

## 🚀 Usage

### Quick Start - Complete Discovery

Run this single function to discover everything:

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
  const orchestrator = new UltraOptimizedOrchestrator();
  
  // Update configuration
  orchestrator.config.set('maxRetries', 5);
  orchestrator.config.set('requestDelay', 800);
  
  // Run discovery
  return orchestrator.runCompleteDiscovery();
}
```

### Individual Functions

```javascript
// Discover reports only
const reports = discoverAllReports();

// Test connection
const connectionTest = testConnection();

// Check system health
const health = getSystemHealth();

// Get progress
const progress = getProgress();
```

## 📊 What You'll Get

### Discovery Summary Sheet
- Overview of all reports found
- Success/failure status for each report
- Row counts and column information
- Performance metrics and timing

### Individual Report Sheets
- One sheet per accessible report type
- Complete data export for each report
- Formatted headers and metadata
- Report details in cell notes
- Automatic chunking for large datasets

### Sample Output Structure
```
📊 Your Discovery Spreadsheet:
├── DISCOVERY SUMMARY (overview of all findings)
├── partner_performance_by_subid (your main performance data)
├── campaign_performance (campaign-level metrics)
├── click_performance (click tracking data)
└── [additional report types based on your account access]
```

## 🔧 Understanding Your Data

### Common Report Types You Might Find:
- **partner_performance_by_subid** - Main affiliate performance metrics
- **campaign_performance** - Campaign-level data
- **creative_performance** - Ad creative performance
- **click_performance** - Click tracking details
- **conversion_performance** - Conversion tracking

### Key Metrics to Look For:
- **Clicks** - Traffic volume
- **Actions/Conversions** - Successful conversions
- **Sale_amount** - Revenue generated
- **Earnings** - Your commission earnings
- **CPC_Cost/Click_Cost** - Cost per click
- **EPC** - Earnings per click
- **AOV** - Average order value

## 📈 Performance Optimization

### Version 4.0 Improvements
- **30-50% faster execution** than previous versions
- **Smart batching** and parallel processing
- **Memory optimization** for large datasets
- **Intelligent polling** with adaptive delays
- **Enhanced resume capability** for interrupted runs
- **Timeout prevention** strategies

### Recommended Settings

For large-scale operations:
```javascript
optimizeConfiguration(); // Applies performance optimizations
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

## 🔧 Troubleshooting

### Common Issues

1. **Authentication Errors (401)**
   - Verify SID and TOKEN are correct
   - Check API credentials in Script Properties
   - Use `testConnection()` to verify

2. **Timeout Errors**
   - Enable smart chunking for large reports
   - Reduce parallel request limit
   - Check network connectivity

3. **Memory Issues**
   - Enable memory optimization
   - Reduce maxRowsPerSheet
   - Process reports in smaller batches

4. **Rate Limiting**
   - Increase requestDelay
   - Reduce parallelRequestLimit
   - Check API quotas

### Debug Mode

Enable debug logging for detailed troubleshooting:

```javascript
updateConfiguration({
  logLevel: 'DEBUG',
  enableDetailedLogging: true,
  enablePerformanceMetrics: true
});
```

## 📊 Monitoring

### Health Checks
```javascript
const health = getSystemHealth();
console.log(`System Status: ${health.status}`);
console.log(`Completed Reports: ${health.completedReports}`);
console.log(`Performance Metrics: ${JSON.stringify(health.metrics)}`);
```

### Progress Tracking
```javascript
const progress = getProgress();
console.log(`Current Phase: ${progress.current?.phase}`);
console.log(`Completed: ${progress.completed.length} reports`);
```

## 🔒 Security

- All credentials are stored securely in Script Properties
- Data sanitization removes potentially malicious content
- Input validation prevents injection attacks
- Audit logging tracks all operations

## 📝 API Reference

### Main Functions

- `runCompleteDiscovery()`: Complete discovery process
- `resumeDiscovery()`: Resume from where you left off
- `restartDiscovery()`: Start fresh (clear progress)
- `discoverAllReports()`: Discover available reports
- `testConnection()`: Test API connection

### Utility Functions

- `getSystemHealth()`: Get system status
- `getProgress()`: Get current progress
- `getPerformanceMetrics()`: Get performance data
- `clearProgress()`: Clear all progress
- `optimizeConfiguration()`: Apply performance optimizations

### Classes

- `UltraOptimizedOrchestrator`: Main orchestrator
- `ImpactConfig`: Configuration management
- `EnhancedLogger`: Logging system
- `EnhancedAPIClient`: API client
- `EnhancedDataProcessor`: Data processing
- `EnhancedSpreadsheetManager`: Spreadsheet operations

## 📁 Project Structure

### **Core Files:**
- `optimized-impact-script-v4.js` - **Main Impact.com data export script**
- `business-intelligence-dashboard.js` - **BI dashboard and analytics system**
- `setup-configuration.js` - **Configuration setup and management**
- `secure-configuration.js` - **Secure credential management**

### **Documentation:**
- `README.md` - **Main project documentation**
- `SECURITY-GUIDE.md` - **Security best practices and setup**
- `BI-Dashboard-Guide.md` - **Business Intelligence dashboard guide**

## 🔄 Version History

### v4.0.0 (Current) - Ultra-Optimized
- Complete rewrite with enterprise features
- Smart batching and parallel processing
- Enhanced error handling and recovery
- Advanced progress tracking and resume
- Memory optimization and timeout prevention
- Performance metrics and monitoring
- **Business Intelligence dashboard system**
- **Secure credential management**

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
4. Use `testConnection()` to verify setup
5. Contact support with detailed error information

---

*Last updated: January 2025*
*Version: 4.0.0 - Ultra-Optimized*