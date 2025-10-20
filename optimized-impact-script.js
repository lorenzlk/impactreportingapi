/**
 * Optimized Impact.com Data Discovery and Export System
 * 
 * This script provides a robust, scalable solution for discovering and exporting
 * data from the Impact.com API with comprehensive error handling, monitoring,
 * and configuration management.
 * 
 * @version 2.0.0
 * @author Logan Lorenz
 * @created 2024
 */

// ============================================================================
// CONFIGURATION MANAGEMENT
// ============================================================================

/**
 * Configuration manager for the Impact.com script
 * Handles all configuration values, validation, and environment management
 * Uses a single JSON property to avoid Google Apps Script's 50 property limit
 */
class ImpactConfig {
  constructor() {
    this.props = PropertiesService.getScriptProperties();
    this.config = this.loadConfiguration();
  }

  /**
   * Load configuration from PropertiesService with defaults
   * @returns {Object} Configuration object
   */
  loadConfiguration() {
    const defaults = {
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

    // Load from single JSON property
    const configJson = this.props.getProperty('IMPACT_CONFIG');
    if (configJson) {
      try {
        const savedConfig = JSON.parse(configJson);
        // Merge with defaults to ensure all keys exist
        return { ...defaults, ...savedConfig };
      } catch (error) {
        console.warn('Failed to parse saved configuration, using defaults:', error.message);
        return defaults;
      }
    }

    return defaults;
  }

  /**
   * Get a configuration value
   * @param {string} key - Configuration key
   * @param {*} defaultValue - Default value if key not found
   * @returns {*} Configuration value
   */
  get(key, defaultValue = null) {
    return this.config.hasOwnProperty(key) ? this.config[key] : defaultValue;
  }

  /**
   * Set a configuration value
   * @param {string} key - Configuration key
   * @param {*} value - Value to set
   */
  set(key, value) {
    this.config[key] = value;
    this.saveConfiguration();
  }

  /**
   * Save entire configuration to PropertiesService
   */
  saveConfiguration() {
    try {
      this.props.setProperty('IMPACT_CONFIG', JSON.stringify(this.config));
    } catch (error) {
      console.error('Failed to save configuration:', error.message);
      throw new Error('Configuration save failed: ' + error.message);
    }
  }

  /**
   * Validate configuration
   * @returns {Object} Validation result
   */
  validate() {
    const errors = [];
    const warnings = [];

    // Required credentials
    if (!this.props.getProperty('IMPACT_SID')) {
      errors.push('IMPACT_SID is required');
    }
    if (!this.props.getProperty('IMPACT_TOKEN')) {
      errors.push('IMPACT_TOKEN is required');
    }

    // Validate numeric ranges
    if (this.config.timeout < 1000 || this.config.timeout > 300000) {
      warnings.push('Timeout should be between 1-300 seconds');
    }

    if (this.config.maxRetries < 1 || this.config.maxRetries > 10) {
      warnings.push('Max retries should be between 1-10');
    }

    if (this.config.maxRowsPerSheet < 1000 || this.config.maxRowsPerSheet > 1000000) {
      warnings.push('Max rows per sheet should be between 1K-1M');
    }

    return {
      isValid: errors.length === 0,
      errors,
      warnings
    };
  }

  /**
   * Get API credentials
   * @returns {Object} Credentials object
   */
  getCredentials() {
    return {
      sid: this.props.getProperty('IMPACT_SID'),
      token: this.props.getProperty('IMPACT_TOKEN')
    };
  }

  /**
   * Get spreadsheet ID (with fallback)
   * @returns {string} Spreadsheet ID
   */
  getSpreadsheetId() {
    return this.props.getProperty('IMPACT_SPREADSHEET_ID') || 
           '1QDOxgElRvl6EvI02JP4knupUd-jLW7D6LJN-VyLS3ZY';
  }
}

// ============================================================================
// LOGGING SYSTEM
// ============================================================================

/**
 * Advanced logging system with multiple levels and structured output
 */
class ImpactLogger {
  constructor(config) {
    this.config = config;
    this.logLevels = {
      DEBUG: 0,
      INFO: 1,
      WARN: 2,
      ERROR: 3,
      FATAL: 4
    };
    this.currentLevel = this.logLevels[config.get('logLevel', 'INFO')];
  }

  /**
   * Log a message with specified level
   * @param {string} level - Log level
   * @param {string} message - Log message
   * @param {Object} context - Additional context
   */
  log(level, message, context = {}) {
    const levelNum = this.logLevels[level];
    if (levelNum < this.currentLevel) return;

    const timestamp = new Date().toISOString();
    const logEntry = {
      timestamp,
      level,
      message,
      context,
      script: 'ImpactScript',
      version: '2.0.0'
    };

    // Console output
    console.log(`[${timestamp}] ${level}: ${message}`, context);

    // Detailed logging to Logger
    if (this.config.get('enableDetailedLogging', true)) {
      Logger.log(JSON.stringify(logEntry));
    }

    // Store in PropertiesService for persistence
    this.storeLogEntry(logEntry);
  }

  /**
   * Store log entry for persistence
   * @param {Object} logEntry - Log entry to store
   */
  storeLogEntry(logEntry) {
    try {
      const logs = JSON.parse(this.config.props.getProperty('IMPACT_LOGS') || '[]');
      logs.push(logEntry);
      
      // Keep only recent logs to prevent property size issues
      const retentionDays = this.config.get('logRetentionDays', 30);
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - retentionDays);
      
      const filteredLogs = logs.filter(log => 
        new Date(log.timestamp) > cutoffDate
      );
      
      // Limit total log entries to prevent property size issues
      const maxLogs = 1000;
      const finalLogs = filteredLogs.slice(-maxLogs);
      
      this.config.props.setProperty('IMPACT_LOGS', JSON.stringify(finalLogs));
    } catch (error) {
      console.error('Failed to store log entry:', error);
    }
  }

  debug(message, context = {}) { this.log('DEBUG', message, context); }
  info(message, context = {}) { this.log('INFO', message, context); }
  warn(message, context = {}) { this.log('WARN', message, context); }
  error(message, context = {}) { this.log('ERROR', message, context); }
  fatal(message, context = {}) { this.log('FATAL', message, context); }
}

// ============================================================================
// ERROR HANDLING & RETRY MECHANISMS
// ============================================================================

/**
 * Circuit breaker pattern implementation
 */
class CircuitBreaker {
  constructor(config) {
    this.config = config;
    this.failureCount = 0;
    this.lastFailureTime = null;
    this.state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN
  }

  /**
   * Check if request should be allowed
   * @returns {boolean} True if request is allowed
   */
  canExecute() {
    if (this.state === 'CLOSED') return true;
    
    if (this.state === 'OPEN') {
      const timeout = this.config.get('circuitBreakerTimeout', 300000);
      if (Date.now() - this.lastFailureTime > timeout) {
        this.state = 'HALF_OPEN';
        return true;
      }
      return false;
    }
    
    return true; // HALF_OPEN
  }

  /**
   * Record successful execution
   */
  recordSuccess() {
    this.failureCount = 0;
    this.state = 'CLOSED';
  }

  /**
   * Record failed execution
   */
  recordFailure() {
    this.failureCount++;
    this.lastFailureTime = Date.now();
    
    const threshold = this.config.get('circuitBreakerThreshold', 5);
    if (this.failureCount >= threshold) {
      this.state = 'OPEN';
    }
  }
}

/**
 * Retry mechanism with exponential backoff
 */
class RetryManager {
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;
  }

  /**
   * Execute function with retry logic
   * @param {Function} fn - Function to execute
   * @param {Array} args - Function arguments
   * @param {Object} options - Retry options
   * @returns {*} Function result
   */
  async executeWithRetry(fn, args = [], options = {}) {
    const maxRetries = options.maxRetries || this.config.get('maxRetries', 3);
    const baseDelay = options.baseDelay || this.config.get('retryDelay', 1000);
    const maxDelay = options.maxDelay || 30000;
    
    let lastError;
    
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        this.logger.debug(`Attempt ${attempt + 1}/${maxRetries + 1}`, { function: fn.name });
        const result = await fn.apply(null, args);
        return result;
      } catch (error) {
        lastError = error;
        
        if (attempt === maxRetries) {
          this.logger.error(`All retry attempts failed`, { 
            function: fn.name, 
            attempts: attempt + 1,
            error: error.message 
          });
          throw error;
        }
        
        const delay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
        this.logger.warn(`Retry in ${delay}ms`, { 
          function: fn.name, 
          attempt: attempt + 1,
          error: error.message 
        });
        
        await this.sleep(delay);
      }
    }
    
    throw lastError;
  }

  /**
   * Sleep for specified milliseconds
   * @param {number} ms - Milliseconds to sleep
   */
  sleep(ms) {
    return new Promise(resolve => Utilities.sleep(ms));
  }
}

// ============================================================================
// API CLIENT
// ============================================================================

/**
 * Impact.com API client with comprehensive error handling
 */
class ImpactAPIClient {
  constructor(config, logger, retryManager) {
    this.config = config;
    this.logger = logger;
    this.retryManager = retryManager;
    this.circuitBreaker = new CircuitBreaker(config);
    this.credentials = config.getCredentials();
  }

  /**
   * Make authenticated API request
   * @param {string} endpoint - API endpoint
   * @param {Object} options - Request options
   * @returns {Object} Response object
   */
  async makeRequest(endpoint, options = {}) {
    if (!this.circuitBreaker.canExecute()) {
      throw new Error('Circuit breaker is OPEN - too many failures');
    }

    const url = `${this.config.get('apiBaseUrl')}${endpoint}`;
    const basicAuth = Utilities.base64Encode(`${this.credentials.sid}:${this.credentials.token}`);
    
    const requestOptions = {
      method: options.method || 'GET',
      headers: {
        'Authorization': `Basic ${basicAuth}`,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        ...options.headers
      },
      muteHttpExceptions: true,
      ...options
    };

    try {
      this.logger.debug('Making API request', { url, method: requestOptions.method });
      
      const response = await this.retryManager.executeWithRetry(
        () => UrlFetchApp.fetch(url, requestOptions)
      );

      const statusCode = response.getResponseCode();
      const content = response.getContentText();

      if (statusCode >= 200 && statusCode < 300) {
        this.circuitBreaker.recordSuccess();
        this.logger.debug('API request successful', { url, statusCode });
        return {
          success: true,
          statusCode,
          data: content ? JSON.parse(content) : null,
          response
        };
      } else {
        this.circuitBreaker.recordFailure();
        const error = new Error(`API request failed: ${statusCode} - ${content}`);
        error.statusCode = statusCode;
        error.response = content;
        throw error;
      }
    } catch (error) {
      this.circuitBreaker.recordFailure();
      this.logger.error('API request failed', { url, error: error.message });
      throw error;
    }
  }

  /**
   * Discover all available reports
   * @returns {Array} Array of accessible reports
   */
  async discoverReports() {
    this.logger.info('Discovering available reports');
    
    const response = await this.makeRequest(`/Mediapartners/${this.credentials.sid}/Reports`);
    const reports = response.data.Reports || [];
    
    const accessible = reports.filter(r => r.ApiAccessible);
    const restricted = reports.filter(r => !r.ApiAccessible);
    
    this.logger.info('Report discovery complete', {
      total: reports.length,
      accessible: accessible.length,
      restricted: restricted.length
    });

    return accessible;
  }

  /**
   * Schedule report export
   * @param {string} reportId - Report ID to export
   * @param {Object} params - Export parameters
   * @returns {Object} Job information
   */
  async scheduleExport(reportId, params = {}) {
    this.logger.info('Scheduling report export', { reportId, params });
    
    const queryParams = new URLSearchParams({
      subid: 'mula',
      ...params
    });
    
    const response = await this.makeRequest(
      `/Mediapartners/${this.credentials.sid}/ReportExport/${reportId}?${queryParams}`
    );
    
    const jobId = response.data.QueuedUri.match(/\/Jobs\/([^/]+)/)[1];
    
    this.logger.info('Export scheduled successfully', { reportId, jobId });
    
    return {
      reportId,
      jobId,
      status: 'scheduled',
      scheduledAt: new Date()
    };
  }

  /**
   * Check job status
   * @param {string} jobId - Job ID to check
   * @returns {Object} Job status information
   */
  async checkJobStatus(jobId) {
    const response = await this.makeRequest(`/Mediapartners/${this.credentials.sid}/Jobs/${jobId}`);
    
    return {
      jobId,
      status: response.data.Status?.toLowerCase(),
      resultUri: response.data.ResultUri,
      error: response.data.Error,
      ...response.data
    };
  }

  /**
   * Download job result
   * @param {string} resultUri - Result URI
   * @returns {string} CSV data
   */
  async downloadResult(resultUri) {
    const url = `${this.config.get('apiBaseUrl')}${resultUri}`;
    const basicAuth = Utilities.base64Encode(`${this.credentials.sid}:${this.credentials.token}`);
    
    const response = await this.retryManager.executeWithRetry(() => 
      UrlFetchApp.fetch(url, {
        headers: { 'Authorization': `Basic ${basicAuth}` },
        muteHttpExceptions: true
      })
    );

    if (response.getResponseCode() !== 200) {
      throw new Error(`Download failed: ${response.getResponseCode()} - ${response.getContentText()}`);
    }

    return response.getContentText();
  }
}

// ============================================================================
// DATA PROCESSING
// ============================================================================

/**
 * Data processor with validation and sanitization
 */
class DataProcessor {
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;
  }

  /**
   * Process and validate CSV data
   * @param {string} csvData - Raw CSV data
   * @param {Object} options - Processing options
   * @returns {Object} Processed data
   */
  processCSVData(csvData, options = {}) {
    this.logger.info('Processing CSV data', { 
      dataLength: csvData.length,
      options 
    });

    try {
      const rows = Utilities.parseCsv(csvData);
      
      if (!rows || rows.length === 0) {
        throw new Error('No data found in CSV');
      }

      const headers = rows[0];
      const dataRows = rows.slice(1);
      
      // Validate data
      const validation = this.validateData(headers, dataRows);
      if (!validation.isValid) {
        this.logger.warn('Data validation issues found', validation);
      }

      // Sanitize data if enabled
      const sanitizedData = this.config.get('enableDataSanitization', true) 
        ? this.sanitizeData(rows)
        : rows;

      const result = {
        headers,
        data: sanitizedData,
        rowCount: dataRows.length,
        columnCount: headers.length,
        validation,
        processedAt: new Date()
      };

      this.logger.info('CSV processing complete', {
        rowCount: result.rowCount,
        columnCount: result.columnCount,
        validationIssues: validation.issues.length
      });

      return result;
    } catch (error) {
      this.logger.error('CSV processing failed', { error: error.message });
      throw error;
    }
  }

  /**
   * Validate data quality
   * @param {Array} headers - Column headers
   * @param {Array} dataRows - Data rows
   * @returns {Object} Validation result
   */
  validateData(headers, dataRows) {
    const issues = [];
    const stats = {
      totalRows: dataRows.length,
      emptyRows: 0,
      duplicateRows: 0,
      invalidData: 0
    };

    // Check for empty rows
    dataRows.forEach((row, index) => {
      if (row.every(cell => !cell || cell.toString().trim() === '')) {
        stats.emptyRows++;
        issues.push({
          type: 'empty_row',
          row: index + 2, // +2 for header and 0-based index
          message: 'Empty row detected'
        });
      }
    });

    // Check for duplicates
    const rowHashes = new Set();
    dataRows.forEach((row, index) => {
      const hash = row.join('|');
      if (rowHashes.has(hash)) {
        stats.duplicateRows++;
        issues.push({
          type: 'duplicate_row',
          row: index + 2,
          message: 'Duplicate row detected'
        });
      } else {
        rowHashes.add(hash);
      }
    });

    // Check for invalid data types
    dataRows.forEach((row, rowIndex) => {
      row.forEach((cell, colIndex) => {
        if (cell && typeof cell === 'string') {
          // Check for suspicious patterns
          if (cell.includes('<script>') || cell.includes('javascript:')) {
            stats.invalidData++;
            issues.push({
              type: 'invalid_data',
              row: rowIndex + 2,
              column: colIndex + 1,
              message: 'Potentially malicious content detected'
            });
          }
        }
      });
    });

    return {
      isValid: issues.length === 0,
      issues,
      stats
    };
  }

  /**
   * Sanitize data
   * @param {Array} rows - Data rows
   * @returns {Array} Sanitized data
   */
  sanitizeData(rows) {
    return rows.map(row => 
      row.map(cell => {
        if (typeof cell === 'string') {
          // Remove potentially dangerous content
          return cell
            .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
            .replace(/javascript:/gi, '')
            .replace(/on\w+\s*=/gi, '')
            .trim();
        }
        return cell;
      })
    );
  }
}

// ============================================================================
// SPREADSHEET MANAGER
// ============================================================================

/**
 * Spreadsheet management with advanced formatting and organization
 */
class SpreadsheetManager {
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;
    this.spreadsheetId = config.getSpreadsheetId();
  }

  /**
   * Get or create spreadsheet
   * @returns {Object} Spreadsheet object
   */
  getSpreadsheet() {
    try {
      return SpreadsheetApp.openById(this.spreadsheetId);
    } catch (error) {
      this.logger.error('Failed to open spreadsheet', { 
        spreadsheetId: this.spreadsheetId,
        error: error.message 
      });
      throw error;
    }
  }

  /**
   * Create or update report sheet
   * @param {string} reportId - Report ID
   * @param {Object} reportData - Processed report data
   * @param {Object} metadata - Report metadata
   * @returns {Object} Sheet information
   */
  createReportSheet(reportId, reportData, metadata = {}) {
    this.logger.info('Creating report sheet', { reportId });

    const spreadsheet = this.getSpreadsheet();
    const sheetName = this.generateSheetName(reportId, metadata.name);
    
    // Delete existing sheet if it exists
    const existingSheet = spreadsheet.getSheetByName(sheetName);
    if (existingSheet) {
      spreadsheet.deleteSheet(existingSheet);
    }

    // Create new sheet
    const sheet = spreadsheet.insertSheet(sheetName);
    
    // Import data
    const { headers, data } = reportData;
    const allData = [headers, ...data];
    
    sheet.getRange(1, 1, allData.length, headers.length).setValues(allData);
    
    // Apply formatting
    this.formatSheet(sheet, headers.length, allData.length);
    
    // Add metadata note
    this.addMetadataNote(sheet, reportId, metadata, reportData);
    
    this.logger.info('Report sheet created successfully', {
      reportId,
      sheetName,
      rowCount: data.length,
      columnCount: headers.length
    });

    return {
      sheetName,
      rowCount: data.length,
      columnCount: headers.length,
      sheet
    };
  }

  /**
   * Generate appropriate sheet name
   * @param {string} reportId - Report ID
   * @param {string} reportName - Report name
   * @returns {string} Sheet name
   */
  generateSheetName(reportId, reportName) {
    const maxLength = 30;
    let name = reportName || reportId;
    
    // Clean name
    name = name.replace(/[^\w\s-]/g, '').trim();
    
    // Truncate if too long
    if (name.length > maxLength) {
      name = name.substring(0, maxLength - 3) + '...';
    }
    
    return name || `Report_${reportId.substring(0, 10)}`;
  }

  /**
   * Format sheet with styling
   * @param {Object} sheet - Sheet object
   * @param {number} columnCount - Number of columns
   * @param {number} rowCount - Number of rows
   */
  formatSheet(sheet, columnCount, rowCount) {
    if (!this.config.get('enableAutoFormatting', true)) return;

    // Format header row
    const headerRange = sheet.getRange(1, 1, 1, columnCount);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#e8f5e8');
    headerRange.setFontColor('#2e7d32');
    sheet.setFrozenRows(1);

    // Auto-resize columns
    sheet.autoResizeColumns(1, columnCount);

    // Add borders
    const dataRange = sheet.getRange(1, 1, rowCount, columnCount);
    dataRange.setBorder(true, true, true, true, true, true);

    // Freeze first column if many columns
    if (columnCount > 5) {
      sheet.setFrozenColumns(1);
    }
  }

  /**
   * Add metadata note to sheet
   * @param {Object} sheet - Sheet object
   * @param {string} reportId - Report ID
   * @param {Object} metadata - Report metadata
   * @param {Object} reportData - Processed data
   */
  addMetadataNote(sheet, reportId, metadata, reportData) {
    if (!this.config.get('enableDataNotes', true)) return;

    const note = [
      `IMPACT.COM REPORT`,
      `Report ID: ${reportId}`,
      `Report Name: ${metadata.name || 'N/A'}`,
      `Data Rows: ${reportData.rowCount}`,
      `Columns: ${reportData.columnCount}`,
      `Headers: ${reportData.headers.join(', ')}`,
      `Processed: ${new Date().toLocaleString()}`,
      `Validation: ${reportData.validation.isValid ? 'PASSED' : 'ISSUES FOUND'}`,
      `Script Version: 2.0.0`
    ].join('\n');

    sheet.getRange('A1').setNote(note);
  }

  /**
   * Create discovery summary sheet
   * @param {Array} results - Discovery results
   * @param {Array} errors - Error results
   */
  createSummarySheet(results, errors) {
    this.logger.info('Creating discovery summary sheet');

    const spreadsheet = this.getSpreadsheet();
    
    // Clear existing summary sheet
    const existingSummary = spreadsheet.getSheetByName('DISCOVERY SUMMARY');
    if (existingSummary) {
      spreadsheet.deleteSheet(existingSummary);
    }

    const summarySheet = spreadsheet.insertSheet('DISCOVERY SUMMARY', 0);
    
    // Build summary data
    const summaryData = [
      ['Report ID', 'Report Name', 'Sheet Name', 'Rows', 'Columns', 'Status', 'Processed At']
    ];

    results.forEach(result => {
      summaryData.push([
        result.reportId,
        result.reportName || 'N/A',
        result.sheetName,
        result.rowCount,
        result.columnCount,
        'SUCCESS',
        result.processedAt?.toLocaleString() || 'N/A'
      ]);
    });

    errors.forEach(error => {
      summaryData.push([
        error.reportId,
        'N/A',
        'N/A',
        0,
        0,
        `ERROR: ${error.error}`,
        'N/A'
      ]);
    });

    // Add summary statistics
    summaryData.push([]);
    summaryData.push(['SUMMARY STATISTICS']);
    summaryData.push(['Total Reports', results.length + errors.length]);
    summaryData.push(['Successful', results.length]);
    summaryData.push(['Failed', errors.length]);
    summaryData.push(['Success Rate', `${((results.length / (results.length + errors.length)) * 100).toFixed(1)}%`]);
    summaryData.push(['Total Rows', results.reduce((sum, r) => sum + r.rowCount, 0)]);
    summaryData.push(['Generated', new Date().toLocaleString()]);

    // Write data
    summarySheet.getRange(1, 1, summaryData.length, summaryData[0].length).setValues(summaryData);
    
    // Format summary
    this.formatSummarySheet(summarySheet, summaryData[0].length, summaryData.length);
    
    this.logger.info('Summary sheet created successfully', {
      totalReports: results.length + errors.length,
      successful: results.length,
      failed: errors.length
    });
  }

  /**
   * Format summary sheet
   * @param {Object} sheet - Sheet object
   * @param {number} columnCount - Number of columns
   * @param {number} rowCount - Number of rows
   */
  formatSummarySheet(sheet, columnCount, rowCount) {
    // Format header
    const headerRange = sheet.getRange(1, 1, 1, columnCount);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#fff3e0');
    headerRange.setFontColor('#e65100');
    sheet.setFrozenRows(1);

    // Format summary section
    const summaryStartRow = sheet.getRange('A:A').getValues().findIndex(row => row[0] === 'SUMMARY STATISTICS') + 1;
    if (summaryStartRow > 0) {
      const summaryRange = sheet.getRange(summaryStartRow, 1, rowCount - summaryStartRow + 1, columnCount);
      summaryRange.setBackground('#f5f5f5');
      summaryRange.setFontWeight('bold');
    }

    // Auto-resize columns
    sheet.autoResizeColumns(1, columnCount);
  }
}

// ============================================================================
// MAIN ORCHESTRATOR
// ============================================================================

/**
 * Main orchestrator class that coordinates the entire discovery process
 */
class ImpactDiscoveryOrchestrator {
  constructor() {
    this.config = new ImpactConfig();
    this.logger = new ImpactLogger(this.config);
    this.retryManager = new RetryManager(this.config, this.logger);
    this.apiClient = new ImpactAPIClient(this.config, this.logger, this.retryManager);
    this.dataProcessor = new DataProcessor(this.config, this.logger);
    this.spreadsheetManager = new SpreadsheetManager(this.config, this.logger);
  }

  /**
   * Run complete discovery process
   * @returns {Object} Discovery results
   */
  async runCompleteDiscovery() {
    this.logger.info('Starting complete Impact.com data discovery', {
      version: '2.0.0',
      config: this.config.config
    });

    try {
      // Validate configuration
      const validation = this.config.validate();
      if (!validation.isValid) {
        throw new Error(`Configuration validation failed: ${validation.errors.join(', ')}`);
      }

      if (validation.warnings.length > 0) {
        this.logger.warn('Configuration warnings', { warnings: validation.warnings });
      }

      // Step 1: Discover reports
      this.logger.info('Phase 1: Discovering reports');
      const reports = await this.apiClient.discoverReports();
      
      if (reports.length === 0) {
        throw new Error('No accessible reports found');
      }

      // Step 2: Schedule exports
      this.logger.info('Phase 2: Scheduling exports');
      const exportResults = await this.scheduleAllExports(reports);
      
      if (exportResults.scheduled.length === 0) {
        throw new Error('No exports were successfully scheduled');
      }

      // Step 3: Wait and fetch data
      this.logger.info('Phase 3: Processing exports');
      const results = await this.processAllExports(exportResults.scheduled);

      // Step 4: Create summary
      this.logger.info('Phase 4: Creating summary');
      this.spreadsheetManager.createSummarySheet(results.successful, results.failed);

      this.logger.info('Discovery process completed successfully', {
        totalReports: reports.length,
        scheduled: exportResults.scheduled.length,
        successful: results.successful.length,
        failed: results.failed.length,
        spreadsheetUrl: `https://docs.google.com/spreadsheets/d/${this.config.getSpreadsheetId()}`
      });

      return results;

    } catch (error) {
      this.logger.fatal('Discovery process failed', { error: error.message });
      throw error;
    }
  }

  /**
   * Schedule exports for all reports
   * @param {Array} reports - Reports to export
   * @returns {Object} Export results
   */
  async scheduleAllExports(reports) {
    const scheduled = [];
    const errors = [];

    for (let i = 0; i < reports.length; i++) {
      const report = reports[i];
      this.logger.info(`Scheduling export ${i + 1}/${reports.length}`, { reportId: report.Id });

      try {
        const job = await this.apiClient.scheduleExport(report.Id);
        scheduled.push({
          ...job,
          reportName: report.Name,
          originalReport: report
        });

        // Rate limiting
        if (i < reports.length - 1) {
          await this.retryManager.sleep(this.config.get('requestDelay', 2000));
        }

      } catch (error) {
        this.logger.error('Failed to schedule export', { 
          reportId: report.Id, 
          error: error.message 
        });
        errors.push({
          reportId: report.Id,
          reportName: report.Name,
          error: error.message
        });
      }
    }

    return { scheduled, errors };
  }

  /**
   * Process all scheduled exports
   * @param {Array} scheduledJobs - Scheduled jobs
   * @returns {Object} Processing results
   */
  async processAllExports(scheduledJobs) {
    const successful = [];
    const failed = [];

    for (let i = 0; i < scheduledJobs.length; i++) {
      const job = scheduledJobs[i];
      this.logger.info(`Processing export ${i + 1}/${scheduledJobs.length}`, { 
        reportId: job.reportId,
        jobId: job.jobId 
      });

      try {
        const result = await this.processSingleExport(job);
        successful.push(result);
      } catch (error) {
        this.logger.error('Failed to process export', { 
          reportId: job.reportId,
          error: error.message 
        });
        failed.push({
          reportId: job.reportId,
          reportName: job.reportName,
          error: error.message
        });
      }
    }

    return { successful, failed };
  }

  /**
   * Process a single export job
   * @param {Object} job - Job information
   * @returns {Object} Processing result
   */
  async processSingleExport(job) {
    // Poll for completion
    const status = await this.pollJobCompletion(job.jobId);
    
    if (status.status !== 'completed') {
      throw new Error(`Job failed or timed out: ${status.status}`);
    }

    // Download data
    const csvData = await this.apiClient.downloadResult(status.resultUri);
    
    // Process data
    const processedData = this.dataProcessor.processCSVData(csvData);
    
    // Create spreadsheet sheet
    const sheetInfo = this.spreadsheetManager.createReportSheet(
      job.reportId,
      processedData,
      {
        name: job.reportName,
        jobId: job.jobId,
        scheduledAt: job.scheduledAt
      }
    );

    return {
      reportId: job.reportId,
      reportName: job.reportName,
      sheetName: sheetInfo.sheetName,
      rowCount: sheetInfo.rowCount,
      columnCount: sheetInfo.columnCount,
      processedAt: new Date(),
      jobId: job.jobId
    };
  }

  /**
   * Poll job until completion
   * @param {string} jobId - Job ID to poll
   * @returns {Object} Final job status
   */
  async pollJobCompletion(jobId) {
    const maxAttempts = this.config.get('maxPollingAttempts', 12);
    const initialDelay = this.config.get('initialPollingDelay', 10000);
    const maxDelay = this.config.get('maxPollingDelay', 60000);
    const multiplier = this.config.get('pollingMultiplier', 1.5);

    let delay = initialDelay;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      this.logger.debug(`Polling job ${jobId} (attempt ${attempt + 1}/${maxAttempts})`);
      
      const status = await this.apiClient.checkJobStatus(jobId);
      
      if (status.status === 'completed') {
        this.logger.info('Job completed successfully', { jobId });
        return status;
      }
      
      if (status.status === 'failed') {
        throw new Error(`Job failed: ${status.error || 'Unknown error'}`);
      }

      this.logger.debug(`Job status: ${status.status}, waiting ${delay}ms`);
      await this.retryManager.sleep(delay);
      
      // Exponential backoff
      delay = Math.min(delay * multiplier, maxDelay);
    }

    throw new Error(`Job polling timed out after ${maxAttempts} attempts`);
  }
}

// ============================================================================
// PUBLIC API FUNCTIONS
// ============================================================================

/**
 * Main entry point for complete discovery
 * @returns {Object} Discovery results
 */
function runCompleteDiscovery() {
  const orchestrator = new ImpactDiscoveryOrchestrator();
  return orchestrator.runCompleteDiscovery();
}

/**
 * Discover all available reports
 * @returns {Array} Accessible reports
 */
function discoverAllReports() {
  const config = new ImpactConfig();
  const logger = new ImpactLogger(config);
  const retryManager = new RetryManager(config, logger);
  const apiClient = new ImpactAPIClient(config, logger, retryManager);
  
  return apiClient.discoverReports();
}

/**
 * Export all accessible reports
 * @returns {Object} Export results
 */
function exportAllReportsRaw() {
  const config = new ImpactConfig();
  const logger = new ImpactLogger(config);
  const retryManager = new RetryManager(config, logger);
  const apiClient = new ImpactAPIClient(config, logger, retryManager);
  
  return apiClient.discoverReports().then(reports => {
    const scheduled = [];
    const errors = [];
    
    reports.forEach(async (report, index) => {
      try {
        const job = await apiClient.scheduleExport(report.Id);
        scheduled.push({ ...job, reportName: report.Name });
        
        if (index < reports.length - 1) {
          await retryManager.sleep(config.get('requestDelay', 2000));
        }
      } catch (error) {
        errors.push({ reportId: report.Id, error: error.message });
      }
    });
    
    return { scheduled, errors };
  });
}

/**
 * Fetch all discovery data
 * @returns {Object} Discovery results
 */
function fetchAllDiscoveryData() {
  const orchestrator = new ImpactDiscoveryOrchestrator();
  return orchestrator.runCompleteDiscovery();
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Get system health status
 * @returns {Object} Health status
 */
function getSystemHealth() {
  const config = new ImpactConfig();
  const logger = new ImpactLogger(config);
  
  const validation = config.validate();
  const logs = JSON.parse(config.props.getProperty('IMPACT_LOGS') || '[]');
  const recentErrors = logs.filter(log => 
    log.level === 'ERROR' && 
    new Date(log.timestamp) > new Date(Date.now() - 24 * 60 * 60 * 1000)
  );
  
  return {
    status: validation.isValid ? 'HEALTHY' : 'UNHEALTHY',
    configValid: validation.isValid,
    configErrors: validation.errors,
    configWarnings: validation.warnings,
    recentErrors: recentErrors.length,
    lastError: recentErrors.length > 0 ? recentErrors[recentErrors.length - 1] : null,
    timestamp: new Date()
  };
}

/**
 * Clear all logs
 */
function clearLogs() {
  const config = new ImpactConfig();
  config.props.deleteProperty('IMPACT_LOGS');
  console.log('All logs cleared');
}

/**
 * Get configuration
 * @returns {Object} Current configuration
 */
function getConfiguration() {
  const config = new ImpactConfig();
  return config.config;
}

/**
 * Update configuration
 * @param {Object} updates - Configuration updates
 */
function updateConfiguration(updates) {
  const config = new ImpactConfig();
  
  // Update multiple values at once
  for (const [key, value] of Object.entries(updates)) {
    config.config[key] = value;
  }
  
  // Save all changes at once
  config.saveConfiguration();
  
  console.log('Configuration updated successfully');
}

// ============================================================================
// CUSTOM DATE RANGE EXPORTS
// ============================================================================

/**
 * Schedule report export with custom date range
 * @param {string} reportId - Report ID to export
 * @param {string} startDate - Start date (YYYY-MM-DD format)
 * @param {string} endDate - End date (YYYY-MM-DD format)
 * @param {Object} params - Additional export parameters
 * @returns {Object} Job information
 */
async function scheduleExportWithDateRange(reportId, startDate, endDate, params = {}) {
  const config = new ImpactConfig();
  const logger = new ImpactLogger(config);
  const retryManager = new RetryManager(config, logger);
  const apiClient = new ImpactAPIClient(config, logger, retryManager);
  
  logger.info('Scheduling report export with date range', { 
    reportId, 
    startDate, 
    endDate, 
    params 
  });
  
  // Build query string manually since URLSearchParams is not available in Google Apps Script
  const queryParams = {
    subid: 'mula',
    startDate: startDate,
    endDate: endDate,
    ...params
  };
  
  const queryString = Object.entries(queryParams)
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
    .join('&');
  
  const response = await apiClient.makeRequest(
    `/Mediapartners/${apiClient.credentials.sid}/ReportExport/${reportId}?${queryString}`
  );
  
  const jobId = response.data.QueuedUri.match(/\/Jobs\/([^/]+)/)[1];
  
  logger.info('Export scheduled successfully', { reportId, jobId, startDate, endDate });
  
  return {
    reportId,
    jobId,
    status: 'scheduled',
    scheduledAt: new Date(),
    startDate,
    endDate
  };
}

/**
 * Export all reports for July 1st to October 7th, 2024
 * @returns {Object} Export results
 */
async function exportAllReportsJulyToOctober() {
  const config = new ImpactConfig();
  const logger = new ImpactLogger(config);
  const retryManager = new RetryManager(config, logger);
  const apiClient = new ImpactAPIClient(config, logger, retryManager);
  
  // Date range: July 1, 2024 to October 7, 2024
  const startDate = '2024-07-01';
  const endDate = '2024-10-07';
  
  logger.info('Starting export for custom date range', { startDate, endDate });
  
  try {
    // Discover all available reports
    const reports = await apiClient.discoverReports();
    logger.info(`Found ${reports.length} accessible reports`);
    
    const scheduled = [];
    const errors = [];
    
    for (let i = 0; i < reports.length; i++) {
      const report = reports[i];
      logger.info(`Scheduling export ${i + 1}/${reports.length}`, { 
        reportId: report.Id,
        reportName: report.Name 
      });
      
      try {
        const job = await scheduleExportWithDateRange(report.Id, startDate, endDate);
        scheduled.push({
          ...job,
          reportName: report.Name,
          originalReport: report
        });
        
        logger.info(`✅ Scheduled ${i + 1}/${reports.length}`, { 
          reportId: report.Id, 
          jobId: job.jobId 
        });
        
        // Rate limiting
        if (i < reports.length - 1) {
          await retryManager.sleep(config.get('requestDelay', 2000));
        }
        
      } catch (error) {
        logger.error('Failed to schedule export', { 
          reportId: report.Id, 
          error: error.message 
        });
        errors.push({
          reportId: report.Id,
          reportName: report.Name,
          error: error.message
        });
      }
    }
    
    logger.info('Export scheduling complete', {
      totalReports: reports.length,
      scheduled: scheduled.length,
      errors: errors.length,
      dateRange: `${startDate} to ${endDate}`
    });
    
    return { scheduled, errors, dateRange: { startDate, endDate } };
    
  } catch (error) {
    logger.fatal('Export scheduling failed', { error: error.message });
    throw error;
  }
}

/**
 * Export specific reports for July 1st to October 7th, 2024
 * @param {Array} reportIds - Array of report IDs to export
 * @returns {Object} Export results
 */
async function exportSpecificReportsJulyToOctober(reportIds) {
  const config = new ImpactConfig();
  const logger = new ImpactLogger(config);
  const retryManager = new RetryManager(config, logger);
  const apiClient = new ImpactAPIClient(config, logger, retryManager);
  
  // Date range: July 1, 2024 to October 7, 2024
  const startDate = '2024-07-01';
  const endDate = '2024-10-07';
  
  logger.info('Starting export for specific reports with custom date range', { 
    reportIds, 
    startDate, 
    endDate 
  });
  
  const scheduled = [];
  const errors = [];
  
  for (let i = 0; i < reportIds.length; i++) {
    const reportId = reportIds[i];
    logger.info(`Scheduling export ${i + 1}/${reportIds.length}`, { reportId });
    
    try {
      const job = await scheduleExportWithDateRange(reportId, startDate, endDate);
      scheduled.push({
        ...job,
        reportId
      });
      
      logger.info(`✅ Scheduled ${i + 1}/${reportIds.length}`, { 
        reportId, 
        jobId: job.jobId 
      });
      
      // Rate limiting
      if (i < reportIds.length - 1) {
        await retryManager.sleep(config.get('requestDelay', 2000));
      }
      
    } catch (error) {
      logger.error('Failed to schedule export', { 
        reportId, 
        error: error.message 
      });
      errors.push({
        reportId,
        error: error.message
      });
    }
  }
  
  logger.info('Specific reports export scheduling complete', {
    totalReports: reportIds.length,
    scheduled: scheduled.length,
    errors: errors.length,
    dateRange: `${startDate} to ${endDate}`
  });
  
  return { scheduled, errors, dateRange: { startDate, endDate } };
}

// ============================================================================
// UTILITY FUNCTIONS FOR DATE RANGE CHECKING
// ============================================================================

/**
 * Check date range of a specific sheet
 * @param {string} sheetName - Name of the sheet to check
 */
function checkDateRange(sheetName) {
  const spreadsheet = SpreadsheetApp.openById('1QDOxgElRvl6EvI02JP4knupUd-jLW7D6LJN-VyLS3ZY');
  const sheet = spreadsheet.getSheetByName(sheetName);
  
  if (!sheet) {
    console.log('Sheet not found');
    return;
  }
  
  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  
  // Find date column
  const dateColumnIndex = headers.findIndex(header => 
    header.toString().toLowerCase().includes('date') || 
    header.toString().toLowerCase().includes('period')
  );
  
  if (dateColumnIndex === -1) {
    console.log('No date column found');
    console.log('Headers:', headers);
    return;
  }
  
  const dates = data.slice(1).map(row => row[dateColumnIndex]).filter(date => date);
  const minDate = new Date(Math.min(...dates.map(d => new Date(d))));
  const maxDate = new Date(Math.max(...dates.map(d => new Date(d))));
  
  console.log(`Date range: ${minDate.toDateString()} to ${maxDate.toDateString()}`);
  console.log(`Total records: ${dates.length}`);
}

/**
 * Check date ranges of multiple sheets
 * @param {Array} sheetNames - Array of sheet names to check
 */
function checkDateRanges(sheetNames) {
  const spreadsheet = SpreadsheetApp.openById('1QDOxgElRvl6EvI02JP4knupUd-jLW7D6LJN-VyLS3ZY');
  
  sheetNames.forEach(sheetName => {
    const sheet = spreadsheet.getSheetByName(sheetName);
    
    if (!sheet) {
      console.log(`${sheetName}: Sheet not found`);
      return;
    }
    
    const data = sheet.getDataRange().getValues();
    const headers = data[0];
    
    // Find date column
    const dateColumnIndex = headers.findIndex(header => 
      header.toString().toLowerCase().includes('date') || 
      header.toString().toLowerCase().includes('period') ||
      header.toString().toLowerCase().includes('time')
    );
    
    if (dateColumnIndex === -1) {
      console.log(`${sheetName}: No date column found`);
      console.log(`Headers: ${headers.join(', ')}`);
      return;
    }
    
    const dates = data.slice(1).map(row => row[dateColumnIndex]).filter(date => date);
    
    if (dates.length === 0) {
      console.log(`${sheetName}: No date data found`);
      return;
    }
    
    const minDate = new Date(Math.min(...dates.map(d => new Date(d))));
    const maxDate = new Date(Math.max(...dates.map(d => new Date(d))));
    
    console.log(`${sheetName}:`);
    console.log(`  Date range: ${minDate.toDateString()} to ${maxDate.toDateString()}`);
    console.log(`  Total records: ${dates.length}`);
    console.log('');
  });
}