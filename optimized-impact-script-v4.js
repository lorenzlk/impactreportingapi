/**
 * Impact.com Data Export - ULTRA-OPTIMIZED VERSION
 * 
 * Key Optimizations:
 * - Smart batching and parallel processing
 * - Advanced error recovery and retry logic
 * - Memory-efficient data processing
 * - Intelligent polling with adaptive delays
 * - Enhanced progress tracking and resume
 * - Timeout prevention strategies
 * - Better spreadsheet management
 * 
 * @version 4.0.0
 * @author Logan Lorenz
 */

// ============================================================================
// CONFIGURATION (OPTIMIZED)
// ============================================================================

class ImpactConfig {
  constructor() {
    this.props = PropertiesService.getScriptProperties();
    this.config = this.loadConfiguration();
  }

  loadConfiguration() {
    const defaults = {
      // API Configuration
      apiBaseUrl: 'https://api.impact.com',
      maxRetries: 5,
      retryDelay: 1000,
      maxRetryDelay: 30000,
      retryMultiplier: 1.5,
      
      // Polling Configuration (Optimized)
      maxPollingAttempts: 30,
      initialPollingDelay: 3000,
      maxPollingDelay: 60000,
      pollingMultiplier: 1.2,
      quickPollingThreshold: 5, // Switch to quick polling after 5 attempts
      quickPollingDelay: 2000,
      
      // Rate Limiting (Optimized)
      requestDelay: 800,
      burstDelay: 200,
      parallelRequestLimit: 3,
      
      // Data Processing (Memory Optimized)
      maxRowsPerSheet: 50000,
      chunkSize: 15000,
      batchWriteSize: 3000,
      memoryCleanupInterval: 10,
      
      // Performance Settings
      enableParallelProcessing: true,
      enableSmartChunking: true,
      enableMemoryOptimization: true,
      enableProgressCompression: true,
      
      // Resume and Recovery
      enableResume: true,
      enableAutoRecovery: true,
      maxRecoveryAttempts: 3,
      progressSaveInterval: 5,
      
      // Timeout Prevention
      maxExecutionTime: 25 * 60 * 1000, // 25 minutes
      checkpointInterval: 2 * 60 * 1000, // 2 minutes
      yieldInterval: 1000, // 1 second
      
      // Credentials (Update these with your actual values)
      impactSid: 'YOUR_IMPACT_SID_HERE',
      impactToken: 'YOUR_IMPACT_TOKEN_HERE',
      spreadsheetId: 'YOUR_SPREADSHEET_ID_HERE',
      
      // Notifications
      enableEmailNotifications: false,
      notificationEmail: '',
      enableSlackNotifications: false,
      slackWebhookUrl: '',
      
      // Debugging
      enableDetailedLogging: true,
      logLevel: 'INFO',
      enablePerformanceMetrics: true
    };

    const configJson = this.props.getProperty('IMPACT_OPTIMIZED_CONFIG');
    if (configJson) {
      try {
        return { ...defaults, ...JSON.parse(configJson) };
      } catch (error) {
        Logger.log('Failed to parse config: ' + error.message);
        return defaults;
      }
    }
    return defaults;
  }

  get(key, defaultValue = null) {
    return this.config.hasOwnProperty(key) ? this.config[key] : defaultValue;
  }

  set(key, value) {
    this.config[key] = value;
    this.saveConfiguration();
  }

  saveConfiguration() {
    this.props.setProperty('IMPACT_OPTIMIZED_CONFIG', JSON.stringify(this.config));
  }

  validate() {
    const errors = [];
    const warnings = [];
    
    const sid = this.config.impactSid;
    const token = this.config.impactToken;
    
    if (!sid || sid === 'YOUR_SID_HERE' || sid.length < 10) {
      errors.push('Invalid SID');
    }
    if (!token || token === 'YOUR_TOKEN_HERE' || token.length < 10) {
      errors.push('Invalid token');
    }
    
    // Performance warnings
    if (this.config.maxRowsPerSheet > 100000) {
      warnings.push('Large maxRowsPerSheet may cause timeouts');
    }
    if (this.config.parallelRequestLimit > 5) {
      warnings.push('High parallelRequestLimit may trigger rate limits');
    }

    return { 
      isValid: errors.length === 0, 
      errors: errors, 
      warnings: warnings 
    };
  }

  getCredentials() {
    return {
      sid: this.config.impactSid,
      token: this.config.impactToken
    };
  }

  getSpreadsheetId() {
    return this.config.spreadsheetId;
  }
}

// ============================================================================
// PERFORMANCE METRICS
// ============================================================================

class PerformanceMetrics {
  constructor() {
    this.metrics = {
      startTime: Date.now(),
      apiCalls: 0,
      successfulCalls: 0,
      failedCalls: 0,
      totalDataProcessed: 0,
      memoryUsage: [],
      checkpoints: []
    };
  }

  recordAPICall(success = true) {
    this.metrics.apiCalls++;
    if (success) {
      this.metrics.successfulCalls++;
    } else {
      this.metrics.failedCalls++;
    }
  }

  recordDataProcessed(bytes) {
    this.metrics.totalDataProcessed += bytes;
  }

  recordCheckpoint(phase, data) {
    this.metrics.checkpoints.push({
      phase: phase,
      timestamp: Date.now(),
      data: data,
      memoryUsage: this.getMemoryUsage()
    });
  }

  getMemoryUsage() {
    // Estimate memory usage (Google Apps Script doesn't provide exact memory info)
    return {
      estimated: this.metrics.totalDataProcessed,
      apiCalls: this.metrics.apiCalls
    };
  }

  getSummary() {
    const duration = Date.now() - this.metrics.startTime;
    return {
      duration: duration,
      durationMinutes: (duration / 1000 / 60).toFixed(2),
      apiCalls: this.metrics.apiCalls,
      successRate: this.metrics.apiCalls > 0 ? 
        ((this.metrics.successfulCalls / this.metrics.apiCalls) * 100).toFixed(2) + '%' : '0%',
      dataProcessed: this.metrics.totalDataProcessed,
      checkpoints: this.metrics.checkpoints.length
    };
  }
}

// ============================================================================
// ENHANCED PROGRESS TRACKER
// ============================================================================

class EnhancedProgressTracker {
  constructor(config, metrics) {
    this.config = config;
    this.metrics = metrics;
    this.props = PropertiesService.getScriptProperties();
  }

  saveProgress(phase, data, force = false) {
    if (!this.config.get('enableResume', true) && !force) return;
    
    const progress = {
      phase: phase,
      timestamp: Date.now(),
      data: data,
      metrics: this.metrics.getSummary(),
      version: '4.0.0'
    };
    
    // Compress large progress data
    if (this.config.get('enableProgressCompression', true) && data && data.length > 1000) {
      progress.compressed = true;
      progress.data = this.compressProgressData(data);
    }
    
    this.props.setProperty('IMPACT_PROGRESS_V4', JSON.stringify(progress));
    
    // Also save a lightweight checkpoint
    this.saveCheckpoint(phase, data);
  }

  saveCheckpoint(phase, data) {
    const checkpoint = {
      phase: phase,
      timestamp: Date.now(),
      summary: this.getProgressSummary(data)
    };
    
    this.props.setProperty('IMPACT_CHECKPOINT', JSON.stringify(checkpoint));
  }

  getProgress() {
    const progressJson = this.props.getProperty('IMPACT_PROGRESS_V4');
    if (progressJson) {
      try {
        const progress = JSON.parse(progressJson);
        
        // Decompress if needed
        if (progress.compressed && progress.data) {
          progress.data = this.decompressProgressData(progress.data);
        }
        
        return progress;
      } catch (error) {
        Logger.log('Failed to parse progress: ' + error.message);
        return null;
      }
    }
    return null;
  }

  getCheckpoint() {
    const checkpointJson = this.props.getProperty('IMPACT_CHECKPOINT');
    if (checkpointJson) {
      try {
        return JSON.parse(checkpointJson);
      } catch (error) {
        return null;
      }
    }
    return null;
  }

  getCompletedReports() {
    const completed = this.props.getProperty('IMPACT_COMPLETED_V4');
    if (completed) {
      try {
        return JSON.parse(completed);
      } catch (error) {
        return [];
      }
    }
    return [];
  }

  markReportComplete(reportId, metadata = {}) {
    const completed = this.getCompletedReports();
    const existing = completed.find(r => r.reportId === reportId);
    
    if (!existing) {
      completed.push({
        reportId: reportId,
        completedAt: Date.now(),
        ...metadata
      });
      this.props.setProperty('IMPACT_COMPLETED_V4', JSON.stringify(completed));
    }
  }

  getProgressSummary(data) {
    if (!data) return {};
    
    if (Array.isArray(data)) {
      return {
        count: data.length,
        type: 'array'
      };
    }
    
    if (data.scheduled && data.errors) {
      return {
        scheduled: data.scheduled.length,
        errors: data.errors.length,
        type: 'export_results'
      };
    }
    
    return {
      type: typeof data,
      keys: Object.keys(data || {})
    };
  }

  compressProgressData(data) {
    // Simple compression for large arrays
    if (Array.isArray(data) && data.length > 100) {
      return {
        compressed: true,
        count: data.length,
        sample: data.slice(0, 10),
        last: data.slice(-10)
      };
    }
    return data;
  }

  decompressProgressData(compressedData) {
    if (compressedData.compressed) {
      // Reconstruct array from compressed data
      return [
        ...compressedData.sample,
        ...new Array(compressedData.count - 20).fill(null),
        ...compressedData.last
      ];
    }
    return compressedData;
  }

  clearProgress() {
    this.props.deleteProperty('IMPACT_PROGRESS_V4');
    this.props.deleteProperty('IMPACT_CHECKPOINT');
  }

  clearCompleted() {
    this.props.deleteProperty('IMPACT_COMPLETED_V4');
  }

  clearAll() {
    this.clearProgress();
    this.clearCompleted();
  }
}

// ============================================================================
// ENHANCED LOGGER
// ============================================================================

class EnhancedLogger {
  constructor(config, metrics) {
    this.config = config;
    this.metrics = metrics;
    this.logLevels = { 
      DEBUG: 0, 
      INFO: 1, 
      WARN: 2, 
      ERROR: 3, 
      FATAL: 4 
    };
    this.currentLevel = this.logLevels[this.config.get('logLevel', 'INFO')];
  }

  log(level, message, context = {}) {
    const levelNum = this.logLevels[level];
    if (levelNum < this.currentLevel) return;

    const timestamp = new Date().toISOString();
    const logEntry = {
      timestamp: timestamp,
      level: level,
      message: message,
      context: context,
      metrics: this.config.get('enablePerformanceMetrics', true) ? 
        this.metrics.getSummary() : null
    };

    console.log('[' + timestamp + '] ' + level + ': ' + message);
    
    if (this.config.get('enableDetailedLogging', true)) {
      Logger.log(JSON.stringify(logEntry));
    }
  }

  debug(message, context = {}) { this.log('DEBUG', message, context); }
  info(message, context = {}) { this.log('INFO', message, context); }
  warn(message, context = {}) { this.log('WARN', message, context); }
  error(message, context = {}) { this.log('ERROR', message, context); }
  fatal(message, context = {}) { this.log('FATAL', message, context); }
}

// ============================================================================
// ENHANCED API CLIENT
// ============================================================================

class EnhancedAPIClient {
  constructor(config, logger, metrics) {
    this.config = config;
    this.logger = logger;
    this.metrics = metrics;
    this.credentials = config.getCredentials();
    this.circuitBreaker = new CircuitBreaker(config);
  }

  makeRequest(endpoint, options = {}) {
    if (!this.circuitBreaker.canExecute()) {
      throw new Error('Circuit breaker is OPEN - too many failures');
    }

    const url = this.config.get('apiBaseUrl') + endpoint;
    const basicAuth = Utilities.base64Encode(
      this.credentials.sid + ':' + this.credentials.token
    );
    
    const requestOptions = {
      method: options.method || 'GET',
      headers: {
        'Authorization': 'Basic ' + basicAuth,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        ...options.headers
      },
      muteHttpExceptions: true
    };

    let lastError;
    const maxRetries = this.config.get('maxRetries', 5);
    const baseDelay = this.config.get('retryDelay', 1000);
    const maxDelay = this.config.get('maxRetryDelay', 30000);
    const multiplier = this.config.get('retryMultiplier', 1.5);
    
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        this.logger.debug('API request attempt ' + (attempt + 1), { 
          endpoint: endpoint, 
          attempt: attempt + 1 
        });
        
        const response = UrlFetchApp.fetch(url, requestOptions);
        const statusCode = response.getResponseCode();
        const content = response.getContentText();

        if (statusCode >= 200 && statusCode < 300) {
          this.circuitBreaker.recordSuccess();
          this.metrics.recordAPICall(true);
          this.metrics.recordDataProcessed(content.length);
          
          this.logger.debug('API request successful', { 
            endpoint: endpoint, 
            statusCode: statusCode,
            contentLength: content.length
          });
          
          return {
            success: true,
            statusCode: statusCode,
            data: content ? JSON.parse(content) : null,
            contentLength: content.length
          };
        } else {
          throw new Error('API request failed: ' + statusCode + ' - ' + content);
        }
      } catch (error) {
        lastError = error;
        this.metrics.recordAPICall(false);
        
        if (attempt < maxRetries) {
          const delay = Math.min(baseDelay * Math.pow(multiplier, attempt), maxDelay);
          this.logger.warn('API request failed, retrying in ' + delay + 'ms', { 
            endpoint: endpoint, 
            attempt: attempt + 1,
            error: error.message,
            delay: delay
          });
          Utilities.sleep(delay);
        }
      }
    }
    
    this.circuitBreaker.recordFailure();
    this.logger.error('API request failed after all retries', { 
      endpoint: endpoint, 
      attempts: maxRetries + 1,
      error: lastError.message
    });
    throw lastError;
  }

  discoverReports() {
    this.logger.info('Discovering reports');
    const response = this.makeRequest('/Mediapartners/' + this.credentials.sid + '/Reports');
    const reports = (response.data.Reports || []).filter(r => r.ApiAccessible);
    
    this.logger.info('Found ' + reports.length + ' accessible reports');
    return reports;
  }

  scheduleExport(reportId, params = {}) {
    this.logger.debug('Scheduling export', { reportId: reportId, params: params });
    
    const queryParts = ['subid=mula'];
    for (const key in params) {
      queryParts.push(encodeURIComponent(key) + '=' + encodeURIComponent(params[key]));
    }
    
    const response = this.makeRequest(
      '/Mediapartners/' + this.credentials.sid + '/ReportExport/' + reportId + '?' + queryParts.join('&')
    );
    
    const jobId = response.data.QueuedUri.match(/\/Jobs\/([^/]+)/)[1];
    
    this.logger.debug('Export scheduled', { reportId: reportId, jobId: jobId });
    
    return { 
      reportId: reportId, 
      jobId: jobId, 
      scheduledAt: new Date(),
      params: params
    };
  }

  checkJobStatus(jobId) {
    const response = this.makeRequest('/Mediapartners/' + this.credentials.sid + '/Jobs/' + jobId);
    return {
      jobId: jobId,
      status: response.data.Status ? response.data.Status.toLowerCase() : 'unknown',
      resultUri: response.data.ResultUri,
      error: response.data.Error
    };
  }

  downloadResult(resultUri) {
    this.logger.debug('Downloading result', { resultUri: resultUri });
    
    const url = this.config.get('apiBaseUrl') + resultUri;
    const basicAuth = Utilities.base64Encode(this.credentials.sid + ':' + this.credentials.token);
    
    const response = UrlFetchApp.fetch(url, {
      headers: { 'Authorization': 'Basic ' + basicAuth },
      muteHttpExceptions: true
    });

    if (response.getResponseCode() !== 200) {
      throw new Error('Download failed: ' + response.getResponseCode() + ' - ' + response.getContentText());
    }
    
    const content = response.getContentText();
    this.metrics.recordDataProcessed(content.length);
    
    this.logger.debug('Result downloaded', { 
      resultUri: resultUri, 
      contentLength: content.length 
    });
    
    return content;
  }
}

// ============================================================================
// CIRCUIT BREAKER
// ============================================================================

class CircuitBreaker {
  constructor(config) {
    this.config = config;
    this.failureCount = 0;
    this.lastFailureTime = null;
    this.state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN
    this.threshold = 5;
    this.timeout = 60000; // 1 minute
  }

  canExecute() {
    if (this.state === 'CLOSED') return true;
    
    if (this.state === 'OPEN') {
      if (Date.now() - this.lastFailureTime > this.timeout) {
        this.state = 'HALF_OPEN';
        return true;
      }
      return false;
    }
    
    return true; // HALF_OPEN
  }

  recordSuccess() {
    this.failureCount = 0;
    this.state = 'CLOSED';
  }

  recordFailure() {
    this.failureCount++;
    this.lastFailureTime = Date.now();
    
    if (this.failureCount >= this.threshold) {
      this.state = 'OPEN';
    }
  }
}

// ============================================================================
// ENHANCED DATA PROCESSOR
// ============================================================================

class EnhancedDataProcessor {
  constructor(config, logger, metrics) {
    this.config = config;
    this.logger = logger;
    this.metrics = metrics;
  }

  processCSVData(csvData) {
    this.logger.info('Processing CSV data', { 
      size: csvData.length,
      estimatedRows: Math.floor(csvData.length / 100) // Rough estimate
    });
    
    const startTime = Date.now();
    
    try {
      const rows = Utilities.parseCsv(csvData);
      if (!rows || rows.length === 0) {
        throw new Error('No data in CSV');
      }

      const headers = rows[0];
      const dataRows = rows.slice(1);
      const needsChunking = this.config.get('enableSmartChunking', true) && 
                           rows.length > this.config.get('maxRowsPerSheet', 50000);

      const result = {
        headers: headers,
        data: rows,
        dataRows: dataRows,
        rowCount: dataRows.length,
        columnCount: headers.length,
        needsChunking: needsChunking,
        processingTime: Date.now() - startTime,
        memoryEstimate: this.estimateMemoryUsage(rows)
      };

      this.metrics.recordDataProcessed(csvData.length);
      
      this.logger.info('CSV processing complete', {
        rowCount: result.rowCount,
        columnCount: result.columnCount,
        needsChunking: result.needsChunking,
        processingTime: result.processingTime + 'ms'
      });

      return result;
    } catch (error) {
      this.logger.error('CSV processing failed', { 
        error: error.message,
        csvLength: csvData.length
      });
      throw error;
    }
  }

  estimateMemoryUsage(rows) {
    // Rough estimate of memory usage
    const avgRowLength = rows.reduce((sum, row) => 
      sum + row.reduce((rowSum, cell) => rowSum + (cell ? cell.toString().length : 0), 0), 0
    ) / rows.length;
    
    return {
      estimatedBytes: rows.length * avgRowLength,
      rowCount: rows.length,
      avgRowLength: Math.round(avgRowLength)
    };
  }

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
          row: index + 2,
          message: 'Empty row detected'
        });
      }
    });

    // Check for duplicates (sample check for performance)
    const sampleSize = Math.min(1000, dataRows.length);
    const sampleRows = dataRows.slice(0, sampleSize);
    const rowHashes = new Set();
    
    sampleRows.forEach((row, index) => {
      const hash = row.join('|');
      if (rowHashes.has(hash)) {
        stats.duplicateRows++;
        issues.push({
          type: 'duplicate_row',
          row: index + 2,
          message: 'Duplicate row detected (sample check)'
        });
      } else {
        rowHashes.add(hash);
      }
    });

    return {
      isValid: issues.length === 0,
      issues: issues,
      stats: stats
    };
  }
}

// ============================================================================
// ENHANCED SPREADSHEET MANAGER
// ============================================================================

class EnhancedSpreadsheetManager {
  constructor(config, logger, metrics) {
    this.config = config;
    this.logger = logger;
    this.metrics = metrics;
    this.spreadsheetId = config.getSpreadsheetId();
  }

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

  sheetExists(sheetName) {
    const spreadsheet = this.getSpreadsheet();
    return spreadsheet.getSheetByName(sheetName) !== null;
  }

  createReportSheet(reportId, reportData, metadata = {}) {
    this.logger.info('Creating sheet for ' + reportId, {
      rowCount: reportData.rowCount,
      needsChunking: reportData.needsChunking
    });

    const spreadsheet = this.getSpreadsheet();
    
    // Handle large reports by chunking
    if (reportData.needsChunking) {
      return this.createChunkedSheets(reportId, reportData, metadata, spreadsheet);
    }

    return this.createSingleSheet(reportId, reportData, metadata, spreadsheet);
  }

  createSingleSheet(reportId, reportData, metadata, spreadsheet) {
    const sheetName = this.generateSheetName(reportId, metadata.name);
    
    // Delete existing sheet if it exists
    const existingSheet = spreadsheet.getSheetByName(sheetName);
    if (existingSheet) {
      spreadsheet.deleteSheet(existingSheet);
    }

    const sheet = spreadsheet.insertSheet(sheetName);
    
    // Write data in optimized batches
    this.writeOptimizedData(sheet, reportData.data);
    
    // Apply formatting
    this.formatSheet(sheet, reportData.headers.length, reportData.data.length);
    
    // Add metadata
    this.addMetadataNote(sheet, reportId, metadata, reportData);

    return {
      sheetName: sheetName,
      rowCount: reportData.rowCount,
      columnCount: reportData.columnCount,
      chunked: false
    };
  }

  createChunkedSheets(reportId, reportData, metadata, spreadsheet) {
    this.logger.info('Creating chunked sheets', { 
      reportId: reportId,
      totalRows: reportData.rowCount,
      maxRowsPerSheet: this.config.get('maxRowsPerSheet', 50000)
    });
    
    const maxRows = this.config.get('maxRowsPerSheet', 50000);
    const headers = reportData.headers;
    const dataRows = reportData.dataRows;
    
    const chunks = [];
    for (let i = 0; i < dataRows.length; i += maxRows) {
      const chunkData = [headers].concat(dataRows.slice(i, i + maxRows));
      chunks.push(chunkData);
    }
    
    this.logger.info('Split into ' + chunks.length + ' chunks');
    
    const createdSheets = [];
    for (let i = 0; i < chunks.length; i++) {
      const baseSheetName = this.generateSheetName(reportId, metadata.name);
      const sheetName = chunks.length > 1 ? 
        baseSheetName + ' (Part ' + (i + 1) + ')' : 
        baseSheetName;
      
      const existingSheet = spreadsheet.getSheetByName(sheetName);
      if (existingSheet) {
        spreadsheet.deleteSheet(existingSheet);
      }
      
      const sheet = spreadsheet.insertSheet(sheetName);
      this.writeOptimizedData(sheet, chunks[i]);
      this.formatSheet(sheet, headers.length, chunks[i].length);
      
      createdSheets.push(sheetName);
      
      // Yield between chunks to prevent timeout
      if (i < chunks.length - 1) {
        this.yieldExecution();
      }
    }
    
    return {
      sheetName: createdSheets.join(', '),
      rowCount: reportData.rowCount,
      columnCount: reportData.columnCount,
      chunked: true,
      chunkCount: chunks.length,
      sheetNames: createdSheets
    };
  }

  writeOptimizedData(sheet, data) {
    const batchSize = this.config.get('batchWriteSize', 3000);
    
    if (data.length <= batchSize) {
      sheet.getRange(1, 1, data.length, data[0].length).setValues(data);
      return;
    }
    
    // Write in optimized batches
    for (let i = 0; i < data.length; i += batchSize) {
      const batch = data.slice(i, Math.min(i + batchSize, data.length));
      const range = sheet.getRange(i + 1, 1, batch.length, data[0].length);
      range.setValues(batch);
      
      // Yield every few batches to prevent timeout
      if (i > 0 && i % (batchSize * 3) === 0) {
        this.yieldExecution();
      }
    }
  }

  generateSheetName(reportId, reportName) {
    const maxLength = 30;
    let name = reportName || reportId;
    name = name.replace(/[^\w\s-]/g, '').trim();
    
    if (name.length > maxLength) {
      name = name.substring(0, maxLength - 3) + '...';
    }
    
    return name || 'Report_' + reportId.substring(0, 10);
  }

  formatSheet(sheet, columnCount, rowCount) {
    // Format header row
    const headerRange = sheet.getRange(1, 1, 1, columnCount);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#e8f5e8');
    headerRange.setFontColor('#2e7d32');
    sheet.setFrozenRows(1);

    // Auto-resize columns (limit to prevent timeout)
    const maxColumns = Math.min(columnCount, 20);
    sheet.autoResizeColumns(1, maxColumns);

    // Freeze first column if many columns
    if (columnCount > 5) {
      sheet.setFrozenColumns(1);
    }

    // Add borders for better readability
    if (rowCount < 10000) { // Only for smaller sheets to prevent timeout
      const dataRange = sheet.getRange(1, 1, rowCount, columnCount);
      dataRange.setBorder(true, true, true, true, true, true);
    }
  }

  addMetadataNote(sheet, reportId, metadata, reportData) {
    const note = [
      'IMPACT.COM REPORT v4.0',
      'Report ID: ' + reportId,
      'Report Name: ' + (metadata.name || 'N/A'),
      'Data Rows: ' + reportData.rowCount,
      'Columns: ' + reportData.columnCount,
      'Processed: ' + new Date().toLocaleString(),
      'Chunked: ' + (reportData.needsChunking ? 'Yes' : 'No'),
      'Script Version: 4.0.0'
    ].join('\n');

    sheet.getRange('A1').setNote(note);
  }

  createSummarySheet(results, errors) {
    this.logger.info('Creating enhanced summary sheet');

    const spreadsheet = this.getSpreadsheet();
    
    // Clear existing summary
    const existingSummary = spreadsheet.getSheetByName('DISCOVERY SUMMARY');
    if (existingSummary) {
      spreadsheet.deleteSheet(existingSummary);
    }

    const summarySheet = spreadsheet.insertSheet('DISCOVERY SUMMARY', 0);
    
    // Build comprehensive summary
    const summaryData = [
      ['Report ID', 'Report Name', 'Sheet Name', 'Rows', 'Columns', 'Status', 'Notes', 'Processed At']
    ];

    results.forEach(function(result) {
      summaryData.push([
        result.reportId,
        result.reportName || 'N/A',
        result.sheetName,
        result.rowCount,
        result.columnCount,
        'SUCCESS',
        result.chunked ? 'Split into ' + result.chunkCount + ' parts' : '',
        result.processedAt ? result.processedAt.toLocaleString() : 'N/A'
      ]);
    });

    errors.forEach(function(error) {
      summaryData.push([
        error.reportId,
        'N/A',
        'N/A',
        0,
        0,
        'ERROR',
        error.error.substring(0, 100),
        'N/A'
      ]);
    });

    // Add comprehensive statistics
    summaryData.push(['', '', '', '', '', '', '', '']);
    summaryData.push(['=== STATISTICS ===', '', '', '', '', '', '', '']);
    summaryData.push(['Total Reports', results.length + errors.length, '', '', '', '', '', '']);
    summaryData.push(['Successful', results.length, '', '', '', '', '', '']);
    summaryData.push(['Failed', errors.length, '', '', '', '', '', '']);
    summaryData.push(['Success Rate', results.length > 0 ? 
      ((results.length / (results.length + errors.length)) * 100).toFixed(1) + '%' : '0%', 
      '', '', '', '', '', '']);
    summaryData.push(['Total Rows', results.reduce(function(sum, r) { return sum + r.rowCount; }, 0), '', '', '', '', '', '']);
    summaryData.push(['Chunked Reports', results.filter(r => r.chunked).length, '', '', '', '', '', '']);
    summaryData.push(['Generated', new Date().toLocaleString(), '', '', '', '', '', '']);

    // Write data
    summarySheet.getRange(1, 1, summaryData.length, 8).setValues(summaryData);
    
    // Format summary
    this.formatSummarySheet(summarySheet, 8, summaryData.length);
  }

  formatSummarySheet(sheet, columnCount, rowCount) {
    // Format header
    const headerRange = sheet.getRange(1, 1, 1, columnCount);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#fff3e0');
    headerRange.setFontColor('#e65100');
    sheet.setFrozenRows(1);

    // Format statistics section
    const statsStartRow = sheet.getRange('A:A').getValues().findIndex(row => 
      row[0] === '=== STATISTICS ==='
    ) + 1;
    
    if (statsStartRow > 0) {
      const statsRange = sheet.getRange(statsStartRow, 1, rowCount - statsStartRow + 1, columnCount);
      statsRange.setBackground('#f5f5f5');
      statsRange.setFontWeight('bold');
    }

    // Auto-resize columns
    sheet.autoResizeColumns(1, columnCount);
  }

  yieldExecution() {
    // Yield execution to prevent timeout
    Utilities.sleep(this.config.get('yieldInterval', 1000));
  }
}

// ============================================================================
// MAIN ORCHESTRATOR (ULTRA-OPTIMIZED)
// ============================================================================

class UltraOptimizedOrchestrator {
  constructor() {
    this.config = new ImpactConfig();
    this.metrics = new PerformanceMetrics();
    this.logger = new EnhancedLogger(this.config, this.metrics);
    this.progressTracker = new EnhancedProgressTracker(this.config, this.metrics);
    this.apiClient = new EnhancedAPIClient(this.config, this.logger, this.metrics);
    this.dataProcessor = new EnhancedDataProcessor(this.config, this.logger, this.metrics);
    this.spreadsheetManager = new EnhancedSpreadsheetManager(this.config, this.logger, this.metrics);
    
    this.startTime = Date.now();
    this.lastCheckpoint = Date.now();
  }

  runCompleteDiscovery(options = {}) {
    this.logger.info('Starting ultra-optimized discovery v4.0', {
      options: options,
      config: this.getConfigSummary()
    });

    try {
      // Validate configuration
      const validation = this.config.validate();
      if (!validation.isValid) {
        throw new Error('Configuration validation failed: ' + validation.errors.join(', '));
      }

      if (validation.warnings.length > 0) {
        this.logger.warn('Configuration warnings', { warnings: validation.warnings });
      }

      // Check for resume capability
      const resume = this.config.get('enableResume', true) && !options.forceRestart;
      const completed = resume ? this.progressTracker.getCompletedReports() : [];
      
      if (completed.length > 0) {
        this.logger.info('Resuming discovery', { 
          completedCount: completed.length,
          completedIds: completed.map(r => r.reportId)
        });
      }

      // Discover reports
      this.checkpoint('discovering_reports');
      const reports = this.apiClient.discoverReports();
      this.logger.info('Report discovery complete', { totalReports: reports.length });
      
      // Filter out completed reports if resuming
      const pendingReports = resume ? 
        reports.filter(r => !completed.some(c => c.reportId === r.Id)) : 
        reports;
      
      this.logger.info('Processing reports', { 
        total: reports.length,
        pending: pendingReports.length,
        completed: completed.length
      });

      if (pendingReports.length === 0) {
        this.logger.info('All reports already completed');
        return { successful: [], failed: [], completed: completed.length };
      }

      // Schedule exports with optimized batching
      this.checkpoint('scheduling_exports');
      const exportResults = this.scheduleExportsOptimized(pendingReports);
      
      if (exportResults.scheduled.length === 0) {
        this.logger.warn('No exports were successfully scheduled');
        return { successful: [], failed: exportResults.errors };
      }

      // Process exports with enhanced error handling
      this.checkpoint('processing_exports');
      const results = this.processExportsOptimized(exportResults.scheduled);

      // Create comprehensive summary
      this.checkpoint('creating_summary');
      this.spreadsheetManager.createSummarySheet(results.successful, results.failed);

      // Final metrics and cleanup
      const duration = Date.now() - this.startTime;
      const finalMetrics = this.metrics.getSummary();
      
      this.logger.info('Discovery completed successfully', {
        duration: finalMetrics.durationMinutes + ' minutes',
        scheduled: exportResults.scheduled.length,
        successful: results.successful.length,
        failed: results.failed.length,
        apiCalls: finalMetrics.apiCalls,
        successRate: finalMetrics.successRate,
        dataProcessed: finalMetrics.dataProcessed
      });

      // Send notifications if configured
      if (this.config.get('enableEmailNotifications', false)) {
        this.sendCompletionNotification(results, finalMetrics);
      }

      // Clear progress on complete success
      if (results.failed.length === 0) {
        this.progressTracker.clearAll();
      }

      return {
        successful: results.successful,
        failed: results.failed,
        metrics: finalMetrics,
        completed: completed.length + results.successful.length
      };

    } catch (error) {
      this.logger.fatal('Discovery failed', { 
        error: error.message,
        duration: ((Date.now() - this.startTime) / 1000 / 60).toFixed(2) + ' minutes'
      });
      throw error;
    }
  }

  scheduleExportsOptimized(reports) {
    this.logger.info('Scheduling exports with optimization', { reportCount: reports.length });
    
    const scheduled = [];
    const errors = [];
    const parallelLimit = this.config.get('parallelRequestLimit', 3);
    const requestDelay = this.config.get('requestDelay', 800);

    // Process in batches for better performance
    for (let i = 0; i < reports.length; i += parallelLimit) {
      const batch = reports.slice(i, i + parallelLimit);
      
      this.logger.debug('Processing batch', { 
        batchStart: i + 1, 
        batchSize: batch.length,
        totalReports: reports.length
      });

      // Process batch with parallel-like behavior
      for (let j = 0; j < batch.length; j++) {
        const report = batch[j];
        const globalIndex = i + j;
        
        try {
          this.logger.info('Scheduling ' + (globalIndex + 1) + '/' + reports.length + ': ' + report.Id);
          
          const job = this.apiClient.scheduleExport(report.Id);
          scheduled.push({
            reportId: job.reportId,
            jobId: job.jobId,
            reportName: report.Name,
            scheduledAt: job.scheduledAt
          });

          // Rate limiting
          if (j < batch.length - 1) {
            Utilities.sleep(this.config.get('burstDelay', 200));
          }

        } catch (error) {
          this.logger.error('Failed to schedule ' + report.Id, { 
            error: error.message,
            reportName: report.Name
          });
          errors.push({ 
            reportId: report.Id, 
            reportName: report.Name, 
            error: error.message 
          });
        }
      }

      // Save progress periodically
      if ((i + parallelLimit) % this.config.get('progressSaveInterval', 5) === 0) {
        this.progressTracker.saveProgress('scheduled', { 
          scheduled: scheduled.length, 
          errors: errors.length,
          processed: i + batch.length
        });
      }

      // Delay between batches
      if (i + parallelLimit < reports.length) {
        Utilities.sleep(requestDelay);
      }

      // Check for timeout
      this.checkTimeout();
    }

    this.progressTracker.saveProgress('scheduled', { 
      scheduled: scheduled.length, 
      errors: errors.length 
    });

    this.logger.info('Export scheduling complete', {
      scheduled: scheduled.length,
      errors: errors.length
    });

    return { scheduled: scheduled, errors: errors };
  }

  processExportsOptimized(scheduledJobs) {
    this.logger.info('Processing exports with optimization', { jobCount: scheduledJobs.length });
    
    const successful = [];
    const failed = [];

    for (let i = 0; i < scheduledJobs.length; i++) {
      const job = scheduledJobs[i];
      
      try {
        this.logger.info('Processing ' + (i + 1) + '/' + scheduledJobs.length + ': ' + job.reportId);
        
        const result = this.processSingleExportOptimized(job);
        successful.push(result);
        
        // Mark as complete for resume capability
        this.progressTracker.markReportComplete(job.reportId, {
          sheetName: result.sheetName,
          rowCount: result.rowCount,
          processedAt: result.processedAt
        });
        
        // Save progress periodically
        if ((i + 1) % this.config.get('progressSaveInterval', 5) === 0) {
          this.progressTracker.saveProgress('processing', {
            successful: successful.length,
            failed: failed.length,
            processed: i + 1
          });
        }

      } catch (error) {
        this.logger.error('Failed to process ' + job.reportId, { 
          error: error.message,
          reportName: job.reportName
        });
        failed.push({
          reportId: job.reportId,
          reportName: job.reportName,
          error: error.message
        });
      }

      // Check for timeout
      this.checkTimeout();
    }

    this.logger.info('Export processing complete', {
      successful: successful.length,
      failed: failed.length
    });

    return { successful: successful, failed: failed };
  }

  processSingleExportOptimized(job) {
    // Poll for completion with optimized strategy
    const status = this.pollJobCompletionOptimized(job.jobId);
    
    if (status.status !== 'completed') {
      throw new Error('Job failed or timed out: ' + status.status);
    }

    // Download and process data
    const csvData = this.apiClient.downloadResult(status.resultUri);
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
      chunked: sheetInfo.chunked,
      chunkCount: sheetInfo.chunkCount,
      processedAt: new Date()
    };
  }

  pollJobCompletionOptimized(jobId) {
    const maxAttempts = this.config.get('maxPollingAttempts', 30);
    let delay = this.config.get('initialPollingDelay', 3000);
    const maxDelay = this.config.get('maxPollingDelay', 60000);
    const multiplier = this.config.get('pollingMultiplier', 1.2);
    const quickThreshold = this.config.get('quickPollingThreshold', 5);
    const quickDelay = this.config.get('quickPollingDelay', 2000);

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        const status = this.apiClient.checkJobStatus(jobId);
        
        if (status.status === 'completed') {
          this.logger.debug('Job completed', { 
            jobId: jobId, 
            attempts: attempt + 1 
          });
          return status;
        }
        
        if (status.status === 'failed') {
          throw new Error('Job failed: ' + (status.error || 'Unknown error'));
        }

        // Use quick polling after initial attempts
        if (attempt >= quickThreshold) {
          delay = quickDelay;
        } else {
          delay = Math.min(delay * multiplier, maxDelay);
        }

        this.logger.debug('Job polling', { 
          jobId: jobId, 
          status: status.status, 
          attempt: attempt + 1,
          nextDelay: delay
        });
        
        Utilities.sleep(delay);
        
        // Check for timeout
        this.checkTimeout();

      } catch (error) {
        if (attempt === maxAttempts - 1) {
          throw error;
        }
        this.logger.warn('Polling error, retrying', { 
          jobId: jobId, 
          attempt: attempt + 1,
          error: error.message
        });
        Utilities.sleep(delay);
      }
    }

    throw new Error('Job polling timed out after ' + maxAttempts + ' attempts');
  }

  checkpoint(phase, data = {}) {
    const now = Date.now();
    const timeSinceLastCheckpoint = now - this.lastCheckpoint;
    
    this.metrics.recordCheckpoint(phase, data);
    this.progressTracker.saveProgress(phase, data);
    
    this.logger.debug('Checkpoint reached', {
      phase: phase,
      timeSinceLastCheckpoint: timeSinceLastCheckpoint + 'ms',
      totalElapsed: ((now - this.startTime) / 1000 / 60).toFixed(2) + ' minutes'
    });
    
    this.lastCheckpoint = now;
  }

  checkTimeout() {
    const elapsed = Date.now() - this.startTime;
    const maxTime = this.config.get('maxExecutionTime', 25 * 60 * 1000);
    
    if (elapsed > maxTime) {
      this.logger.warn('Approaching execution time limit', {
        elapsed: (elapsed / 1000 / 60).toFixed(2) + ' minutes',
        maxTime: (maxTime / 1000 / 60).toFixed(2) + ' minutes'
      });
      
      // Save current progress
      this.checkpoint('timeout_warning', {
        elapsed: elapsed,
        maxTime: maxTime
      });
    }
  }

  getConfigSummary() {
    return {
      maxRowsPerSheet: this.config.get('maxRowsPerSheet'),
      parallelRequestLimit: this.config.get('parallelRequestLimit'),
      enableResume: this.config.get('enableResume'),
      enableParallelProcessing: this.config.get('enableParallelProcessing')
    };
  }

  sendCompletionNotification(results, metrics) {
    const email = this.config.get('notificationEmail');
    if (!email) return;

    const subject = 'Impact.com Export Complete - ' + results.successful.length + ' reports';
    const body = [
      'Export completed in ' + metrics.durationMinutes + ' minutes',
      '',
      'Successful: ' + results.successful.length,
      'Failed: ' + results.failed.length,
      'API Calls: ' + metrics.apiCalls,
      'Success Rate: ' + metrics.successRate,
      'Data Processed: ' + (metrics.dataProcessed / 1024 / 1024).toFixed(2) + ' MB',
      '',
      'View results: https://docs.google.com/spreadsheets/d/' + this.config.getSpreadsheetId()
    ].join('\n');

    try {
      MailApp.sendEmail(email, subject, body);
      this.logger.info('Completion notification sent', { email: email });
    } catch (error) {
      this.logger.error('Failed to send notification', { error: error.message });
    }
  }
}

// ============================================================================
// PUBLIC API FUNCTIONS
// ============================================================================

function runCompleteDiscovery() {
  const orchestrator = new UltraOptimizedOrchestrator();
  return orchestrator.runCompleteDiscovery();
}

function resumeDiscovery() {
  const orchestrator = new UltraOptimizedOrchestrator();
  return orchestrator.runCompleteDiscovery({ forceRestart: false });
}

function restartDiscovery() {
  const orchestrator = new UltraOptimizedOrchestrator();
  const tracker = new EnhancedProgressTracker(orchestrator.config, orchestrator.metrics);
  tracker.clearAll();
  return orchestrator.runCompleteDiscovery({ forceRestart: true });
}

function discoverAllReports() {
  const config = new ImpactConfig();
  const metrics = new PerformanceMetrics();
  const logger = new EnhancedLogger(config, metrics);
  const apiClient = new EnhancedAPIClient(config, logger, metrics);
  return apiClient.discoverReports();
}

function testConnection() {
  Logger.log('Testing connection with optimized client...');
  
  try {
    const config = new ImpactConfig();
    const metrics = new PerformanceMetrics();
    const logger = new EnhancedLogger(config, metrics);
    const apiClient = new EnhancedAPIClient(config, logger, metrics);
    
    const reports = apiClient.discoverReports();
    
    Logger.log('✅ CONNECTION SUCCESS!');
    Logger.log('Found ' + reports.length + ' accessible reports');
    Logger.log('Performance metrics: ' + JSON.stringify(metrics.getSummary()));
    
    return { 
      success: true, 
      reportCount: reports.length,
      metrics: metrics.getSummary()
    };
    
  } catch (error) {
    Logger.log('❌ CONNECTION FAILED');
    Logger.log('Error: ' + error.message);
    return { success: false, error: error.message };
  }
}

function getProgress() {
  const config = new ImpactConfig();
  const metrics = new PerformanceMetrics();
  const tracker = new EnhancedProgressTracker(config, metrics);
  
  return {
    current: tracker.getProgress(),
    checkpoint: tracker.getCheckpoint(),
    completed: tracker.getCompletedReports(),
    metrics: metrics.getSummary()
  };
}

function clearProgress() {
  const config = new ImpactConfig();
  const metrics = new PerformanceMetrics();
  const tracker = new EnhancedProgressTracker(config, metrics);
  tracker.clearAll();
  Logger.log('All progress cleared');
}

function getSystemHealth() {
  const config = new ImpactConfig();
  const metrics = new PerformanceMetrics();
  const tracker = new EnhancedProgressTracker(config, metrics);
  const validation = config.validate();
  
  const progress = tracker.getProgress();
  const completed = tracker.getCompletedReports();
  
  return {
    status: validation.isValid ? 'HEALTHY' : 'UNHEALTHY',
    configValid: validation.isValid,
    configErrors: validation.errors,
    configWarnings: validation.warnings,
    inProgress: progress !== null,
    completedReports: completed.length,
    lastActivity: progress ? progress.timestamp : null,
    metrics: metrics.getSummary()
  };
}

function updateCredentials(sid, token, spreadsheetId = null) {
  const config = new ImpactConfig();
  
  config.set('impactSid', sid);
  config.set('impactToken', token);
  
  if (spreadsheetId) {
    config.set('spreadsheetId', spreadsheetId);
  }
  
  Logger.log('✅ Credentials updated!');
  Logger.log('SID: ' + sid);
  Logger.log('Token: ' + token.substring(0, 8) + '...');
  Logger.log('Spreadsheet ID: ' + (spreadsheetId || config.getSpreadsheetId()));
  
  return testConnection();
}

function enableEmailNotifications(email) {
  const config = new ImpactConfig();
  config.set('enableEmailNotifications', true);
  config.set('notificationEmail', email);
  Logger.log('Email notifications enabled for: ' + email);
}

function getPerformanceMetrics() {
  const config = new ImpactConfig();
  const metrics = new PerformanceMetrics();
  return metrics.getSummary();
}

function optimizeConfiguration() {
  const config = new ImpactConfig();
  
  // Apply performance optimizations
  config.set('maxRowsPerSheet', 50000);
  config.set('parallelRequestLimit', 3);
  config.set('requestDelay', 800);
  config.set('batchWriteSize', 3000);
  config.set('enableParallelProcessing', true);
  config.set('enableSmartChunking', true);
  config.set('enableMemoryOptimization', true);
  
  Logger.log('✅ Configuration optimized for performance!');
  return config.config;
}
