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
      maxRowsPerSheet: 30000, // Lowered from 50000 to prevent timeout on large writes
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
      maxExecutionTime: 28 * 60 * 1000, // 28 minutes (2 min buffer for cleanup before 30 min hard limit)
      checkpointInterval: 2 * 60 * 1000, // 2 minutes
      yieldInterval: 1000, // 1 second
      timeoutBuffer: 2 * 60 * 1000, // 2 minutes buffer before hard limit

      // Data Freshness Management
      dataFreshnessHours: 24, // Hours before data is considered stale
      forceRefresh: false, // Force refresh all data regardless of freshness
      enableDataFreshness: true, // Enable freshness checking

      // Date Range Filtering
      enableDateFiltering: false, // Enable date range filtering
      startDate: null, // Start date for reports (YYYY-MM-DD format)
      endDate: null, // End date for reports (YYYY-MM-DD format)
      dateRangePresets: {
        'august-2025': { start: '2025-08-01', end: '2025-08-31' },
        'september-2025': { start: '2025-09-01', end: '2025-09-30' },
        'october-2025': { start: '2025-10-01', end: '2025-10-31' },
        'q3-2025': { start: '2025-07-01', end: '2025-09-30' },
        'q4-2025': { start: '2025-10-01', end: '2025-12-31' }
      },

      // Report Exclusion
      excludedReports: [
        'capital_one_mp_action_listing_sku',
        '12172',
        'custom_partner_payable_click_data',
        'getty_adplacement_mp_action_listing_sku',
        'mp_action_listing_sku_ipsos',
        'mp_action_listing_sku_and_permissions',
        'mp_adv_contacts',
        'mp_bonus_make_good_listing',
        'mp_category_exception_list_active_IO',
        'mp_action_listing_cpc_action',
        'mp_io_history',
        'custom_partner_perf_by_vlink_by_day',
        'withdrawal_details',
        'action_listing_withdrawal',
        'invoice_details_action_earnings_gaap',
        'mp_invoice_history',
        'other_earnings',
        'partner_funds_transfer_listing',
        'partner_payable_click_data',
        'PaystubActions',
        'mp_pending_insertion_orders',
        'PerformanceByCampaigns',
        'partner_seller_Perf_by_product',
        'seller_perf_by_program',
        'mp_sku_exception_list_active_IO',
        'mp_assigned_tracking_values',
        'mp_assigned_tracking_values',
        'partner_perf_by_vlink',
        'mp_monthly_close'
      ], // Reports to exclude from discovery and processing

      // Report Inclusion (Overrides exclusion if set)
      includedReports: [], // Add Report IDs or Names here to ONLY collect these reports

      // Credentials (Loaded from Script Properties for security)
      impactSid: this.getSecureCredential('IMPACT_SID'),
      impactToken: this.getSecureCredential('IMPACT_TOKEN'),
      spreadsheetId: this.getSecureCredential('IMPACT_SPREADSHEET_ID'),

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

  /**
   * Get secure credential from Script Properties
   */
  getSecureCredential(propertyName) {
    const value = this.props.getProperty(propertyName);
    if (!value) {
      throw new Error(`Credential ${propertyName} not found. Run quickSetupWithCredentials() first.`);
    }
    return value;
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
// CUSTOM ERROR CLASSES
// ============================================================================

/**
 * Custom error for timeout situations
 * Allows graceful handling and progress saving before timeout
 */
class TimeoutError extends Error {
  constructor(message, elapsed, maxTime) {
    super(message);
    this.name = 'TimeoutError';
    this.elapsed = elapsed;
    this.maxTime = maxTime;
    this.isTimeout = true;
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
    const excludedReports = this.config.get('excludedReports', []);
    const includedReports = this.config.get('includedReports', []);

    const reports = (response.data.Reports || [])
      .filter(r => r.ApiAccessible)
      .filter(r => {
        // If includedReports is set and not empty, ONLY include those
        if (includedReports && includedReports.length > 0) {
          const isIncluded = includedReports.some(included =>
            r.Id === included || r.Name === included ||
            r.Id.toString() === included || r.Name.toString() === included
          );
          return isIncluded;
        }

        // Otherwise check exclusions
        const isExcluded = excludedReports.some(excluded =>
          r.Id === excluded || r.Name === excluded ||
          r.Id.toString() === excluded || r.Name.toString() === excluded
        );
        if (isExcluded) {
          this.logger.debug('Excluding report', { reportId: r.Id, reportName: r.Name });
        }
        return !isExcluded;
      });

    const totalAccessible = (response.data.Reports || []).filter(r => r.ApiAccessible).length;
    const excludedCount = totalAccessible - reports.length;

    if (excludedCount > 0) {
      this.logger.info('Filtered out ' + excludedCount + ' report(s)');
    }
    this.logger.info('Found ' + reports.length + ' reports to process');
    return reports;
  }

  scheduleExport(reportId, params = {}) {
    this.logger.debug('Scheduling export', { reportId: reportId, params: params });

    const queryParts = ['subid=mula'];

    // Add date range parameters if configured
    if (this.config.get('enableDateFiltering', false)) {
      const startDate = this.config.get('startDate');
      const endDate = this.config.get('endDate');

      if (startDate) {
        queryParts.push('startdate=' + encodeURIComponent(startDate));
        this.logger.debug('Added start date', { startDate: startDate });
      }

      if (endDate) {
        queryParts.push('enddate=' + encodeURIComponent(endDate));
        this.logger.debug('Added end date', { endDate: endDate });
      }
    }

    // Add any additional parameters
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
  constructor(config, logger, metrics, progressTracker = null) {
    this.config = config;
    this.logger = logger;
    this.metrics = metrics;
    this.progressTracker = progressTracker;
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

    // Check if we need to refresh this data
    const shouldRefresh = this.shouldRefreshData(reportId, metadata, spreadsheet);

    if (!shouldRefresh) {
      this.logger.info('Skipping ' + reportId + ' - data is fresh', {
        reportId: reportId,
        lastUpdated: metadata.lastUpdated
      });

      // Return existing sheet info without recreating
      const existingSheet = spreadsheet.getSheetByName(sheetName);
      if (existingSheet) {
        return {
          sheetName: sheetName,
          rowCount: existingSheet.getLastRow() - 1,
          columnCount: existingSheet.getLastColumn(),
          chunked: false,
          skipped: true,
          reason: 'Data is fresh'
        };
      }
    }

    // Delete existing sheet if it exists and we're refreshing
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

    // Store data freshness info
    this.storeDataFreshness(reportId, metadata);

    return {
      sheetName: sheetName,
      rowCount: reportData.rowCount,
      columnCount: reportData.columnCount,
      chunked: false,
      refreshed: true
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
    this.logger.info('Creating enhanced summary sheet', {
      successfulCount: results ? results.length : 0,
      errorCount: errors ? errors.length : 0
    });

    let spreadsheet;
    try {
      spreadsheet = this.getSpreadsheet();
    } catch (spreadsheetError) {
      this.logger.error('Cannot access spreadsheet for summary update', {
        error: spreadsheetError.message,
        spreadsheetId: this.spreadsheetId
      });
      throw new Error('Failed to access spreadsheet: ' + spreadsheetError.message);
    }

    // Clear existing summary
    let existingSummary;
    try {
      existingSummary = spreadsheet.getSheetByName('DISCOVERY SUMMARY');
      if (existingSummary) {
        spreadsheet.deleteSheet(existingSummary);
        this.logger.debug('Deleted existing DISCOVERY SUMMARY sheet');
      }
    } catch (deleteError) {
      this.logger.warn('Could not delete existing summary sheet', {
        error: deleteError.message
      });
      // Continue anyway - might not exist
    }

    let summarySheet;
    try {
      summarySheet = spreadsheet.insertSheet('DISCOVERY SUMMARY', 0);
      this.logger.debug('Created new DISCOVERY SUMMARY sheet');
    } catch (insertError) {
      this.logger.error('Failed to create summary sheet', {
        error: insertError.message
      });
      throw new Error('Failed to create summary sheet: ' + insertError.message);
    }

    // Get historical completed reports
    const completedReports = this.progressTracker ? this.progressTracker.getCompletedReports() : [];
    const safeResults = results || [];
    const safeErrors = errors || [];

    this.logger.info('Including historical data', {
      currentResults: safeResults.length,
      currentErrors: safeErrors.length,
      historicalCompleted: completedReports.length
    });

    // Build comprehensive summary
    const summaryData = [
      ['Report ID', 'Report Name', 'Sheet Name', 'Rows', 'Columns', 'Status', 'Notes', 'Processed At']
    ];

    // Add historical completed reports first
    const currentTimestamp = new Date();
    if (completedReports && completedReports.length > 0) {
      completedReports.forEach(function (completed) {
        // Build notes field with original info plus last verified date
        const notes = [];
        if (completed.chunked) {
          notes.push('Split into ' + completed.chunkCount + ' parts');
        }
        notes.push('Last verified: ' + currentTimestamp.toLocaleString());

        summaryData.push([
          completed.reportId,
          completed.reportName || 'N/A',
          completed.sheetName || 'N/A',
          completed.rowCount || 0,
          completed.columnCount || 0,
          'SUCCESS (Historical)',
          notes.join(' | '),
          completed.processedAt ? new Date(completed.processedAt).toLocaleString() : 'N/A'
        ]);
      });
    }

    // Add current successful results
    if (safeResults.length > 0) {
      safeResults.forEach(function (result) {
        summaryData.push([
          result.reportId,
          result.reportName || 'N/A',
          result.sheetName || 'N/A',
          result.rowCount || 0,
          result.columnCount || 0,
          'SUCCESS (Current)',
          result.chunked ? 'Split into ' + (result.chunkCount || 0) + ' parts' : '',
          result.processedAt ? (result.processedAt.toLocaleString ? result.processedAt.toLocaleString() : new Date(result.processedAt).toLocaleString()) : new Date().toLocaleString()
        ]);
      });
    }

    // Add current errors
    if (safeErrors.length > 0) {
      const currentTimestamp = new Date();
      this.logger.debug('Adding ' + safeErrors.length + ' error entries to summary', {
        errorCount: safeErrors.length,
        errorIds: safeErrors.map(e => e.reportId || 'UNKNOWN')
      });

      safeErrors.forEach(function (error) {
        summaryData.push([
          error.reportId || 'UNKNOWN',
          error.reportName || 'N/A',
          'N/A',
          0,
          0,
          'ERROR (Current)',
          (error.error || error.message || 'Unknown error').substring(0, 100),
          currentTimestamp.toLocaleString()
        ]);
      });

      this.logger.debug('Added ' + safeErrors.length + ' error entries to summary data');
    } else {
      this.logger.debug('No errors to add to summary');
    }

    // Calculate totals
    const totalHistorical = completedReports ? completedReports.length : 0;
    const totalCurrent = safeResults.length + safeErrors.length;
    const totalSuccessful = totalHistorical + safeResults.length;
    const totalFailed = safeErrors.length;
    const totalReports = totalHistorical + totalCurrent;
    const totalRows = (completedReports ? completedReports.reduce((sum, r) => sum + (r.rowCount || 0), 0) : 0) +
      safeResults.reduce((sum, r) => sum + (r.rowCount || 0), 0);

    // Add comprehensive statistics
    summaryData.push(['', '', '', '', '', '', '', '']);
    summaryData.push(['=== COMPREHENSIVE STATISTICS ===', '', '', '', '', '', '', '']);
    summaryData.push(['Total Reports (All Time)', totalReports, '', '', '', '', '', '']);
    summaryData.push(['Historical Completed', totalHistorical, '', '', '', '', '', '']);
    summaryData.push(['Current Run - Successful', safeResults.length, '', '', '', '', '', '']);
    summaryData.push(['Current Run - Failed', safeErrors.length, '', '', '', '', '', '']);
    summaryData.push(['Total Successful', totalSuccessful, '', '', '', '', '', '']);
    summaryData.push(['Total Failed', totalFailed, '', '', '', '', '', '']);
    summaryData.push(['Overall Success Rate', totalReports > 0 ?
      ((totalSuccessful / totalReports) * 100).toFixed(1) + '%' : '0%',
      '', '', '', '', '', '']);
    summaryData.push(['Total Rows Processed', totalRows, '', '', '', '', '', '']);
    const chunkedHistorical = completedReports ? completedReports.filter(r => r.chunked).length : 0;
    const chunkedCurrent = safeResults.filter(r => r.chunked).length;
    summaryData.push(['Chunked Reports', chunkedHistorical + chunkedCurrent, '', '', '', '', '', '']);
    // Add separator and last updated timestamp
    summaryData.push(['', '', '', '', '', '', '', '']);
    summaryData.push(['=== SUMMARY LAST UPDATED ===', '', '', '', '', '', '', '']);
    summaryData.push(['Last Updated', new Date().toLocaleString(), '', '', '', '', '', '']);
    summaryData.push(['Date', new Date().toLocaleDateString(), '', '', '', '', '', '']);
    summaryData.push(['Time', new Date().toLocaleTimeString(), '', '', '', '', '', '']);

    // Write data
    try {
      summarySheet.getRange(1, 1, summaryData.length, 8).setValues(summaryData);
      this.logger.debug('Summary data written to sheet', {
        rowCount: summaryData.length,
        columnCount: 8
      });
    } catch (writeError) {
      this.logger.error('Failed to write summary data', {
        error: writeError.message,
        rowCount: summaryData.length
      });
      throw new Error('Failed to write summary data: ' + writeError.message);
    }

    // Format summary
    try {
      this.formatSummarySheet(summarySheet, 8, summaryData.length);
      this.logger.debug('Summary sheet formatted');
    } catch (formatError) {
      this.logger.warn('Failed to format summary sheet', {
        error: formatError.message
      });
      // Don't fail if formatting fails - data is already written
    }

    this.logger.info('Enhanced summary sheet created successfully', {
      totalReports: totalReports,
      historicalCompleted: totalHistorical,
      currentSuccessful: results ? results.length : 0,
      currentFailed: errors ? errors.length : 0,
      totalSuccessful: totalSuccessful,
      lastUpdated: new Date().toISOString()
    });
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

  /**
   * Check if data should be refreshed based on freshness rules
   */
  shouldRefreshData(reportId, metadata, spreadsheet) {
    const freshnessHours = this.config.get('dataFreshnessHours', 24); // Default 24 hours
    const forceRefresh = this.config.get('forceRefresh', false);

    if (forceRefresh) {
      this.logger.info('Force refresh enabled for ' + reportId);
      return true;
    }

    // Check if we have freshness data for this report
    const freshnessData = this.getDataFreshness(reportId);
    if (!freshnessData) {
      this.logger.info('No freshness data for ' + reportId + ' - will refresh');
      return true;
    }

    const lastUpdated = new Date(freshnessData.lastUpdated);
    const now = new Date();
    const hoursSinceUpdate = (now - lastUpdated) / (1000 * 60 * 60);

    if (hoursSinceUpdate >= freshnessHours) {
      this.logger.info('Data is stale for ' + reportId, {
        hoursSinceUpdate: hoursSinceUpdate.toFixed(1),
        freshnessThreshold: freshnessHours
      });
      return true;
    }

    this.logger.info('Data is fresh for ' + reportId, {
      hoursSinceUpdate: hoursSinceUpdate.toFixed(1),
      freshnessThreshold: freshnessHours
    });
    return false;
  }

  /**
   * Store data freshness information
   */
  storeDataFreshness(reportId, metadata) {
    const freshnessData = {
      reportId: reportId,
      lastUpdated: new Date().toISOString(),
      reportName: metadata.name,
      dataHash: this.calculateDataHash(metadata),
      rowCount: metadata.rowCount || 0
    };

    const props = PropertiesService.getScriptProperties();
    const existingData = props.getProperty('IMPACT_DATA_FRESHNESS');
    let freshnessMap = {};

    if (existingData) {
      try {
        freshnessMap = JSON.parse(existingData);
      } catch (error) {
        this.logger.warn('Failed to parse existing freshness data');
      }
    }

    freshnessMap[reportId] = freshnessData;
    props.setProperty('IMPACT_DATA_FRESHNESS', JSON.stringify(freshnessMap));

    this.logger.info('Stored freshness data for ' + reportId, {
      lastUpdated: freshnessData.lastUpdated,
      rowCount: freshnessData.rowCount
    });
  }

  /**
   * Get data freshness information for a report
   */
  getDataFreshness(reportId) {
    const props = PropertiesService.getScriptProperties();
    const existingData = props.getProperty('IMPACT_DATA_FRESHNESS');

    if (!existingData) {
      return null;
    }

    try {
      const freshnessMap = JSON.parse(existingData);
      return freshnessMap[reportId] || null;
    } catch (error) {
      this.logger.warn('Failed to parse freshness data');
      return null;
    }
  }

  /**
   * Calculate a simple hash for data comparison
   */
  calculateDataHash(metadata) {
    const hashData = {
      reportId: metadata.reportId,
      reportName: metadata.name,
      rowCount: metadata.rowCount,
      columnCount: metadata.columnCount,
      timestamp: new Date().toISOString().split('T')[0] // Date only for daily comparison
    };

    return Utilities.computeDigest(Utilities.DigestAlgorithm.MD5,
      JSON.stringify(hashData),
      Utilities.Charset.UTF_8
    ).map(b => b.toString(16).padStart(2, '0')).join('');
  }

  /**
   * Clear all freshness data (useful for full refresh)
   */
  clearDataFreshness() {
    const props = PropertiesService.getScriptProperties();
    props.deleteProperty('IMPACT_DATA_FRESHNESS');
    this.logger.info('Cleared all data freshness information');
  }

  /**
   * Get freshness summary for all reports
   */
  getFreshnessSummary() {
    const props = PropertiesService.getScriptProperties();
    const existingData = props.getProperty('IMPACT_DATA_FRESHNESS');

    if (!existingData) {
      return { totalReports: 0, freshReports: 0, staleReports: 0, reports: [] };
    }

    try {
      const freshnessMap = JSON.parse(existingData);
      const now = new Date();
      const freshnessHours = this.config.get('dataFreshnessHours', 24);

      let freshCount = 0;
      let staleCount = 0;
      const reports = [];

      for (const [reportId, data] of Object.entries(freshnessMap)) {
        const lastUpdated = new Date(data.lastUpdated);
        const hoursSinceUpdate = (now - lastUpdated) / (1000 * 60 * 60);
        const isFresh = hoursSinceUpdate < freshnessHours;

        if (isFresh) freshCount++;
        else staleCount++;

        reports.push({
          reportId: reportId,
          reportName: data.reportName,
          lastUpdated: data.lastUpdated,
          hoursSinceUpdate: hoursSinceUpdate.toFixed(1),
          isFresh: isFresh,
          rowCount: data.rowCount
        });
      }

      return {
        totalReports: Object.keys(freshnessMap).length,
        freshReports: freshCount,
        staleReports: staleCount,
        reports: reports.sort((a, b) => new Date(b.lastUpdated) - new Date(a.lastUpdated))
      };
    } catch (error) {
      this.logger.error('Failed to parse freshness summary', { error: error.message });
      return { totalReports: 0, freshReports: 0, staleReports: 0, reports: [] };
    }
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
    this.spreadsheetManager = new EnhancedSpreadsheetManager(this.config, this.logger, this.metrics, this.progressTracker);

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

      // Filter reports based on completion and freshness
      let pendingReports;
      if (resume) {
        const freshnessHours = this.config.get('dataFreshnessHours', 24);
        const enableFreshness = this.config.get('enableDataFreshness', true);
        const now = new Date();

        pendingReports = reports.filter(r => {
          const completedReport = completed.find(c => c.reportId === r.Id);

          // If not completed, include it
          if (!completedReport) {
            return true;
          }

          // If completed, check freshness
          if (enableFreshness) {
            let freshnessData = this.spreadsheetManager.getDataFreshness(r.Id);
            let lastUpdated;

            // If no freshness data, use the completed report's processedAt as fallback
            if (!freshnessData) {
              if (completedReport.processedAt) {
                lastUpdated = new Date(completedReport.processedAt);
                this.logger.info('Completed report ' + r.Id + ' has no freshness data, using processedAt: ' + lastUpdated.toISOString());
              } else {
                // No data at all - assume stale and reprocess
                this.logger.info('Completed report ' + r.Id + ' has no freshness or processedAt data - will reprocess');
                return true;
              }
            } else {
              lastUpdated = new Date(freshnessData.lastUpdated);
            }

            // Check if data is stale
            const hoursSinceUpdate = (now - lastUpdated) / (1000 * 60 * 60);

            if (hoursSinceUpdate >= freshnessHours) {
              this.logger.info('Completed report ' + r.Id + ' is stale (' + hoursSinceUpdate.toFixed(1) + 'h old) - will reprocess', {
                hoursSinceUpdate: hoursSinceUpdate.toFixed(1),
                threshold: freshnessHours,
                lastUpdated: lastUpdated.toISOString()
              });
              return true; // Include stale reports for reprocessing
            } else {
              this.logger.debug('Completed report ' + r.Id + ' is fresh (' + hoursSinceUpdate.toFixed(1) + 'h old) - will skip', {
                hoursSinceUpdate: hoursSinceUpdate.toFixed(1),
                threshold: freshnessHours
              });
              return false; // Skip fresh reports
            }
          }

          // If freshness checking is disabled, skip all completed reports
          return false;
        });

        const skippedCount = reports.length - pendingReports.length;
        if (skippedCount > 0) {
          this.logger.info('Skipped ' + skippedCount + ' fresh/completed reports');
        }
      } else {
        pendingReports = reports;
      }

      this.logger.info('Processing reports', {
        total: reports.length,
        pending: pendingReports.length,
        completed: completed.length
      });

      if (pendingReports.length === 0) {
        this.logger.info('All reports already completed');

        // Still update summary to show current state
        this.checkpoint('creating_summary');
        this.logger.info('Updating discovery summary (all reports completed)', {
          successfulCount: 0,
          failedCount: 0,
          historicalCompleted: completed.length
        });

        try {
          this.spreadsheetManager.createSummarySheet([], []);
          this.logger.info('Discovery summary updated successfully (all reports already completed)');
        } catch (summaryError) {
          this.logger.error('Failed to update discovery summary', {
            error: summaryError.message
          });
          console.error('CRITICAL: Discovery summary update failed:', summaryError.message);
        }

        return { successful: [], failed: [], completed: completed.length };
      }

      // Schedule exports with optimized batching
      this.checkpoint('scheduling_exports');
      let exportResults;
      try {
        exportResults = this.scheduleExportsOptimized(pendingReports);
      } catch (timeoutError) {
        if (timeoutError.isTimeout) {
          // Handle timeout during scheduling
          this.logger.warn('Timeout during scheduling - saving partial progress', {
            error: timeoutError.message
          });

          // Try to get any partial results
          const progress = this.progressTracker.getProgress();
          exportResults = {
            scheduled: progress && progress.data && progress.data.scheduled ? progress.data.scheduled : [],
            errors: progress && progress.data && progress.data.errors ? progress.data.errors : [],
            timeout: true
          };
        } else {
          throw timeoutError; // Re-throw if not a timeout
        }
      }

      // Handle timeout case
      if (exportResults.timeout) {
        this.logger.warn('Scheduling stopped due to timeout', {
          scheduled: exportResults.scheduled.length,
          errors: exportResults.errors ? exportResults.errors.length : 0
        });

        // Update summary with partial results
        try {
          this.spreadsheetManager.createSummarySheet(
            [],
            exportResults.errors || []
          );
        } catch (summaryError) {
          this.logger.error('Failed to update summary after timeout', {
            error: summaryError.message
          });
        }

        return {
          successful: [],
          failed: exportResults.errors || [],
          timeout: true,
          message: exportResults.message || 'Scheduling stopped due to timeout. Progress saved. Run resumeDiscovery() to continue.',
          scheduled: exportResults.scheduled.length
        };
      }

      if (exportResults.scheduled.length === 0) {
        this.logger.warn('No exports were successfully scheduled');

        // Still update summary to show failed attempts
        this.checkpoint('creating_summary');
        this.logger.info('Updating discovery summary (no exports scheduled)', {
          successfulCount: 0,
          failedCount: exportResults.errors ? exportResults.errors.length : 0
        });

        try {
          const errorsToPass = exportResults.errors || [];
          this.logger.info('Passing errors to summary sheet', {
            errorCount: errorsToPass.length,
            errorReportIds: errorsToPass.map(e => e.reportId || 'UNKNOWN')
          });

          this.spreadsheetManager.createSummarySheet([], errorsToPass);
          this.logger.info('Discovery summary updated successfully (no exports scheduled)', {
            errorsIncluded: errorsToPass.length
          });
        } catch (summaryError) {
          this.logger.error('Failed to update discovery summary', {
            error: summaryError.message
          });
          console.error('CRITICAL: Discovery summary update failed:', summaryError.message);
        }

        return { successful: [], failed: exportResults.errors };
      }

      // Process exports with enhanced error handling
      this.checkpoint('processing_exports');
      let results;
      try {
        results = this.processExportsOptimized(exportResults.scheduled);
      } catch (timeoutError) {
        if (timeoutError.isTimeout) {
          // Handle timeout during processing
          this.logger.warn('Timeout during processing - saving partial progress', {
            error: timeoutError.message
          });

          // Try to get any partial results from progress tracker
          const progress = this.progressTracker.getProgress();
          results = {
            successful: progress && progress.data && progress.data.successful ? progress.data.successful : [],
            failed: progress && progress.data && progress.data.failed ? progress.data.failed : [],
            timeout: true
          };
        } else {
          throw timeoutError; // Re-throw if not a timeout
        }
      }

      // Handle timeout case in processing
      if (results.timeout) {
        this.logger.warn('Processing stopped due to timeout', {
          successful: results.successful ? results.successful.length : 0,
          failed: results.failed ? results.failed.length : 0
        });

        // Update summary with partial results
        try {
          this.spreadsheetManager.createSummarySheet(
            results.successful || [],
            results.failed || []
          );
        } catch (summaryError) {
          this.logger.error('Failed to update summary after timeout', {
            error: summaryError.message
          });
        }

        const duration = Date.now() - this.startTime;
        const finalMetrics = this.metrics.getSummary();

        return {
          successful: results.successful || [],
          failed: results.failed || [],
          metrics: finalMetrics,
          timeout: true,
          message: results.message || 'Processing stopped due to timeout. Progress saved. Run resumeDiscovery() to continue.',
          completed: completed.length + (results.successful ? results.successful.length : 0)
        };
      }

      // Create comprehensive summary (with error handling)
      this.checkpoint('creating_summary');
      this.logger.info('Updating discovery summary', {
        successfulCount: results.successful ? results.successful.length : 0,
        failedCount: results.failed ? results.failed.length : 0
      });

      try {
        this.spreadsheetManager.createSummarySheet(
          results.successful || [],
          results.failed || []
        );
        this.logger.info('Discovery summary updated successfully', {
          successful: results.successful ? results.successful.length : 0,
          failed: results.failed ? results.failed.length : 0
        });
      } catch (summaryError) {
        this.logger.error('Failed to update discovery summary', {
          error: summaryError.message,
          stack: summaryError.stack,
          successfulCount: results.successful ? results.successful.length : 0,
          failedCount: results.failed ? results.failed.length : 0
        });
        // Log to console for visibility
        console.error('CRITICAL: Discovery summary update failed:', summaryError.message);
        // Don't fail the entire run if summary update fails, but log it prominently
      }

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
      // Handle timeout errors gracefully
      if (error.isTimeout) {
        this.logger.warn('Discovery stopped due to timeout', {
          error: error.message,
          duration: ((Date.now() - this.startTime) / 1000 / 60).toFixed(2) + ' minutes',
          elapsed: error.elapsed,
          maxTime: error.maxTime
        });

        // Try to save final progress
        try {
          this.progressTracker.saveProgress('timeout_final', {
            timestamp: new Date().toISOString(),
            elapsed: error.elapsed,
            maxTime: error.maxTime
          }, true);
        } catch (saveError) {
          this.logger.error('Failed to save final progress', {
            error: saveError.message
          });
        }

        // Return timeout result instead of throwing
        return {
          successful: [],
          failed: [],
          timeout: true,
          message: error.message,
          metrics: this.metrics.getSummary()
        };
      }

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
      try {
        this.checkTimeout();
      } catch (timeoutError) {
        if (timeoutError.isTimeout) {
          // Save progress before exiting
          this.progressTracker.saveProgress('scheduled_timeout', {
            scheduled: scheduled.length,
            errors: errors.length,
            processed: i + batch.length,
            remainingReports: reports.length - (i + batch.length)
          }, true);

          this.logger.warn('Timeout during scheduling - progress saved', {
            scheduled: scheduled.length,
            errors: errors.length,
            processed: i + batch.length,
            remaining: reports.length - (i + batch.length)
          });

          // Return partial results so caller can handle gracefully
          return {
            scheduled: scheduled,
            errors: errors,
            timeout: true,
            message: 'Scheduling stopped due to timeout. Progress saved. Run resumeDiscovery() to continue.'
          };
        }
        throw timeoutError; // Re-throw if not a timeout error
      }
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
          reportName: result.reportName,
          sheetName: result.sheetName,
          rowCount: result.rowCount,
          columnCount: result.columnCount,
          chunked: result.chunked,
          chunkCount: result.chunkCount,
          processedAt: result.processedAt
        });

        // Save progress periodically and update summary incrementally
        if ((i + 1) % this.config.get('progressSaveInterval', 5) === 0) {
          this.progressTracker.saveProgress('processing', {
            successful: successful.length,
            failed: failed.length,
            processed: i + 1
          });

          // Update summary incrementally so user can see progress
          try {
            this.spreadsheetManager.createSummarySheet(successful, failed);
            this.logger.debug('Summary updated incrementally', {
              successful: successful.length,
              failed: failed.length
            });
          } catch (summaryError) {
            this.logger.warn('Failed to update summary incrementally', {
              error: summaryError.message
            });
            // Continue processing even if summary update fails
          }
        }

      } catch (error) {
        // Check if it's a timeout error
        if (error.isTimeout) {
          // Save progress before exiting
          this.progressTracker.saveProgress('processing_timeout', {
            successful: successful.length,
            failed: failed.length,
            processed: i + 1,
            remainingJobs: scheduledJobs.length - (i + 1)
          }, true);

          this.logger.warn('Timeout during processing - progress saved', {
            successful: successful.length,
            failed: failed.length,
            processed: i + 1,
            remaining: scheduledJobs.length - (i + 1)
          });

          // Return partial results so caller can handle gracefully
          return {
            successful: successful,
            failed: failed,
            timeout: true,
            message: 'Processing stopped due to timeout. Progress saved. Run resumeDiscovery() to continue.'
          };
        }

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
      try {
        this.checkTimeout();
      } catch (timeoutError) {
        if (timeoutError.isTimeout) {
          // Save progress before exiting
          this.progressTracker.saveProgress('processing_timeout', {
            successful: successful.length,
            failed: failed.length,
            processed: i + 1,
            remainingJobs: scheduledJobs.length - (i + 1)
          }, true);

          this.logger.warn('Timeout during processing - progress saved', {
            successful: successful.length,
            failed: failed.length,
            processed: i + 1,
            remaining: scheduledJobs.length - (i + 1)
          });

          // Return partial results so caller can handle gracefully
          return {
            successful: successful,
            failed: failed,
            timeout: true,
            message: 'Processing stopped due to timeout. Progress saved. Run resumeDiscovery() to continue.'
          };
        }
        throw timeoutError; // Re-throw if not a timeout error
      }
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
        try {
          this.checkTimeout();
        } catch (timeoutError) {
          if (timeoutError.isTimeout) {
            // Re-throw timeout error to propagate up
            throw timeoutError;
          }
          // If not a timeout, continue normally
        }

      } catch (error) {
        // If it's a timeout error, propagate it up immediately
        if (error.isTimeout) {
          throw error;
        }

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
    const maxTime = this.config.get('maxExecutionTime', 28 * 60 * 1000);
    const timeoutBuffer = this.config.get('timeoutBuffer', 2 * 60 * 1000);
    const hardLimit = 30 * 60 * 1000; // Google Apps Script hard limit

    // Check if we're approaching the hard limit (with buffer)
    if (elapsed >= (hardLimit - timeoutBuffer)) {
      this.logger.warn('Execution time limit reached - saving progress and stopping', {
        elapsed: (elapsed / 1000 / 60).toFixed(2) + ' minutes',
        maxTime: (maxTime / 1000 / 60).toFixed(2) + ' minutes',
        hardLimit: (hardLimit / 1000 / 60).toFixed(2) + ' minutes'
      });

      // Save current progress before timeout
      try {
        this.checkpoint('timeout_reached', {
          elapsed: elapsed,
          maxTime: maxTime,
          hardLimit: hardLimit,
          timestamp: new Date().toISOString()
        });

        // Save progress to allow resume
        this.progressTracker.saveProgress('timeout', {
          elapsed: elapsed,
          maxTime: maxTime,
          timestamp: new Date().toISOString()
        }, true); // Force save

        this.logger.info('Progress saved before timeout - script can be resumed');
      } catch (saveError) {
        this.logger.error('Failed to save progress before timeout', {
          error: saveError.message
        });
      }

      // Throw timeout error to stop execution gracefully
      throw new TimeoutError(
        'Execution time limit reached (' + (elapsed / 1000 / 60).toFixed(2) + ' minutes). ' +
        'Progress has been saved. Run resumeDiscovery() to continue.',
        elapsed,
        maxTime
      );
    }

    // Warn when approaching limit (but don't stop yet)
    if (elapsed > maxTime * 0.9) {
      this.logger.warn('Approaching execution time limit', {
        elapsed: (elapsed / 1000 / 60).toFixed(2) + ' minutes',
        maxTime: (maxTime / 1000 / 60).toFixed(2) + ' minutes',
        remaining: ((maxTime - elapsed) / 1000 / 60).toFixed(2) + ' minutes'
      });

      // Save checkpoint
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
// SECURITY FUNCTIONS
// ============================================================================

/**
 * Security audit - check for exposed credentials
 */
function securityAudit() {
  console.log('Running security audit...');

  const issues = [];

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
 * Validate credentials are properly configured
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
 * Setup secure credentials with your actual values
 * Replace the values below with your real credentials
 */
function setupSecureCredentials() {
  console.log('Setting up secure credentials...');

  // REPLACE THESE WITH YOUR ACTUAL CREDENTIALS
  const sid = 'YOUR_ACTUAL_SID_HERE';  // Replace with your real SID
  const token = 'YOUR_ACTUAL_TOKEN_HERE';  // Replace with your real Token
  const spreadsheetId = 'YOUR_SPREADSHEET_ID_HERE';  // Replace with your Spreadsheet ID (optional)

  if (sid === 'YOUR_ACTUAL_SID_HERE' || token === 'YOUR_ACTUAL_TOKEN_HERE') {
    console.log('❌ Please update the credentials in setupSecureCredentials() function');
    console.log('Replace YOUR_ACTUAL_SID_HERE and YOUR_ACTUAL_TOKEN_HERE with your real values');
    console.log('Then run this function again');
    return false;
  }

  const props = PropertiesService.getScriptProperties();

  // Store credentials securely in Script Properties
  props.setProperty('IMPACT_SID', sid);
  props.setProperty('IMPACT_TOKEN', token);

  if (spreadsheetId && spreadsheetId !== 'YOUR_SPREADSHEET_ID_HERE' && spreadsheetId.trim() !== '') {
    props.setProperty('IMPACT_SPREADSHEET_ID', spreadsheetId);
  }

  console.log('✅ Credentials stored securely in Script Properties');
  console.log('SID: ' + sid.substring(0, 8) + '...');
  console.log('Token: ' + token.substring(0, 8) + '...');

  return true;
}

/**
 * Quick setup with your known credentials
 * Use this function with your actual values
 */
function quickSetupWithCredentials() {
  console.log('Quick setup with credentials...');

  // EDIT THESE VALUES WITH YOUR ACTUAL CREDENTIALS
  const sid = 'IRVS6cDH8DnE3783091LoyPwNc8YkkMTF1';  // Your SID
  const token = 'CrH~iNtpeA5dygjPdnSaXFAxKAtp~F4w';  // Your Token
  const spreadsheetId = '1QDOxgElRvl6EvI02JP4knupUd-jLW7D6LJN-VyLS3ZY';  // Your Spreadsheet ID

  const props = PropertiesService.getScriptProperties();

  // Store credentials securely in Script Properties
  props.setProperty('IMPACT_SID', sid);
  props.setProperty('IMPACT_TOKEN', token);
  props.setProperty('IMPACT_SPREADSHEET_ID', spreadsheetId);

  console.log('✅ Credentials stored securely in Script Properties');
  console.log('SID: ' + sid.substring(0, 8) + '...');
  console.log('Token: ' + token.substring(0, 8) + '...');

  return true;
}

// ============================================================================
// PUBLIC API FUNCTIONS
// ============================================================================

function runCompleteDiscovery() {
  const orchestrator = new UltraOptimizedOrchestrator();
  return orchestrator.runCompleteDiscovery();
}

/**
 * Force refresh all reports (ignore freshness checks)
 * Use this when you want to pull fresh data regardless of when it was last pulled
 */
function forceRefreshAllReports() {
  const orchestrator = new UltraOptimizedOrchestrator();

  // Disable resume and force refresh
  orchestrator.config.set('forceRefresh', true);
  orchestrator.config.set('enableResume', false);

  Logger.log('🔄 Force refresh mode enabled');
  Logger.log('   - All reports will be refreshed regardless of freshness');
  Logger.log('   - Resume logic disabled');
  Logger.log('   - Starting discovery...');
  Logger.log('');

  return orchestrator.runCompleteDiscovery({ forceRestart: true });
}

/**
 * Check current configuration that affects data freshness
 * Also outputs to console for easier viewing
 */
function checkRefreshConfiguration() {
  const config = new ImpactConfig();

  const configInfo = {
    enableResume: config.get('enableResume', true),
    enableDataFreshness: config.get('enableDataFreshness', true),
    freshnessHours: config.get('dataFreshnessHours', 24),
    forceRefresh: config.get('forceRefresh', false),
    enableDateFiltering: config.get('enableDateFiltering', false),
    startDate: config.get('startDate'),
    endDate: config.get('endDate')
  };

  console.log('=== REFRESH CONFIGURATION ===');
  console.log('');
  console.log('Resume Enabled: ' + configInfo.enableResume);
  console.log('Data Freshness Enabled: ' + configInfo.enableDataFreshness);
  console.log('Freshness Threshold: ' + configInfo.freshnessHours + ' hours');
  console.log('Force Refresh: ' + configInfo.forceRefresh);
  console.log('Date Filtering Enabled: ' + configInfo.enableDateFiltering);

  if (configInfo.enableDateFiltering) {
    console.log('⚠️  DATE FILTERING IS ENABLED!');
    console.log('   Start Date: ' + configInfo.startDate);
    console.log('   End Date: ' + configInfo.endDate);
    console.log('   This may limit what data is returned!');
  }

  console.log('');
  console.log('💡 To force refresh all data:');
  console.log('   Run: forceRefreshAllReports()');
  console.log('');
  console.log('💡 To disable date filtering:');
  console.log('   Run: clearDateRange()');

  // Also log to Logger for execution log
  Logger.log('=== REFRESH CONFIGURATION ===');
  Logger.log('Resume Enabled: ' + configInfo.enableResume);
  Logger.log('Data Freshness Enabled: ' + configInfo.enableDataFreshness);
  Logger.log('Freshness Threshold: ' + configInfo.freshnessHours + ' hours');
  Logger.log('Force Refresh: ' + configInfo.forceRefresh);
  Logger.log('Date Filtering Enabled: ' + configInfo.enableDateFiltering);

  if (configInfo.enableDateFiltering) {
    Logger.log('⚠️  DATE FILTERING IS ENABLED!');
    Logger.log('   Start Date: ' + configInfo.startDate);
    Logger.log('   End Date: ' + configInfo.endDate);
  }

  return configInfo;
}

function resumeDiscovery() {
  const orchestrator = new UltraOptimizedOrchestrator();

  // Ensure resume is enabled (may have been disabled by forceRefreshAllReports)
  orchestrator.config.set('enableResume', true);

  Logger.log('🔄 Resuming discovery from saved progress...');
  Logger.log('   - Resume logic enabled');
  Logger.log('   - Will skip already completed reports');
  Logger.log('');

  return orchestrator.runCompleteDiscovery({ forceRestart: false });
}

function restartDiscovery() {
  const orchestrator = new UltraOptimizedOrchestrator();
  const tracker = new EnhancedProgressTracker(orchestrator.config, orchestrator.metrics);
  tracker.clearAll();
  return orchestrator.runCompleteDiscovery({ forceRestart: true });
}

/**
 * Retry a specific large report with chunking enabled
 * Use this for reports that timeout during write operations
 * @param {string} reportId - The report ID to retry
 * @param {number} chunkSize - Optional chunk size (default: 25000 rows)
 */
function retryLargeReportWithChunking(reportId, chunkSize) {
  // Validate reportId parameter
  if (!reportId || typeof reportId !== 'string' || reportId.trim() === '') {
    const errorMsg = '❌ ERROR: Report ID is required!\n' +
      '   Usage: retryLargeReportWithChunking("report_id", chunkSize)\n' +
      '   Example: retryLargeReportWithChunking("mp_sku_exception_list_active_IO", 25000)';
    Logger.log(errorMsg);
    console.log(errorMsg);
    throw new Error('Report ID is required. Usage: retryLargeReportWithChunking("report_id", chunkSize)');
  }

  const config = new ImpactConfig();

  // Lower the chunking threshold for this retry
  const originalThreshold = config.get('maxRowsPerSheet', 50000);
  const newThreshold = chunkSize || 25000;

  // Set the new threshold in config (will be used by retryFailedReport)
  config.set('maxRowsPerSheet', newThreshold);

  Logger.log('🔄 Retrying large report with chunking enabled');
  Logger.log('   Report ID: ' + reportId);
  Logger.log('   Chunk size: ' + newThreshold + ' rows (was ' + originalThreshold + ')');
  Logger.log('');

  // Call retryFailedReport which will use the updated config
  return retryFailedReport(reportId, {
    forceRefresh: true
  });
}

/**
 * Refresh the summary sheet with all historical data
 * This will show all completed reports from previous runs
 */
function refreshSummarySheet() {
  const orchestrator = new UltraOptimizedOrchestrator();
  const completedReports = orchestrator.progressTracker.getCompletedReports();

  console.log('Refreshing summary sheet with historical data...');
  console.log('Found ' + completedReports.length + ' historical completed reports');

  // Create summary with historical data only
  orchestrator.spreadsheetManager.createSummarySheet([], []);

  console.log('Summary sheet refreshed with all historical data');
  return {
    historicalReports: completedReports.length,
    message: 'Summary sheet refreshed with all historical data'
  };
}

/**
 * Check data freshness for all reports
 */
function checkDataFreshness() {
  const orchestrator = new UltraOptimizedOrchestrator();
  const summary = orchestrator.spreadsheetManager.getFreshnessSummary();

  console.log('Data Freshness Summary:');
  console.log('Total Reports: ' + summary.totalReports);
  console.log('Fresh Reports: ' + summary.freshReports);
  console.log('Stale Reports: ' + summary.staleReports);

  if (summary.reports.length > 0) {
    console.log('\nReport Details:');
    summary.reports.forEach(report => {
      const status = report.isFresh ? '✅ FRESH' : '⚠️ STALE';
      console.log(`${status} ${report.reportId} - ${report.hoursSinceUpdate}h ago (${report.rowCount} rows)`);
    });
  }

  return summary;
}

/**
 * Force refresh all data (ignore freshness)
 */
function forceRefreshAllData() {
  const orchestrator = new UltraOptimizedOrchestrator();

  // Enable force refresh
  orchestrator.config.set('forceRefresh', true);

  console.log('Force refresh enabled - all data will be refreshed');
  console.log('Run runCompleteDiscovery() to refresh all data');

  return {
    message: 'Force refresh enabled',
    nextStep: 'Run runCompleteDiscovery() to refresh all data'
  };
}

/**
 * Set data freshness threshold (in hours)
 */
function setDataFreshnessHours(hours) {
  const orchestrator = new UltraOptimizedOrchestrator();
  orchestrator.config.set('dataFreshnessHours', hours);

  console.log('Data freshness threshold set to ' + hours + ' hours');
  console.log('Reports older than ' + hours + ' hours will be refreshed');

  return {
    message: 'Freshness threshold updated',
    hours: hours
  };
}

/**
 * Clear all freshness data (useful for full reset)
 */
function clearDataFreshness() {
  const orchestrator = new UltraOptimizedOrchestrator();
  orchestrator.spreadsheetManager.clearDataFreshness();

  console.log('All data freshness information cleared');
  console.log('Next run will refresh all data');

  return {
    message: 'Freshness data cleared',
    nextStep: 'Next run will refresh all data'
  };
}

/**
 * Get detailed freshness report
 */
function getDetailedFreshnessReport() {
  const orchestrator = new UltraOptimizedOrchestrator();
  const summary = orchestrator.spreadsheetManager.getFreshnessSummary();

  const report = {
    timestamp: new Date().toISOString(),
    configuration: {
      freshnessHours: orchestrator.config.get('dataFreshnessHours', 24),
      forceRefresh: orchestrator.config.get('forceRefresh', false),
      enableFreshness: orchestrator.config.get('enableDataFreshness', true)
    },
    summary: summary
  };

  console.log('Detailed Freshness Report:');
  console.log(JSON.stringify(report, null, 2));

  return report;
}

/**
 * Set date range for reports
 * @param {string} startDate - Start date in YYYY-MM-DD format
 * @param {string} endDate - End date in YYYY-MM-DD format
 */
function setDateRange(startDate, endDate) {
  const orchestrator = new UltraOptimizedOrchestrator();

  // Validate date format
  const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
  if (!dateRegex.test(startDate)) {
    throw new Error('Start date must be in YYYY-MM-DD format');
  }
  if (!dateRegex.test(endDate)) {
    throw new Error('End date must be in YYYY-MM-DD format');
  }

  // Validate date logic
  const start = new Date(startDate);
  const end = new Date(endDate);
  if (start > end) {
    throw new Error('Start date cannot be after end date');
  }

  orchestrator.config.set('enableDateFiltering', true);
  orchestrator.config.set('startDate', startDate);
  orchestrator.config.set('endDate', endDate);

  console.log('Date range set successfully:');
  console.log('Start Date: ' + startDate);
  console.log('End Date: ' + endDate);
  console.log('Run runCompleteDiscovery() to get reports for this date range');

  return {
    message: 'Date range set successfully',
    startDate: startDate,
    endDate: endDate,
    nextStep: 'Run runCompleteDiscovery() to get reports for this date range'
  };
}

/**
 * Set date range using preset
 * @param {string} preset - Preset name (august-2025, september-2025, october-2025, q3-2025, q4-2025)
 */
function setDateRangePreset(preset) {
  const orchestrator = new UltraOptimizedOrchestrator();
  const presets = orchestrator.config.get('dateRangePresets', {});

  if (!presets[preset]) {
    const availablePresets = Object.keys(presets).join(', ');
    throw new Error('Invalid preset. Available presets: ' + availablePresets);
  }

  const dateRange = presets[preset];
  return setDateRange(dateRange.start, dateRange.end);
}

/**
 * Clear date range filtering (get all data)
 */
function clearDateRange() {
  const orchestrator = new UltraOptimizedOrchestrator();

  orchestrator.config.set('enableDateFiltering', false);
  orchestrator.config.set('startDate', null);
  orchestrator.config.set('endDate', null);

  console.log('Date range filtering cleared');
  console.log('Next run will get all available data');

  return {
    message: 'Date range filtering cleared',
    nextStep: 'Next run will get all available data'
  };
}

/**
 * Get current date range configuration
 */
function getDateRangeConfig() {
  const orchestrator = new UltraOptimizedOrchestrator();

  const config = {
    enableDateFiltering: orchestrator.config.get('enableDateFiltering', false),
    startDate: orchestrator.config.get('startDate'),
    endDate: orchestrator.config.get('endDate'),
    availablePresets: Object.keys(orchestrator.config.get('dateRangePresets', {}))
  };

  console.log('Current Date Range Configuration:');
  console.log('Date Filtering Enabled: ' + config.enableDateFiltering);
  if (config.enableDateFiltering) {
    console.log('Start Date: ' + config.startDate);
    console.log('End Date: ' + config.endDate);
  }
  console.log('Available Presets: ' + config.availablePresets.join(', '));

  return config;
}

/**
 * Run discovery for specific months (August, September, October 2025)
 */
function runMonthlyReports2025() {
  console.log('Running reports for August, September, and October 2025...');

  const results = {
    august: null,
    september: null,
    october: null
  };

  try {
    // August 2025
    console.log('\n=== AUGUST 2025 ===');
    setDateRangePreset('august-2025');
    results.august = runCompleteDiscovery();

    // September 2025
    console.log('\n=== SEPTEMBER 2025 ===');
    setDateRangePreset('september-2025');
    results.september = runCompleteDiscovery();

    // October 2025
    console.log('\n=== OCTOBER 2025 ===');
    setDateRangePreset('october-2025');
    results.october = runCompleteDiscovery();

    console.log('\n=== MONTHLY REPORTS COMPLETE ===');
    console.log('August 2025: ' + (results.august.successful?.length || 0) + ' reports');
    console.log('September 2025: ' + (results.september.successful?.length || 0) + ' reports');
    console.log('October 2025: ' + (results.october.successful?.length || 0) + ' reports');

    return results;

  } catch (error) {
    console.log('Error running monthly reports: ' + error.message);
    throw error;
  }
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

/**
 * Run ONLY the SkuLevelAction report
 * This bypasses discovery and goes straight to the specific report
 */
function runSkuLevelActionOnly() {
  const config = new ImpactConfig();
  const metrics = new PerformanceMetrics();
  const logger = new EnhancedLogger(config, metrics);
  const apiClient = new EnhancedAPIClient(config, logger, metrics);
  const dataProcessor = new EnhancedDataProcessor(config, logger, metrics);
  const spreadsheetManager = new EnhancedSpreadsheetManager(config, logger, metrics);

  const reportId = 'SkuLevelActions';
  const reportName = 'SkuLevelAction';

  logger.info('Running SkuLevelAction report only', { reportId: reportId });

  try {
    // Step 1: Schedule the export
    logger.info('Scheduling SkuLevelAction export...');
    const job = apiClient.scheduleExport(reportId);
    logger.info('Export scheduled successfully', { jobId: job.jobId });

    // Step 2: Poll for completion
    logger.info('Waiting for export to complete...');
    const status = pollJobCompletion(job.jobId, apiClient, logger);

    if (status.status !== 'completed') {
      throw new Error('Export failed: ' + (status.error || 'Unknown error'));
    }

    logger.info('Export completed successfully');

    // Step 3: Download the data
    logger.info('Downloading export data...');
    const csvData = apiClient.downloadResult(status.resultUri);
    logger.info('Data downloaded', { size: csvData.length });

    // Step 4: Process the CSV data
    logger.info('Processing CSV data...');
    const processedData = dataProcessor.processCSVData(csvData);
    logger.info('Data processed', {
      rows: processedData.rowCount,
      columns: processedData.columnCount
    });

    // Step 5: Create the spreadsheet sheet
    logger.info('Creating spreadsheet sheet...');
    const sheetInfo = spreadsheetManager.createReportSheet(
      reportId,
      processedData,
      {
        name: reportName,
        jobId: job.jobId,
        scheduledAt: job.scheduledAt
      }
    );

    logger.info('SkuLevelAction report completed successfully!', {
      sheetName: sheetInfo.sheetName,
      rowCount: sheetInfo.rowCount,
      columnCount: sheetInfo.columnCount,
      chunked: sheetInfo.chunked
    });

    return {
      success: true,
      reportId: reportId,
      reportName: reportName,
      sheetName: sheetInfo.sheetName,
      rowCount: sheetInfo.rowCount,
      columnCount: sheetInfo.columnCount,
      chunked: sheetInfo.chunked,
      metrics: metrics.getSummary()
    };

  } catch (error) {
    logger.error('SkuLevelAction report failed', {
      error: error.message,
      reportId: reportId
    });

    return {
      success: false,
      reportId: reportId,
      reportName: reportName,
      error: error.message,
      metrics: metrics.getSummary()
    };
  }
}

/**
 * Helper function to poll job completion
 */
function pollJobCompletion(jobId, apiClient, logger) {
  const maxAttempts = 30;
  let delay = 3000;
  const maxDelay = 60000;
  const multiplier = 1.2;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const status = apiClient.checkJobStatus(jobId);

      if (status.status === 'completed') {
        logger.info('Job completed', { jobId: jobId, attempts: attempt + 1 });
        return status;
      }

      if (status.status === 'failed') {
        throw new Error('Job failed: ' + (status.error || 'Unknown error'));
      }

      delay = Math.min(delay * multiplier, maxDelay);
      logger.debug('Job polling', {
        jobId: jobId,
        status: status.status,
        attempt: attempt + 1,
        nextDelay: delay
      });

      Utilities.sleep(delay);

    } catch (error) {
      if (attempt === maxAttempts - 1) {
        throw error;
      }
      logger.warn('Polling error, retrying', {
        jobId: jobId,
        attempt: attempt + 1,
        error: error.message
      });
      Utilities.sleep(delay);
    }
  }

  throw new Error('Job polling timed out after ' + maxAttempts + ' attempts');
}

/**
 * Run SkuLevelAction with date range filtering
 * @param {string} startDate - Start date in YYYY-MM-DD format
 * @param {string} endDate - End date in YYYY-MM-DD format
 */
function runSkuLevelActionWithDateRange(startDate, endDate) {
  const config = new ImpactConfig();

  // Enable date filtering
  config.set('enableDateFiltering', true);
  config.set('startDate', startDate);
  config.set('endDate', endDate);

  Logger.log('Running SkuLevelAction with date range: ' + startDate + ' to ' + endDate);

  return runSkuLevelActionOnly();
}

/**
 * Analyze daily Mula revenue from SkuLevelAction data
 * Shows revenue per day and date range of the data
 */
function analyzeDailyMulaRevenue() {
  console.log('=== DAILY MULA REVENUE ANALYSIS ===\n');

  try {
    const config = new ImpactConfig();
    const metrics = new PerformanceMetrics();
    const logger = new EnhancedLogger(config, metrics);
    const spreadsheetManager = new EnhancedSpreadsheetManager(config, logger, metrics);

    const spreadsheet = spreadsheetManager.getSpreadsheet();
    const skuSheet = spreadsheet.getSheetByName('SkuLevelAction') ||
      spreadsheet.getSheetByName('SkuLevelActions');

    if (!skuSheet) {
      console.log('❌ No SkuLevelAction sheet found. Run runSkuLevelActionOnly() first.');
      return null;
    }

    const data = skuSheet.getDataRange().getValues();
    const headers = data[0];
    const rows = data.slice(1);

    // Find column indices
    const dateIndex = headers.findIndex(h =>
      h && (h.toLowerCase() === 'date' || h.toLowerCase() === 'period' || h.toLowerCase() === 'actiondate')
    );
    const saleAmountIndex = headers.findIndex(h =>
      h && (h.toLowerCase() === 'saleamount' || h.toLowerCase() === 'sale_amount' || h.toLowerCase() === 'revenue')
    );
    const earningsIndex = headers.findIndex(h =>
      h && (h.toLowerCase() === 'earnings' || h.toLowerCase() === 'payout' || h.toLowerCase() === 'commission')
    );
    const pubSubid1Index = headers.findIndex(h =>
      h && h.toLowerCase() === 'pubsubid1'
    );
    const subidIndex = headers.findIndex(h =>
      h && h.toLowerCase() === 'subid'
    );

    if (dateIndex === -1) {
      console.log('❌ Date column not found. Available columns:');
      headers.forEach((h, i) => console.log('  ' + (i + 1) + '. ' + h));
      return null;
    }

    if (saleAmountIndex === -1 && earningsIndex === -1) {
      console.log('❌ Neither SaleAmount nor Earnings column found.');
      console.log('Available columns:');
      headers.forEach((h, i) => console.log('  ' + (i + 1) + '. ' + h));
      return null;
    }

    // Show what we're using
    if (saleAmountIndex >= 0) {
      console.log('📊 Using SaleAmount column (Gross Sales)');
    }
    if (earningsIndex >= 0) {
      console.log('💰 Using Earnings column (Net Earnings/Commission)');
    }
    console.log('');

    // Filter for Mula data
    const mulaRows = rows.filter(row => {
      const pubSubid1 = pubSubid1Index >= 0 ? (row[pubSubid1Index] || '').toString().toLowerCase() : '';
      const subid = subidIndex >= 0 ? (row[subidIndex] || '').toString().toLowerCase() : '';
      return pubSubid1 === 'mula' || subid === 'mula' || subid.includes('mula');
    });

    console.log('📊 Total Mula records: ' + mulaRows.length);
    console.log('📊 Total records in sheet: ' + rows.length);
    console.log('');

    if (mulaRows.length === 0) {
      console.log('⚠️  No Mula records found. Check PubSubid1 or SubID columns.');
      return null;
    }

    // Group by date and calculate daily revenue
    const dailyRevenue = {};
    const dates = [];

    mulaRows.forEach(row => {
      const dateValue = row[dateIndex];
      const saleAmount = saleAmountIndex >= 0 ? parseFloat((row[saleAmountIndex] || 0).toString().replace(/[^0-9.-]/g, '')) || 0 : 0;
      const earnings = earningsIndex >= 0 ? parseFloat((row[earningsIndex] || 0).toString().replace(/[^0-9.-]/g, '')) || 0 : 0;

      if (!dateValue) return;

      // Parse date (handle various formats)
      let date;
      if (dateValue instanceof Date) {
        date = dateValue;
      } else if (typeof dateValue === 'string') {
        date = new Date(dateValue);
      } else {
        return;
      }

      if (isNaN(date.getTime())) return;

      // Format as YYYY-MM-DD for grouping
      const dateKey = date.toISOString().split('T')[0];

      if (!dailyRevenue[dateKey]) {
        dailyRevenue[dateKey] = {
          date: dateKey,
          revenue: 0,
          earnings: 0,
          conversions: 0,
          displayDate: date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
        };
        dates.push(dateKey);
      }

      dailyRevenue[dateKey].revenue += saleAmount;
      dailyRevenue[dateKey].earnings += earnings;
      dailyRevenue[dateKey].conversions += 1;
    });

    // Sort dates
    dates.sort();

    // Calculate totals and date range
    const totalRevenue = Object.values(dailyRevenue).reduce((sum, day) => sum + day.revenue, 0);
    const totalEarnings = Object.values(dailyRevenue).reduce((sum, day) => sum + day.earnings, 0);
    const totalConversions = Object.values(dailyRevenue).reduce((sum, day) => sum + day.conversions, 0);
    const dateRange = {
      start: dates[0],
      end: dates[dates.length - 1],
      days: dates.length
    };

    // Display results
    console.log('📅 DATE RANGE:');
    console.log('   Start: ' + (dateRange.start ? new Date(dateRange.start).toLocaleDateString() : 'N/A'));
    console.log('   End: ' + (dateRange.end ? new Date(dateRange.end).toLocaleDateString() : 'N/A'));
    console.log('   Days with data: ' + dateRange.days);
    console.log('');

    // Show both sales and earnings if available
    if (earningsIndex >= 0) {
      console.log('💰 DAILY BREAKDOWN (Sales vs Earnings):');
      console.log('Date'.padEnd(15) + 'Gross Sales'.padEnd(18) + 'Net Earnings'.padEnd(18) + 'Conversions'.padEnd(15) + 'Commission %');
      console.log('━'.repeat(80));

      // Display daily breakdown
      Object.keys(dailyRevenue).sort().forEach(date => {
        const day = dailyRevenue[date];
        const sales = day.sales || 0;
        const earnings = day.earnings || 0;
        const conversions = day.conversions || 0;
        const commission = sales > 0 ? ((earnings / sales) * 100).toFixed(2) : '0.00';

        console.log(
          date.padEnd(15) +
          '$' + sales.toFixed(2).padStart(17) +
          '$' + earnings.toFixed(2).padStart(17) +
          conversions.toString().padStart(14) +
          commission + '%'.padStart(4)
        );
      });

      console.log('');
    }

    return {
      dateRange: dateRange,
      dailyRevenue: dailyRevenue,
      totals: {
        revenue: totalRevenue,
        conversions: totalConversions,
        avgDailyRevenue: avgDailyRevenue,
        avgOrderValue: totalConversions > 0 ? totalRevenue / totalConversions : 0
      },
      mulaRecordCount: mulaRows.length,
      totalRecordCount: rows.length
    };

  } catch (error) {
    console.error('❌ Error analyzing daily Mula revenue: ' + error.message);
    console.error(error.stack);
    return null;
  }
}

/**
 * Check date range of any report sheet
 * @param {string} sheetName - Name of the sheet to check (optional, defaults to 'SkuLevelAction')
 */
function checkReportDateRange(sheetName) {
  // Default to SkuLevelAction if no sheet name provided
  if (!sheetName) {
    sheetName = 'SkuLevelAction';
    console.log('ℹ️  No sheet name provided, checking SkuLevelAction by default');
    console.log('   Use checkReportDateRange("SheetName") to check a specific sheet\n');
  }

  console.log('=== CHECKING REPORT DATE RANGE ===\n');
  console.log('Sheet: ' + sheetName);
  console.log('');

  try {
    const config = new ImpactConfig();
    const metrics = new PerformanceMetrics();
    const logger = new EnhancedLogger(config, metrics);
    const spreadsheetManager = new EnhancedSpreadsheetManager(config, logger, metrics);

    const spreadsheet = spreadsheetManager.getSpreadsheet();
    const sheet = spreadsheet.getSheetByName(sheetName);

    if (!sheet) {
      console.log('❌ Sheet "' + sheetName + '" not found.');
      console.log('\nAvailable sheets:');
      const allSheets = spreadsheet.getSheets();
      allSheets.forEach(s => console.log('  - ' + s.getName()));
      console.log('\n💡 Tip: Use checkReportDateRange("SheetName") with quotes around the sheet name');
      return null;
    }

    const data = sheet.getDataRange().getValues();
    if (data.length < 2) {
      console.log('⚠️  Sheet has no data rows.');
      return null;
    }

    const headers = data[0];
    const rows = data.slice(1);

    // Find date column using same patterns as findDateColumn()
    const datePatterns = [
      'date', 'period', 'actiondate', 'action_date',
      'transactiondate', 'transaction_date', 'eventdate', 'event_date',
      'clearingdate', 'clearing_date', 'postdate', 'post_date'
    ];

    const dateIndex = headers.findIndex(h => {
      if (!h) return false;
      const lower = h.toLowerCase().trim();
      return datePatterns.some(pattern => lower === pattern || lower.includes(pattern));
    });

    if (dateIndex === -1) {
      console.log('❌ No date column found. Available columns:');
      headers.forEach((h, i) => console.log('  ' + (i + 1) + '. ' + h));
      return null;
    }

    // Extract all dates
    const dates = [];
    rows.forEach(row => {
      const dateValue = row[dateIndex];
      if (!dateValue) return;

      let date;
      if (dateValue instanceof Date) {
        date = dateValue;
      } else if (typeof dateValue === 'string') {
        date = new Date(dateValue);
      } else {
        return;
      }

      if (!isNaN(date.getTime())) {
        dates.push(date);
      }
    });

    if (dates.length === 0) {
      console.log('⚠️  No valid dates found in the sheet.');
      return null;
    }

    dates.sort((a, b) => a - b);

    const startDate = dates[0];
    const endDate = dates[dates.length - 1];
    const days = Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24)) + 1;
    const daysWithData = new Set(dates.map(d => d.toISOString().split('T')[0])).size;

    console.log('📅 DATE RANGE:');
    console.log('   Start Date: ' + startDate.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' }));
    console.log('   End Date: ' + endDate.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' }));
    console.log('   Total Days in Range: ' + days);
    console.log('   Days with Data: ' + daysWithData);
    console.log('   Total Records: ' + rows.length);
    console.log('   Records with Dates: ' + dates.length);
    console.log('');

    return {
      sheetName: sheetName,
      startDate: startDate,
      endDate: endDate,
      totalDays: days,
      daysWithData: daysWithData,
      totalRecords: rows.length,
      recordsWithDates: dates.length
    };

  } catch (error) {
    console.error('❌ Error checking date range: ' + error.message);
    return null;
  }
}

/**
 * Quick helper: Check SkuLevelAction date range
 * Convenience function that checks the most common sheet
 */
function checkSkuDateRange() {
  return checkReportDateRange('SkuLevelAction');
}

/**
 * Quick helper: Check all report date ranges
 * Shows date ranges for all major report sheets
 */
function checkAllReportDateRanges() {
  console.log('=== CHECKING ALL REPORT DATE RANGES ===\n');

  const commonSheets = [
    'SkuLevelAction',
    'SkuLevelActions',
    'Performance by Day',
    'Performance by Month',
    'partner_performance_by_subid',
    'PerformanceByCampaign'
  ];

  const results = [];

  commonSheets.forEach(sheetName => {
    console.log('\n' + '='.repeat(50));
    const result = checkReportDateRange(sheetName);
    if (result) {
      results.push(result);
    }
  });

  console.log('\n' + '='.repeat(50));
  console.log('SUMMARY:');
  console.log('Checked ' + commonSheets.length + ' sheets');
  console.log('Found data in ' + results.length + ' sheets');

  if (results.length > 0) {
    console.log('\nDate Range Overview:');
    results.forEach(r => {
      const start = r.startDate.toLocaleDateString();
      const end = r.endDate.toLocaleDateString();
      console.log('  ' + r.sheetName + ': ' + start + ' to ' + end + ' (' + r.daysWithData + ' days)');
    });
  }

  return results;
}

// ============================================================================
// REPORT STATUS PARSING AND ERROR HANDLING
// ============================================================================

/**
 * Parse report status data from summary sheet or text format
 * Handles both tab-separated and multi-space-separated formats
 * @param {string} statusText - Tab-separated or formatted status text
 * @returns {Array} Array of parsed report status objects
 */
function parseReportStatuses(statusText) {
  const lines = statusText.split('\n').filter(line => line.trim());
  const reports = [];

  lines.forEach(line => {
    // Try tab-separated first, then fall back to multi-space separation
    let parts = line.split('\t');

    // If tab separation didn't work well, try splitting on multiple spaces
    if (parts.length < 6) {
      parts = line.split(/\s{2,}/); // Split on 2+ spaces
    }

    // If still not enough parts, try single space but be more careful
    if (parts.length < 6) {
      // For single space, we need to be smarter about parsing
      // Look for patterns: reportId, reportName, displayName, number, number, STATUS, notes...
      const spaceParts = line.trim().split(/\s+/);
      if (spaceParts.length >= 6) {
        // Try to identify where status starts (usually contains SUCCESS or ERROR)
        let statusIndex = -1;
        for (let i = 0; i < spaceParts.length; i++) {
          if (spaceParts[i].includes('SUCCESS') || spaceParts[i].includes('ERROR')) {
            statusIndex = i;
            break;
          }
        }

        if (statusIndex >= 5) {
          // Reconstruct parts
          parts = [
            spaceParts[0], // reportId
            spaceParts.slice(1, statusIndex - 2).join(' '), // reportName (may have spaces)
            spaceParts[statusIndex - 2], // displayName
            spaceParts[statusIndex - 1], // rows
            spaceParts[statusIndex], // columns (but this might be status)
            spaceParts[statusIndex] + (spaceParts[statusIndex + 1] ? ' ' + spaceParts[statusIndex + 1] : ''), // status
            spaceParts.slice(statusIndex + 2).join(' ') // notes
          ];
        } else {
          parts = spaceParts;
        }
      }
    }

    if (parts.length < 6) return;

    const reportId = parts[0].trim();
    const reportName = parts[1].trim();
    const displayName = parts[2].trim();
    const rows = parseInt(parts[3]) || 0;
    const columns = parseInt(parts[4]) || 0;
    const status = parts[5].trim();
    const notes = parts[6] || '';
    const processedAt = parts[7] || '';

    // Skip header rows and summary rows
    if (reportId === 'Report ID' || reportId === '=== COMPREHENSIVE STATISTICS ===' ||
      reportId.startsWith('===') || reportId === '') {
      return;
    }

    reports.push({
      reportId: reportId,
      reportName: reportName,
      displayName: displayName,
      rows: rows,
      columns: columns,
      status: status,
      notes: notes,
      processedAt: processedAt,
      isError: status.includes('ERROR'),
      isSuccess: status.includes('SUCCESS')
    });
  });

  return reports;
}

/**
 * Read report statuses directly from the DISCOVERY SUMMARY sheet
 * @param {string} spreadsheetId - Optional spreadsheet ID (uses config default if not provided)
 * @returns {Array} Array of parsed report status objects
 */
function readReportStatusesFromSheet(spreadsheetId = null) {
  const config = new ImpactConfig();
  const metrics = new PerformanceMetrics();
  const logger = new EnhancedLogger(config, metrics);
  const spreadsheetManager = new EnhancedSpreadsheetManager(config, logger, metrics);

  try {
    const spreadsheet = spreadsheetId ?
      SpreadsheetApp.openById(spreadsheetId) :
      spreadsheetManager.getSpreadsheet();

    const summarySheet = spreadsheet.getSheetByName('DISCOVERY SUMMARY');
    if (!summarySheet) {
      Logger.log('DISCOVERY SUMMARY sheet not found');
      return [];
    }

    const data = summarySheet.getDataRange().getValues();
    if (data.length < 2) {
      Logger.log('No data found in DISCOVERY SUMMARY sheet');
      return [];
    }

    // Convert sheet data to text format for parsing
    const statusText = data.slice(1) // Skip header
      .map(row => row.join('\t'))
      .join('\n');

    return parseReportStatuses(statusText);

  } catch (error) {
    Logger.log('Error reading report statuses from sheet: ' + error.message);
    return [];
  }
}

/**
 * Identify failed reports from status data
 * @param {string} statusText - Tab-separated status text
 * @returns {Array} Array of failed report objects with error details
 */
function identifyFailedReports(statusText) {
  const reports = parseReportStatuses(statusText);
  return reports.filter(r => r.isError);
}

/**
 * Retry a specific failed report
 * @param {string} reportId - The report ID to retry
 * @param {Object} options - Retry options
 * @returns {Object} Result object with success status and details
 */
function retryFailedReport(reportId, options = {}) {
  const config = new ImpactConfig();
  const metrics = new PerformanceMetrics();
  const logger = new EnhancedLogger(config, metrics);
  const apiClient = new EnhancedAPIClient(config, logger, metrics);
  const dataProcessor = new EnhancedDataProcessor(config, logger, metrics);
  const spreadsheetManager = new EnhancedSpreadsheetManager(config, logger, metrics);
  const orchestrator = new UltraOptimizedOrchestrator();

  logger.info('Retrying failed report', { reportId: reportId });

  try {
    // Step 1: Schedule the export
    const scheduleResponse = apiClient.scheduleExport(reportId);
    if (!scheduleResponse.success || !scheduleResponse.jobId) {
      throw new Error('Failed to schedule export: ' + (scheduleResponse.error || 'Unknown error'));
    }

    const jobId = scheduleResponse.jobId;
    logger.info('Export scheduled', { reportId: reportId, jobId: jobId });

    // Step 2: Poll for completion with enhanced timeout handling
    const status = orchestrator.pollJobCompletionOptimized(jobId);

    if (status.status !== 'completed') {
      throw new Error('Job did not complete: ' + (status.error || 'Unknown status'));
    }

    // Step 3: Download and process data
    const downloadResponse = apiClient.downloadExport(jobId);
    if (!downloadResponse.success || !downloadResponse.data) {
      throw new Error('Failed to download export: ' + (downloadResponse.error || 'Unknown error'));
    }

    // Step 4: Process and write to spreadsheet
    const processedData = dataProcessor.processExportData(
      downloadResponse.data,
      reportId,
      options.reportName || reportId
    );

    // Step 5: Write to spreadsheet
    const writeResult = spreadsheetManager.writeDataToSheet(
      processedData,
      reportId,
      options.reportName || reportId
    );

    logger.info('Report retry successful', {
      reportId: reportId,
      rows: writeResult.rowCount,
      columns: writeResult.columnCount
    });

    return {
      success: true,
      reportId: reportId,
      reportName: options.reportName || reportId,
      rowCount: writeResult.rowCount,
      columnCount: writeResult.columnCount,
      chunked: writeResult.chunked,
      chunkCount: writeResult.chunkCount,
      processedAt: new Date()
    };

  } catch (error) {
    logger.error('Report retry failed', {
      reportId: reportId,
      error: error.message
    });

    return {
      success: false,
      reportId: reportId,
      error: error.message,
      metrics: metrics.getSummary()
    };
  }
}

/**
 * Retry multiple failed reports
 * @param {Array} reportIds - Array of report IDs to retry
 * @param {Object} options - Retry options
 * @returns {Object} Summary of retry results
 */
function retryFailedReports(reportIds, options = {}) {
  const config = new ImpactConfig();
  const metrics = new PerformanceMetrics();
  const logger = new EnhancedLogger(config, metrics);

  logger.info('Retrying multiple failed reports', {
    count: reportIds.length,
    reportIds: reportIds
  });

  const results = {
    successful: [],
    failed: [],
    total: reportIds.length
  };

  reportIds.forEach((reportId, index) => {
    logger.info('Retrying report ' + (index + 1) + ' of ' + reportIds.length, {
      reportId: reportId
    });

    try {
      const result = retryFailedReport(reportId, options);
      if (result.success) {
        results.successful.push(result);
      } else {
        results.failed.push({
          reportId: reportId,
          error: result.error
        });
      }
    } catch (error) {
      results.failed.push({
        reportId: reportId,
        error: error.message
      });
    }

    // Add delay between retries to avoid rate limiting
    if (index < reportIds.length - 1) {
      Utilities.sleep(options.delayBetweenRetries || 2000);
    }
  });

  logger.info('Batch retry completed', {
    successful: results.successful.length,
    failed: results.failed.length,
    total: results.total
  });

  return results;
}

/**
 * Retry reports that failed with specific error types
 * @param {string} statusText - Status text from summary (optional if reading from sheet)
 * @param {Array} errorTypes - Array of error types to retry (e.g., ['timeout', 'unknown'])
 * @param {boolean} readFromSheet - If true, read statuses from DISCOVERY SUMMARY sheet
 * @returns {Object} Summary of retry results
 */
function retryReportsByErrorType(statusText = null, errorTypes = ['timeout', 'unknown'], readFromSheet = false) {
  let failedReports;

  if (readFromSheet || !statusText) {
    const allReports = readReportStatusesFromSheet();
    failedReports = allReports.filter(r => r.isError);
  } else {
    failedReports = identifyFailedReports(statusText);
  }

  // Filter by error type
  const reportsToRetry = failedReports.filter(report => {
    const errorLower = (report.notes || '').toLowerCase();
    return errorTypes.some(type => errorLower.includes(type));
  });

  if (reportsToRetry.length === 0) {
    Logger.log('No reports found matching error types: ' + errorTypes.join(', '));
    return { successful: [], failed: [], total: 0 };
  }

  Logger.log('Found ' + reportsToRetry.length + ' reports to retry:');
  reportsToRetry.forEach(r => {
    Logger.log('  - ' + r.reportId + ': ' + r.notes);
  });

  const reportIds = reportsToRetry.map(r => r.reportId);
  return retryFailedReports(reportIds);
}

/**
 * Retry the two currently failing reports:
 * - mp_monthly_close (Unknown error)
 * - mp_sku_exception_list_active_IO (Timeout)
 */
function retryCurrentFailedReports() {
  Logger.log('=== RETRYING CURRENTLY FAILED REPORTS ===\n');

  const failedReportIds = [
    'mp_monthly_close',
    'mp_sku_exception_list_active_IO'
  ];

  Logger.log('Reports to retry:');
  failedReportIds.forEach(id => Logger.log('  - ' + id));
  Logger.log('');

  const results = retryFailedReports(failedReportIds, {
    delayBetweenRetries: 3000 // 3 second delay between retries
  });

  Logger.log('\n=== RETRY RESULTS ===');
  Logger.log('Successful: ' + results.successful.length);
  Logger.log('Failed: ' + results.failed.length);
  Logger.log('Total: ' + results.total);

  if (results.successful.length > 0) {
    Logger.log('\n✅ Successfully retried:');
    results.successful.forEach(r => {
      Logger.log('  - ' + r.reportId + ': ' + r.rowCount + ' rows');
    });
  }

  if (results.failed.length > 0) {
    Logger.log('\n❌ Failed to retry:');
    results.failed.forEach(r => {
      Logger.log('  - ' + r.reportId + ': ' + r.error);
    });
  }

  return results;
}

/**
 * Get error summary from status text or sheet
 * @param {string} statusText - Status text from summary (optional if reading from sheet)
 * @param {boolean} readFromSheet - If true, read statuses from DISCOVERY SUMMARY sheet
 * @returns {Object} Error summary with counts and details
 */
function getErrorSummary(statusText = null, readFromSheet = false) {
  let reports;

  if (readFromSheet || !statusText) {
    reports = readReportStatusesFromSheet();
  } else {
    reports = parseReportStatuses(statusText);
  }

  const failed = reports.filter(r => r.isError);
  const successful = reports.filter(r => r.isSuccess);

  // Group errors by type
  const errorTypes = {};
  failed.forEach(report => {
    const errorNote = (report.notes || '').toLowerCase();
    let errorType = 'unknown';

    if (errorNote.includes('timeout')) {
      errorType = 'timeout';
    } else if (errorNote.includes('unknown error')) {
      errorType = 'unknown';
    } else if (errorNote.includes('failed')) {
      errorType = 'failed';
    } else if (errorNote.includes('error')) {
      errorType = 'error';
    }

    if (!errorTypes[errorType]) {
      errorTypes[errorType] = [];
    }
    errorTypes[errorType].push(report);
  });

  return {
    total: reports.length,
    successful: successful.length,
    failed: failed.length,
    successRate: reports.length > 0 ? ((successful.length / reports.length) * 100).toFixed(1) + '%' : '0%',
    errorTypes: errorTypes,
    failedReports: failed
  };
}

/**
 * Display error summary in a readable format
 * @param {string} statusText - Optional status text (will read from sheet if not provided)
 * @param {boolean} readFromSheet - If true, read statuses from DISCOVERY SUMMARY sheet
 */
function displayErrorSummary(statusText = null, readFromSheet = true) {
  const summary = getErrorSummary(statusText, readFromSheet);

  Logger.log('\n=== ERROR SUMMARY ===');
  Logger.log('Total Reports: ' + summary.total);
  Logger.log('Successful: ' + summary.successful);
  Logger.log('Failed: ' + summary.failed);
  Logger.log('Success Rate: ' + summary.successRate);
  Logger.log('');

  if (summary.failed > 0) {
    Logger.log('Failed Reports by Error Type:');
    Object.keys(summary.errorTypes).forEach(errorType => {
      const reports = summary.errorTypes[errorType];
      Logger.log('  ' + errorType.toUpperCase() + ': ' + reports.length);
      reports.forEach(r => {
        Logger.log('    - ' + r.reportId + ': ' + r.notes.substring(0, 80));
      });
    });
  } else {
    Logger.log('✅ No failed reports!');
  }

  return summary;
}

// ============================================================================
// DATA FRESHNESS ANALYSIS
// ============================================================================

/**
 * Parse a date string in various formats
 * @param {string} dateStr - Date string to parse
 * @returns {Date|null} Parsed date or null if invalid
 */
function parseDateString(dateStr) {
  if (!dateStr || dateStr === 'N/A') return null;

  // Try parsing as-is
  let date = new Date(dateStr);
  if (!isNaN(date.getTime())) return date;

  // Try common formats
  // Format: "11/15/2025, 1:32:12 PM" or "11/11/2025, 9:59:18 AM"
  const formats = [
    /(\d{1,2})\/(\d{1,2})\/(\d{4}),\s+(\d{1,2}):(\d{2}):(\d{2})\s+(AM|PM)/i,
    /(\d{4})-(\d{2})-(\d{2})/,
    /(\d{1,2})\/(\d{1,2})\/(\d{4})/
  ];

  for (const format of formats) {
    const match = dateStr.match(format);
    if (match) {
      if (format === formats[0]) {
        // MM/DD/YYYY, HH:MM:SS AM/PM
        const month = parseInt(match[1]) - 1;
        const day = parseInt(match[2]);
        const year = parseInt(match[3]);
        let hour = parseInt(match[4]);
        const minute = parseInt(match[5]);
        const second = parseInt(match[6]);
        const ampm = match[7].toUpperCase();

        if (ampm === 'PM' && hour !== 12) hour += 12;
        if (ampm === 'AM' && hour === 12) hour = 0;

        date = new Date(year, month, day, hour, minute, second);
        if (!isNaN(date.getTime())) return date;
      } else if (format === formats[1]) {
        // YYYY-MM-DD
        date = new Date(match[1], parseInt(match[2]) - 1, match[3]);
        if (!isNaN(date.getTime())) return date;
      } else if (format === formats[2]) {
        // MM/DD/YYYY
        date = new Date(parseInt(match[3]), parseInt(match[1]) - 1, match[2]);
        if (!isNaN(date.getTime())) return date;
      }
    }
  }

  return null;
}

/**
 * Calculate data freshness metrics for reports
 * @param {string} statusText - Optional status text (will read from sheet if not provided)
 * @param {boolean} readFromSheet - If true, read statuses from DISCOVERY SUMMARY sheet
 * @param {number} staleThresholdHours - Hours after which data is considered stale (default: 24)
 * @returns {Object} Freshness analysis with age, freshness status, and recommendations
 */
function analyzeDataFreshness(statusText = null, readFromSheet = true, staleThresholdHours = 24) {
  let reports;

  if (readFromSheet || !statusText) {
    reports = readReportStatusesFromSheet();
  } else {
    reports = parseReportStatuses(statusText);
  }

  const now = new Date();
  const staleThresholdMs = staleThresholdHours * 60 * 60 * 1000;

  const freshnessData = reports.map(report => {
    const processedDate = parseDateString(report.processedAt);

    if (!processedDate) {
      return {
        ...report,
        processedDate: null,
        ageHours: null,
        ageDays: null,
        isStale: true,
        freshnessStatus: 'UNKNOWN',
        freshnessMessage: 'No processed date available'
      };
    }

    const ageMs = now - processedDate;
    const ageHours = Math.floor(ageMs / (60 * 60 * 1000));
    const ageDays = Math.floor(ageHours / 24);
    const isStale = ageMs > staleThresholdMs;

    let freshnessStatus;
    let freshnessMessage;

    if (ageHours < 1) {
      freshnessStatus = 'FRESH';
      freshnessMessage = 'Less than 1 hour old';
    } else if (ageHours < 6) {
      freshnessStatus = 'FRESH';
      freshnessMessage = ageHours + ' hours old';
    } else if (ageHours < 24) {
      freshnessStatus = 'RECENT';
      freshnessMessage = ageHours + ' hours old (' + ageDays + ' day' + (ageDays !== 1 ? 's' : '') + ')';
    } else if (ageDays < 7) {
      freshnessStatus = 'AGING';
      freshnessMessage = ageDays + ' day' + (ageDays !== 1 ? 's' : '') + ' old';
    } else {
      freshnessStatus = 'STALE';
      freshnessMessage = ageDays + ' day' + (ageDays !== 1 ? 's' : '') + ' old';
    }

    return {
      ...report,
      processedDate: processedDate,
      ageHours: ageHours,
      ageDays: ageDays,
      isStale: isStale,
      freshnessStatus: freshnessStatus,
      freshnessMessage: freshnessMessage
    };
  });

  // Calculate statistics
  const successfulReports = freshnessData.filter(r => r.isSuccess);
  const staleReports = freshnessData.filter(r => r.isStale && r.isSuccess);
  const freshReports = freshnessData.filter(r => !r.isStale && r.isSuccess);

  const oldestReport = successfulReports
    .filter(r => r.processedDate)
    .sort((a, b) => a.processedDate - b.processedDate)[0];

  const newestReport = successfulReports
    .filter(r => r.processedDate)
    .sort((a, b) => b.processedDate - a.processedDate)[0];

  const avgAgeHours = successfulReports
    .filter(r => r.ageHours !== null)
    .reduce((sum, r) => sum + r.ageHours, 0) /
    successfulReports.filter(r => r.ageHours !== null).length;

  return {
    totalReports: reports.length,
    successfulReports: successfulReports.length,
    freshReports: freshReports.length,
    staleReports: staleReports.length,
    stalePercentage: successfulReports.length > 0 ?
      ((staleReports.length / successfulReports.length) * 100).toFixed(1) + '%' : '0%',
    avgAgeHours: avgAgeHours ? Math.round(avgAgeHours * 10) / 10 : null,
    oldestReport: oldestReport ? {
      reportId: oldestReport.reportId,
      reportName: oldestReport.reportName,
      processedAt: oldestReport.processedAt,
      ageDays: oldestReport.ageDays,
      freshnessMessage: oldestReport.freshnessMessage
    } : null,
    newestReport: newestReport ? {
      reportId: newestReport.reportId,
      reportName: newestReport.reportName,
      processedAt: newestReport.processedAt,
      ageHours: newestReport.ageHours,
      freshnessMessage: newestReport.freshnessMessage
    } : null,
    reports: freshnessData,
    staleThresholdHours: staleThresholdHours
  };
}

/**
 * Display data freshness analysis in a readable format
 * @param {string} statusText - Optional status text (will read from sheet if not provided)
 * @param {boolean} readFromSheet - If true, read statuses from DISCOVERY SUMMARY sheet
 * @param {number} staleThresholdHours - Hours after which data is considered stale (default: 24)
 */
function displayDataFreshness(statusText = null, readFromSheet = true, staleThresholdHours = 24) {
  const analysis = analyzeDataFreshness(statusText, readFromSheet, staleThresholdHours);

  Logger.log('\n=== DATA FRESHNESS ANALYSIS ===');
  Logger.log('');
  Logger.log('📊 SUMMARY:');
  Logger.log('  Total Reports: ' + analysis.totalReports);
  Logger.log('  Successful Reports: ' + analysis.successfulReports);
  Logger.log('  Fresh Reports (< ' + staleThresholdHours + ' hours): ' + analysis.freshReports);
  Logger.log('  Stale Reports (≥ ' + staleThresholdHours + ' hours): ' + analysis.staleReports);
  Logger.log('  Stale Percentage: ' + analysis.stalePercentage);

  if (analysis.avgAgeHours !== null) {
    Logger.log('  Average Age: ' + analysis.avgAgeHours + ' hours');
  }

  Logger.log('');

  if (analysis.oldestReport) {
    Logger.log('📅 OLDEST DATA:');
    Logger.log('  Report: ' + analysis.oldestReport.reportId);
    Logger.log('  Name: ' + analysis.oldestReport.reportName);
    Logger.log('  Processed At: ' + analysis.oldestReport.processedAt);
    Logger.log('  Age: ' + analysis.oldestReport.freshnessMessage);
    Logger.log('');
  }

  if (analysis.newestReport) {
    Logger.log('🆕 NEWEST DATA:');
    Logger.log('  Report: ' + analysis.newestReport.reportId);
    Logger.log('  Name: ' + analysis.newestReport.reportName);
    Logger.log('  Processed At: ' + analysis.newestReport.processedAt);
    Logger.log('  Age: ' + analysis.newestReport.freshnessMessage);
    Logger.log('');
  }

  if (analysis.staleReports > 0) {
    Logger.log('⚠️  STALE REPORTS (need refresh):');
    const stale = analysis.reports
      .filter(r => r.isStale && r.isSuccess)
      .sort((a, b) => (b.ageHours || 0) - (a.ageHours || 0))
      .slice(0, 10); // Show top 10 oldest

    stale.forEach(r => {
      Logger.log('  - ' + r.reportId + ': ' + r.freshnessMessage +
        ' (processed: ' + r.processedAt + ')');
    });

    if (analysis.staleReports > 10) {
      Logger.log('  ... and ' + (analysis.staleReports - 10) + ' more stale reports');
    }
    Logger.log('');
  }

  Logger.log('💡 KEY INSIGHTS:');
  Logger.log('  • "Processed At" = When data was actually pulled from API (THIS IS THE FRESHNESS DATE)');
  Logger.log('  • "Last verified" = When summary was checked (NOT when data was refreshed)');
  Logger.log('  • Data older than ' + staleThresholdHours + ' hours is considered stale');
  Logger.log('');

  return analysis;
}

/**
 * Get a quick freshness summary for a specific report
 * @param {string} reportId - Report ID to check
 * @param {boolean} readFromSheet - If true, read from DISCOVERY SUMMARY sheet
 * @returns {Object} Freshness info for the report
 */
function getReportFreshness(reportId, readFromSheet = true) {
  const reports = readFromSheet ?
    readReportStatusesFromSheet() :
    parseReportStatuses('');

  const report = reports.find(r => r.reportId === reportId);
  if (!report) {
    return {
      found: false,
      message: 'Report not found: ' + reportId
    };
  }

  const processedDate = parseDateString(report.processedAt);
  if (!processedDate) {
    return {
      found: true,
      reportId: reportId,
      reportName: report.reportName,
      processedAt: report.processedAt,
      freshnessStatus: 'UNKNOWN',
      message: 'No processed date available - cannot determine freshness'
    };
  }

  const now = new Date();
  const ageMs = now - processedDate;
  const ageHours = Math.floor(ageMs / (60 * 60 * 1000));
  const ageDays = Math.floor(ageHours / 24);

  let freshnessStatus;
  if (ageHours < 6) {
    freshnessStatus = 'FRESH';
  } else if (ageHours < 24) {
    freshnessStatus = 'RECENT';
  } else if (ageDays < 7) {
    freshnessStatus = 'AGING';
  } else {
    freshnessStatus = 'STALE';
  }

  return {
    found: true,
    reportId: reportId,
    reportName: report.reportName,
    processedAt: report.processedAt,
    processedDate: processedDate.toISOString(),
    ageHours: ageHours,
    ageDays: ageDays,
    freshnessStatus: freshnessStatus,
    message: ageHours < 24 ?
      'Data is ' + ageHours + ' hours old' :
      'Data is ' + ageDays + ' day' + (ageDays !== 1 ? 's' : '') + ' old'
  };
}

// ============================================================================
// DATA RANGE ANALYSIS (What dates are covered in the actual data)
// ============================================================================

/**
 * Find date column in sheet headers
 * @param {Array} headers - Array of header strings
 * @returns {number} Index of date column or -1 if not found
 */
function findDateColumn(headers) {
  const datePatterns = [
    'date', 'period', 'actiondate', 'action_date',
    'transactiondate', 'transaction_date', 'eventdate', 'event_date',
    'clearingdate', 'clearing_date', 'postdate', 'post_date'
  ];

  return headers.findIndex(h => {
    if (!h) return false;
    const lower = h.toLowerCase().trim();
    return datePatterns.some(pattern => lower === pattern || lower.includes(pattern));
  });
}

/**
 * Extract date range from a sheet's actual data
 * @param {string} sheetName - Name of the sheet to analyze
 * @param {string} spreadsheetId - Optional spreadsheet ID
 * @returns {Object} Date range information or null if not found
 */
function extractDataDateRange(sheetName, spreadsheetId = null) {
  try {
    const config = new ImpactConfig();
    const metrics = new PerformanceMetrics();
    const logger = new EnhancedLogger(config, metrics);
    const spreadsheetManager = new EnhancedSpreadsheetManager(config, logger, metrics);

    const spreadsheet = spreadsheetId ?
      SpreadsheetApp.openById(spreadsheetId) :
      spreadsheetManager.getSpreadsheet();

    const sheet = spreadsheet.getSheetByName(sheetName);
    if (!sheet) {
      return {
        found: false,
        error: 'Sheet not found: ' + sheetName
      };
    }

    const data = sheet.getDataRange().getValues();
    if (data.length < 2) {
      return {
        found: true,
        sheetName: sheetName,
        hasData: false,
        error: 'Sheet has no data rows'
      };
    }

    const headers = data[0];
    const rows = data.slice(1);
    const dateIndex = findDateColumn(headers);

    if (dateIndex === -1) {
      return {
        found: true,
        sheetName: sheetName,
        hasData: true,
        hasDateColumn: false,
        error: 'No date column found',
        availableColumns: headers.filter(h => h)
      };
    }

    // Extract all dates
    const dates = [];
    rows.forEach((row, rowIndex) => {
      const dateValue = row[dateIndex];
      if (!dateValue) return;

      let date;
      if (dateValue instanceof Date) {
        date = dateValue;
      } else if (typeof dateValue === 'string') {
        date = new Date(dateValue);
      } else if (typeof dateValue === 'number') {
        // Excel serial date number
        date = new Date((dateValue - 25569) * 86400 * 1000);
      } else {
        return;
      }

      if (!isNaN(date.getTime())) {
        dates.push(date);
      }
    });

    if (dates.length === 0) {
      return {
        found: true,
        sheetName: sheetName,
        hasData: true,
        hasDateColumn: true,
        hasValidDates: false,
        error: 'No valid dates found in date column'
      };
    }

    dates.sort((a, b) => a - b);

    const startDate = dates[0];
    const endDate = dates[dates.length - 1];
    const totalDays = Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24)) + 1;
    const uniqueDates = new Set(dates.map(d => d.toISOString().split('T')[0]));
    const daysWithData = uniqueDates.size;

    // Check for gaps (missing days in range)
    const gaps = [];
    const start = new Date(startDate);
    const end = new Date(endDate);
    const dateSet = uniqueDates;

    for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
      const dateKey = d.toISOString().split('T')[0];
      if (!dateSet.has(dateKey)) {
        gaps.push(new Date(d));
      }
    }

    return {
      found: true,
      sheetName: sheetName,
      hasData: true,
      hasDateColumn: true,
      hasValidDates: true,
      dateColumnName: headers[dateIndex],
      startDate: startDate,
      endDate: endDate,
      startDateISO: startDate.toISOString().split('T')[0],
      endDateISO: endDate.toISOString().split('T')[0],
      totalDays: totalDays,
      daysWithData: daysWithData,
      coveragePercentage: ((daysWithData / totalDays) * 100).toFixed(1) + '%',
      totalRecords: rows.length,
      recordsWithDates: dates.length,
      hasGaps: gaps.length > 0,
      gapCount: gaps.length,
      gaps: gaps.slice(0, 10), // First 10 gaps
      allGaps: gaps
    };

  } catch (error) {
    return {
      found: false,
      error: error.message,
      sheetName: sheetName
    };
  }
}

/**
 * Analyze data ranges for all reports from summary
 * Combines freshness (when pulled) with actual data ranges (what dates are covered)
 * @param {boolean} readFromSheet - If true, read from DISCOVERY SUMMARY sheet
 * @returns {Object} Comprehensive analysis of data ranges and freshness
 */
function analyzeAllDataRanges(readFromSheet = true) {
  const reports = readFromSheet ?
    readReportStatusesFromSheet() :
    parseReportStatuses('');

  const config = new ImpactConfig();
  const metrics = new PerformanceMetrics();
  const logger = new EnhancedLogger(config, metrics);
  const spreadsheetManager = new EnhancedSpreadsheetManager(config, logger, metrics);

  const successfulReports = reports.filter(r => r.isSuccess);
  const analysis = {
    totalReports: reports.length,
    successfulReports: successfulReports.length,
    analyzedReports: 0,
    reportsWithDataRanges: [],
    reportsWithoutDataRanges: [],
    overallDateRange: null,
    dateRangeStats: {
      earliestDate: null,
      latestDate: null,
      totalDaysCovered: 0,
      uniqueDates: new Set()
    }
  };

  successfulReports.forEach(report => {
    const sheetName = report.sheetName || report.reportId || report.reportName;
    if (!sheetName || sheetName === 'N/A') {
      analysis.reportsWithoutDataRanges.push({
        reportId: report.reportId,
        reportName: report.reportName,
        reason: 'No sheet name available'
      });
      return;
    }

    analysis.analyzedReports++;
    const dateRange = extractDataDateRange(sheetName);

    if (dateRange.found && dateRange.hasValidDates) {
      const reportAnalysis = {
        reportId: report.reportId,
        reportName: report.reportName,
        sheetName: sheetName,
        processedAt: report.processedAt,
        processedDate: parseDateString(report.processedAt),
        dataStartDate: dateRange.startDate,
        dataEndDate: dateRange.endDate,
        dataStartDateISO: dateRange.startDateISO,
        dataEndDateISO: dateRange.endDateISO,
        daysWithData: dateRange.daysWithData,
        totalDays: dateRange.totalDays,
        coveragePercentage: dateRange.coveragePercentage,
        totalRecords: dateRange.totalRecords,
        hasGaps: dateRange.hasGaps,
        gapCount: dateRange.gapCount,
        dateColumnName: dateRange.dateColumnName
      };

      // Calculate age of data (how old is the newest data point)
      if (dateRange.endDate) {
        const now = new Date();
        const dataAgeMs = now - dateRange.endDate;
        reportAnalysis.newestDataAgeHours = Math.floor(dataAgeMs / (60 * 60 * 1000));
        reportAnalysis.newestDataAgeDays = Math.floor(reportAnalysis.newestDataAgeHours / 24);
      }

      analysis.reportsWithDataRanges.push(reportAnalysis);

      // Update overall stats
      if (!analysis.dateRangeStats.earliestDate || dateRange.startDate < analysis.dateRangeStats.earliestDate) {
        analysis.dateRangeStats.earliestDate = dateRange.startDate;
      }
      if (!analysis.dateRangeStats.latestDate || dateRange.endDate > analysis.dateRangeStats.latestDate) {
        analysis.dateRangeStats.latestDate = dateRange.endDate;
      }
    } else {
      analysis.reportsWithoutDataRanges.push({
        reportId: report.reportId,
        reportName: report.reportName,
        sheetName: sheetName,
        reason: dateRange.error || 'Could not extract date range',
        hasDateColumn: dateRange.hasDateColumn || false
      });
    }
  });

  // Calculate overall date range
  if (analysis.dateRangeStats.earliestDate && analysis.dateRangeStats.latestDate) {
    const totalDays = Math.ceil(
      (analysis.dateRangeStats.latestDate - analysis.dateRangeStats.earliestDate) /
      (1000 * 60 * 60 * 24)
    ) + 1;

    analysis.overallDateRange = {
      startDate: analysis.dateRangeStats.earliestDate,
      endDate: analysis.dateRangeStats.latestDate,
      startDateISO: analysis.dateRangeStats.earliestDate.toISOString().split('T')[0],
      endDateISO: analysis.dateRangeStats.latestDate.toISOString().split('T')[0],
      totalDays: totalDays
    };
  }

  return analysis;
}

/**
 * Display comprehensive data range analysis
 * Shows both when data was pulled (freshness) and what dates are covered (data range)
 * @param {boolean} readFromSheet - If true, read from DISCOVERY SUMMARY sheet
 */
function displayAllDataRanges(readFromSheet = true) {
  const analysis = analyzeAllDataRanges(readFromSheet);

  Logger.log('\n=== COMPREHENSIVE DATA RANGE ANALYSIS ===');
  Logger.log('');
  Logger.log('📊 SUMMARY:');
  Logger.log('  Total Reports: ' + analysis.totalReports);
  Logger.log('  Successful Reports: ' + analysis.successfulReports);
  Logger.log('  Reports Analyzed: ' + analysis.analyzedReports);
  Logger.log('  Reports with Date Ranges: ' + analysis.reportsWithDataRanges.length);
  Logger.log('  Reports without Date Ranges: ' + analysis.reportsWithoutDataRanges.length);
  Logger.log('');

  if (analysis.overallDateRange) {
    Logger.log('📅 OVERALL DATA DATE RANGE (across all reports):');
    Logger.log('  Start Date: ' + analysis.overallDateRange.startDate.toLocaleDateString('en-US', {
      month: 'long', day: 'numeric', year: 'numeric'
    }));
    Logger.log('  End Date: ' + analysis.overallDateRange.endDate.toLocaleDateString('en-US', {
      month: 'long', day: 'numeric', year: 'numeric'
    }));
    Logger.log('  Total Days in Range: ' + analysis.overallDateRange.totalDays);
    Logger.log('');
  }

  if (analysis.reportsWithDataRanges.length > 0) {
    Logger.log('📋 REPORTS WITH DATA RANGES:');
    Logger.log('');

    // Sort by data end date (newest first)
    const sorted = analysis.reportsWithDataRanges.sort((a, b) =>
      (b.dataEndDate || 0) - (a.dataEndDate || 0)
    );

    sorted.slice(0, 20).forEach(r => {
      Logger.log('  ' + r.reportId + ' (' + r.sheetName + '):');
      Logger.log('    Data Range: ' + r.dataStartDateISO + ' to ' + r.dataEndDateISO);
      Logger.log('    Days with Data: ' + r.daysWithData + ' / ' + r.totalDays +
        ' (' + r.coveragePercentage + ' coverage)');
      Logger.log('    Records: ' + r.totalRecords);
      if (r.newestDataAgeDays !== undefined) {
        Logger.log('    Newest Data Age: ' + r.newestDataAgeDays + ' day' +
          (r.newestDataAgeDays !== 1 ? 's' : '') + ' old');
      }
      if (r.hasGaps) {
        Logger.log('    ⚠️  Has ' + r.gapCount + ' gap' + (r.gapCount !== 1 ? 's' : '') + ' in date coverage');
      }
      if (r.processedAt) {
        Logger.log('    Pulled At: ' + r.processedAt);
      }
      Logger.log('');
    });

    if (sorted.length > 20) {
      Logger.log('  ... and ' + (sorted.length - 20) + ' more reports');
      Logger.log('');
    }
  }

  if (analysis.reportsWithoutDataRanges.length > 0) {
    Logger.log('⚠️  REPORTS WITHOUT DATE RANGES:');
    analysis.reportsWithoutDataRanges.slice(0, 10).forEach(r => {
      Logger.log('  - ' + r.reportId + ': ' + r.reason);
    });
    if (analysis.reportsWithoutDataRanges.length > 10) {
      Logger.log('  ... and ' + (analysis.reportsWithoutDataRanges.length - 10) + ' more');
    }
    Logger.log('');
  }

  Logger.log('💡 KEY INSIGHTS:');
  Logger.log('  • "Data Range" = What dates are actually covered in the data');
  Logger.log('  • "Pulled At" = When the data was fetched from the API');
  Logger.log('  • "Newest Data Age" = How old is the newest data point (may lag behind pull date)');
  Logger.log('  • Gaps indicate missing days in the date range');
  Logger.log('');

  return analysis;
}

/**
 * Get data range for a specific report
 * @param {string} reportId - Report ID to check
 * @param {boolean} readFromSheet - If true, read from DISCOVERY SUMMARY sheet
 * @returns {Object} Combined freshness and data range info
 */
function getReportDataRange(reportId, readFromSheet = true) {
  const reports = readFromSheet ?
    readReportStatusesFromSheet() :
    parseReportStatuses('');

  const report = reports.find(r => r.reportId === reportId);
  if (!report) {
    return {
      found: false,
      message: 'Report not found: ' + reportId
    };
  }

  const freshness = getReportFreshness(reportId, readFromSheet);
  const sheetName = report.sheetName || report.reportId || report.reportName;
  const dateRange = sheetName && sheetName !== 'N/A' ?
    extractDataDateRange(sheetName) :
    null;

  return {
    found: true,
    reportId: reportId,
    reportName: report.reportName,
    sheetName: sheetName,
    freshness: freshness,
    dataRange: dateRange,
    summary: {
      pulledAt: report.processedAt,
      dataCovers: dateRange && dateRange.hasValidDates ?
        dateRange.startDateISO + ' to ' + dateRange.endDateISO :
        'Unknown',
      newestDataAge: dateRange && dateRange.hasValidDates && dateRange.endDate ?
        Math.floor((new Date() - dateRange.endDate) / (1000 * 60 * 60 * 24)) + ' days' :
        'Unknown'
    }
  };
}

// ============================================================================
// TESTING AND VALIDATION FUNCTIONS
// ============================================================================

/**
 * Test all new analysis functions with existing data
 * Run this first to test without pulling new data
 */
function testAnalysisFunctions() {
  Logger.log('=== TESTING ANALYSIS FUNCTIONS ===\n');

  try {
    // Test 1: Read report statuses from sheet
    Logger.log('Test 1: Reading report statuses from DISCOVERY SUMMARY sheet...');
    const reports = readReportStatusesFromSheet();
    Logger.log('✅ Found ' + reports.length + ' reports');
    Logger.log('');

    // Test 2: Display error summary
    Logger.log('Test 2: Error Summary...');
    displayErrorSummary();
    Logger.log('');

    // Test 3: Display data freshness
    Logger.log('Test 3: Data Freshness Analysis...');
    displayDataFreshness();
    Logger.log('');

    // Test 4: Display data ranges
    Logger.log('Test 4: Data Range Analysis...');
    Logger.log('(This may take a moment as it analyzes actual data in sheets)');
    displayAllDataRanges();
    Logger.log('');

    // Test 5: Test a specific report (if available)
    if (reports.length > 0) {
      const testReport = reports[0];
      Logger.log('Test 5: Testing specific report: ' + testReport.reportId);
      const reportInfo = getReportDataRange(testReport.reportId);
      Logger.log('Result: ' + JSON.stringify(reportInfo.summary, null, 2));
      Logger.log('');
    }

    Logger.log('✅ All tests completed successfully!');
    Logger.log('');
    Logger.log('💡 Next steps:');
    Logger.log('  • Run displayDataFreshness() to see freshness details');
    Logger.log('  • Run displayAllDataRanges() to see data date ranges');
    Logger.log('  • Run displayErrorSummary() to see error details');
    Logger.log('  • Run runCompleteDiscovery() if you want fresh data');

    return {
      success: true,
      reportsFound: reports.length,
      message: 'All tests passed'
    };

  } catch (error) {
    Logger.log('❌ Test failed: ' + error.message);
    Logger.log(error.stack);
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Quick test: Check if DISCOVERY SUMMARY sheet exists and has data
 */
function quickTest() {
  Logger.log('=== QUICK TEST ===\n');

  try {
    const config = new ImpactConfig();
    const metrics = new PerformanceMetrics();
    const logger = new EnhancedLogger(config, metrics);
    const spreadsheetManager = new EnhancedSpreadsheetManager(config, logger, metrics);

    const spreadsheet = spreadsheetManager.getSpreadsheet();
    const summarySheet = spreadsheet.getSheetByName('DISCOVERY SUMMARY');

    if (!summarySheet) {
      Logger.log('❌ DISCOVERY SUMMARY sheet not found.');
      Logger.log('💡 You need to run runCompleteDiscovery() first to create the summary.');
      return {
        success: false,
        hasSummary: false,
        message: 'No DISCOVERY SUMMARY sheet found'
      };
    }

    const data = summarySheet.getDataRange().getValues();
    Logger.log('✅ DISCOVERY SUMMARY sheet found');
    Logger.log('   Rows: ' + data.length);
    Logger.log('   Columns: ' + (data[0] ? data[0].length : 0));
    Logger.log('');

    if (data.length < 2) {
      Logger.log('⚠️  Sheet has no data rows (only header)');
      Logger.log('💡 Run runCompleteDiscovery() to populate data');
      return {
        success: true,
        hasSummary: true,
        hasData: false,
        message: 'Summary sheet exists but has no data'
      };
    }

    Logger.log('✅ Sheet has data! You can now test the analysis functions:');
    Logger.log('   • testAnalysisFunctions() - Run all tests');
    Logger.log('   • displayDataFreshness() - Check data freshness');
    Logger.log('   • displayAllDataRanges() - Check data date ranges');
    Logger.log('   • displayErrorSummary() - Check errors');

    return {
      success: true,
      hasSummary: true,
      hasData: true,
      rowCount: data.length - 1, // Exclude header
      message: 'Ready to test analysis functions'
    };

  } catch (error) {
    Logger.log('❌ Error: ' + error.message);
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Helper function to list all available reports
 * Run this to see what reports are available to your account
 */
function listAvailableReports() {
  const config = new ImpactConfig();
  const metrics = new PerformanceMetrics();
  const logger = new EnhancedLogger(config, metrics);
  const client = new EnhancedAPIClient(config, logger, metrics);

  try {
    const response = client.makeRequest('/Mediapartners/' + config.getCredentials().sid + '/Reports');
    const reports = (response.data.Reports || []).filter(r => r.ApiAccessible);

    Logger.log('=== AVAILABLE REPORTS ===');
    Logger.log('Total Accessible Reports: ' + reports.length);

    reports.forEach(r => {
      Logger.log(`[${r.Id}] ${r.Name}`);
    });

    Logger.log('=========================');
    return reports;
  } catch (error) {
    Logger.log('Failed to list reports: ' + error.message);
    throw error;
  }
}

/**
 * Cleans up sheets that correspond to excluded reports
 * Run this to remove tabs for reports you no longer want
 */
function cleanupExcludedReportSheets() {
  const config = new ImpactConfig();
  const logger = new EnhancedLogger(config, new PerformanceMetrics());
  const spreadsheetManager = new EnhancedSpreadsheetManager(config, logger, new PerformanceMetrics());

  const spreadsheet = spreadsheetManager.getSpreadsheet();

  // Use the full hardcoded list to ensure we catch everything, ignoring old script properties
  const excludedReports = [
    'capital_one_mp_action_listing_sku',
    '12172',
    'custom_partner_payable_click_data',
    'getty_adplacement_mp_action_listing_sku',
    'mp_action_listing_sku_ipsos',
    'mp_action_listing_sku_and_permissions',
    'mp_adv_contacts',
    'mp_bonus_make_good_listing',
    'mp_category_exception_list_active_IO',
    'mp_action_listing_cpc_action',
    'mp_io_history',
    'custom_partner_perf_by_vlink_by_day',
    'withdrawal_details',
    'action_listing_withdrawal',
    'invoice_details_action_earnings_gaap',
    'mp_invoice_history',
    'other_earnings',
    'partner_funds_transfer_listing',
    'partner_payable_click_data',
    'PaystubActions',
    'mp_pending_insertion_orders',
    'PerformanceByCampaigns',
    'partner_seller_Perf_by_product',
    'seller_perf_by_program',
    'mp_sku_exception_list_active_IO',
    'mp_assigned_tracking_values',
    'partner_perf_by_vlink',
    'mp_monthly_close'
  ];

  logger.info('Starting cleanup of excluded report sheets...');

  let deletedCount = 0;
  const sheets = spreadsheet.getSheets();

  sheets.forEach(sheet => {
    const sheetName = sheet.getName();

    // Check if sheet name matches any excluded report ID or Name
    // Note: This is a basic check. Sheet names might be formatted differently.
    const isExcluded = excludedReports.some(excluded => {
      // Check exact match
      if (sheetName === excluded) return true;

      // Check if sheet name starts with excluded ID/Name (common pattern)
      if (sheetName.startsWith(excluded)) return true;

      // Check if excluded item is part of the sheet name (e.g. "Report - mp_monthly_close")
      if (sheetName.includes(excluded)) return true;

      return false;
    });

    if (isExcluded) {
      try {
        spreadsheet.deleteSheet(sheet);
        logger.info('Deleted sheet: ' + sheetName);
        deletedCount++;
      } catch (e) {
        logger.error('Failed to delete sheet: ' + sheetName, { error: e.message });
      }
    }
  });

  logger.info('Cleanup complete. Deleted ' + deletedCount + ' sheets.');
  return 'Deleted ' + deletedCount + ' sheets.';
}

/**
 * Manually delete a specific sheet by name
 * @param {string} sheetName - The exact name of the sheet to delete
 */
function deleteSpecificSheet(sheetName) {
  if (!sheetName) {
    return 'Please provide a sheet name.';
  }

  const config = new ImpactConfig();
  const spreadsheetId = config.getSpreadsheetId();
  const spreadsheet = SpreadsheetApp.openById(spreadsheetId);

  const sheet = spreadsheet.getSheetByName(sheetName);
  if (sheet) {
    spreadsheet.deleteSheet(sheet);
    console.log('Deleted sheet: ' + sheetName);
    return 'Deleted sheet: ' + sheetName;
  } else {
    console.log('Sheet not found: ' + sheetName);
    return 'Sheet not found: ' + sheetName;
  }
}
