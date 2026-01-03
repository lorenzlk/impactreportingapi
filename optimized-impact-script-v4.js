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
      retryDelay: 5000, // Increased from 1000 to 5000
      maxRetryDelay: 60000, // Increased from 30000 to 60000
      retryMultiplier: 2.0, // Increased from 1.5 to 2.0

      // Polling Configuration (Optimized)
      maxPollingAttempts: 30,
      initialPollingDelay: 3000,
      maxPollingDelay: 60000,
      pollingMultiplier: 1.2,
      quickPollingThreshold: 5, // Switch to quick polling after 5 attempts
      quickPollingDelay: 2000,

      // Rate Limiting (Optimized)
      requestDelay: 2000, // Increased from 800 to 2000
      burstDelay: 500, // Increased from 200 to 500
      parallelRequestLimit: 1,

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
      enableDateFiltering: true, // Enable date range filtering
      startDate: '2025-09-01T00:00:00Z', // Start date for reports (ISO 8601 format) - Updated to Sep 1, 2025
      endDate: new Date().toISOString().split('.')[0] + 'Z', // Dynamic: Current time, NO milliseconds (API requirement)
      dateRangePresets: {
        'august-2025': { start: '2025-08-01', end: '2025-08-31' },
        'september-2025': { start: '2025-09-01', end: '2025-09-30' },
        'october-2025': { start: '2025-10-01', end: '2025-10-31' },
        'q3-2025': { start: '2025-07-01', end: '2025-09-30' },
        'q4-2025': { start: '2025-10-01', end: '2025-12-31' }
      },

      // Report Exclusion
      excludedReports: [
        'mp_action_listing_fast',
        'mp_action_listing_sku',
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
        'mp_monthly_close',
        'partner_performance_by_ad',
        'partner_performance_by_ad_legacy',
        'partner_performance_by_program',
        'partner_performance_by_program_legacy',
        'partner_performance_by_category',
        'partner_performance_by_country',
        'partner_performance_by_day',
        'partner_performance_by_day_legacy',
        'partner_performance_by_device',
        'partner_performance_by_event_type',
        'partner_performance_by_month',
        'partner_performance_by_month_legacy',
        'partner_performance_by_product',
        'partner_performance_by_promo_code',
        'partner_performance_by_referral_type',
        'partner_performance_by_ref_domain',
        'partner_performance_by_ref_url',
        'mp_vanity_links',
        'Withdrawals',
        'mp_paystub_history',
        'mp_action_sku_listing',
        'action_listing_paystub',
        'mp_action_listing_clearing_date'
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
        const headers = response.getHeaders();

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
        } else if (statusCode === 429) {
          // Extract Retry-After header if available
          let retryAfter = 60; // Default 60 seconds
          const rawRetryAfter = headers['Retry-After'] || headers['retry-after'];

          if (rawRetryAfter) {
            this.logger.warn('Received Retry-After header', { value: rawRetryAfter });
            const parsed = parseInt(rawRetryAfter, 10);
            if (!isNaN(parsed)) {
              retryAfter = parsed;
            }
          }

          // Cap retryAfter to avoid Utilities.sleep errors (max is usually 300000ms)
          // And to avoid hanging the script for too long
          if (retryAfter > 300) { // If wait is > 5 minutes
            const resumeTime = new Date(Date.now() + (retryAfter * 1000));
            this.logger.error('Rate limit wait time too long (' + retryAfter + 's), failing fast');
            this.logger.error('RESUME AT: ' + resumeTime.toISOString());
            throw new Error('API request failed: 429 - Rate Limit Exceeded (Wait time ' + retryAfter + 's > 5m limit). Resume at: ' + resumeTime.toISOString());
          } else if (retryAfter > 60) {
            this.logger.warn('Retry-After value ' + retryAfter + 's exceeds cap, limiting to 60s');
            retryAfter = 60;
          }

          throw new Error('API request failed: 429 - Rate Limit Exceeded (Retry-After: ' + retryAfter + ')');
        } else {
          throw new Error('API request failed: ' + statusCode + ' - ' + content);
        }
      } catch (error) {
        lastError = error;
        this.metrics.recordAPICall(false);

        // Check for fail-fast condition
        if (error.message.includes('> 5m limit')) {
          throw error; // Re-throw immediately to stop retries
        }

        if (attempt < maxRetries) {
          let delay;

          // Special handling for rate limits (429)
          if (error.message.includes('429')) {
            // Extract delay from error message if possible, or default to 60s
            const match = error.message.match(/Retry-After: (\d+)/);
            const retryAfterSeconds = match ? parseInt(match[1], 10) : 60;
            delay = retryAfterSeconds * 1000;

            this.logger.warn('Rate limit hit (429), waiting ' + retryAfterSeconds + 's before retry', {
              endpoint: endpoint,
              attempt: attempt + 1
            });
          } else {
            delay = Math.min(baseDelay * Math.pow(multiplier, attempt), maxDelay);
            this.logger.warn('API request failed, retrying in ' + delay + 'ms', {
              endpoint: endpoint,
              attempt: attempt + 1,
              error: error.message,
              delay: delay
            });
          }

          Utilities.sleep(delay);
        }
      }
    }

    this.circuitBreaker.recordFailure(lastError);
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
    this.logger.info('DEBUG: scheduleExport called', { reportId: reportId, params: params });
    this.logger.debug('Scheduling export', { reportId: reportId, params: params });

    const queryParts = [];

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

    const queryString = queryParts.join('&');
    this.logger.info('DEBUG: Final query string', { queryString: queryString });

    const response = this.makeRequest(
      '/Mediapartners/' + this.credentials.sid + '/ReportExport/' + reportId + '?' + queryString
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

  recordFailure(error) {
    // Don't count rate limits as system failures
    if (error && (error.message.includes('429') || error.message.includes('Rate Limit'))) {
      return;
    }

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
    const shouldRefresh = this.shouldRefreshData(reportId, metadata);

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
          result.status || 'SUCCESS (Current)',
          result.chunked ? 'Split into ' + (result.chunkCount || 0) + ' parts' : (result.notes || ''),
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
  shouldRefreshData(reportId, metadata) {
    const freshnessHours = this.config.get('dataFreshnessHours', 24); // Default 24 hours
    const forceRefresh = this.config.get('forceRefresh', false);

    if (forceRefresh) {
      this.logger.info('Force refresh enabled for ' + reportId + ' (Config: ' + forceRefresh + ')');
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

        // Merge skipped reports into successful results for summary
        if (exportResults.skipped && exportResults.skipped.length > 0) {
          this.logger.info('Adding ' + exportResults.skipped.length + ' skipped reports to summary');
          // If successful is undefined (no exports scheduled), initialize it
          if (!exportResults.successful) exportResults.successful = [];
          exportResults.successful = exportResults.successful.concat(exportResults.skipped);
        }
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

    // Initial delay to let any previous API calls settle (e.g. discovery)
    Utilities.sleep(2000);

    const scheduled = [];
    const skipped = [];
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
          // Check if we need to refresh this data
          const shouldRefresh = this.spreadsheetManager.shouldRefreshData(report.Id, {
            name: report.Name,
            rowCount: 0 // We don't know row count yet
          });

          if (!shouldRefresh) {
            this.logger.info('Skipping ' + report.Id + ' - data is fresh');
            skipped.push({
              reportId: report.Id,
              reportName: report.Name,
              status: 'SKIPPED (Fresh)',
              notes: 'Data is fresh (< 24h)',
              processedAt: new Date()
            });

            // Mark as complete in progress tracker so it shows up in history/refresh
            this.progressTracker.markReportComplete(report.Id, {
              reportName: report.Name,
              sheetName: this.spreadsheetManager.generateSheetName(report.Id, report.Name),
              status: 'SKIPPED (Fresh)',
              notes: 'Data is fresh (< 24h)',
              processedAt: new Date().toISOString()
            });

            continue;
          }

          this.logger.info('Scheduling ' + (globalIndex + 1) + '/' + reports.length + ': ' + report.Id);

          // Add default parameters for Action Listing reports to ensure we get ALL statuses
          const params = {};
          if (report.Id.includes('action_listing')) {
            params.ActionStatus = 'APPROVED,PENDING,REVERSED';
            this.logger.info('Added ActionStatus=APPROVED,PENDING,REVERSED for ' + report.Id);
          }

          const job = this.apiClient.scheduleExport(report.Id, params);
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
            skipped: skipped,
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

// ============================================================================
// CONVENIENCE FUNCTIONS
// ============================================================================

/**
 * START HERE: Run this function to start a fresh discovery
 * This will CLEAR all previous progress and force a new run
 */

/**
 * START HERE: Run this function to start a fresh discovery
 * This will CLEAR all previous progress and force a new run
 */
function startFreshDiscovery() {
  console.log('🚀 Starting FRESH discovery run... [VERSION CHECK: ' + new Date().toISOString() + ']');

  // 1. Force set the correct Spreadsheet ID
  const CORRECT_SPREADSHEET_ID = '1aLKEEw7Nx0O1DbZjnXnhOeDjcLRloLN0Y8K2SscZKIc';
  const props = PropertiesService.getScriptProperties();
  props.setProperty('IMPACT_SPREADSHEET_ID', CORRECT_SPREADSHEET_ID);
  console.log('✅ Enforced correct Spreadsheet ID:', CORRECT_SPREADSHEET_ID);

  // 2. Clear saved config to ensure defaults (and exclusions) are used
  // This fixes the issue where old config might override new exclusions
  props.deleteProperty('IMPACT_OPTIMIZED_CONFIG');
  console.log('✅ Cleared saved configuration to enforce defaults');

  console.log('This will ignore previous progress and re-process all reports.');

  const orchestrator = new UltraOptimizedOrchestrator();

  // Force restart option
  orchestrator.runCompleteDiscovery({
    forceRestart: true
  });
}

/**
 * Run this if you want to resume a stopped run
 */
function resumeDiscovery() {
  console.log('🔄 Resuming discovery run...');
  const orchestrator = new UltraOptimizedOrchestrator();
  orchestrator.runCompleteDiscovery({
    forceRestart: false
  });
}

/**
 * Clear all discovery state (use if stuck)
 */
function clearDiscoveryState() {
  const props = PropertiesService.getScriptProperties();
  props.deleteProperty('IMPACT_PROGRESS_V4');
  props.deleteProperty('IMPACT_COMPLETED_V4');
  props.deleteProperty('IMPACT_CHECKPOINT');
  props.deleteProperty('IMPACT_DATA_FRESHNESS');
  console.log('✅ Cleared all discovery state. You can now run startFreshDiscovery()');
}

/**
 * MASTER AUTOMATION TRIGGER
 *
 * Configure this single function to run on a Time-Driven trigger (e.g., Daily at Midnight)
 * It will orchestrate the entire pipeline:
 * 1. Fetch fresh Impact.com data
 * 2. Run Team SKU Analysis and Mapping
 * 3. Update Business Intelligence Dashboard
 */
function runDailyAutomation() {
  console.log('🚀 STARING DAILY AUTOMATION PIPELINE...');
  const startTime = new Date();

  try {
    // Step 1: Fetch fresh Impact.com data
    console.log('\n📡 STEP 1: Fetching Impact.com Data...');
    startFreshDiscovery();

    // Step 2: Run Team Analysis and Mapping
    console.log('\n🏈 STEP 2: Running Team SKU Analysis & Mapping...');
    try {
      if (typeof runCompleteTeamAnalysisPipeline !== 'undefined') {
        runCompleteTeamAnalysisPipeline();
      } else {
        // Fallback: Try calling it anyway, assuming global scope
        console.log('Function not found in strict check, trying global call...');
        // We can't actually call it if undefined, BUT in GAS top-level functions are methods of the global object.
        // Let's just assume it's there or try to find it.
        // Actually, let's just log and try standard call.
        runCompleteTeamAnalysisPipeline();
      }
    } catch (e) {
      if (e.message.indexOf('not defined') > -1) {
        console.error('❌ runCompleteTeamAnalysisPipeline NOT FOUND. Please ensure team-sku-analysis.js is saved and deployed.');
      } else {
        console.error('❌ Error running Team Analysis: ' + e.message);
      }
    }

    // Step 3: Business Intelligence Dashboard
    // Ensure data formatting is consistent before dashboarding
    console.log('\n📊 STEP 3: Updating BI Dashboard...');
    try {
      if (typeof createBIDashboard !== 'undefined') {
        createBIDashboard();
      } else if (typeof refreshBIDashboard !== 'undefined') {
        refreshBIDashboard();
      } else {
        console.log('BI functions not found in check, trying direct call...');
        createBIDashboard();
      }
    } catch (e) {
      console.error('❌ Error updating BI Dashboard: ' + e.message);
    }

    const duration = (new Date() - startTime) / 1000 / 60;
    console.log('\n✅ DAILY AUTOMATION COMPLETE');
    console.log('⏱️ Total Duration: ' + duration.toFixed(1) + ' minutes');

  } catch (error) {
    console.error('❌ AUTOMATION FAILED: ' + error.message);
    console.error(error.stack);
  }
}

/**
 * ONE-TIME SETUP: Run this function ONCE to set up the daily trigger
 * It creates a trigger to run 'runDailyAutomation' every day between 1am and 2am
 */
function setupDailyTrigger() {
  const functionName = 'runDailyAutomation';

  // 1. Delete existing triggers for this function to avoid duplicates
  const triggers = ScriptApp.getProjectTriggers();
  let deletedCount = 0;

  triggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === functionName) {
      ScriptApp.deleteTrigger(trigger);
      deletedCount++;
    }
  });

  if (deletedCount > 0) {
    console.log(`Deleted ${deletedCount} existing trigger(s) for ${functionName}`);
  }

  // 2. Create the new daily trigger for 1am - 2am
  ScriptApp.newTrigger(functionName)
    .timeBased()
    .everyDays(1)
    .atHour(1) // 1am
    .create();

  console.log(`✅ SUCCESS: Daily trigger created for '${functionName}'`);
  console.log('⏰ Schedule: Daily between 1 AM and 2 AM');
}
/**
 * Team-Level SKU Analysis for Impact.com Reports
 * 
 * This module adds team attribution to Mula conversations and SKU reporting
 * Track revenue by team, analyze which teams drive the most conversions
 * 
 * @version 1.0.0
 * @author Logan Lorenz
 */

// ============================================================================
// TEAM CONFIGURATION
// ============================================================================

class TeamConfig {
  constructor() {
    this.props = PropertiesService.getScriptProperties();
    this.config = this.loadTeamConfiguration();
  }

  loadTeamConfiguration() {
    const defaults = {
      // Team mapping rules - map conversations/partners/subids to teams
      teamMappingRules: {
        // Example: Map by partner name patterns
        partnerPatterns: {
          'Team Alpha': ['partner_alpha', 'alpha_'],
          'Team Beta': ['partner_beta', 'beta_'],
          'Team Gamma': ['partner_gamma', 'gamma_']
        },

        // Example: Map by SubID patterns (Mula conversation identifiers)
        subidPatterns: {
          'Team Alpha': ['alpha', 'team_a'],
          'Team Beta': ['beta', 'team_b'],
          'Team Gamma': ['gamma', 'team_c']
        },

        // Example: Map by campaign patterns
        campaignPatterns: {
          'Team Alpha': ['campaign_a', 'promo_alpha'],
          'Team Beta': ['campaign_b', 'promo_beta'],
          'Team Gamma': ['campaign_c', 'promo_gamma']
        },

        // Manual mappings - specific IDs to teams
        manualMappings: {
          // 'specific_partner_id': 'Team Alpha',
          // 'specific_subid': 'Team Beta'
        }
      },

      // Default team for unmapped conversations
      defaultTeam: 'Unassigned',

      // Team metadata
      teams: {
        'Team Alpha': {
          name: 'Team Alpha',
          description: 'Sales Team Alpha',
          lead: '',
          target: 50000, // Monthly revenue target
          active: true
        },
        'Team Beta': {
          name: 'Team Beta',
          description: 'Sales Team Beta',
          lead: '',
          target: 40000,
          active: true
        },
        'Team Gamma': {
          name: 'Team Gamma',
          description: 'Sales Team Gamma',
          lead: '',
          target: 30000,
          active: true
        },
        'Unassigned': {
          name: 'Unassigned',
          description: 'Conversations not yet assigned to a team',
          lead: '',
          target: 0,
          active: true
        }
      },

      // Analysis settings
      enableTeamAnalysis: true,
      enableSKUTracking: true,
      enableRevenueAttribution: true,

      // Report settings
      createTeamDashboard: true,
      createSKUBreakdown: true,

      // Spreadsheet configuration
      teamAnalysisSpreadsheetId: null, // Will use same spreadsheet as main reports
      teamSheetPrefix: 'TEAM_'
    };

    const configJson = this.props.getProperty('TEAM_SKU_CONFIG');
    if (configJson) {
      try {
        return { ...defaults, ...JSON.parse(configJson) };
      } catch (error) {
        Logger.log('Failed to parse team config: ' + error.message);
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
    this.props.setProperty('TEAM_SKU_CONFIG', JSON.stringify(this.config));
  }

  /**
   * Add a new team
   */
  addTeam(teamName, metadata = {}) {
    const teams = this.config.teams || {};
    teams[teamName] = {
      name: teamName,
      description: metadata.description || '',
      lead: metadata.lead || '',
      target: metadata.target || 0,
      active: metadata.active !== undefined ? metadata.active : true
    };
    this.config.teams = teams;
    this.saveConfiguration();
  }

  /**
   * Add team mapping rule
   */
  addTeamMappingRule(teamName, ruleType, patterns) {
    const rules = this.config.teamMappingRules || {};
    if (!rules[ruleType]) {
      rules[ruleType] = {};
    }
    rules[ruleType][teamName] = patterns;
    this.config.teamMappingRules = rules;
    this.saveConfiguration();
  }

  /**
   * Get all active teams
   */
  getActiveTeams() {
    const teams = this.config.teams || {};
    return Object.values(teams).filter(team => team.active);
  }
}

// ============================================================================
// MANUAL TEAM MAPPINGS
// ============================================================================

/**
 * Manages manual team assignments that override automatic detection
 * Mappings are stored in PropertiesService and persist across runs
 */
class ManualTeamMappings {
  constructor() {
    this.props = PropertiesService.getScriptProperties();
    this.mappings = this.loadMappings();
  }

  loadMappings() {
    const json = this.props.getProperty('MANUAL_TEAM_MAPPINGS');
    if (json) {
      try {
        return JSON.parse(json);
      } catch (error) {
        console.log('Failed to parse manual mappings: ' + error.message);
        return {};
      }
    }
    return {};
  }

  saveMappings() {
    this.props.setProperty('MANUAL_TEAM_MAPPINGS', JSON.stringify(this.mappings));
  }

  /**
   * Add a manual mapping for an ActionId
   * @param {string} actionId - The ActionId from the SKU data
   * @param {string} teamName - The team name to assign
   */
  addMapping(actionId, teamName) {
    this.mappings[actionId] = teamName;
    this.saveMappings();
    console.log('✅ Mapped ActionId ' + actionId + ' to team: ' + teamName);
  }

  /**
   * Add multiple mappings at once
   * @param {Object} mappingsObject - Object with ActionId keys and team name values
   */
  addBulkMappings(mappingsObject) {
    Object.assign(this.mappings, mappingsObject);
    this.saveMappings();
    console.log('✅ Added ' + Object.keys(mappingsObject).length + ' manual mappings');
  }

  /**
   * Get team assignment for an ActionId
   * @param {string} actionId - The ActionId to lookup
   * @returns {string|null} - Team name or null if not mapped
   */
  getTeam(actionId) {
    return this.mappings[actionId] || null;
  }

  /**
   * Remove a manual mapping
   * @param {string} actionId - The ActionId to remove
   */
  removeMapping(actionId) {
    if (this.mappings[actionId]) {
      delete this.mappings[actionId];
      this.saveMappings();
      console.log('✅ Removed mapping for ActionId: ' + actionId);
      return true;
    }
    return false;
  }

  /**
   * Get all mappings
   */
  getAllMappings() {
    return { ...this.mappings };
  }

  /**
   * Get count of manual mappings
   */
  getCount() {
    return Object.keys(this.mappings).length;
  }

  /**
   * Clear all mappings
   */
  clearAll() {
    this.mappings = {};
    this.saveMappings();
    console.log('✅ Cleared all manual mappings');
  }

  /**
   * Get mappings for a specific team
   */
  getMappingsForTeam(teamName) {
    const teamMappings = {};
    Object.entries(this.mappings).forEach(([actionId, team]) => {
      if (team === teamName) {
        teamMappings[actionId] = team;
      }
    });
    return teamMappings;
  }
}

// ============================================================================
// TEAM DISPLAY FORMATTER
// ============================================================================

/**
 * Converts URL-friendly team names to display-friendly names
 * e.g., "auburn-tigers" -> "Auburn Tigers"
 */
class TeamDisplayFormatter {
  constructor() {
    // Map of URL-friendly names to display names
    this.displayNames = {
      'usc-trojans': 'USC Trojans',
      'florida-gators': 'Florida Gators',
      'lsu-tigers': 'LSU Tigers',
      'auburn-tigers': 'Auburn Tigers',
      'michigan-wolverines': 'Michigan Wolverines',
      'ohio-state-buckeyes': 'Ohio State Buckeyes',
      'penn-state-nittany-lions': 'Penn State Nittany Lions',
      'notre-dame-fighting-irish': 'Notre Dame Fighting Irish',
      'nc-state-wolfpack': 'NC State Wolfpack',
      'ole-miss-rebels': 'Ole Miss Rebels'
    };
  }

  /**
   * Convert URL-friendly name to display name
   * @param {string} urlName - URL-friendly team name (e.g., "auburn-tigers")
   * @returns {string} - Display name (e.g., "Auburn Tigers")
   */
  toDisplayName(urlName) {
    if (!urlName) return 'Unassigned';

    const lowerName = urlName.toLowerCase().trim();

    // Check if we have a predefined mapping
    if (this.displayNames[lowerName]) {
      return this.displayNames[lowerName];
    }

    // Fall back to auto-formatting: "auburn-tigers" -> "Auburn Tigers"
    return urlName
      .split('-')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join(' ');
  }

  /**
   * Get list of expected team names in URL format
   * @returns {Array<string>} - Array of URL-friendly team names
   */
  getExpectedTeamNames() {
    return Object.keys(this.displayNames);
  }
}

// ============================================================================
// TEAM MAPPER
// ============================================================================

class TeamMapper {
  constructor(teamConfig) {
    this.teamConfig = teamConfig;
    this.formatter = new TeamDisplayFormatter();
  }

  /**
   * Map a conversation/row to a team based on various identifiers
   */
  /**
   * Map a conversation/row to a team based on various identifiers
   */
  mapToTeam(row) {
    const rules = this.teamConfig.get('teamMappingRules', {});

    // Helper to find value by fuzzy key match
    const getValue = (obj, targetKey) => {
      // Direct match
      if (obj[targetKey]) return obj[targetKey];

      // Case-insensitive match
      const lowerTarget = targetKey.toLowerCase();
      if (obj[lowerTarget]) return obj[lowerTarget];

      // Normalized match (remove spaces, underscores, special chars)
      const normalize = k => k.toString().toLowerCase().replace(/[^a-z0-9]/g, '');
      const normalizedTarget = normalize(targetKey);

      const key = Object.keys(obj).find(k => normalize(k) === normalizedTarget);
      return key ? obj[key] : '';
    };

    // Extract identifiers from row using robust matching
    const partner = (getValue(row, 'Partner') || '').toString().toLowerCase();
    const subid = (getValue(row, 'SubID') || '').toString().toLowerCase();
    const campaign = (getValue(row, 'Campaign') || '').toString().toLowerCase();
    const conversationId = (getValue(row, 'ConversationID') || '').toString().toLowerCase();

    // robustly find PubSubid3
    const pubSubid3 = (getValue(row, 'PubSubid3') || '').toString().trim();

    // Debug log if PubSubid3 is missing but might be expected (optional, can be noisy)
    // console.log(`Mapping row: PubSubid3=${pubSubid3}, SubID=${subid}`);

    // 1. PubSubid3 (Highest Priority - Direct Mapping)
    if (pubSubid3) {
      const teamName = this.formatter.toDisplayName(pubSubid3);
      if (teamName && teamName !== 'Unassigned') {
        return teamName;
      }
    }

    // Try manual mappings first (highest priority)
    if (rules.manualMappings) {
      for (const [identifier, teamName] of Object.entries(rules.manualMappings)) {
        if (partner === identifier.toLowerCase() ||
          subid === identifier.toLowerCase() ||
          conversationId === identifier.toLowerCase()) {
          return teamName;
        }
      }
    }

    // Try SubID patterns (high priority for Mula conversations)
    if (rules.subidPatterns && subid) {
      for (const [teamName, patterns] of Object.entries(rules.subidPatterns)) {
        if (this.matchesPattern(subid, patterns)) {
          return teamName;
        }
      }
    }

    // Try partner patterns
    if (rules.partnerPatterns && partner) {
      for (const [teamName, patterns] of Object.entries(rules.partnerPatterns)) {
        if (this.matchesPattern(partner, patterns)) {
          return teamName;
        }
      }
    }

    // Try campaign patterns
    if (rules.campaignPatterns && campaign) {
      for (const [teamName, patterns] of Object.entries(rules.campaignPatterns)) {
        if (this.matchesPattern(campaign, patterns)) {
          return teamName;
        }
      }
    }

    // 5. Fallback: Check PubSubid1 for "Mula" (for historical data before PubSubid3 was used)
    const pubSubid1 = (getValue(row, 'PubSubid1') || '').toString().toLowerCase();
    if (pubSubid1.includes('mula')) {
      return 'Mula';
    }

    // Return default team if no match found
    return this.teamConfig.get('defaultTeam', 'Unassigned');
  }

  /**
   * Check if a value matches any of the patterns
   */
  matchesPattern(value, patterns) {
    if (!value || !patterns) return false;

    return patterns.some(pattern => {
      const patternLower = pattern.toLowerCase();
      return value.includes(patternLower);
    });
  }

  /**
   * Batch map multiple rows
   */
  batchMapToTeams(rows) {
    return rows.map(row => ({
      ...row,
      team: this.mapToTeam(row)
    }));
  }
}

// ============================================================================
// SKU TEAM ANALYZER
// ============================================================================

class SKUTeamAnalyzer {
  constructor(teamConfig, teamMapper) {
    this.teamConfig = teamConfig;
    this.teamMapper = teamMapper;
  }

  /**
   * Analyze SKU data with team attribution
   */
  analyzeSKUsByTeam(skuData) {
    console.log('Analyzing ' + skuData.length + ' SKU records by team...');

    // Check if Team column already exists in the data
    const hasTeamColumn = skuData.length > 0 && skuData[0].hasOwnProperty('Team');

    let enrichedData;
    if (hasTeamColumn) {
      // Use existing Team column - just rename it to 'team' (lowercase) for consistency
      console.log('Using existing Team column from spreadsheet...');
      enrichedData = skuData.map(row => ({
        ...row,
        team: row['Team'] || 'Unassigned'
      }));
    } else {
      // Fall back to team mapper for data without Team column
      console.log('No Team column found, applying team mapping rules...');
      enrichedData = this.teamMapper.batchMapToTeams(skuData);
    }

    // FILTER: Remove Unassigned records as requested
    // "Only things with PubsubID Mula and a team name or Mula as team name"
    const initialCount = enrichedData.length;
    enrichedData = enrichedData.filter(row => {
      const team = (row.team || '').toString().trim();
      const pubSubid1 = (row['PubSubid1'] || row['PubSubid1_'] || row['pubsubid1'] || '').toString().toLowerCase().trim();

      // strict check: Team must not be 'Unassigned' AND PubSubid1 should be 'mula' (if it exists in data)
      const isUnassigned = team.toLowerCase() === 'unassigned';

      // Note: If PubSubid1 is missing from the source data, we trust the Team mapping.
      // If it exists, we enforce it must be 'mula'.
      const isMulaPartner = pubSubid1 === '' || pubSubid1 === 'mula';

      return team && !isUnassigned && isMulaPartner;
    });
    const filteredCount = enrichedData.length;

    if (initialCount !== filteredCount) {
      console.log(`ℹ️ Filtered out ${initialCount - filteredCount} Unassigned records.`);
    }

    // Aggregate by team
    const teamStats = {};
    const skuByTeam = {};

    enrichedData.forEach(row => {
      const team = row.team;
      const sku = row['SKU'] || row['sku'] || row['Sku'] || row['Product_SKU'] || 'Unknown';
      const revenue = this.parseNumber(row['SaleAmount'] || row['Sale_amount'] || row['Revenue'] || row['revenue'] || 0);
      const commission = this.parseNumber(row['Earnings'] || row['earnings'] || row['Commission'] || row['commission'] || row['Payout'] || row['payout'] || 0);
      const quantity = this.parseNumber(row['Quantity'] || row['quantity'] || row['Items'] || 1);
      const conversions = this.parseNumber(row['Actions'] || row['Conversions'] || row['conversions'] || 1);

      // Initialize team stats
      if (!teamStats[team]) {
        teamStats[team] = {
          team: team,
          totalRevenue: 0,
          totalCommission: 0,
          totalConversions: 0,
          totalQuantity: 0,
          uniqueSKUs: new Set(),
          conversations: []
        };
      }

      // Initialize SKU tracking for team
      if (!skuByTeam[team]) {
        skuByTeam[team] = {};
      }

      // Update team stats
      teamStats[team].totalRevenue += revenue;
      teamStats[team].totalCommission += commission;
      teamStats[team].totalConversions += conversions;
      teamStats[team].totalQuantity += quantity;
      teamStats[team].uniqueSKUs.add(sku);
      teamStats[team].conversations.push(row);

      // Track SKU-level stats within team
      if (!skuByTeam[team][sku]) {
        skuByTeam[team][sku] = {
          sku: sku,
          team: team,
          revenue: 0,
          commission: 0,
          conversions: 0,
          quantity: 0,
          avgOrderValue: 0
        };
      }

      skuByTeam[team][sku].revenue += revenue;
      skuByTeam[team][sku].commission += commission;
      skuByTeam[team][sku].conversions += conversions;
      skuByTeam[team][sku].quantity += quantity;
    });

    // Calculate derived metrics and convert Sets to counts
    Object.keys(teamStats).forEach(team => {
      teamStats[team].uniqueSKUCount = teamStats[team].uniqueSKUs.size;
      teamStats[team].avgOrderValue = teamStats[team].totalConversions > 0 ?
        teamStats[team].totalRevenue / teamStats[team].totalConversions : 0;
      teamStats[team].avgCommission = teamStats[team].totalConversions > 0 ?
        teamStats[team].totalCommission / teamStats[team].totalConversions : 0;
      teamStats[team].commissionRate = teamStats[team].totalRevenue > 0 ?
        (teamStats[team].totalCommission / teamStats[team].totalRevenue) * 100 : 0;
      delete teamStats[team].uniqueSKUs; // Remove Set object
      delete teamStats[team].conversations; // Remove detailed data to save memory

      // Calculate AOV for each SKU
      Object.keys(skuByTeam[team]).forEach(sku => {
        skuByTeam[team][sku].avgOrderValue = skuByTeam[team][sku].conversions > 0 ?
          skuByTeam[team][sku].revenue / skuByTeam[team][sku].conversions : 0;
      });
    });

    console.log('Analysis complete. Found ' + Object.keys(teamStats).length + ' teams');

    return {
      teamStats: teamStats,
      skuByTeam: skuByTeam,
      enrichedData: enrichedData,
      totalRecords: skuData.length,
      analyzedAt: new Date()
    };
  }

  /**
   * Get top performing teams by revenue
   */
  getTopTeamsByRevenue(analysis, limit = 10) {
    const teams = Object.values(analysis.teamStats);
    return teams
      .sort((a, b) => b.totalRevenue - a.totalRevenue)
      .slice(0, limit);
  }

  /**
   * Get top SKUs for a specific team
   */
  getTopSKUsForTeam(analysis, teamName, limit = 20) {
    const skus = analysis.skuByTeam[teamName];
    if (!skus) return [];

    return Object.values(skus)
      .sort((a, b) => b.revenue - a.revenue)
      .slice(0, limit);
  }

  /**
   * Get conversations for a specific team
   */
  getTeamConversations(analysis, teamName) {
    return analysis.enrichedData.filter(row => row.team === teamName);
  }

  parseNumber(value) {
    if (typeof value === 'number') return value;
    if (typeof value === 'string') {
      const cleaned = value.replace(/[$,]/g, '');
      const parsed = parseFloat(cleaned);
      return isNaN(parsed) ? 0 : parsed;
    }
    return 0;
  }
}

// ============================================================================
// TEAM REPORT GENERATOR
// ============================================================================

class TeamReportGenerator {
  constructor(spreadsheetManager, teamConfig) {
    this.spreadsheetManager = spreadsheetManager;
    this.teamConfig = teamConfig;
  }

  /**
   * Create comprehensive team analysis sheets
   */
  createTeamAnalysisSheets(analysis) {
    console.log('Creating team analysis sheets...');

    const spreadsheet = this.spreadsheetManager.getSpreadsheet();

    // 1. Team Summary Dashboard
    this.createTeamSummarySheet(spreadsheet, analysis);

    // 2. Individual team sheets
    const teams = Object.keys(analysis.teamStats);
    teams.forEach(teamName => {
      this.createTeamDetailSheet(spreadsheet, analysis, teamName);
    });

    // 3. SKU Performance by Team
    this.createSKUPerformanceSheet(spreadsheet, analysis);

    // 4. Team Comparison Sheet
    this.createTeamComparisonSheet(spreadsheet, analysis);

    console.log('Team analysis sheets created successfully');
  }

  /**
   * Create team summary dashboard
   */
  createTeamSummarySheet(spreadsheet, analysis) {
    const sheetName = 'TEAM_SUMMARY_DASHBOARD';

    // Delete existing sheet if it exists
    const existingSheet = spreadsheet.getSheetByName(sheetName);
    if (existingSheet) {
      spreadsheet.deleteSheet(existingSheet);
    }

    const sheet = spreadsheet.insertSheet(sheetName, 0);

    // Calculate timeframe
    const dateRange = this.calculateDateRange(analysis.enrichedData);
    const timeframe = dateRange.start && dateRange.end
      ? `${dateRange.start.toLocaleDateString()} - ${dateRange.end.toLocaleDateString()}`
      : 'All Time';

    // Prepare summary data
    const summaryData = [
      ['Team Performance Summary - Mula Conversations', '', '', '', '', '', '', '', '', ''],
      ['Timeframe: ' + timeframe + ' | Generated: ' + new Date().toLocaleString(), '', '', '', '', '', '', '', '', ''],
      ['', '', '', '', '', '', '', '', '', ''],
      ['Team', 'Total Revenue', 'Total Commission/Payout', 'Commission Rate', 'Conversions', 'Quantity', 'Unique SKUs', 'Avg Order Value', 'Target', '% of Target']
    ];

    // Add team data sorted by revenue
    const teams = Object.values(analysis.teamStats)
      .sort((a, b) => b.totalRevenue - a.totalRevenue);

    teams.forEach(team => {
      const teamMeta = this.teamConfig.get('teams', {})[team.team] || {};
      const target = teamMeta.target || 0;
      const percentOfTarget = target > 0 ? (team.totalRevenue / target * 100).toFixed(1) + '%' : 'N/A';

      summaryData.push([
        team.team,
        '$' + team.totalRevenue.toFixed(2),
        '$' + (team.totalCommission || 0).toFixed(2),
        (team.commissionRate || 0).toFixed(2) + '%',
        team.totalConversions,
        team.totalQuantity,
        team.uniqueSKUCount,
        '$' + team.avgOrderValue.toFixed(2),
        target > 0 ? '$' + target.toFixed(2) : 'N/A',
        percentOfTarget
      ]);
    });

    // Add totals
    const totalRevenue = teams.reduce((sum, t) => sum + t.totalRevenue, 0);
    const totalCommission = teams.reduce((sum, t) => sum + (t.totalCommission || 0), 0);
    const totalConversions = teams.reduce((sum, t) => sum + t.totalConversions, 0);
    const totalQuantity = teams.reduce((sum, t) => sum + t.totalQuantity, 0);
    const overallCommissionRate = totalRevenue > 0 ? (totalCommission / totalRevenue * 100) : 0;

    summaryData.push(['', '', '', '', '', '', '', '', '', '']);
    summaryData.push([
      'TOTAL',
      '$' + totalRevenue.toFixed(2),
      '$' + totalCommission.toFixed(2),
      overallCommissionRate.toFixed(2) + '%',
      totalConversions,
      totalQuantity,
      '',
      '$' + (totalConversions > 0 ? (totalRevenue / totalConversions).toFixed(2) : '0.00'),
      '',
      ''
    ]);

    // Write data
    sheet.getRange(1, 1, summaryData.length, 10).setValues(summaryData);

    // Format sheet
    this.formatSummarySheet(sheet);

    console.log('Team summary dashboard created');
  }

  /**
   * Create detailed sheet for individual team
   */
  createTeamDetailSheet(spreadsheet, analysis, teamName) {
    const sheetName = 'TEAM_' + this.sanitizeSheetName(teamName);

    // Delete existing sheet if it exists
    const existingSheet = spreadsheet.getSheetByName(sheetName);
    if (existingSheet) {
      spreadsheet.deleteSheet(existingSheet);
    }

    const sheet = spreadsheet.insertSheet(sheetName);

    // Get team conversations
    const conversations = analysis.enrichedData.filter(row => row.team === teamName);

    if (conversations.length === 0) {
      sheet.getRange(1, 1).setValue('No conversations found for team: ' + teamName);
      return;
    }

    // Prepare headers based on available columns
    const firstRow = conversations[0];
    const headers = Object.keys(firstRow).filter(key => key !== 'team');
    headers.unshift('Team'); // Add team column at the beginning

    // Calculate timeframe for this team
    const dateRange = this.calculateDateRange(conversations);
    const timeframe = dateRange.start && dateRange.end
      ? `${dateRange.start.toLocaleDateString()} - ${dateRange.end.toLocaleDateString()}`
      : 'All Time';

    sheet.getRange('A1').setNote(`Timeframe: ${timeframe}\n` + sheet.getRange('A1').getNote());

    // Prepare data rows
    const dataRows = [headers];
    conversations.forEach(row => {
      const rowData = [row.team];
      headers.slice(1).forEach(header => {
        rowData.push(row[header] !== undefined ? row[header] : '');
      });
      dataRows.push(rowData);
    });

    // Write data
    const numCols = headers.length;
    sheet.getRange(1, 1, dataRows.length, numCols).setValues(dataRows);

    // Format sheet
    this.formatDataSheet(sheet, numCols, dataRows.length);

    // Add team summary at the top
    const teamStats = analysis.teamStats[teamName];
    const note = [
      'TEAM: ' + teamName,
      'Total Revenue: $' + teamStats.totalRevenue.toFixed(2),
      'Total Commission/Payout: $' + (teamStats.totalCommission || 0).toFixed(2),
      'Commission Rate: ' + (teamStats.commissionRate || 0).toFixed(2) + '%',
      'Conversions: ' + teamStats.totalConversions,
      'Unique SKUs: ' + teamStats.uniqueSKUCount,
      'Avg Order Value: $' + teamStats.avgOrderValue.toFixed(2),
      'Total Records: ' + conversations.length
    ].join('\n');

    sheet.getRange('A1').setNote(note);

    console.log('Created detail sheet for ' + teamName + ' with ' + conversations.length + ' conversations');
  }

  /**
   * Create SKU performance sheet
   */
  createSKUPerformanceSheet(spreadsheet, analysis) {
    const sheetName = 'SKU_PERFORMANCE_BY_TEAM';

    // Delete existing sheet if it exists
    const existingSheet = spreadsheet.getSheetByName(sheetName);
    if (existingSheet) {
      spreadsheet.deleteSheet(existingSheet);
    }

    const sheet = spreadsheet.insertSheet(sheetName);

    // Calculate timeframe
    const dateRange = this.calculateDateRange(analysis.enrichedData);
    const timeframe = dateRange.start && dateRange.end
      ? `${dateRange.start.toLocaleDateString()} - ${dateRange.end.toLocaleDateString()}`
      : 'All Time';

    // Prepare data
    const data = [
      ['SKU Performance by Team', '', '', '', '', '', ''],
      ['Timeframe: ' + timeframe + ' | Generated: ' + new Date().toLocaleString(), '', '', '', '', '', ''],
      ['', '', '', '', '', '', ''],
      ['Team', 'SKU', 'Revenue', 'Commission/Payout', 'Conversions', 'Quantity', 'Avg Order Value']
    ];

    // Collect all SKUs across all teams
    const allSKUs = [];
    Object.keys(analysis.skuByTeam).forEach(teamName => {
      Object.values(analysis.skuByTeam[teamName]).forEach(skuData => {
        allSKUs.push(skuData);
      });
    });

    // Sort by revenue
    allSKUs.sort((a, b) => b.revenue - a.revenue);

    // Add SKU data
    allSKUs.forEach(sku => {
      data.push([
        sku.team,
        sku.sku,
        '$' + sku.revenue.toFixed(2),
        '$' + (sku.commission || 0).toFixed(2),
        sku.conversions,
        sku.quantity,
        '$' + sku.avgOrderValue.toFixed(2)
      ]);
    });

    // Write data
    sheet.getRange(1, 1, data.length, 7).setValues(data);
    // Format sheet
    this.formatDataSheet(sheet, 7, data.length);

    console.log('SKU performance sheet created with ' + allSKUs.length + ' SKUs');
  }

  /**
   * Calculate date range from data rows
   */
  calculateDateRange(data) {
    let start = null;
    let end = null;

    data.forEach(row => {
      const dateStr = row['Date'] || row['date'] || row['Period'] || row['period'];
      if (dateStr) {
        const date = new Date(dateStr);
        if (!isNaN(date.getTime())) {
          if (!start || date < start) start = date;
          if (!end || date > end) end = date;
        }
      }
    });

    return { start, end };
  }

  /**
   * Create team comparison sheet
   */
  createTeamComparisonSheet(spreadsheet, analysis) {
    const sheetName = 'TEAM_COMPARISON';

    // Delete existing sheet if it exists
    const existingSheet = spreadsheet.getSheetByName(sheetName);
    if (existingSheet) {
      spreadsheet.deleteSheet(existingSheet);
    }

    const sheet = spreadsheet.insertSheet(sheetName);

    // Prepare comparison metrics
    const teams = Object.values(analysis.teamStats)
      .sort((a, b) => b.totalRevenue - a.totalRevenue);

    // Create padding for title rows to match team count
    const padding = new Array(teams.length).fill('');

    // Calculate timeframe
    const dateRange = this.calculateDateRange(analysis.enrichedData);
    const timeframe = dateRange.start && dateRange.end
      ? `${dateRange.start.toLocaleDateString()} - ${dateRange.end.toLocaleDateString()}`
      : 'All Time';

    const data = [
      ['Team Performance Comparison', ...padding],
      ['Timeframe: ' + timeframe + ' | Generated: ' + new Date().toLocaleString(), ...padding],
      ['', ...padding],
      ['Metric', ...teams.map(t => t.team)],
      ['Total Revenue', ...teams.map(t => '$' + t.totalRevenue.toFixed(2))],
      ['Total Commission/Payout', ...teams.map(t => '$' + (t.totalCommission || 0).toFixed(2))],
      ['Commission Rate', ...teams.map(t => (t.commissionRate || 0).toFixed(2) + '%')],
      ['Total Conversions', ...teams.map(t => t.totalConversions)],
      ['Total Quantity', ...teams.map(t => t.totalQuantity)],
      ['Unique SKUs', ...teams.map(t => t.uniqueSKUCount)],
      ['Avg Order Value', ...teams.map(t => '$' + t.avgOrderValue.toFixed(2))],
      ['', ...padding],
      ['Market Share (%)', ...teams.map(t => {
        const totalRevenue = teams.reduce((sum, team) => sum + team.totalRevenue, 0);
        return totalRevenue > 0 ? ((t.totalRevenue / totalRevenue) * 100).toFixed(1) + '%' : '0%';
      })]
    ];

    // Write data
    const numCols = teams.length + 1;
    sheet.getRange(1, 1, data.length, numCols).setValues(data);

    // Format sheet
    this.formatComparisonSheet(sheet, numCols, data.length);

    console.log('Team comparison sheet created');
  }

  formatSummarySheet(sheet) {
    // Format title row
    const titleRange = sheet.getRange(1, 1, 1, 10);
    titleRange.setFontSize(14);
    titleRange.setFontWeight('bold');
    titleRange.setBackground('#1976D2');
    titleRange.setFontColor('#FFFFFF');
    sheet.getRange(1, 1, 1, 10).merge();

    // Format header row
    const headerRange = sheet.getRange(4, 1, 1, 10);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#4CAF50');
    headerRange.setFontColor('#FFFFFF');
    sheet.setFrozenRows(4);

    // Auto-resize columns
    sheet.autoResizeColumns(1, 10);
  }

  formatDataSheet(sheet, columnCount, rowCount) {
    // Format header row
    const headerRange = sheet.getRange(1, 1, 1, columnCount);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#2E7D32');
    headerRange.setFontColor('#FFFFFF');
    sheet.setFrozenRows(1);

    // Auto-resize columns (limit to prevent timeout)
    const maxColumns = Math.min(columnCount, 20);
    sheet.autoResizeColumns(1, maxColumns);
  }

  formatComparisonSheet(sheet, columnCount, rowCount) {
    // Format title
    const titleRange = sheet.getRange(1, 1, 1, columnCount);
    titleRange.merge();
    titleRange.setFontSize(14);
    titleRange.setFontWeight('bold');
    titleRange.setBackground('#FF9800');
    titleRange.setFontColor('#FFFFFF');

    // Format headers
    const headerRange = sheet.getRange(4, 1, 1, columnCount);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#FFC107');
    headerRange.setFontColor('#000000');

    // Auto-resize
    sheet.autoResizeColumns(1, columnCount);
  }

  sanitizeSheetName(name) {
    return name.replace(/[^\w\s-]/g, '').substring(0, 25);
  }
}

// ============================================================================
// DATA ENRICHMENT - ADD TEAM COLUMN
// ============================================================================

/**
 * Enriches SKU data by adding a Team column based on PubSubid3
 * Priority: 1) PubSubid3 (ONLY), 2) Unassigned
 * This modifies the raw CSV data before it's written to the sheet
 * 
 * @param {Array} csvData - Raw CSV data (array of arrays, first row is headers)
 * @returns {Array} - Enhanced CSV data with Team column added
 */
function enrichSKUDataWithTeams(csvData) {
  // Safeguard: Check if function is run directly without arguments
  if (!csvData) {
    console.log('⚠️  WARNING: You are running a helper function directly.');
    console.log('To enrich your existing sheet data, run: enrichExistingSkuSheet()');
    console.log('To download and enrich new data, run: runSkuLevelActionWithTeams()');
    return null;
  }

  if (csvData.length === 0) {
    console.log('No data to enrich');
    return csvData;
  }

  const formatter = new TeamDisplayFormatter();
  const headers = csvData[0];
  const dataRows = csvData.slice(1);

  // Find required columns
  const pubSubid3Index = headers.findIndex(h =>
    h && h.toLowerCase() === 'pubsubid3'
  );
  const pubSubid1Index = headers.findIndex(h =>
    h && h.toLowerCase() === 'pubsubid1' || h.toLowerCase().includes('subid1')
  );
  const skuIndex = headers.findIndex(h =>
    h && h.toLowerCase() === 'sku'
  );

  if (pubSubid3Index === -1) {
    console.log('Warning: PubSubid3 column not found. Cannot add Team column from PubSubid3.');
  }

  if (skuIndex === -1) {
    console.log('Warning: SKU column not found. Cannot add Fanatics search URLs.');
  }

  // Add Team column header (insert after PubSubid4 if it exists, otherwise at the end)
  const pubSubid4Index = headers.findIndex(h =>
    h && h.toLowerCase() === 'pubsubid4'
  );
  const teamColumnIndex = pubSubid4Index >= 0 ? pubSubid4Index + 1 : headers.length;

  // Add Fanatics Search URL column right after Team
  const fanaticsUrlColumnIndex = teamColumnIndex + 1;

  const newHeaders = [...headers];
  newHeaders.splice(teamColumnIndex, 0, 'Team');
  newHeaders.splice(fanaticsUrlColumnIndex, 0, 'Fanatics Search URL');

  // Track stats
  let pubsubCount = 0;
  let unassignedCount = 0;

  // Process each data row and add team name + Fanatics URL
  const newDataRows = dataRows.map(row => {
    const newRow = [...row];
    let teamName = 'Unassigned';

    // Check PubSubid3 (ONLY source for team name)
    if (pubSubid3Index >= 0) {
      const pubSubid3Value = row[pubSubid3Index];
      if (pubSubid3Value && typeof pubSubid3Value === 'string' && pubSubid3Value.trim()) {
        const urlFriendlyName = pubSubid3Value.trim();
        teamName = formatter.toDisplayName(urlFriendlyName);
        pubsubCount++;
      }
    }

    // Fallback: Check PubSubid1 for "Mula"
    if (teamName === 'Unassigned' && pubSubid1Index >= 0) {
      const pubSubid1Value = row[pubSubid1Index];
      if (pubSubid1Value && pubSubid1Value.toString().toLowerCase().includes('mula')) {
        teamName = 'Mula';
        // Count as pubsub since we found a match? Or separate? Let's just track it works.
      }
    }

    // Count unassigned
    if (teamName === 'Unassigned') {
      unassignedCount++;
    }

    // Create Fanatics search URL
    let fanaticsUrl = '';
    if (skuIndex >= 0 && row[skuIndex]) {
      const sku = row[skuIndex].toString().trim();
      if (sku) {
        fanaticsUrl = 'https://www.fanatics.com/?query=' + sku + '&_ref=p-SRP:m-SEARCH';
      }
    }

    // Add both columns (Team first, then Fanatics URL)
    newRow.splice(teamColumnIndex, 0, teamName);
    newRow.splice(fanaticsUrlColumnIndex, 0, fanaticsUrl);

    return newRow;
  });

  console.log('✅ Added Team column at position ' + (teamColumnIndex + 1));
  console.log('   🔄 PubSubid3 auto: ' + pubsubCount);
  console.log('   ❓ Unassigned: ' + unassignedCount);

  const uniqueTeams = [...new Set(newDataRows.map(row => row[teamColumnIndex]))].filter(t => t !== 'Unassigned');
  if (uniqueTeams.length > 0) {
    console.log('   Teams found: ' + uniqueTeams.join(', '));
  }

  return [newHeaders, ...newDataRows];
}

/**
 * Enhanced version of runSkuLevelActionOnly that adds Team column
 * Use this instead of the base version to get team-enriched data
 */
function runSkuLevelActionWithTeams() {
  const config = new ImpactConfig();
  const metrics = new PerformanceMetrics();
  const logger = new EnhancedLogger(config, metrics);
  const apiClient = new EnhancedAPIClient(config, logger, metrics);
  const dataProcessor = new EnhancedDataProcessor(config, logger, metrics);
  const spreadsheetManager = new EnhancedSpreadsheetManager(config, logger, metrics);

  const reportId = 'SkuLevelActions';
  const reportName = 'SkuLevelAction';

  logger.info('Running SkuLevelAction report with Team enrichment', { reportId: reportId });

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

    // Step 5: ENHANCE DATA WITH TEAM COLUMN
    logger.info('Enriching data with Team column...');
    const enrichedData = enrichSKUDataWithTeams(processedData.data);

    // Update processedData with enriched version
    processedData.data = enrichedData;
    processedData.headers = enrichedData[0];
    processedData.columnCount = enrichedData[0].length;

    logger.info('Data enriched', {
      newColumnCount: processedData.columnCount
    });

    // Step 6: Create the spreadsheet sheet
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
      teamColumnAdded: true,
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
 * Enriches the EXISTING SkuLevelAction sheet with Team columns
 * Run this if you already have data and just want to add the Team column
 */
function enrichExistingSkuSheet() {
  console.log('Enriching existing SkuLevelAction sheet...');

  try {
    const impactConfig = new ImpactConfig();
    const metrics = new PerformanceMetrics();
    const logger = new EnhancedLogger(impactConfig, metrics);
    const spreadsheetManager = new EnhancedSpreadsheetManager(impactConfig, logger, metrics);

    const spreadsheet = spreadsheetManager.getSpreadsheet();
    const sheet = spreadsheet.getSheetByName('SkuLevelAction') ||
      spreadsheet.getSheetByName('SkuLevelActions');

    if (!sheet) {
      throw new Error('SkuLevelAction sheet not found. Please run runSkuLevelActionWithTeams() first.');
    }

    // Read existing data
    const range = sheet.getDataRange();
    const data = range.getValues();

    if (data.length < 2) {
      console.log('Sheet is empty or has no data rows.');
      return;
    }

    // Check if already enriched
    const headers = data[0];
    if (headers.includes('Team')) {
      console.log('⚠️  Sheet already has a "Team" column. Skipping enrichment.');
      return;
    }

    console.log('Found ' + (data.length - 1) + ' rows. Processing...');

    // Enrich data
    const enrichedData = enrichSKUDataWithTeams(data);

    if (!enrichedData) {
      throw new Error('Enrichment failed.');
    }

    // Clear and write new data
    sheet.clear();

    const newNumRows = enrichedData.length;
    const newNumCols = enrichedData[0].length;

    sheet.getRange(1, 1, newNumRows, newNumCols).setValues(enrichedData);

    // Format header
    sheet.getRange(1, 1, 1, newNumCols).setFontWeight('bold').setBackground('#E0E0E0');

    console.log('✅ Success! Enriched sheet with Team column.');
    console.log('Total rows: ' + newNumRows);

  } catch (error) {
    console.error('Enrichment failed: ' + error.message);
  }
}

/**
 * Helper function to poll job completion (duplicated from optimized-impact-script-v4.js for convenience)
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

// ============================================================================
// PUBLIC API FUNCTIONS
// ============================================================================

/**
 * Main function to run team-level SKU analysis
 */
function runTeamSKUAnalysis() {
  console.log('Starting Team-Level SKU Analysis...');

  try {
    // Initialize components
    const impactConfig = new ImpactConfig();
    const teamConfig = new TeamConfig();
    const metrics = new PerformanceMetrics();
    const logger = new EnhancedLogger(impactConfig, metrics);
    const spreadsheetManager = new EnhancedSpreadsheetManager(impactConfig, logger, metrics);

    // Get SKU data from existing sheet
    const spreadsheet = spreadsheetManager.getSpreadsheet();
    const skuSheet = spreadsheet.getSheetByName('SkuLevelAction') ||
      spreadsheet.getSheetByName('SkuLevelActions');

    if (!skuSheet) {
      throw new Error('SKU data sheet not found. Please run runSkuLevelActionOnly() first.');
    }

    console.log('Found SKU data sheet: ' + skuSheet.getName());

    // Read SKU data
    const data = skuSheet.getDataRange().getValues();
    const headers = data[0];
    const rows = data.slice(1);

    console.log('Loaded ' + rows.length + ' SKU records');

    // Convert to objects
    const skuData = rows.map(row => {
      const obj = {};
      headers.forEach((header, index) => {
        obj[header] = row[index];
      });
      return obj;
    });

    // Initialize team components
    const teamMapper = new TeamMapper(teamConfig);
    const analyzer = new SKUTeamAnalyzer(teamConfig, teamMapper);
    const reportGenerator = new TeamReportGenerator(spreadsheetManager, teamConfig);

    // Run analysis
    console.log('Analyzing SKU data by team...');
    const analysis = analyzer.analyzeSKUsByTeam(skuData);

    // Generate reports
    console.log('Generating team analysis reports...');
    reportGenerator.createTeamAnalysisSheets(analysis);

    // Print summary
    console.log('\n=== TEAM ANALYSIS COMPLETE ===');
    console.log('Total Records Analyzed: ' + analysis.totalRecords);
    console.log('Teams Found: ' + Object.keys(analysis.teamStats).length);

    const topTeams = analyzer.getTopTeamsByRevenue(analysis, 5);
    console.log('\nTop 5 Teams by Revenue:');
    topTeams.forEach((team, index) => {
      console.log((index + 1) + '. ' + team.team + ': $' + team.totalRevenue.toFixed(2) +
        ' (' + team.totalConversions + ' conversions)');
    });

    return {
      success: true,
      analysis: analysis,
      message: 'Team analysis complete. Check your spreadsheet for detailed reports.'
    };

  } catch (error) {
    console.error('Team SKU Analysis failed: ' + error.message);
    console.error(error.stack);
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Configure team mapping rules
 */
function configureTeamMapping() {
  console.log('Configuring team mapping...');

  const teamConfig = new TeamConfig();

  // Example: Configure your team mapping rules here
  // This is a template - customize based on your actual team structure

  // Add teams
  teamConfig.addTeam('Sales Team North', {
    description: 'Northern region sales team',
    lead: 'John Doe',
    target: 100000,
    active: true
  });

  teamConfig.addTeam('Sales Team South', {
    description: 'Southern region sales team',
    lead: 'Jane Smith',
    target: 85000,
    active: true
  });

  teamConfig.addTeam('Sales Team West', {
    description: 'Western region sales team',
    lead: 'Bob Johnson',
    target: 75000,
    active: true
  });

  // Add mapping rules - customize these patterns to match your data
  teamConfig.addTeamMappingRule('Sales Team North', 'subidPatterns', ['north', 'team_n', 'stn']);
  teamConfig.addTeamMappingRule('Sales Team South', 'subidPatterns', ['south', 'team_s', 'sts']);
  teamConfig.addTeamMappingRule('Sales Team West', 'subidPatterns', ['west', 'team_w', 'stw']);

  console.log('Team mapping configured successfully!');
  console.log('Teams added:');
  console.log('- Sales Team North (Target: $100,000)');
  console.log('- Sales Team South (Target: $85,000)');
  console.log('- Sales Team West (Target: $75,000)');
  console.log('\nRun runTeamSKUAnalysis() to analyze your data with team attribution.');

  return {
    success: true,
    message: 'Team configuration complete'
  };
}

/**
 * View current team configuration
 */
function viewTeamConfiguration() {
  const teamConfig = new TeamConfig();
  const config = teamConfig.config;

  console.log('=== TEAM CONFIGURATION ===');
  console.log('\nActive Teams:');
  const teams = teamConfig.getActiveTeams();
  teams.forEach(team => {
    console.log('- ' + team.name + ' (Target: $' + team.target.toFixed(2) + ')');
    console.log('  Lead: ' + (team.lead || 'Not assigned'));
    console.log('  Description: ' + (team.description || 'N/A'));
  });

  console.log('\nMapping Rules:');
  const rules = config.teamMappingRules;
  if (rules.subidPatterns) {
    console.log('SubID Patterns:');
    Object.entries(rules.subidPatterns).forEach(([team, patterns]) => {
      console.log('  ' + team + ': ' + patterns.join(', '));
    });
  }

  return config;
}

/**
 * Get team performance summary (quick check)
 */
function getTeamPerformanceSummary() {
  const impactConfig = new ImpactConfig();
  const teamConfig = new TeamConfig();
  const metrics = new PerformanceMetrics();
  const logger = new EnhancedLogger(impactConfig, metrics);
  const spreadsheetManager = new EnhancedSpreadsheetManager(impactConfig, logger, metrics);

  const spreadsheet = spreadsheetManager.getSpreadsheet();
  const summarySheet = spreadsheet.getSheetByName('TEAM_SUMMARY_DASHBOARD');

  if (!summarySheet) {
    console.log('Team summary not found. Run runTeamSKUAnalysis() first.');
    return null;
  }

  const data = summarySheet.getDataRange().getValues();

  console.log('=== TEAM PERFORMANCE SUMMARY ===');
  data.forEach(row => {
    if (row[0] && row[0] !== '') {
      console.log(row.join(' | '));
    }
  });

  return data;
}

// ============================================================================
// MANUAL MAPPING HELPER FUNCTIONS
// ============================================================================

/**
 * Get all unassigned records from current SKU data
 * Returns ActionIds that need manual team assignment
 */
function getUnassignedRecords() {
  console.log('Finding unassigned records...');

  try {
    const impactConfig = new ImpactConfig();
    const metrics = new PerformanceMetrics();
    const logger = new EnhancedLogger(impactConfig, metrics);
    const spreadsheetManager = new EnhancedSpreadsheetManager(impactConfig, logger, metrics);

    const spreadsheet = spreadsheetManager.getSpreadsheet();
    const skuSheet = spreadsheet.getSheetByName('SkuLevelAction');

    if (!skuSheet) {
      console.log('❌ No SkuLevelAction sheet found. Run forceRefreshSkuDataWithTeams() first.');
      return [];
    }

    const data = skuSheet.getDataRange().getValues();
    const headers = data[0];
    const rows = data.slice(1);

    const actionIdIndex = headers.indexOf('ActionId');
    const teamIndex = headers.indexOf('Team');
    const skuIndex = headers.indexOf('Sku');
    const categoryIndex = headers.indexOf('Category');
    const saleAmountIndex = headers.indexOf('SaleAmount');

    if (actionIdIndex === -1 || teamIndex === -1) {
      console.log('❌ Required columns not found');
      return [];
    }

    const unassigned = rows
      .filter(row => row[teamIndex] === 'Unassigned')
      .map(row => ({
        actionId: row[actionIdIndex],
        sku: row[skuIndex] || 'N/A',
        category: row[categoryIndex] || 'N/A',
        saleAmount: row[saleAmountIndex] || 0
      }));

    console.log('\n📊 UNASSIGNED RECORDS SUMMARY:');
    console.log('Total unassigned: ' + unassigned.length);
    console.log('\nTop 20 by revenue:');
    console.log('ActionId\t\tSKU\t\tCategory\t\tSale Amount');
    console.log('─'.repeat(80));

    unassigned
      .sort((a, b) => b.saleAmount - a.saleAmount)
      .slice(0, 20)
      .forEach(record => {
        console.log(
          record.actionId + '\t' +
          record.sku + '\t' +
          record.category + '\t' +
          '$' + record.saleAmount.toFixed(2)
        );
      });

    return unassigned;

  } catch (error) {
    console.error('Failed to get unassigned records: ' + error.message);
    return [];
  }
}

/**
 * Manually assign a team to specific ActionId(s)
 * Use this for historical data that doesn't have PubSubid3
 * 
 * @param {string|Array} actionIds - Single ActionId or array of ActionIds
 * @param {string} teamName - Team name (e.g., "Auburn Tigers", "Florida Gators")
 */
function assignTeamManually(actionIds, teamName) {
  const manualMappings = new ManualTeamMappings();

  // Convert single ID to array for consistency
  const ids = Array.isArray(actionIds) ? actionIds : [actionIds];

  console.log('Assigning ' + ids.length + ' ActionId(s) to team: ' + teamName);

  const mappings = {};
  ids.forEach(id => {
    mappings[id] = teamName;
  });

  manualMappings.addBulkMappings(mappings);

  console.log('✅ SUCCESS! Assigned ' + ids.length + ' records to ' + teamName);
  console.log('');
  console.log('⚠️  IMPORTANT: Run forceRefreshSkuDataWithTeams() to apply changes to the sheet!');

  return {
    success: true,
    count: ids.length,
    team: teamName,
    message: 'Manual mappings saved. Run forceRefreshSkuDataWithTeams() to see changes.'
  };
}

/**
 * View all manual team mappings
 */
function viewManualMappings() {
  const manualMappings = new ManualTeamMappings();
  const all = manualMappings.getAllMappings();
  const count = Object.keys(all).length;

  console.log('📋 MANUAL TEAM MAPPINGS');
  console.log('Total mappings: ' + count);
  console.log('');

  if (count === 0) {
    console.log('No manual mappings yet.');
    console.log('');
    console.log('HOW TO ADD:');
    console.log('1. Run: getUnassignedRecords()');
    console.log('2. Run: assignTeamManually(actionId, "Team Name")');
    return;
  }

  // Group by team
  const byTeam = {};
  Object.entries(all).forEach(([actionId, team]) => {
    if (!byTeam[team]) {
      byTeam[team] = [];
    }
    byTeam[team].push(actionId);
  });

  console.log('Mappings by team:');
  Object.entries(byTeam).forEach(([team, actionIds]) => {
    console.log('  ' + team + ': ' + actionIds.length + ' records');
  });

  console.log('');
  console.log('To see details, check PropertiesService: MANUAL_TEAM_MAPPINGS');

  return byTeam;
}

/**
 * Remove a manual mapping (let it use automatic detection)
 */
function removeManualMapping(actionId) {
  const manualMappings = new ManualTeamMappings();
  const removed = manualMappings.removeMapping(actionId);

  if (removed) {
    console.log('✅ Removed manual mapping for ActionId: ' + actionId);
    console.log('⚠️  Run forceRefreshSkuDataWithTeams() to apply changes!');
  } else {
    console.log('❌ No manual mapping found for ActionId: ' + actionId);
  }

  return removed;
}

/**
 * Clear all manual mappings
 */
function clearAllManualMappings() {
  console.log('⚠️  WARNING: This will clear ALL manual team assignments!');
  console.log('Are you sure? Run clearAllManualMappingsConfirmed() to proceed.');
}

function clearAllManualMappingsConfirmed() {
  const manualMappings = new ManualTeamMappings();
  manualMappings.clearAll();
  console.log('✅ All manual mappings cleared');
  console.log('⚠️  Run forceRefreshSkuDataWithTeams() to apply changes!');
}

/**
 * Example: Bulk assign multiple ActionIds to a team
 * 
 * Example usage:
 * bulkAssignTeam([
 *   '12345',
 *   '12346',
 *   '12347'
 * ], 'Auburn Tigers');
 */
function bulkAssignTeam(actionIds, teamName) {
  return assignTeamManually(actionIds, teamName);
}

/**
 * Check what PubSubid1 values the unassigned records have
 * This will confirm if unassigned = non-Mula traffic
 */
function checkUnassignedPubSubid1() {
  console.log('Checking PubSubid1 for unassigned records...\n');

  try {
    const impactConfig = new ImpactConfig();
    const metrics = new PerformanceMetrics();
    const logger = new EnhancedLogger(impactConfig, metrics);
    const spreadsheetManager = new EnhancedSpreadsheetManager(impactConfig, logger, metrics);

    const spreadsheet = spreadsheetManager.getSpreadsheet();
    const skuSheet = spreadsheet.getSheetByName('SkuLevelAction');

    if (!skuSheet) {
      console.log('❌ No SkuLevelAction sheet found.');
      return;
    }

    const data = skuSheet.getDataRange().getValues();
    const headers = data[0];
    const rows = data.slice(1);

    const pubSubid1Index = headers.indexOf('PubSubid1');
    const teamIndex = headers.indexOf('Team');

    // Filter for unassigned records
    const unassigned = rows.filter(row =>
      !row[teamIndex] || row[teamIndex] === 'Unassigned' || row[teamIndex] === ''
    );

    console.log('📊 UNASSIGNED RECORDS ANALYSIS:');
    console.log('Total unassigned: ' + unassigned.length);
    console.log('');

    // Group by PubSubid1
    const pubSubid1Counts = {};
    unassigned.forEach(row => {
      const pubSubid1 = row[pubSubid1Index] || '(empty)';
      pubSubid1Counts[pubSubid1] = (pubSubid1Counts[pubSubid1] || 0) + 1;
    });

    console.log('Breakdown by PubSubid1:');
    Object.entries(pubSubid1Counts).forEach(([pubSubid1, count]) => {
      console.log('  ' + pubSubid1 + ': ' + count + ' records');
    });

    // Check if any are from "mula"
    const mulaUnassigned = unassigned.filter(row =>
      row[pubSubid1Index] && row[pubSubid1Index].toLowerCase() === 'mula'
    );

    console.log('');
    if (mulaUnassigned.length > 0) {
      console.log('⚠️  WARNING: ' + mulaUnassigned.length + ' Mula records still unassigned!');
      console.log('   You need to assign teams to these.');
    } else {
      console.log('✅ All Mula records have teams assigned!');
      console.log('   The ' + unassigned.length + ' unassigned are from other sources.');
    }

  } catch (error) {
    console.error('❌ Error: ' + error.message);
  }
}

/**
 * Check what columns exist and show sample data
 * Use this to see where your team names are
 */
function checkSheetColumns() {
  console.log('Checking sheet columns and data...\n');

  try {
    const impactConfig = new ImpactConfig();
    const metrics = new PerformanceMetrics();
    const logger = new EnhancedLogger(impactConfig, metrics);
    const spreadsheetManager = new EnhancedSpreadsheetManager(impactConfig, logger, metrics);

    const spreadsheet = spreadsheetManager.getSpreadsheet();
    const skuSheet = spreadsheet.getSheetByName('SkuLevelAction');

    if (!skuSheet) {
      console.log('❌ No SkuLevelAction sheet found.');
      return;
    }

    const data = skuSheet.getDataRange().getValues();
    const headers = data[0];
    const firstRow = data[1] || [];

    console.log('📋 COLUMN HEADERS (with column letters):');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

    headers.forEach((header, index) => {
      const columnLetter = String.fromCharCode(65 + index); // A, B, C, etc.
      const sampleValue = firstRow[index] || '(empty)';
      const preview = sampleValue.toString().substring(0, 30);
      console.log(columnLetter + ': ' + header + ' = ' + preview);
    });

    console.log('\n🔍 KEY COLUMNS:');
    const actionIdIndex = headers.indexOf('ActionId');
    const teamIndex = headers.indexOf('Team');
    const pubSubid3Index = headers.indexOf('PubSubid3');
    const skuIndex = headers.indexOf('Sku');

    if (actionIdIndex >= 0) {
      console.log('✅ ActionId found at column ' + String.fromCharCode(65 + actionIdIndex));
    } else {
      console.log('❌ ActionId NOT found');
    }

    if (teamIndex >= 0) {
      console.log('✅ Team found at column ' + String.fromCharCode(65 + teamIndex));

      // Show team distribution
      const teamCounts = {};
      data.slice(1).forEach(row => {
        const team = row[teamIndex] || 'Unassigned';
        teamCounts[team] = (teamCounts[team] || 0) + 1;
      });

      console.log('\n   Team distribution:');
      Object.entries(teamCounts).forEach(([team, count]) => {
        console.log('   - ' + team + ': ' + count + ' records');
      });
    } else {
      console.log('❌ Team column NOT found - needs to be created!');
    }

    if (pubSubid3Index >= 0) {
      console.log('✅ PubSubid3 found at column ' + String.fromCharCode(65 + pubSubid3Index));
    } else {
      console.log('❌ PubSubid3 NOT found');
    }

    if (skuIndex >= 0) {
      console.log('✅ Sku found at column ' + String.fromCharCode(65 + skuIndex));
    } else {
      console.log('❌ Sku NOT found');
    }

    console.log('\n💡 WHAT TO DO:');
    if (teamIndex === -1) {
      console.log('The Team column does not exist yet!');
      console.log('Run: forceRefreshSkuDataWithTeams()');
      console.log('This will create the Team column automatically.');
    } else if (teamCounts['Unassigned'] === data.length - 1) {
      console.log('Team column exists but all records are Unassigned.');
      console.log('Fill in team names in column ' + String.fromCharCode(65 + teamIndex) + ', then run:');
      console.log('importManualTeamAssignments()');
    } else {
      console.log('Ready to import! Run: importManualTeamAssignments()');
    }

  } catch (error) {
    console.error('❌ Error: ' + error.message);
  }
}

/**
 * Import manual team assignments from the spreadsheet
 * Reads the Team column and saves any manual assignments
 * This makes your manual edits permanent and prevents them from being overwritten
 * 
 * @param {number} teamColumnIndex - Optional: specify exact column index (0-based) to read from
 * @param {string} sheetName - Optional: specify which sheet to read from (default: 'SkuLevelAction')
 */
function importManualTeamAssignments(teamColumnIndex, sheetName) {
  console.log('Importing manual team assignments from spreadsheet...');

  try {
    const impactConfig = new ImpactConfig();
    const metrics = new PerformanceMetrics();
    const logger = new EnhancedLogger(impactConfig, metrics);
    const spreadsheetManager = new EnhancedSpreadsheetManager(impactConfig, logger, metrics);

    const spreadsheet = spreadsheetManager.getSpreadsheet();

    // Default to SkuLevelAction if no sheet specified
    const targetSheet = sheetName || 'SkuLevelAction';
    const skuSheet = spreadsheet.getSheetByName(targetSheet);

    console.log('Reading from sheet: ' + targetSheet);

    if (!skuSheet) {
      console.log('❌ Sheet "' + targetSheet + '" not found.');
      return { success: false, error: 'Sheet not found: ' + targetSheet };
    }

    const data = skuSheet.getDataRange().getValues();
    const headers = data[0];
    const rows = data.slice(1);

    const actionIdIndex = headers.indexOf('ActionId');
    const pubSubid3Index = headers.indexOf('PubSubid3');

    // Allow manual specification of team column, or find it by name
    let teamIndex;
    if (teamColumnIndex !== undefined) {
      teamIndex = teamColumnIndex;
      console.log('Using specified column index: ' + teamColumnIndex + ' (' + String.fromCharCode(65 + teamColumnIndex) + ')');
    } else {
      teamIndex = headers.indexOf('Team');
      console.log('Found Team column at index: ' + teamIndex + ' (' + String.fromCharCode(65 + teamIndex) + ')');
    }

    if (actionIdIndex === -1) {
      console.log('❌ ActionId column not found');
      return { success: false, error: 'ActionId column not found' };
    }

    if (teamIndex === -1) {
      console.log('❌ Team column not found');
      return { success: false, error: 'Team column not found' };
    }

    console.log('Analyzing ' + rows.length + ' records...');

    const formatter = new TeamDisplayFormatter();
    const manualMappings = new ManualTeamMappings();
    const mappingsToAdd = {};

    let importCount = 0;
    let skippedAuto = 0;
    let skippedUnassigned = 0;
    let skippedEmpty = 0;

    rows.forEach(row => {
      const actionId = row[actionIdIndex];
      const teamName = row[teamIndex];
      const pubSubid3 = row[pubSubid3Index] || '';

      // Skip if no ActionId
      if (!actionId) {
        skippedEmpty++;
        return;
      }

      // Skip if no team assigned or still "Unassigned"
      if (!teamName || teamName === 'Unassigned' || teamName === '') {
        skippedUnassigned++;
        return;
      }

      // Check if this team matches what PubSubid3 would auto-assign
      if (pubSubid3) {
        const autoTeam = formatter.toDisplayName(pubSubid3.trim());
        if (autoTeam === teamName) {
          // This is an automatic assignment, no need to save manually
          skippedAuto++;
          return;
        }
      }

      // This is a manual assignment - save it!
      mappingsToAdd[actionId] = teamName;
      importCount++;
    });

    // Save all mappings at once
    if (importCount > 0) {
      manualMappings.addBulkMappings(mappingsToAdd);
    }

    console.log('\n✅ IMPORT COMPLETE!');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('📥 Imported: ' + importCount + ' manual assignments');
    console.log('⏭️  Skipped (auto-assigned): ' + skippedAuto);
    console.log('⏭️  Skipped (unassigned): ' + skippedUnassigned);
    console.log('⏭️  Skipped (empty): ' + skippedEmpty);
    console.log('');

    if (importCount > 0) {
      // Show breakdown by team
      const byTeam = {};
      Object.values(mappingsToAdd).forEach(team => {
        byTeam[team] = (byTeam[team] || 0) + 1;
      });

      console.log('📊 Breakdown by team:');
      Object.entries(byTeam).forEach(([team, count]) => {
        console.log('   ' + team + ': ' + count + ' records');
      });
      console.log('');
      console.log('🎉 Your manual assignments are now saved permanently!');
      console.log('   They will persist even when you refresh data.');
    } else {
      console.log('ℹ️  No manual assignments to import.');
      console.log('   All teams are either auto-assigned or unassigned.');
    }

    return {
      success: true,
      imported: importCount,
      skippedAuto: skippedAuto,
      skippedUnassigned: skippedUnassigned,
      mappings: mappingsToAdd
    };

  } catch (error) {
    console.error('❌ Import failed: ' + error.message);
    return { success: false, error: error.message };
  }
}

/**
 * Force refresh SKU data with teams (bypasses freshness check)
 * Use this to force-update the sheet even if data appears fresh
 */
function forceRefreshSkuDataWithTeams() {
  const config = new ImpactConfig();
  const metrics = new PerformanceMetrics();
  const logger = new EnhancedLogger(config, metrics);
  const apiClient = new EnhancedAPIClient(config, logger, metrics);
  const dataProcessor = new EnhancedDataProcessor(config, logger, metrics);
  const spreadsheetManager = new EnhancedSpreadsheetManager(config, logger, metrics);

  const reportId = 'SkuLevelActions';
  const reportName = 'SkuLevelAction';

  logger.info('FORCE REFRESH: Running SkuLevelAction report with Team enrichment', { reportId: reportId });

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

    // Step 5: ENHANCE DATA WITH TEAM COLUMN
    logger.info('Enriching data with Team column...');
    const enrichedData = enrichSKUDataWithTeams(processedData.data);

    // Update processedData with enriched version
    processedData.data = enrichedData;
    processedData.headers = enrichedData[0];
    processedData.columnCount = enrichedData[0].length;

    logger.info('Data enriched', {
      newColumnCount: processedData.columnCount
    });

    // Step 6: DELETE OLD SHEET AND CREATE NEW ONE
    const spreadsheet = spreadsheetManager.getSpreadsheet();
    const oldSheet = spreadsheet.getSheetByName(reportName);
    if (oldSheet) {
      logger.info('Deleting old sheet to force refresh...');
      spreadsheet.deleteSheet(oldSheet);
    }

    // Step 7: Create the new spreadsheet sheet
    logger.info('Creating new spreadsheet sheet...');
    const sheetInfo = spreadsheetManager.createReportSheet(
      reportId,
      processedData,
      {
        name: reportName,
        jobId: job.jobId,
        scheduledAt: job.scheduledAt
      }
    );

    logger.info('SkuLevelAction report with teams completed successfully!', {
      sheetName: sheetInfo.sheetName,
      rowCount: sheetInfo.rowCount,
      columnCount: sheetInfo.columnCount,
      chunked: sheetInfo.chunked
    });

    console.log('✅ SUCCESS! Sheet recreated with Team column!');
    console.log('   Column count: ' + sheetInfo.columnCount + ' (includes new Team column)');

    return {
      success: true,
      reportId: reportId,
      reportName: reportName,
      sheetName: sheetInfo.sheetName,
      rowCount: sheetInfo.rowCount,
      columnCount: sheetInfo.columnCount,
      chunked: sheetInfo.chunked,
      teamColumnAdded: true,
      forceRefreshed: true,
      metrics: metrics.getSummary()
    };

  } catch (error) {
    logger.error('Force refresh failed', {
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
 * Run complete SKU + Team analysis pipeline
 */
function runCompleteTeamAnalysisPipeline(startDate = null, endDate = null) {
  console.log('=== COMPLETE TEAM ANALYSIS PIPELINE ===');
  console.log('This will:');
  console.log('1. Pull fresh SKU data from Impact.com (WITH TEAM COLUMN)');
  console.log('2. Analyze by team');
  console.log('3. Generate comprehensive reports');
  console.log('');

  try {
    // Step 1: Pull SKU data WITH TEAM ENRICHMENT
    console.log('Step 1: Pulling SKU data with team attribution...');

    // NOTE: Date range filtering with team enrichment not yet implemented
    // For now, use the enriched version without date filtering
    if (startDate && endDate) {
      console.log('⚠️  Date range filtering with team enrichment not yet implemented.');
      console.log('    Using full data pull with teams...');
    }

    // Use the FORCE REFRESH version to ensure Team column is added
    console.log('🔄 Force refreshing to ensure Team column is present...');
    forceRefreshSkuDataWithTeams();

    // Step 2: Run team analysis
    console.log('\nStep 2: Running team analysis...');
    const result = runTeamSKUAnalysis();

    if (result.success) {
      console.log('\n✅ PIPELINE COMPLETE!');
      console.log('');
      console.log('📊 Your spreadsheet now has:');
      console.log('✓ SkuLevelAction sheet WITH Team column');
      console.log('✓ TEAM_SUMMARY_DASHBOARD (overview)');
      console.log('✓ TEAM_[name] sheets (detailed team data)');
      console.log('✓ SKU_PERFORMANCE_BY_TEAM (SKU breakdown)');
      console.log('✓ TEAM_COMPARISON (side-by-side comparison)');
    }

    return result;

  } catch (error) {
    console.error('Pipeline failed: ' + error.message);
    return { success: false, error: error.message };
  }
}

/**
 * Investigate why a specific ActionId was assigned to a team
 * Shows manual mapping status, PubSubid3 value, and what team it would auto-assign to
 * @param {string} actionId - The ActionId to investigate
 */
function investigateTeamAssignment(actionId) {
  const manualMappings = new ManualTeamMappings();
  const formatter = new TeamDisplayFormatter();

  console.log('🔍 INVESTIGATING TEAM ASSIGNMENT');
  console.log('ActionId: ' + actionId);
  console.log('');

  // Check manual mapping
  const manualTeam = manualMappings.getTeam(actionId);
  if (manualTeam) {
    console.log('✅ MANUAL MAPPING FOUND:');
    console.log('   Team: ' + manualTeam);
    console.log('   ⚠️  This overrides PubSubid3!');
    console.log('');
  } else {
    console.log('❌ NO MANUAL MAPPING');
    console.log('   Will use PubSubid3 for auto-assignment');
    console.log('');
  }

  // Try to find this ActionId in multiple sheets
  try {
    const config = new ImpactConfig();
    const spreadsheet = SpreadsheetApp.openById(config.get('spreadsheetId'));

    // Check SkuLevelAction first, then TEAM sheets
    const sheetsToCheck = ['SkuLevelAction'];
    const allSheets = spreadsheet.getSheets();
    allSheets.forEach(s => {
      const name = s.getName();
      if (name.startsWith('TEAM_') && !sheetsToCheck.includes(name)) {
        sheetsToCheck.push(name);
      }
    });

    let foundRow = null;
    let foundSheet = null;
    let foundHeaders = null;

    // Search through sheets
    for (const sheetName of sheetsToCheck) {
      const sheet = spreadsheet.getSheetByName(sheetName);
      if (!sheet) continue;

      const data = sheet.getDataRange().getValues();
      if (data.length < 2) continue;

      const headers = data[0];

      // Try multiple possible ActionId column names
      const actionIdIndex = headers.findIndex(h => {
        if (!h) return false;
        const lower = h.toLowerCase().trim();
        return lower === 'actionid' || lower === 'action_id' || lower === 'action id' ||
          lower.includes('actionid') || lower.includes('action id');
      });

      if (actionIdIndex === -1) {
        // If no ActionId column, try searching all columns for the ActionId value
        for (let colIdx = 0; colIdx < headers.length; colIdx++) {
          const row = data.find(r => r[colIdx] && r[colIdx].toString().trim() === actionId);
          if (row) {
            foundRow = row;
            foundSheet = sheetName;
            foundHeaders = headers;
            // Store the column index where ActionId was found
            const tempActionIdIndex = colIdx;
            break;
          }
        }
        if (foundRow) break;
        continue;
      }

      // Find the row with this ActionId (try exact match and trimmed match)
      const row = data.find(r => {
        const val = r[actionIdIndex];
        if (!val) return false;
        return val.toString().trim() === actionId || val.toString() === actionId;
      });

      if (row) {
        foundRow = row;
        foundSheet = sheetName;
        foundHeaders = headers;
        break;
      }
    }

    if (!foundRow) {
      console.log('❌ ActionId not found in any sheet');
      console.log('   Checked: ' + sheetsToCheck.join(', '));
      console.log('   Make sure the ActionId is correct');
      console.log('');
      console.log('💡 DEBUGGING: Checking TEAM_Notre Dame Fighting Irish sheet structure...');
      const debugSheet = spreadsheet.getSheetByName('TEAM_Notre Dame Fighting Irish');
      if (debugSheet) {
        const debugData = debugSheet.getDataRange().getValues();
        if (debugData.length > 0) {
          console.log('   Available columns:');
          debugData[0].forEach((h, i) => {
            if (h) console.log('     ' + (i + 1) + '. ' + h);
          });
          if (debugData.length > 1) {
            console.log('   Sample row 2 (first data row):');
            debugData[1].slice(0, 10).forEach((val, i) => {
              console.log('     ' + debugData[0][i] + ': ' + (val || '(empty)'));
            });
          }
        }
      }
      return;
    }

    console.log('✅ Found in sheet: ' + foundSheet);
    console.log('');

    const headers = foundHeaders;
    const row = foundRow;

    // Find ActionId column (might have been found by searching all columns)
    let actionIdIndex = headers.findIndex(h => {
      if (!h) return false;
      const lower = h.toLowerCase().trim();
      return lower === 'actionid' || lower === 'action_id' || lower === 'action id' ||
        lower.includes('actionid') || lower.includes('action id');
    });

    // If still not found, search for the value in all columns
    if (actionIdIndex === -1) {
      for (let i = 0; i < row.length; i++) {
        if (row[i] && row[i].toString().trim() === actionId) {
          actionIdIndex = i;
          break;
        }
      }
    }

    const pubSubid3Index = headers.findIndex(h => h && h.toLowerCase() === 'pubsubid3');
    const teamIndex = headers.findIndex(h => h && h.toLowerCase() === 'team');
    const skuIndex = headers.findIndex(h => h && h.toLowerCase() === 'sku');

    console.log('📊 FOUND IN SHEET:');

    if (pubSubid3Index >= 0) {
      const pubSubid3 = row[pubSubid3Index];
      console.log('   PubSubid3: ' + (pubSubid3 || '(empty)'));

      if (pubSubid3 && typeof pubSubid3 === 'string' && pubSubid3.trim()) {
        const autoTeam = formatter.toDisplayName(pubSubid3.trim());
        console.log('   Would auto-assign to: ' + autoTeam);

        if (manualTeam && manualTeam !== autoTeam) {
          console.log('   ⚠️  MISMATCH: Manual mapping (' + manualTeam + ') differs from PubSubid3 (' + autoTeam + ')');
        }
      }
    } else {
      console.log('   ⚠️  PubSubid3 column not found');
    }

    if (teamIndex >= 0) {
      const currentTeam = row[teamIndex];
      console.log('   Current Team in sheet: ' + (currentTeam || '(empty)'));
    }

    if (skuIndex >= 0) {
      const sku = row[skuIndex];
      console.log('   SKU: ' + (sku || '(empty)'));
    }

    console.log('');
    console.log('💡 TO FIX:');
    if (manualTeam) {
      console.log('   Remove manual mapping: removeManualMapping("' + actionId + '")');
      console.log('   Then refresh: forceRefreshSkuDataWithTeams()');
    } else {
      console.log('   Check PubSubid3 value in Impact.com - it may be incorrect');
    }

  } catch (error) {
    console.error('Error investigating: ' + error.message);
  }
}

/**
 * Check a specific row in a TEAM sheet to see team assignment details
 * @param {string} sheetName - Name of the TEAM sheet (e.g., 'TEAM_Notre Dame Fighting Irish')
 * @param {number} rowNumber - Row number (1-based, includes header row, so data row 24 = rowNumber 24)
 */
function checkTeamSheetRow(sheetName, rowNumber) {
  try {
    const config = new ImpactConfig();
    const formatter = new TeamDisplayFormatter();
    const manualMappings = new ManualTeamMappings();
    const spreadsheet = SpreadsheetApp.openById(config.get('spreadsheetId'));
    const sheet = spreadsheet.getSheetByName(sheetName);

    if (!sheet) {
      console.log('❌ Sheet "' + sheetName + '" not found');
      return;
    }

    const data = sheet.getDataRange().getValues();
    if (data.length < rowNumber) {
      console.log('❌ Row ' + rowNumber + ' does not exist (sheet has ' + data.length + ' rows)');
      return;
    }

    const headers = data[0];
    const row = data[rowNumber - 1]; // Convert to 0-based index

    console.log('🔍 CHECKING ROW ' + rowNumber + ' IN ' + sheetName);
    console.log('');

    // Find key columns
    const actionIdIndex = headers.findIndex(h => h && h.toLowerCase() === 'actionid');
    const pubSubid3Index = headers.findIndex(h => h && h.toLowerCase() === 'pubsubid3');
    const teamIndex = headers.findIndex(h => h && h.toLowerCase() === 'team');
    const skuIndex = headers.findIndex(h => h && h.toLowerCase() === 'sku');

    if (actionIdIndex >= 0) {
      const actionId = row[actionIdIndex];
      console.log('📋 ActionId: ' + (actionId || '(empty)'));

      // Check manual mapping
      if (actionId) {
        const manualTeam = manualMappings.getTeam(actionId.toString());
        if (manualTeam) {
          console.log('   ✅ Manual mapping: ' + manualTeam);
        } else {
          console.log('   ❌ No manual mapping');
        }
      }
      console.log('');
    }

    if (pubSubid3Index >= 0) {
      const pubSubid3 = row[pubSubid3Index];
      console.log('📋 PubSubid3: ' + (pubSubid3 || '(empty)'));

      if (pubSubid3 && typeof pubSubid3 === 'string' && pubSubid3.trim()) {
        const autoTeam = formatter.toDisplayName(pubSubid3.trim());
        console.log('   Would auto-assign to: ' + autoTeam);
      }
      console.log('');
    }

    if (teamIndex >= 0) {
      const currentTeam = row[teamIndex];
      console.log('📋 Current Team: ' + (currentTeam || '(empty)'));
      console.log('');
    }

    if (skuIndex >= 0) {
      const sku = row[skuIndex];
      console.log('📋 SKU: ' + (sku || '(empty)'));
      console.log('');
    }

    // Show all columns for debugging
    console.log('📊 ALL COLUMNS IN THIS ROW:');
    headers.forEach((h, i) => {
      if (h) {
        console.log('   ' + h + ': ' + (row[i] || '(empty)'));
      }
    });

  } catch (error) {
    console.error('Error: ' + error.message);
  }
}

/**
 * Check what Team is assigned to a specific ActionId in the SkuLevelAction sheet
 * This helps verify if the Team column is correct before regenerating TEAM_* sheets
 * @param {string} actionId - The ActionId to check (e.g., '9663.6523.1405259')
 */
function checkSkuLevelActionTeam(actionId) {
  try {
    const config = new ImpactConfig();
    const formatter = new TeamDisplayFormatter();
    const manualMappings = new ManualTeamMappings();
    const spreadsheet = SpreadsheetApp.openById(config.get('spreadsheetId'));
    const skuSheet = spreadsheet.getSheetByName('SkuLevelAction') ||
      spreadsheet.getSheetByName('SkuLevelActions');

    if (!skuSheet) {
      console.log('❌ SkuLevelAction sheet not found');
      return;
    }

    const data = skuSheet.getDataRange().getValues();
    if (data.length < 2) {
      console.log('❌ No data in SkuLevelAction sheet');
      return;
    }

    const headers = data[0];
    const rows = data.slice(1);

    // Find columns
    const actionIdIndex = headers.findIndex(h => h && h.toLowerCase() === 'actionid');
    const pubSubid3Index = headers.findIndex(h => h && h.toLowerCase() === 'pubsubid3');
    const teamIndex = headers.findIndex(h => h && h.toLowerCase() === 'team');

    if (actionIdIndex === -1) {
      console.log('❌ ActionId column not found');
      return;
    }

    // Find the row with this ActionId
    const row = rows.find(r => {
      const val = r[actionIdIndex];
      if (!val) return false;
      return val.toString().trim() === actionId || val.toString() === actionId;
    });

    if (!row) {
      console.log('❌ ActionId "' + actionId + '" not found in SkuLevelAction sheet');
      console.log('   Available ActionIds (first 10):');
      rows.slice(0, 10).forEach((r, i) => {
        console.log('   ' + (i + 2) + '. ' + (r[actionIdIndex] || '(empty)'));
      });
      return;
    }

    console.log('✅ FOUND ActionId "' + actionId + '" in SkuLevelAction sheet');
    console.log('');

    // Check manual mapping
    const manualTeam = manualMappings.getTeam(actionId);
    if (manualTeam) {
      console.log('⚠️  MANUAL MAPPING EXISTS: ' + manualTeam);
      console.log('   This overrides PubSubid3!');
    } else {
      console.log('✅ NO MANUAL MAPPING (will use PubSubid3)');
    }
    console.log('');

    // Check PubSubid3
    let pubSubid3 = null;
    let expectedTeam = null;

    if (pubSubid3Index >= 0) {
      pubSubid3 = row[pubSubid3Index];
      console.log('📋 PubSubid3: ' + (pubSubid3 || '(empty)'));

      if (pubSubid3 && typeof pubSubid3 === 'string' && pubSubid3.trim()) {
        expectedTeam = formatter.toDisplayName(pubSubid3.trim());
        console.log('   Should be assigned to: ' + expectedTeam);
      }
      console.log('');
    }

    // Check Team column
    let assignedTeam = null;

    if (teamIndex >= 0) {
      assignedTeam = row[teamIndex];
      console.log('📋 Team Column in Sheet: ' + (assignedTeam || '(empty)'));

      if (expectedTeam) {
        if (assignedTeam !== expectedTeam) {
          console.log('   ⚠️  MISMATCH! Expected: ' + expectedTeam + ', Found: ' + assignedTeam);
          console.log('   This suggests the sheet needs to be refreshed!');
        } else {
          console.log('   ✅ Matches expected team from PubSubid3');
        }
      }
      console.log('');
    } else {
      console.log('⚠️  Team column not found in sheet');
    }

    console.log('💡 TO FIX:');
    if (manualTeam) {
      console.log('   1. Remove manual mapping: removeManualMapping("' + actionId + '")');
      console.log('   2. Refresh: forceRefreshSkuDataWithTeams()');
    } else if (teamIndex >= 0 && expectedTeam && assignedTeam !== expectedTeam) {
      console.log('   1. Refresh: forceRefreshSkuDataWithTeams()');
    }
    console.log('   2. Regenerate TEAM_* sheets: runTeamSKUAnalysis()');

  } catch (error) {
    console.error('Error: ' + error.message);
  }
}

/**
 * Impact.com Business Intelligence Dashboard System
 * 
 * A comprehensive BI solution that transforms Impact.com data into actionable insights
 * with automated dashboards, visualizations, and performance analytics.
 * 
 * @version 1.0.0
 * @author Logan Lorenz
 */

// ============================================================================
// CONFIGURATION
// ============================================================================

class BIConfig {
  constructor() {
    this.props = PropertiesService.getScriptProperties();
    this.config = this.loadConfiguration();
  }

  loadConfiguration() {
    const defaults = {
      // Source data configuration
      sourceSpreadsheetId: this.props.getProperty('IMPACT_SPREADSHEET_ID') || '1aLKEEw7Nx0O1DbZjnXnhOeDjcLRloLN0Y8K2SscZKIc',
      biSpreadsheetId: '1aLKEEw7Nx0O1DbZjnXnhOeDjcLRloLN0Y8K2SscZKIc', // Use the same sheet for output

      // Dashboard settings
      enableAutoRefresh: true,
      refreshInterval: 24, // hours
      enableAlerts: true,
      alertThresholds: {
        revenueDrop: 0.15, // 15% drop triggers alert
        conversionDrop: 0.20, // 20% drop triggers alert
        clickDrop: 0.25 // 25% drop triggers alert
      },

      // Visualization settings
      chartColors: {
        primary: '#2E7D32',
        secondary: '#1976D2',
        success: '#4CAF50',
        warning: '#FF9800',
        danger: '#F44336',
        info: '#2196F3'
      },

      // Date ranges for analysis
      dateRanges: {
        last7Days: 7,
        last30Days: 30,
        last90Days: 90,
        lastYear: 365
      },

      // Performance metrics
      keyMetrics: [
        'revenue',
        'conversions',
        'clicks',
        'earnings',
        'epc',
        'conversion_rate',
        'aov'
      ],

      // Partner segmentation
      partnerTiers: {
        'Top Performers': { minRevenue: 10000, minConversions: 100 },
        'High Performers': { minRevenue: 5000, minConversions: 50 },
        'Medium Performers': { minRevenue: 1000, minConversions: 10 },
        'Low Performers': { minRevenue: 0, minConversions: 0 }
      }
    };

    const configJson = this.props.getProperty('BI_DASHBOARD_CONFIG');
    if (configJson) {
      try {
        return { ...defaults, ...JSON.parse(configJson) };
      } catch (error) {
        Logger.log('Failed to parse BI config: ' + error.message);
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
    this.props.setProperty('BI_DASHBOARD_CONFIG', JSON.stringify(this.config));
  }
}

// ============================================================================
// DATA PROCESSOR
// ============================================================================

class BIDataProcessor {
  constructor(config) {
    this.config = config;
  }

  /**
   * Helper to find value by fuzzy key match
   */
  getValue(row, targetKey) {
    if (!row) return undefined;

    // Direct match
    if (row[targetKey] !== undefined) return row[targetKey];

    // Case-insensitive match
    const lowerTarget = targetKey.toLowerCase();
    // Check if any key matches case-insensitively
    let key = Object.keys(row).find(k => k.toLowerCase() === lowerTarget);
    if (key) return row[key];

    // Normalized match (remove spaces, underscores, special chars)
    const normalize = k => k.toString().toLowerCase().replace(/[^a-z0-9]/g, '');
    const normalizedTarget = normalize(targetKey);

    key = Object.keys(row).find(k => normalize(k) === normalizedTarget);
    return key ? row[key] : undefined;
  }

  /**
   * Extract and normalize data from Impact.com reports
   */
  processSourceData() {
    const sourceSpreadsheet = SpreadsheetApp.openById(this.config.get('sourceSpreadsheetId'));
    const sheets = sourceSpreadsheet.getSheets();

    const processedData = {
      partnerPerformance: [],
      campaignPerformance: [],
      clickPerformance: [],
      conversionPerformance: [],
      creativePerformance: [],
      teamPerformance: [], // Added for Team Analysis
      skuMetadata: {}, // Map of SKU -> ItemName
      summary: {
        totalRevenue: 0,
        totalConversions: 0,
        totalClicks: 0,
        totalEarnings: 0,
        dateRange: { start: null, end: null },
        reportCount: 0
      }
    };

    sheets.forEach(sheet => {
      const sheetName = sheet.getName();
      const lowerSheetName = sheetName.toLowerCase();

      // Skip Discovery Summary and Dashboard Sheets
      if (lowerSheetName === 'discovery summary') return;
      if (sheetName.match(/^[📊📈🏆🤝🎯💰📅]/)) return; // Skip sheets starting with emojis
      if (lowerSheetName.includes('analysis') && !lowerSheetName.includes('sku')) return; // Skip analysis sheets but keep SkuLevelAction
      if (lowerSheetName.includes('summary') && !lowerSheetName.includes('sku')) return; // Skip summary sheets

      try {
        const rawData = this.extractSheetData(sheet);
        if (rawData.length === 0) return;

        // Process specific data types
        if (lowerSheetName.includes('partner') || lowerSheetName.includes('subid') || lowerSheetName.includes('sub id')) {
          const partnerData = this.processPartnerData(rawData);
          processedData.partnerPerformance = partnerData;
        } else if (lowerSheetName.includes('campaign')) {
          processedData.campaignPerformance = this.processCampaignData(rawData);
        } else if (lowerSheetName.includes('click')) {
          processedData.clickPerformance = this.processClickData(rawData);
        } else if (lowerSheetName.includes('conversion')) {
          processedData.conversionPerformance = this.processConversionData(rawData);
        } else if (lowerSheetName.includes('creative')) {
          processedData.creativePerformance = this.processCreativeData(rawData);
        } else if (lowerSheetName.includes('skulevelaction')) {
          // This is our Team data source
          const teamData = this.processTeamData(rawData);
          processedData.teamPerformance = teamData;
        } else if (lowerSheetName.includes('action sku listing') || lowerSheetName.includes('actionsku')) {
          // Extract metadata (Item Names) for products
          this.processSkuMetadata(rawData, processedData.skuMetadata);
        }

      } catch (error) {
        Logger.log('Error processing sheet ' + sheetName + ': ' + error.message);
      }
    });

    // Check for potential duplicate data sources
    const sources = [];
    if (processedData.partnerPerformance && processedData.partnerPerformance.length > 0) sources.push('Partner (' + processedData.partnerPerformance.length + ' rows)');
    if (processedData.teamPerformance && processedData.teamPerformance.length > 0) sources.push('Team (' + processedData.teamPerformance.length + ' rows)');
    if (processedData.campaignPerformance && processedData.campaignPerformance.length > 0) sources.push('Campaign (' + processedData.campaignPerformance.length + ' rows)');

    Logger.log('📊 Data Sources Summary: ' + JSON.stringify(sources));

    if (sources.length > 1) {
      Logger.log('⚠️ WARNING: Multiple data sources found. Using ' + sources[0] + ' data for summary totals.');
    }

    // Calculate summary from a SINGLE source of truth to avoid double counting
    // Priority: Partner Data > Team Data > Campaign Data
    if (processedData.partnerPerformance && processedData.partnerPerformance.length > 0) {
      Logger.log('Using Partner Performance for Summary. Sample Row: ' + JSON.stringify(processedData.partnerPerformance[0]));
      this.calculateSummaryFromData(processedData.summary, processedData.partnerPerformance);
    } else if (processedData.teamPerformance && processedData.teamPerformance.length > 0) {
      Logger.log('Using Team Performance for Summary. Sample Row: ' + JSON.stringify(processedData.teamPerformance[0]));
      this.calculateSummaryFromData(processedData.summary, processedData.teamPerformance);
    } else if (processedData.campaignPerformance && processedData.campaignPerformance.length > 0) {
      this.calculateSummaryFromData(processedData.summary, processedData.campaignPerformance);
    } else {
      Logger.log('❌ CRITICAL: No data found for summary calculation!');
    }

    // Run QA Checks
    this.performDataQA(processedData);

    return processedData;
  }

  /**
   * Perform Quality Assurance checks on processed data
   */
  performDataQA(data) {
    Logger.log('🔍 Starting Data QA Checks...');
    const warnings = [];

    // 1. Consistency Check
    // If we have both Partner and Team data, their totals should be roughly similar
    if (data.partnerPerformance && data.partnerPerformance.length > 0 &&
      data.teamPerformance && data.teamPerformance.length > 0) {

      const partnerRevenue = data.partnerPerformance.reduce((sum, p) => sum + p.revenue, 0);
      const teamRevenue = data.teamPerformance.reduce((sum, t) => sum + t.revenue, 0);

      // Allow for small differences due to rounding or slight data mismatches
      const diff = Math.abs(partnerRevenue - teamRevenue);
      const diffPercent = partnerRevenue > 0 ? (diff / partnerRevenue) * 100 : 0;

      if (diffPercent > 5) {
        warnings.push(`Consistency Warning: Partner Revenue ($${partnerRevenue.toFixed(2)}) and Team Revenue ($${teamRevenue.toFixed(2)}) differ by ${diffPercent.toFixed(2)}%`);
      }
    }

    // 2. Coverage Check (Unassigned Teams)
    if (data.teamPerformance && data.teamPerformance.length > 0) {
      // Aggregate team data first to handle multiple rows per team if any
      const teamTotals = {};
      data.teamPerformance.forEach(t => {
        if (!teamTotals[t.team]) teamTotals[t.team] = 0;
        teamTotals[t.team] += t.revenue;
      });

      const unassignedRevenue = teamTotals['Unassigned'] || 0;
      const totalRevenue = data.summary.totalRevenue;

      if (totalRevenue > 0) {
        const unassignedShare = (unassignedRevenue / totalRevenue) * 100;
        if (unassignedShare > 10) {
          warnings.push(`Coverage Warning: ${unassignedShare.toFixed(2)}% of revenue is Unassigned. Check team mapping rules.`);
        }
      }
    }

    // 3. Zero Check
    if (data.summary.totalRevenue === 0) warnings.push('Critical Warning: Total Revenue is $0.00');
    if (data.summary.totalConversions === 0) warnings.push('Critical Warning: Total Conversions is 0');

    // 4. Date Check
    if (!data.summary.dateRange.start || !data.summary.dateRange.end) {
      warnings.push('Data Warning: Invalid or missing date range.');
    }

    // Log results
    if (warnings.length > 0) {
      Logger.log('⚠️ QA Warnings Found:');
      warnings.forEach(w => Logger.log('   - ' + w));
    } else {
      Logger.log('✅ QA Checks Passed: Data looks consistent.');
    }

    return warnings;
  }

  /**
   * Calculate summary metrics from a single data set
   */
  calculateSummaryFromData(summary, data) {
    data.forEach(row => {
      summary.totalRevenue += (row.revenue || 0);
      summary.totalConversions += (row.conversions || 0);
      summary.totalClicks += (row.clicks || 0);
      summary.totalEarnings += (row.earnings || 0);

      const date = row.date;
      if (date) {
        if (!summary.dateRange.start || date < summary.dateRange.start) {
          summary.dateRange.start = date;
        }
        if (!summary.dateRange.end || date > summary.dateRange.end) {
          summary.dateRange.end = date;
        }
      }
    });
  }

  extractSheetData(sheet) {
    const data = sheet.getDataRange().getValues();
    if (data.length < 2) return [];

    // Find the header row
    // Look for a row that contains at least one of these common columns
    const commonColumns = ['Partner', 'Campaign', 'Impact Media Partner Id', 'Sale Amount', 'Revenue', 'Clicks', 'Actions', 'Conversions', 'PubSubid3', 'Team'];

    let headerRowIndex = -1;
    for (let i = 0; i < Math.min(data.length, 20); i++) {
      const row = data[i].map(cell => cell.toString().toLowerCase());
      const match = commonColumns.some(col =>
        row.some(cell => cell.includes(col.toLowerCase()))
      );

      if (match) {
        headerRowIndex = i;
        break;
      }
    }

    if (headerRowIndex === -1) {
      Logger.log('⚠️ Could not find a valid header row in sheet: ' + sheet.getName());
      return [];
    }

    const headers = data[headerRowIndex];
    Logger.log('✅ Found headers in ' + sheet.getName() + ' (Row ' + (headerRowIndex + 1) + '): ' + headers.join(', '));
    const rows = data.slice(headerRowIndex + 1);

    return rows.map(row => {
      const obj = {};
      headers.forEach((header, index) => {
        // Only add if header is not empty
        if (header) {
          obj[header] = row[index];
        }
      });
      return obj;
    });
  }

  processPartnerData(data) {
    return data.map(row => ({
      partner: this.getValue(row, 'Partner') || this.getValue(row, 'SubID') || this.getValue(row, 'pubsubid1_') || 'Unknown',
      revenue: this.parseNumber(this.getValue(row, 'Sale_amount') || this.getValue(row, 'sale_amount') || this.getValue(row, 'Revenue') || this.getValue(row, 'Total Revenue') || 0),
      conversions: this.parseNumber(this.getValue(row, 'Actions') || this.getValue(row, 'Conversions') || 0),
      clicks: this.parseNumber(this.getValue(row, 'Clicks') || this.getValue(row, 'raw_clicks') || 0),
      earnings: this.parseNumber(this.getValue(row, 'Earnings') || this.getValue(row, 'Total Cost') || 0),
      epc: this.parseNumber(this.getValue(row, 'EPC') || 0),
      conversionRate: this.calculateConversionRate(
        this.parseNumber(this.getValue(row, 'Clicks') || 0),
        this.parseNumber(this.getValue(row, 'Actions') || this.getValue(row, 'Conversions') || 0)
      ),
      aov: this.calculateAOV(
        this.parseNumber(this.getValue(row, 'Sale_amount') || this.getValue(row, 'Revenue') || 0),
        this.parseNumber(this.getValue(row, 'Actions') || this.getValue(row, 'Conversions') || 0)
      ),
      date: this.parseDate(this.getValue(row, 'Date') || this.getValue(row, 'Period'))
    }));
  }

  processCampaignData(data) {
    return data.map(row => ({
      campaign: this.getValue(row, 'Campaign') || 'Unknown',
      revenue: this.parseNumber(this.getValue(row, 'Sale_amount') || this.getValue(row, 'Revenue') || 0),
      conversions: this.parseNumber(this.getValue(row, 'Actions') || this.getValue(row, 'Conversions') || 0),
      clicks: this.parseNumber(this.getValue(row, 'Clicks') || 0),
      earnings: this.parseNumber(this.getValue(row, 'Earnings') || 0),
      cpc: this.parseNumber(this.getValue(row, 'CPC_Cost') || this.getValue(row, 'Click_Cost') || this.getValue(row, 'cpc') || 0),
      date: this.parseDate(this.getValue(row, 'Date') || this.getValue(row, 'Period'))
    }));
  }

  processClickData(data) {
    return data.map(row => ({
      partner: this.getValue(row, 'Partner') || this.getValue(row, 'SubID') || 'Unknown',
      campaign: this.getValue(row, 'Campaign') || 'Unknown',
      clicks: this.parseNumber(this.getValue(row, 'Clicks') || 0),
      impressions: this.parseNumber(this.getValue(row, 'Impressions') || 0),
      ctr: this.parseNumber(this.getValue(row, 'CTR') || 0),
      date: this.parseDate(this.getValue(row, 'Date') || this.getValue(row, 'Period'))
    }));
  }

  processConversionData(data) {
    return data.map(row => ({
      partner: this.getValue(row, 'Partner') || this.getValue(row, 'SubID') || 'Unknown',
      campaign: this.getValue(row, 'Campaign') || 'Unknown',
      conversions: this.parseNumber(this.getValue(row, 'Actions') || this.getValue(row, 'Conversions') || 0),
      revenue: this.parseNumber(this.getValue(row, 'Sale_amount') || this.getValue(row, 'Revenue') || 0),
      earnings: this.parseNumber(this.getValue(row, 'Earnings') || 0),
      date: this.parseDate(this.getValue(row, 'Date') || this.getValue(row, 'Period'))
    }));
  }

  processCreativeData(data) {
    return data.map(row => ({
      creative: this.getValue(row, 'Creative') || 'Unknown',
      campaign: this.getValue(row, 'Campaign') || 'Unknown',
      clicks: this.parseNumber(this.getValue(row, 'Clicks') || 0),
      conversions: this.parseNumber(this.getValue(row, 'Actions') || this.getValue(row, 'Conversions') || 0),
      revenue: this.parseNumber(this.getValue(row, 'Sale_amount') || this.getValue(row, 'Revenue') || 0),
      ctr: this.parseNumber(this.getValue(row, 'CTR') || 0),
      date: this.parseDate(this.getValue(row, 'Date') || this.getValue(row, 'Period'))
    }));
  }

  processTeamData(data) {
    // Extract Team data from SkuLevelAction reports
    // Looks for 'Team' column which is added by team-sku-analysis.js
    // OR falls back to PubSubid3 if Team column is missing (though it should be there)

    return data.map(row => {
      let team = this.getValue(row, 'Team') || 'Unassigned';
      const pubSubid1 = (this.getValue(row, 'PubSubid1') || '').toString().toLowerCase();
      const pubSubid3 = (this.getValue(row, 'PubSubid3') || '').toString();

      // Fallback logic if Team column is missing but PubSubid3 exists
      if ((team === 'Unassigned' || !team) && pubSubid3) {
        // Simple formatter fallback
        team = pubSubid3.split('-')
          .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
          .join(' ');
      }

      // Robust column mapping
      const revenue = this.parseNumber(this.getValue(row, 'SaleAmount') || this.getValue(row, 'Sale Amount') || this.getValue(row, 'Revenue') || 0);
      const quantity = this.parseNumber(this.getValue(row, 'Quantity') || 1);
      const conversions = this.parseNumber(this.getValue(row, 'Actions') || this.getValue(row, 'Conversions') || quantity || 1); // Default to quantity or 1 for SKU reports
      const earnings = this.parseNumber(this.getValue(row, 'Commission') || this.getValue(row, 'Earnings') || 0);

      // Product Details
      const productUrl = this.getValue(row, 'Fanatics Search URL') || '';
      const sku = this.getValue(row, 'Sku') || this.getValue(row, 'SKU') || 'Unknown';
      const category = this.getValue(row, 'Category') || 'Unknown';
      const itemName = this.getValue(row, 'ItemName') || this.getValue(row, 'Item Name') || '';

      return {
        team: team,
        isMula: pubSubid1.includes('mula'),
        revenue: revenue,
        conversions: conversions,
        earnings: earnings,
        quantity: quantity,
        productUrl: productUrl,
        sku: sku,
        category: category,
        itemName: itemName,
        itemName: itemName,
        date: this.parseDate(this.getValue(row, 'ActionDate') || this.getValue(row, 'Action Date') || this.getValue(row, 'Date') || this.getValue(row, 'Period')),

        // Status Breakdown
        status: this.getValue(row, 'Action Status') || this.getValue(row, 'Status') || 'Pending',
        revenueApproved: (this.getValue(row, 'Action Status') || '').toString().toLowerCase() === 'approved' ? revenue : 0,
        revenuePending: (this.getValue(row, 'Action Status') || '').toString().toLowerCase() === 'pending' ? revenue : 0
      };
    });
  }

  processSkuMetadata(data, metadataStore) {
    data.forEach(row => {
      const sku = this.getValue(row, 'SKU') || this.getValue(row, 'Sku');
      let itemName = this.getValue(row, 'ItemName') || this.getValue(row, 'Item Name');
      const category = this.getValue(row, 'Category');

      // Fallback strategies for missing Item Names
      if (!itemName || itemName === '') {
        if (category && category !== '') {
          itemName = category; // Use Category as name if ItemName is missing
        } else {
          itemName = 'Product ' + sku; // Last resort
        }
      }

      if (sku) {
        metadataStore[sku] = itemName;
      }
    });
    Logger.log('Processed metadata for ' + Object.keys(metadataStore).length + ' SKUs');
  }

  parseNumber(value) {
    if (typeof value === 'number') return value;
    if (typeof value === 'string') {
      return parseFloat(value.replace(/[^0-9.-]/g, '')) || 0;
    }
    return 0;
  }

  parseDate(value) {
    if (!value) return null;
    if (value instanceof Date) return value;
    if (typeof value === 'string') {
      const date = new Date(value);
      return isNaN(date.getTime()) ? null : date;
    }
    return null;
  }

  calculateConversionRate(clicks, conversions) {
    return clicks > 0 ? (conversions / clicks) * 100 : 0;
  }

  calculateAOV(revenue, conversions) {
    return conversions > 0 ? revenue / conversions : 0;
  }
}

// ============================================================================
// DASHBOARD BUILDER
// ============================================================================

class BIDashboardBuilder {
  constructor(config, dataProcessor) {
    this.config = config;
    this.dataProcessor = dataProcessor;
    this.biSpreadsheet = null;
  }

  /**
   * Create the complete BI dashboard
   */
  createDashboard() {
    Logger.log('Creating Business Intelligence Dashboard...');

    // Create or get BI spreadsheet
    this.biSpreadsheet = this.getOrCreateBISpreadsheet();

    // Process source data
    const data = this.dataProcessor.processSourceData();

    // Create dashboard sheets
    this.createExecutiveSummary(data);
    this.createTeamAnalysis(data); // New Team Analysis Sheet
    this.createFinancialOverview(data);

    Logger.log('BI Dashboard created successfully!');
    return this.biSpreadsheet.getUrl();
  }

  getOrCreateBISpreadsheet() {
    const biSpreadsheetId = this.config.get('biSpreadsheetId');

    if (biSpreadsheetId) {
      try {
        return SpreadsheetApp.openById(biSpreadsheetId);
      } catch (error) {
        Logger.log('BI Spreadsheet not found, creating new one...');
      }
    }

    // Create new BI spreadsheet
    const biSpreadsheet = SpreadsheetApp.create('Impact.com Business Intelligence Dashboard');
    this.config.set('biSpreadsheetId', biSpreadsheet.getId());

    return biSpreadsheet;
  }

  createExecutiveSummary(data) {
    const sheet = this.getOrCreateSheet('📊 Executive Summary');

    // Clear existing content
    sheet.clear();

    // Header
    sheet.getRange('A1').setValue('Impact.com Business Intelligence Dashboard');
    sheet.getRange('A1').setFontSize(20).setFontWeight('bold');
    sheet.getRange('A1:F1').merge();

    // Clear existing charts to prevent stacking
    const charts = sheet.getCharts();
    charts.forEach(c => sheet.removeChart(c));

    // Timeframe & Last Updated
    const dateRange = data.summary.dateRange;
    const timeframe = dateRange.start && dateRange.end
      ? `${dateRange.start.toLocaleDateString()} - ${dateRange.end.toLocaleDateString()}`
      : 'All Time';

    sheet.getRange('A2').setValue(`Timeframe: ${timeframe} | Last Updated: ${new Date().toLocaleString()}`);
    sheet.getRange('A2').setFontStyle('italic').setFontSize(10);

    // Key Metrics Row
    const metricsRow = 4;
    const metrics = [
      ['Total Revenue', this.formatCurrency(data.summary.totalRevenue)],
      ['Total Conversions', this.formatNumber(data.summary.totalConversions)],
      ['Total Clicks', this.formatNumber(data.summary.totalClicks)],
      ['Total Earnings', this.formatCurrency(data.summary.totalEarnings)],
      ['Conversion Rate', this.formatPercentage(this.calculateOverallConversionRate(data))],
      ['Average Order Value', this.formatCurrency(this.calculateOverallAOV(data))]
    ];

    // Headers
    sheet.getRange(metricsRow, 1, 1, 2).setValues([['Metric', 'Value']]);
    sheet.getRange(metricsRow, 1, 1, 2).setFontWeight('bold').setBackground('#2E7D32').setFontColor('white');

    // Metrics data
    sheet.getRange(metricsRow + 1, 1, metrics.length, 2).setValues(metrics);

    // Format metrics
    const metricsRange = sheet.getRange(metricsRow + 1, 1, metrics.length, 2);
    metricsRange.setBorder(true, true, true, true, true, true);

    // Add charts
    this.addRevenueChart(sheet, data, 'H4');
    this.addConversionChart(sheet, data, 'H20');

    // Auto-resize columns
    sheet.autoResizeColumns(1, 10);
  }

  createTeamAnalysis(data) {
    const sheet = this.getOrCreateSheet('🏆 Team Analysis');

    sheet.clear();
    const charts = sheet.getCharts();
    charts.forEach(c => sheet.removeChart(c));

    sheet.getRange('A1').setValue('Team Performance Analysis');
    sheet.getRange('A1').setFontSize(16).setFontWeight('bold');

    // Timeframe
    const dateRange = data.summary.dateRange;
    const timeframe = dateRange.start && dateRange.end
      ? `${dateRange.start.toLocaleDateString()} - ${dateRange.end.toLocaleDateString()}`
      : 'All Time';
    sheet.getRange('A2').setValue(`Timeframe: ${timeframe}`);
    sheet.getRange('A2').setFontStyle('italic');

    // Aggregate Team Data (All)
    const teamData = this.aggregateTeamData(data.teamPerformance);

    // Aggregate Team Data (Mula Only)
    // Ensure we are filtering correctly
    const mulaTeamData = this.aggregateTeamData(data.teamPerformance.filter(d => d.isMula));

    if (teamData.length === 0) {
      sheet.getRange('A3').setValue('No Team data available. Ensure SkuLevelAction reports are processed.');
      return;
    }

    // --- Section 1: Overall Team Performance ---
    sheet.getRange('A3').setValue('Overall Team Performance');
    sheet.getRange('A3').setFontSize(12).setFontWeight('bold').setFontColor('#1565C0');
    this.createTeamPerformanceTable(sheet, teamData, 4);
    this.addTeamRevenueChart(sheet, teamData, 'H4', 'Top Teams by Revenue (Overall)');

    // --- Section 2: Mula Traffic Analysis ---
    const mulaRow = 4 + Math.min(teamData.length, 20) + 4; // Dynamic positioning
    sheet.getRange('A' + mulaRow).setValue('Mula Traffic Analysis (SubId1 = "mula")');
    sheet.getRange('A' + mulaRow).setFontSize(12).setFontWeight('bold').setFontColor('#2E7D32');

    if (mulaTeamData.length > 0) {
      this.createTeamPerformanceTable(sheet, mulaTeamData, mulaRow + 1);
      this.addTeamRevenueChart(sheet, mulaTeamData, 'H' + (mulaRow + 1), 'Top Teams by Revenue (Mula Only)');
    } else {
      sheet.getRange('A' + (mulaRow + 1)).setValue('No Mula traffic data found. Check if SubId1="mula" exists in source data.');
    }

    // --- Section 3: NIL Powerhouse Matrix ---
    this.createPowerhouseMatrix(sheet, teamData);

    // --- Section 4: Top Products ---
    // Add significant padding for the chart (approx 25 rows)
    // createPowerhouseMatrix starts at getLastRow() + 3, and is 400px high (~20 rows)
    // We'll pad with empty rows to ensure we don't write over it
    this.createTopProductsTable(sheet, teamData, data.skuMetadata, 25);

    sheet.autoResizeColumns(1, 10);
  }

  createFinancialOverview(data) {
    const sheet = this.getOrCreateSheet('💰 Financial Overview');

    sheet.clear();
    sheet.getRange('A1').setValue('Financial Performance Overview');
    sheet.getRange('A1').setFontSize(16).setFontWeight('bold');

    // Timeframe
    const dateRange = data.summary.dateRange;
    const timeframe = dateRange.start && dateRange.end
      ? `${dateRange.start.toLocaleDateString()} - ${dateRange.end.toLocaleDateString()}`
      : 'All Time';
    sheet.getRange('A2').setValue(`Timeframe: ${timeframe}`);
    sheet.getRange('A2').setFontStyle('italic');

    // Financial summary
    const financialData = this.calculateFinancialMetrics(data);
    this.createFinancialTable(sheet, financialData, 3);

    // Revenue breakdown
    const revenueBreakdown = this.calculateRevenueBreakdown(data);
    this.createRevenueBreakdownTable(sheet, revenueBreakdown, 10);

    // Financial charts
    this.addRevenueBreakdownChart(sheet, revenueBreakdown, 'H3');
    this.addEarningsChart(sheet, financialData, 'H15');

    sheet.autoResizeColumns(1, 10);
  }



  // Helper methods for data processing and visualization
  getOrCreateSheet(sheetName) {
    let sheet = this.biSpreadsheet.getSheetByName(sheetName);
    if (!sheet) {
      sheet = this.biSpreadsheet.insertSheet(sheetName);
    }
    return sheet;
  }

  aggregateTeamData(teamData) {
    const aggregated = {};

    if (!teamData) return [];

    teamData.forEach(item => {
      const key = item.team;
      if (!aggregated[key]) {
        aggregated[key] = {
          team: key,
          revenue: 0,
          revenueApproved: 0,
          revenuePending: 0,
          conversions: 0,
          earnings: 0,
          quantity: 0
        };
      }

      aggregated[key].revenue += item.revenue;
      aggregated[key].revenueApproved += (item.revenueApproved || 0);
      aggregated[key].revenuePending += (item.revenuePending || 0);
      aggregated[key].conversions += item.conversions;
      aggregated[key].earnings += item.earnings;
      aggregated[key].quantity += item.quantity;
    });

    // Calculate derived metrics
    Object.values(aggregated).forEach(team => {
      team.aov = team.conversions > 0 ? team.revenue / team.conversions : 0;
      team.commissionRate = team.revenue > 0 ? (team.earnings / team.revenue) * 100 : 0;
    });

    return Object.values(aggregated).sort((a, b) => b.revenue - a.revenue);
  }

  createTeamPerformanceTable(sheet, data, startRow) {
    const headers = ['Rank', 'Team', 'Total Revenue', 'Approved Rev', 'Pending Rev', 'Conversions', 'Earnings', 'AOV', 'Comm. Rate'];

    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#1976D2').setFontColor('white');

    const rows = data.map((team, index) => [
      index + 1,
      team.team,
      this.formatCurrency(team.revenue),
      this.formatCurrency(team.revenueApproved),
      this.formatCurrency(team.revenuePending),
      this.formatNumber(team.conversions),
      this.formatCurrency(team.earnings),
      this.formatCurrency(team.aov),
      this.formatPercentage(team.commissionRate)
    ]);

    if (rows.length > 0) {
      sheet.getRange(startRow + 1, 1, rows.length, headers.length).setValues(rows);
      sheet.getRange(startRow + 1, 1, rows.length, headers.length).setBorder(true, true, true, true, true, true);
    }
  }

  addTeamRevenueChart(sheet, data, position, title = 'Top Teams by Revenue') {
    if (data.length === 0) return;

    const chart = sheet.newChart()
      .setChartType(Charts.ChartType.BAR)
      .addRange(sheet.getRange(sheet.getRange(position).getRow() + 1, 2, Math.min(data.length, 10), 2)) // Team and Revenue
      .setPosition(sheet.getRange(position).getRow(), 8, 0, 0)
      .setOption('title', title)
      .setOption('legend', { position: 'none' })
      .build();

    sheet.insertChart(chart);
  }

  createPowerhouseMatrix(sheet, teamData) {
    if (!teamData || teamData.length === 0) return;

    // Find row to start (append to bottom)
    const row = sheet.getLastRow() + 3;

    // Section Header
    sheet.getRange(row, 1).setValue('🏆 NIL Powerhouse Matrix (Value vs. Volume)');
    sheet.getRange(row, 1).setFontSize(12).setFontWeight('bold').setFontColor('#673AB7');
    sheet.getRange(row + 1, 1).setValue('A strategic view of team performance: High Revenue (Volume) vs. High Ticket Size (Value)');
    sheet.getRange(row + 1, 1).setFontStyle('italic').setFontSize(10);

    // Filter significant data to reduce noise/clutter on chart
    // Only include teams with > 0 revenue for meaningful scatter
    const significantTeams = teamData.filter(t => t.revenue > 0);

    if (significantTeams.length === 0) {
      sheet.getRange(row + 3, 1).setValue('Not enough data for matrix.');
      return;
    }

    // Write data for chart (Side area to keep main view clean)
    // We'll put it starting at Column M (13)
    const chartDataRow = row + 1;
    const chartDataCol = 13; // Column M

    // Headers: Team, Revenue (X), AOV (Y)
    sheet.getRange(chartDataRow, chartDataCol).setValue('Team');
    sheet.getRange(chartDataRow, chartDataCol + 1).setValue('Volume ($ Revenue)');
    sheet.getRange(chartDataRow, chartDataCol + 2).setValue('Value ($ AOV)');

    const chartData = significantTeams.map(t => [t.team, t.revenue, t.aov]);
    sheet.getRange(chartDataRow + 1, chartDataCol, chartData.length, 3).setValues(chartData);

    try {
      const chart = sheet.newChart()
        .setChartType(Charts.ChartType.SCATTER)
        .addRange(sheet.getRange(chartDataRow, chartDataCol, chartData.length + 1, 3))
        .setPosition(row + 3, 1, 0, 0)
        .setOption('title', 'NIL Powerhouse Matrix: Value ($ AOV) vs. Volume ($ Revenue)')
        .setOption('hAxis', { title: 'Total Revenue Volume ($)', format: '$#' })
        .setOption('vAxis', { title: 'Average Order Value ($)', format: '$#' })
        .setOption('legend', { position: 'right' })
        .setOption('pointSize', 7)
        .setOption('height', 400)
        .setOption('width', 600)
        .build();

      sheet.insertChart(chart);
    } catch (e) {
      Logger.log('Failed to create Powerhouse Matrix chart: ' + e.message);
      sheet.getRange(row + 3, 1).setValue('Error creating chart: ' + e.message);
    }
  }

  aggregateProductData(teamData, skuMetadata = {}) {
    const products = {};

    teamData.forEach(item => {
      // Create a unique key for the product (URL is best, fallback to SKU)
      const key = item.productUrl || item.sku || 'Unknown';

      if (!products[key]) {
        products[key] = {
          productUrl: item.productUrl,
          sku: item.sku,
          category: item.category,
          itemName: item.itemName,
          revenue: 0,
          conversions: 0,
          quantity: 0
        };
      }

      // Enrich Name if missing
      if ((!products[key].itemName || products[key].itemName === '') && products[key].sku && skuMetadata[products[key].sku]) {
        products[key].itemName = skuMetadata[products[key].sku];
      }

      products[key].revenue += item.revenue;
      products[key].conversions += item.conversions;
      products[key].quantity += item.quantity;

      // Update metadata if it was missing and now present
      if (!products[key].sku || products[key].sku === 'Unknown') products[key].sku = item.sku;
      if (!products[key].itemName) products[key].itemName = item.itemName;
    });

    return Object.values(products).sort((a, b) => b.revenue - a.revenue);
  }

  createTopProductsTable(sheet, teamData, skuMetadata, paddingRows = 4) {
    if (!teamData || teamData.length === 0) return;

    // Use padding to clear previous elements (like charts)
    const row = sheet.getLastRow() + paddingRows;

    // Header
    sheet.getRange(row, 1).setValue('🛍️ Top Selling Products');
    sheet.getRange(row, 1).setFontSize(12).setFontWeight('bold').setFontColor('#E91E63'); // Pink/Red for products

    // Aggregate Data
    const products = this.aggregateProductData(teamData, skuMetadata).slice(0, 20); // Top 20

    if (products.length === 0) {
      sheet.getRange(row + 1, 1).setValue('No product data available.');
      return;
    }

    const startRow = row + 1;
    const headers = ['Rank', 'SKU', 'Category', 'Item Name', 'Revenue', 'Units Sold', 'Product Link'];

    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#AD1457').setFontColor('white');

    const tableData = products.map((p, index) => {
      const linkFormula = p.productUrl
        ? `=HYPERLINK("${p.productUrl}", "View Item 🔗")`
        : 'No Link';

      return [
        index + 1,
        p.sku,
        p.category,
        p.itemName,
        this.formatCurrency(p.revenue),
        this.formatNumber(p.quantity),
        linkFormula
      ];
    });

    sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setValues(tableData);
    sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setBorder(true, true, true, true, true, true);
  }

  addTeamConversionChart(sheet, data, position) {
    if (data.length === 0) return;

    const chart = sheet.newChart()
      .setChartType(Charts.ChartType.PIE)
      .addRange(sheet.getRange(4, 2, Math.min(data.length, 5), 1)) // Teams
      .addRange(sheet.getRange(4, 4, Math.min(data.length, 5), 1)) // Conversions
      .setPosition(15, 8, 0, 0)
      .setOption('title', 'Conversion Share by Team')
      .build();

    sheet.insertChart(chart);
  }

  aggregatePartnerData(partnerData) {
    const aggregated = {};

    partnerData.forEach(partner => {
      const key = partner.partner;
      if (!aggregated[key]) {
        aggregated[key] = {
          partner: key,
          revenue: 0,
          conversions: 0,
          clicks: 0,
          earnings: 0,
          epc: 0,
          conversionRate: 0,
          aov: 0
        };
      }

      aggregated[key].revenue += partner.revenue;
      aggregated[key].conversions += partner.conversions;
      aggregated[key].clicks += partner.clicks;
      aggregated[key].earnings += partner.earnings;
    });

    // Calculate derived metrics
    Object.values(aggregated).forEach(partner => {
      partner.epc = partner.clicks > 0 ? partner.earnings / partner.clicks : 0;
      partner.conversionRate = partner.clicks > 0 ? (partner.conversions / partner.clicks) * 100 : 0;
      partner.aov = partner.conversions > 0 ? partner.revenue / partner.conversions : 0;
    });

    return Object.values(aggregated).sort((a, b) => b.revenue - a.revenue);
  }

  aggregateCampaignData(campaignData) {
    const aggregated = {};

    campaignData.forEach(campaign => {
      const key = campaign.campaign;
      if (!aggregated[key]) {
        aggregated[key] = {
          campaign: key,
          revenue: 0,
          conversions: 0,
          clicks: 0,
          earnings: 0,
          cpc: 0
        };
      }

      aggregated[key].revenue += campaign.revenue;
      aggregated[key].conversions += campaign.conversions;
      aggregated[key].clicks += campaign.clicks;
      aggregated[key].earnings += campaign.earnings;
    });

    Object.values(aggregated).forEach(item => {
      item.roas = item.earnings > 0 ? item.revenue / item.earnings : 0;
      item.conversionRate = item.clicks > 0 ? (item.conversions / item.clicks) : 0;
      item.cpc = item.clicks > 0 ? item.earnings / item.clicks : 0;
    });

    return Object.values(aggregated).sort((a, b) => b.revenue - a.revenue);
  }

  calculateDailyTrends(data) {
    const trends = {};

    // Combine all data sources
    const allData = [
      ...data.partnerPerformance,
      ...data.campaignPerformance,
      ...data.conversionPerformance
    ];

    allData.forEach(item => {
      if (!item.date) return;

      const dateKey = item.date.toISOString().split('T')[0];
      if (!trends[dateKey]) {
        trends[dateKey] = {
          date: dateKey,
          revenue: 0,
          conversions: 0,
          clicks: 0,
          earnings: 0
        };
      }

      trends[dateKey].revenue += item.revenue || 0;
      trends[dateKey].conversions += item.conversions || 0;
      trends[dateKey].clicks += item.clicks || 0;
      trends[dateKey].earnings += item.earnings || 0;
    });

    return Object.values(trends).sort((a, b) => new Date(a.date) - new Date(b.date));
  }

  analyzePartnerTiers(partnerData) {
    const aggregated = this.aggregatePartnerData(partnerData);
    const tiers = this.config.get('partnerTiers');
    const tierAnalysis = {};

    Object.keys(tiers).forEach(tierName => {
      tierAnalysis[tierName] = {
        tier: tierName,
        count: 0,
        totalRevenue: 0,
        totalConversions: 0,
        avgRevenue: 0,
        avgConversions: 0
      };
    });

    aggregated.forEach(partner => {
      Object.keys(tiers).forEach(tierName => {
        const tier = tiers[tierName];
        if (partner.revenue >= tier.minRevenue && partner.conversions >= tier.minConversions) {
          tierAnalysis[tierName].count++;
          tierAnalysis[tierName].totalRevenue += partner.revenue;
          tierAnalysis[tierName].totalConversions += partner.conversions;
        }
      });
    });

    // Calculate averages
    Object.values(tierAnalysis).forEach(tier => {
      if (tier.count > 0) {
        tier.avgRevenue = tier.totalRevenue / tier.count;
        tier.avgConversions = tier.totalConversions / tier.count;
      }
    });

    return Object.values(tierAnalysis);
  }

  getTopPerformers(partnerData, limit = 10) {
    const aggregated = this.aggregatePartnerData(partnerData);
    return aggregated.slice(0, limit);
  }

  calculateCampaignEfficiency(campaignData) {
    const aggregated = this.aggregateCampaignData(campaignData);

    return aggregated.map(campaign => ({
      campaign: campaign.campaign,
      revenue: campaign.revenue,
      conversions: campaign.conversions,
      clicks: campaign.clicks,
      earnings: campaign.earnings,
      roas: campaign.revenue > 0 ? campaign.earnings / campaign.revenue : 0,
      conversionRate: campaign.clicks > 0 ? (campaign.conversions / campaign.clicks) * 100 : 0,
      cpc: campaign.clicks > 0 ? campaign.earnings / campaign.clicks : 0
    })).sort((a, b) => b.roas - a.roas);
  }

  calculateFinancialMetrics(data) {
    const summary = data.summary;

    return {
      totalRevenue: summary.totalRevenue,
      totalEarnings: summary.totalEarnings,
      totalClicks: summary.totalClicks,
      totalConversions: summary.totalConversions,
      avgOrderValue: summary.totalConversions > 0 ? summary.totalRevenue / summary.totalConversions : 0,
      earningsPerClick: summary.totalClicks > 0 ? summary.totalEarnings / summary.totalClicks : 0,
      conversionRate: summary.totalClicks > 0 ? (summary.totalConversions / summary.totalClicks) * 100 : 0,
      roi: summary.totalEarnings > 0 ? (summary.totalRevenue / summary.totalEarnings) * 100 : 0
    };
  }

  calculateRevenueBreakdown(data) {
    const partnerData = this.aggregatePartnerData(data.partnerPerformance);
    const totalRevenue = partnerData.reduce((sum, p) => sum + p.revenue, 0);

    return partnerData.map(partner => ({
      partner: partner.partner,
      revenue: partner.revenue,
      percentage: totalRevenue > 0 ? (partner.revenue / totalRevenue) * 100 : 0
    })).sort((a, b) => b.revenue - a.revenue);
  }

  generateAlerts(data) {
    const alerts = [];
    const thresholds = this.config.get('alertThresholds');

    // Revenue drop alert
    const recentRevenue = this.calculateRecentRevenue(data, 7);
    const previousRevenue = this.calculateRecentRevenue(data, 14, 7);

    if (previousRevenue > 0) {
      const revenueDrop = (previousRevenue - recentRevenue) / previousRevenue;
      if (revenueDrop > thresholds.revenueDrop) {
        alerts.push({
          type: 'Revenue Drop',
          severity: 'High',
          message: `Revenue dropped by ${(revenueDrop * 100).toFixed(1)}% in the last 7 days`,
          recommendation: 'Investigate top-performing partners and campaigns'
        });
      }
    }

    // Conversion rate drop alert
    const recentConversions = this.calculateRecentConversions(data, 7);
    const recentClicks = this.calculateRecentClicks(data, 7);
    const recentRate = recentClicks > 0 ? (recentConversions / recentClicks) * 100 : 0;

    const previousConversions = this.calculateRecentConversions(data, 14, 7);
    const previousClicks = this.calculateRecentClicks(data, 14, 7);
    const previousRate = previousClicks > 0 ? (previousConversions / previousClicks) * 100 : 0;

    if (previousRate > 0) {
      const rateDrop = (previousRate - recentRate) / previousRate;
      if (rateDrop > thresholds.conversionDrop) {
        alerts.push({
          type: 'Conversion Rate Drop',
          severity: 'Medium',
          message: `Conversion rate dropped by ${(rateDrop * 100).toFixed(1)}% in the last 7 days`,
          recommendation: 'Review campaign targeting and creative performance'
        });
      }
    }

    return alerts;
  }

  generateInsights(data) {
    const insights = [];

    // Top performer insight
    const topPerformers = this.getTopPerformers(data.partnerPerformance, 3);
    if (topPerformers.length > 0) {
      const topPartner = topPerformers[0];
      insights.push({
        type: 'Top Performer',
        insight: `${topPartner.partner} is your top performer with ${this.formatCurrency(topPartner.revenue)} in revenue`,
        action: 'Consider increasing budget allocation or creating similar partnerships'
      });
    }

    // Conversion rate insight
    const overallRate = this.calculateOverallConversionRate(data);
    if (overallRate > 5) {
      insights.push({
        type: 'High Performance',
        insight: `Your conversion rate of ${overallRate.toFixed(2)}% is above industry average`,
        action: 'Maintain current strategies and consider scaling successful campaigns'
      });
    }

    return insights;
  }

  generateRecommendations(data) {
    const recommendations = [];

    // Revenue optimization
    const partnerData = this.aggregatePartnerData(data.partnerPerformance);
    const lowPerformers = partnerData.filter(p => p.revenue < 1000 && p.conversions < 10);

    if (lowPerformers.length > 0) {
      recommendations.push({
        category: 'Partner Optimization',
        recommendation: `Consider optimizing or replacing ${lowPerformers.length} low-performing partners`,
        priority: 'Medium',
        impact: 'Revenue Growth'
      });
    }

    // Campaign efficiency
    const campaignData = this.calculateCampaignEfficiency(data.campaignPerformance);
    const inefficientCampaigns = campaignData.filter(c => c.roas < 0.1);

    if (inefficientCampaigns.length > 0) {
      recommendations.push({
        category: 'Campaign Optimization',
        recommendation: `Review ${inefficientCampaigns.length} campaigns with low ROAS`,
        priority: 'High',
        impact: 'Cost Reduction'
      });
    }

    return recommendations;
  }

  // Utility methods for calculations
  calculateOverallConversionRate(data) {
    const totalClicks = data.summary.totalClicks;
    const totalConversions = data.summary.totalConversions;
    return totalClicks > 0 ? (totalConversions / totalClicks) * 100 : 0;
  }

  calculateOverallAOV(data) {
    const totalRevenue = data.summary.totalRevenue;
    const totalConversions = data.summary.totalConversions;
    return totalConversions > 0 ? totalRevenue / totalConversions : 0;
  }

  calculateRecentRevenue(data, days, offset = 0) {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - days - offset);
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - days);

    return data.partnerPerformance
      .filter(p => p.date && p.date >= cutoffDate && p.date < startDate)
      .reduce((sum, p) => sum + p.revenue, 0);
  }

  calculateRecentConversions(data, days, offset = 0) {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - days - offset);
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - days);

    return data.partnerPerformance
      .filter(p => p.date && p.date >= cutoffDate && p.date < startDate)
      .reduce((sum, p) => sum + p.conversions, 0);
  }

  calculateRecentClicks(data, days, offset = 0) {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - days - offset);
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - days);

    return data.partnerPerformance
      .filter(p => p.date && p.date >= cutoffDate && p.date < startDate)
      .reduce((sum, p) => sum + p.clicks, 0);
  }

  // Formatting methods
  formatCurrency(value) {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD'
    }).format(value || 0);
  }

  formatNumber(value) {
    return new Intl.NumberFormat('en-US').format(value || 0);
  }

  formatPercentage(value) {
    return (value || 0).toFixed(2) + '%';
  }

  // Chart creation methods (simplified - would need actual chart creation in Google Sheets)
  addRevenueChart(sheet, data, startCell) {
    // This would create an actual chart in Google Sheets
    // For now, we'll add a placeholder
    sheet.getRange(startCell).setValue('📊 Revenue Chart Placeholder');
  }

  addConversionChart(sheet, data, startCell) {
    sheet.getRange(startCell).setValue('📈 Conversion Chart Placeholder');
  }

  addPartnerPerformanceChart(sheet, data, startCell) {
    sheet.getRange(startCell).setValue('📊 Partner Performance Chart Placeholder');
  }

  addCampaignPerformanceChart(sheet, data, startCell) {
    sheet.getRange(startCell).setValue('📈 Campaign Performance Chart Placeholder');
  }

  addRevenueTrendChart(sheet, data, startCell) {
    sheet.getRange(startCell).setValue('📈 Revenue Trend Chart Placeholder');
  }

  addConversionTrendChart(sheet, data, startCell) {
    sheet.getRange(startCell).setValue('📊 Conversion Trend Chart Placeholder');
  }

  addPartnerTierChart(sheet, data, startCell) {
    sheet.getRange(startCell).setValue('📊 Partner Tier Chart Placeholder');
  }

  addTopPerformersChart(sheet, data, startCell) {
    sheet.getRange(startCell).setValue('📈 Top Performers Chart Placeholder');
  }

  addCampaignRevenueChart(sheet, data, startCell) {
    sheet.getRange(startCell).setValue('📊 Campaign Revenue Chart Placeholder');
  }

  addEfficiencyChart(sheet, data, startCell) {
    sheet.getRange(startCell).setValue('📈 Efficiency Chart Placeholder');
  }

  addRevenueBreakdownChart(sheet, data, startCell) {
    sheet.getRange(startCell).setValue('📊 Revenue Breakdown Chart Placeholder');
  }

  addEarningsChart(sheet, data, startCell) {
    sheet.getRange(startCell).setValue('💰 Earnings Chart Placeholder');
  }

  // Table creation methods
  createPartnerPerformanceTable(sheet, data, startRow) {
    const headers = ['Partner', 'Revenue', 'Conversions', 'Clicks', 'Earnings', 'EPC', 'Conv Rate', 'AOV'];
    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#2E7D32').setFontColor('white');

    const tableData = data.map(p => [
      p.partner,
      this.formatCurrency(p.revenue),
      this.formatNumber(p.conversions),
      this.formatNumber(p.clicks),
      this.formatCurrency(p.earnings),
      this.formatCurrency(p.epc),
      this.formatPercentage(p.conversionRate),
      this.formatCurrency(p.aov)
    ]);

    if (tableData.length > 0) {
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setValues(tableData);
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setBorder(true, true, true, true, true, true);
    } else {
      sheet.getRange(startRow + 1, 1).setValue('No data available');
    }
  }

  createCampaignPerformanceTable(sheet, data, startRow) {
    const headers = ['Campaign', 'Revenue', 'Conversions', 'Clicks', 'Earnings', 'CPC'];
    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#1976D2').setFontColor('white');

    const tableData = data.map(c => [
      c.campaign,
      this.formatCurrency(c.revenue),
      this.formatNumber(c.conversions),
      this.formatNumber(c.clicks),
      this.formatCurrency(c.earnings),
      this.formatCurrency(c.cpc)
    ]);

    if (tableData.length > 0) {
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setValues(tableData);
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setBorder(true, true, true, true, true, true);
    } else {
      sheet.getRange(startRow + 1, 1).setValue('No data available');
    }
  }

  createTrendTable(sheet, data, startRow) {
    const headers = ['Date', 'Revenue', 'Conversions', 'Clicks', 'Earnings'];
    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#FF9800').setFontColor('white');

    const tableData = data.map(d => [
      d.date,
      this.formatCurrency(d.revenue),
      this.formatNumber(d.conversions),
      this.formatNumber(d.clicks),
      this.formatCurrency(d.earnings)
    ]);

    if (tableData.length > 0) {
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setValues(tableData);
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setBorder(true, true, true, true, true, true);
    } else {
      sheet.getRange(startRow + 1, 1).setValue('No data available');
    }
  }

  createPartnerTierTable(sheet, data, startRow) {
    const headers = ['Tier', 'Count', 'Total Revenue', 'Total Conversions', 'Avg Revenue', 'Avg Conversions'];
    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#9C27B0').setFontColor('white');

    const tableData = data.map(t => [
      t.tier,
      t.count,
      this.formatCurrency(t.totalRevenue),
      this.formatNumber(t.totalConversions),
      this.formatCurrency(t.avgRevenue),
      this.formatNumber(t.avgConversions)
    ]);

    if (tableData.length > 0) {
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setValues(tableData);
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setBorder(true, true, true, true, true, true);
    } else {
      sheet.getRange(startRow + 1, 1).setValue('No data available');
    }
  }

  createTopPerformersTable(sheet, data, startRow) {
    const headers = ['Rank', 'Partner', 'Revenue', 'Conversions', 'EPC', 'Conv Rate'];
    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#4CAF50').setFontColor('white');

    const tableData = data.map((p, index) => [
      index + 1,
      p.partner,
      this.formatCurrency(p.revenue),
      this.formatNumber(p.conversions),
      this.formatCurrency(p.epc),
      this.formatPercentage(p.conversionRate)
    ]);

    if (tableData.length > 0) {
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setValues(tableData);
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setBorder(true, true, true, true, true, true);
    } else {
      sheet.getRange(startRow + 1, 1).setValue('No data available');
    }
  }

  createCampaignTable(sheet, data, startRow) {
    const headers = ['Campaign', 'Revenue', 'Conversions', 'Clicks', 'Earnings', 'ROAS', 'Conv Rate'];
    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#2196F3').setFontColor('white');

    const tableData = data.map(c => [
      c.campaign,
      this.formatCurrency(c.revenue),
      this.formatNumber(c.conversions),
      this.formatNumber(c.clicks),
      this.formatCurrency(c.earnings),
      c.roas.toFixed(2),
      this.formatPercentage(c.conversionRate)
    ]);

    if (tableData.length > 0) {
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setValues(tableData);
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setBorder(true, true, true, true, true, true);
    } else {
      sheet.getRange(startRow + 1, 1).setValue('No data available');
    }
  }

  createEfficiencyTable(sheet, data, startRow) {
    const headers = ['Campaign', 'Revenue', 'Earnings', 'ROAS', 'Conv Rate', 'CPC'];
    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#FF5722').setFontColor('white');

    const tableData = data.map(c => [
      c.campaign,
      this.formatCurrency(c.revenue),
      this.formatCurrency(c.earnings),
      c.roas.toFixed(2),
      this.formatPercentage(c.conversionRate),
      this.formatCurrency(c.cpc)
    ]);

    if (tableData.length > 0) {
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setValues(tableData);
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setBorder(true, true, true, true, true, true);
    } else {
      sheet.getRange(startRow + 1, 1).setValue('No data available');
    }
  }

  createFinancialTable(sheet, data, startRow) {
    const headers = ['Metric', 'Value'];
    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#795548').setFontColor('white');

    const tableData = [
      ['Total Revenue', this.formatCurrency(data.totalRevenue)],
      ['Total Earnings', this.formatCurrency(data.totalEarnings)],
      ['Total Clicks', this.formatNumber(data.totalClicks)],
      ['Total Conversions', this.formatNumber(data.totalConversions)],
      ['Average Order Value', this.formatCurrency(data.avgOrderValue)],
      ['Earnings Per Click', this.formatCurrency(data.earningsPerClick)],
      ['Conversion Rate', this.formatPercentage(data.conversionRate)],
      ['ROI', this.formatPercentage(data.roi)]
    ];

    if (tableData.length > 0) {
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setValues(tableData);
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setBorder(true, true, true, true, true, true);
    } else {
      sheet.getRange(startRow + 1, 1).setValue('No data available');
    }
  }

  createRevenueBreakdownTable(sheet, data, startRow) {
    const headers = ['Partner', 'Revenue', 'Percentage'];
    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#607D8B').setFontColor('white');

    const tableData = data.map(p => [
      p.partner,
      this.formatCurrency(p.revenue),
      this.formatPercentage(p.percentage)
    ]);

    if (tableData.length > 0) {
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setValues(tableData);
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setBorder(true, true, true, true, true, true);
    } else {
    }
  }
}

// ============================================================================
// AUTOMATION MANAGER
// ============================================================================

class BIAutomationManager {
  constructor(config) {
    this.config = config;
  }

  /**
   * Set up automated refresh of the BI dashboard
   */
  setupAutoRefresh() {
    if (!this.config.get('enableAutoRefresh', true)) return;

    const refreshInterval = this.config.get('refreshInterval', 24); // hours

    // Create a time-driven trigger
    ScriptApp.newTrigger('refreshBIDashboard')
      .timeBased()
      .everyHours(refreshInterval)
      .create();

    Logger.log('Auto-refresh setup: Every ' + refreshInterval + ' hours');
  }

  /**
   * Refresh the BI dashboard with latest data
   */
  refreshBIDashboard() {
    Logger.log('Refreshing BI Dashboard...');

    try {
      const config = new BIConfig();
      const dataProcessor = new BIDataProcessor(config);
      const dashboardBuilder = new BIDashboardBuilder(config, dataProcessor);

      const dashboardUrl = dashboardBuilder.createDashboard();
      Logger.log('BI Dashboard refreshed: ' + dashboardUrl);

      // Send notification if configured
      this.sendRefreshNotification(dashboardUrl);

    } catch (error) {
      Logger.log('Error refreshing BI Dashboard: ' + error.message);
    }
  }

  sendRefreshNotification(dashboardUrl) {
    const email = this.config.get('notificationEmail');
    if (!email) return;

    const subject = 'Impact.com BI Dashboard Updated';
    const body = [
      'Your Business Intelligence Dashboard has been automatically updated.',
      '',
      'Dashboard URL: ' + dashboardUrl,
      'Updated: ' + new Date().toLocaleString(),
      '',
      'View the latest insights and performance metrics.'
    ].join('\n');

    try {
      MailApp.sendEmail(email, subject, body);
    } catch (error) {
      Logger.log('Failed to send refresh notification: ' + error.message);
    }
  }
}

// ============================================================================
// PUBLIC API FUNCTIONS
// ============================================================================

/**
 * Create the complete Business Intelligence Dashboard
 */
function createBIDashboard() {
  const config = new BIConfig();
  // Force the correct spreadsheet ID
  config.set('biSpreadsheetId', '1aLKEEw7Nx0O1DbZjnXnhOeDjcLRloLN0Y8K2SscZKIc');

  const dataProcessor = new BIDataProcessor(config);
  const dashboardBuilder = new BIDashboardBuilder(config, dataProcessor);

  return dashboardBuilder.createDashboard();
}

/**
 * Refresh the BI dashboard with latest data
 */
function refreshBIDashboard() {
  const automationManager = new BIAutomationManager(new BIConfig());
  return automationManager.refreshBIDashboard();
}

/**
 * Set up automated refresh
 */
function setupBIAutoRefresh() {
  const automationManager = new BIAutomationManager(new BIConfig());
  return automationManager.setupAutoRefresh();
}

/**
 * Configure BI dashboard settings
 */
function configureBIDashboard(settings) {
  const config = new BIConfig();

  Object.keys(settings).forEach(key => {
    config.set(key, settings[key]);
  });

  Logger.log('BI Dashboard configuration updated');
  return config.config;
}

/**
 * Get BI dashboard configuration
 */
function getBIConfiguration() {
  const config = new BIConfig();
  return config.config;
}

/**
 * Test BI dashboard data processing
 */
function testBIDataProcessing() {
  const config = new BIConfig();
  const dataProcessor = new BIDataProcessor(config);

  try {
    const data = dataProcessor.processSourceData();
    Logger.log('Data processing test successful');
    Logger.log('Summary: ' + JSON.stringify(data.summary));
    return { success: true, data: data };
  } catch (error) {
    Logger.log('Data processing test failed: ' + error.message);
    return { success: false, error: error.message };
  }
}

/**
 * Quick setup for BI dashboard
 */
function quickBISetup() {
  const settings = {
    sourceSpreadsheetId: '1QDOxgElRvl6EvI02JP4knupUd-jLW7D6LJN-VyLS3ZY',
    enableAutoRefresh: true,
    refreshInterval: 24,
    enableAlerts: true,
    alertThresholds: {
      revenueDrop: 0.15,
      conversionDrop: 0.20,
      clickDrop: 0.25
    }
  };

  configureBIDashboard(settings);
  setupBIAutoRefresh();

  Logger.log('✅ BI Dashboard quick setup complete!');
  Logger.log('Run createBIDashboard() to generate your dashboard');

  return settings;
}

/**
 * DIAGNOSTIC: Analyze what represents "Unassigned" traffic
 * Run this to see what PubSubid1/Partners make up the Unassigned category
 */
function analyzeUnassignedRecords() {
  console.log('🔍 Analyzing Unassigned Records...');
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = spreadsheet.getSheetByName('SkuLevelAction') || spreadsheet.getSheetByName('SkuLevelActions');

  if (!sheet) {
    console.error('❌ SkuLevelAction sheet not found. Please run the automation first.');
    return;
  }

  const data = sheet.getDataRange().getValues();
  const headers = data[0];

  // Find column indices
  const teamIdx = headers.findIndex(h => h.toString().toLowerCase() === 'team');
  const pubSub1Idx = headers.findIndex(h => h.toString().toLowerCase().includes('pubsubid1'));
  const partnerIdx = headers.findIndex(h => h.toString().toLowerCase() === 'partner') ||
    headers.findIndex(h => h.toString().toLowerCase() === 'campaign'); // Fallback

  if (teamIdx === -1) {
    console.error('❌ Team column not found in sheet.');
    return;
  }

  const unassignedRows = data.slice(1).filter(r => r[teamIdx] === 'Unassigned');
  console.log(`\n📊 Found ${unassignedRows.length} 'Unassigned' records out of ${data.length - 1} total.`);

  if (unassignedRows.length === 0) {
    console.log('✅ Good news! There are no unassigned records.');
    return;
  }

  // Analyze PubSubid1 makeup
  const pubSubCounts = {};
  const partnerCounts = {};

  unassignedRows.forEach(row => {
    const pubSub = row[pubSub1Idx] || '(empty)';
    const partner = row[partnerIdx] || '(empty)';

    pubSubCounts[pubSub] = (pubSubCounts[pubSub] || 0) + 1;
    partnerCounts[partner] = (partnerCounts[partner] || 0) + 1;
  });

  console.log('\n🔎 Top PubSubid1 values in Unassigned:');
  Object.entries(pubSubCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .forEach(([key, count]) => {
      const percent = ((count / unassignedRows.length) * 100).toFixed(1);
      console.log(`   - "${key}": ${count} (${percent}%)`);
    });

  console.log('\n🔎 Top Partners/Campaigns in Unassigned:');
  Object.entries(partnerCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .forEach(([key, count]) => {
      console.log(`   - "${key}": ${count}`);
    });
}

/**
 * MASTER UPDATE FUNCTION
 * Runs the complete data pipeline in order:
 * 1. Discovery (Fetch new data)
 * 2. BI Dashboard (Update main dashboard)
 * 3. Team Analysis (Update team/SKU dashboards)
 */
function runMasterUpdate() {
  Logger.log('🚀 STARTING MASTER UPDATE PIPELINE');
  const startTime = Date.now();

  try {
    // 1. Fetch Data
    Logger.log('\n📦 STEP 1/3: Fetching Data (Discovery)...');
    runCompleteDiscovery();

    // 2. Update BI Dashboard
    Logger.log('\n📊 STEP 2/3: Updating BI Dashboard...');
    refreshBIDashboard();

    // 3. Update Team Analysis
    Logger.log('\n🏆 STEP 3/3: Updating Team Analysis...');
    runCompleteTeamAnalysisPipeline();

    const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
    Logger.log('\n✅ MASTER UPDATE COMPLETE');
    Logger.log('⏱️ Total time: ' + duration + ' minutes');

  } catch (error) {
    Logger.log('\n❌ MASTER UPDATE FAILED: ' + error.message);
    throw error;
  }
}
