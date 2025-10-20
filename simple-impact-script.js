/**
 * Simple Impact.com Data Discovery and Export System
 * Based on the working original script
 * 
 * @version 1.0.0
 * @author Logan Lorenz
 */

// ============================================================================
// CONFIGURATION
// ============================================================================

/**
 * Get configuration from PropertiesService
 */
function getConfig() {
  const props = PropertiesService.getScriptProperties();
  return {
    sid: props.getProperty('IMPACT_SID'),
    token: props.getProperty('IMPACT_TOKEN'),
    spreadsheetId: props.getProperty('IMPACT_SPREADSHEET_ID') || '1QDOxgElRvl6EvI02JP4knupUd-jLW7D6LJN-VyLS3ZY'
  };
}

/**
 * Make API request
 */
function makeRequest(endpoint) {
  const config = getConfig();
  const url = `https://api.impact.com${endpoint}`;
  const basicAuth = Utilities.base64Encode(`${config.sid}:${config.token}`);
  
  const response = UrlFetchApp.fetch(url, {
    headers: {
      'Authorization': `Basic ${basicAuth}`,
      'Accept': 'application/json'
    },
    muteHttpExceptions: true
  });
  
  if (response.getResponseCode() !== 200) {
    throw new Error(`API request failed: ${response.getResponseCode()} - ${response.getContentText()}`);
  }
  
  return JSON.parse(response.getContentText());
}

// ============================================================================
// MAIN FUNCTIONS
// ============================================================================

/**
 * Discover all available reports
 */
function discoverAllReports() {
  console.log('Discovering available reports...');
  
  const response = makeRequest(`/Mediapartners/${getConfig().sid}/Reports`);
  const reports = response.Reports || [];
  
  const accessible = reports.filter(r => r.ApiAccessible);
  console.log(`Found ${accessible.length} accessible reports out of ${reports.length} total`);
  
  return accessible;
}

/**
 * Schedule export for a single report
 */
function scheduleExport(reportId) {
  console.log(`Scheduling export for ${reportId}...`);
  
  // Build query string manually (URLSearchParams not available in GAS)
  const queryString = 'subid=mula';
  
  const response = makeRequest(`/Mediapartners/${getConfig().sid}/ReportExport/${reportId}?${queryString}`);
  
  const jobId = response.QueuedUri.match(/\/Jobs\/([^/]+)/)[1];
  console.log(`Export scheduled: ${jobId}`);
  
  return {
    reportId,
    jobId,
    status: 'scheduled',
    scheduledAt: new Date()
  };
}

/**
 * Check job status
 */
function checkJobStatus(jobId) {
  const response = makeRequest(`/Mediapartners/${getConfig().sid}/Jobs/${jobId}`);
  
  return {
    jobId,
    status: response.Status?.toLowerCase(),
    resultUri: response.ResultUri,
    error: response.Error
  };
}

/**
 * Download job result
 */
function downloadResult(resultUri) {
  const config = getConfig();
  const url = `https://api.impact.com${resultUri}`;
  const basicAuth = Utilities.base64Encode(`${config.sid}:${config.token}`);
  
  const response = UrlFetchApp.fetch(url, {
    headers: { 'Authorization': `Basic ${basicAuth}` },
    muteHttpExceptions: true
  });

  if (response.getResponseCode() !== 200) {
    throw new Error(`Download failed: ${response.getResponseCode()}`);
  }

  return response.getContentText();
}

/**
 * Process CSV data and create spreadsheet
 */
function processReportData(reportId, reportName, csvData) {
  console.log(`Processing data for ${reportId}...`);
  
  const rows = Utilities.parseCsv(csvData);
  if (!rows || rows.length === 0) {
    throw new Error('No data found in CSV');
  }
  
  const headers = rows[0];
  const dataRows = rows.slice(1);
  
  // Create spreadsheet sheet
  const spreadsheet = SpreadsheetApp.openById(getConfig().spreadsheetId);
  const sheetName = reportName ? reportName.substring(0, 30) : `Report_${reportId}`;
  
  // Delete existing sheet if it exists
  const existingSheet = spreadsheet.getSheetByName(sheetName);
  if (existingSheet) {
    spreadsheet.deleteSheet(existingSheet);
  }
  
  // Create new sheet
  const sheet = spreadsheet.insertSheet(sheetName);
  
  // Import data
  const allData = [headers, ...dataRows];
  sheet.getRange(1, 1, allData.length, headers.length).setValues(allData);
  
  // Format header
  const headerRange = sheet.getRange(1, 1, 1, headers.length);
  headerRange.setFontWeight('bold');
  headerRange.setBackground('#e8f5e8');
  sheet.setFrozenRows(1);
  
  // Auto-resize columns
  sheet.autoResizeColumns(1, headers.length);
  
  console.log(`Created sheet: ${sheetName} with ${dataRows.length} rows`);
  
  return {
    reportId,
    reportName,
    sheetName,
    rowCount: dataRows.length,
    columnCount: headers.length
  };
}

/**
 * Wait for job completion with simple polling
 */
function waitForJobCompletion(jobId, maxAttempts = 12) {
  console.log(`Waiting for job ${jobId} to complete...`);
  
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const status = checkJobStatus(jobId);
    
    if (status.status === 'completed') {
      console.log(`Job ${jobId} completed successfully`);
      return status;
    }
    
    if (status.status === 'failed') {
      throw new Error(`Job failed: ${status.error || 'Unknown error'}`);
    }
    
    console.log(`Job status: ${status.status}, waiting 10 seconds...`);
    Utilities.sleep(10000); // Wait 10 seconds
  }
  
  throw new Error(`Job ${jobId} timed out after ${maxAttempts} attempts`);
}

// ============================================================================
// BATCH PROCESSING FUNCTIONS
// ============================================================================

/**
 * Schedule exports for all reports (with progress saving)
 */
function scheduleAllExports() {
  console.log('Starting export scheduling...');
  
  const reports = discoverAllReports();
  const scheduled = [];
  const errors = [];
  
  // Load existing progress
  const progressJson = PropertiesService.getScriptProperties().getProperty('EXPORT_PROGRESS');
  let progress = progressJson ? JSON.parse(progressJson) : { scheduled: [], errors: [], processed: 0 };
  
  const startIndex = progress.processed;
  
  for (let i = startIndex; i < reports.length; i++) {
    const report = reports[i];
    console.log(`Scheduling ${i + 1}/${reports.length}: ${report.Id}`);
    
    try {
      const job = scheduleExport(report.Id);
      scheduled.push({
        ...job,
        reportName: report.Name,
        originalReport: report
      });
      
      // Save progress after each successful export
      progress.scheduled.push(scheduled[scheduled.length - 1]);
      progress.processed = i + 1;
      PropertiesService.getScriptProperties().setProperty('EXPORT_PROGRESS', JSON.stringify(progress));
      
      console.log(`✅ Scheduled ${i + 1}/${reports.length}`);
      
      // Small delay between requests
      if (i < reports.length - 1) {
        Utilities.sleep(1000);
      }
      
    } catch (error) {
      console.error(`❌ Failed to schedule ${report.Id}: ${error.message}`);
      errors.push({
        reportId: report.Id,
        reportName: report.Name,
        error: error.message
      });
      
      // Save progress even after errors
      progress.errors.push(errors[errors.length - 1]);
      progress.processed = i + 1;
      PropertiesService.getScriptProperties().setProperty('EXPORT_PROGRESS', JSON.stringify(progress));
    }
  }
  
  console.log(`Export scheduling complete: ${scheduled.length} scheduled, ${errors.length} errors`);
  return { scheduled, errors };
}

/**
 * Process all scheduled exports
 */
function processAllExports() {
  console.log('Starting export processing...');
  
  const progressJson = PropertiesService.getScriptProperties().getProperty('EXPORT_PROGRESS');
  if (!progressJson) {
    throw new Error('No export progress found. Run scheduleAllExports() first.');
  }
  
  const progress = JSON.parse(progressJson);
  const scheduled = progress.scheduled;
  const results = [];
  const errors = [];
  
  for (let i = 0; i < scheduled.length; i++) {
    const job = scheduled[i];
    console.log(`Processing ${i + 1}/${scheduled.length}: ${job.reportId}`);
    
    try {
      // Wait for completion
      const status = waitForJobCompletion(job.jobId);
      
      // Download data
      const csvData = downloadResult(status.resultUri);
      
      // Process and create sheet
      const result = processReportData(job.reportId, job.reportName, csvData);
      results.push(result);
      
      console.log(`✅ Processed ${i + 1}/${scheduled.length}`);
      
    } catch (error) {
      console.error(`❌ Failed to process ${job.reportId}: ${error.message}`);
      errors.push({
        reportId: job.reportId,
        reportName: job.reportName,
        error: error.message
      });
    }
  }
  
  console.log(`Export processing complete: ${results.length} successful, ${errors.length} failed`);
  return { results, errors };
}

/**
 * Run complete discovery process
 */
function runCompleteDiscovery() {
  console.log('Starting complete discovery process...');
  
  try {
    // Step 1: Schedule all exports
    const exportResults = scheduleAllExports();
    
    if (exportResults.scheduled.length === 0) {
      throw new Error('No exports were successfully scheduled');
    }
    
    // Step 2: Process all exports
    const processResults = processAllExports();
    
    console.log('Discovery process completed successfully!');
    console.log(`Total reports: ${exportResults.scheduled.length}`);
    console.log(`Successful: ${processResults.results.length}`);
    console.log(`Failed: ${processResults.errors.length}`);
    
    return processResults;
    
  } catch (error) {
    console.error('Discovery process failed:', error.message);
    throw error;
  }
}

/**
 * Check current progress
 */
function checkProgress() {
  const progressJson = PropertiesService.getScriptProperties().getProperty('EXPORT_PROGRESS');
  if (!progressJson) {
    return { status: 'No progress found' };
  }
  
  const progress = JSON.parse(progressJson);
  return {
    status: 'In progress',
    scheduled: progress.scheduled.length,
    errors: progress.errors.length,
    processed: progress.processed,
    lastUpdate: new Date().toISOString()
  };
}

/**
 * Clear progress and start fresh
 */
function clearProgress() {
  PropertiesService.getScriptProperties().deleteProperty('EXPORT_PROGRESS');
  console.log('Progress cleared. Ready to start fresh.');
}

/**
 * Resume from where we left off
 */
function resumeDiscovery() {
  console.log('Resuming discovery...');
  
  const progress = checkProgress();
  if (progress.status === 'No progress found') {
    throw new Error('No progress found. Run runCompleteDiscovery() first.');
  }
  
  console.log(`Found progress: ${progress.scheduled} scheduled, ${progress.errors} errors`);
  
  // Continue with processing
  return processAllExports();
}
