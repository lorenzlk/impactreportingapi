/**
 * Minimal Impact.com Data Discovery and Export System
 * Ultra-simple version to avoid timeouts
 */

// ============================================================================
// CONFIGURATION
// ============================================================================

function getConfig() {
  const props = PropertiesService.getScriptProperties();
  return {
    sid: props.getProperty('IMPACT_SID'),
    token: props.getProperty('IMPACT_TOKEN'),
    spreadsheetId: props.getProperty('IMPACT_SPREADSHEET_ID') || '1QDOxgElRvl6EvI02JP4knupUd-jLW7D6LJN-VyLS3ZY'
  };
}

// ============================================================================
// API FUNCTIONS
// ============================================================================

function makeRequest(endpoint) {
  const config = getConfig();
  const url = `https://api.impact.com${endpoint}`;
  const basicAuth = Utilities.base64Encode(`${config.sid}:${config.token}`);
  
  const response = UrlFetchApp.fetch(url, {
    headers: { 'Authorization': `Basic ${basicAuth}` },
    muteHttpExceptions: true
  });
  
  if (response.getResponseCode() !== 200) {
    throw new Error(`API failed: ${response.getResponseCode()}`);
  }
  
  return JSON.parse(response.getContentText());
}

function discoverReports() {
  console.log('Discovering reports...');
  const response = makeRequest(`/Mediapartners/${getConfig().sid}/Reports`);
  const reports = response.Reports || [];
  const accessible = reports.filter(r => r.ApiAccessible);
  console.log(`Found ${accessible.length} accessible reports`);
  return accessible;
}

function scheduleExport(reportId) {
  console.log(`Scheduling ${reportId}...`);
  
  // Build query string manually
  const queryString = 'subid=mula';
  const response = makeRequest(`/Mediapartners/${getConfig().sid}/ReportExport/${reportId}?${queryString}`);
  
  const jobId = response.QueuedUri.match(/\/Jobs\/([^/]+)/)[1];
  console.log(`Scheduled: ${jobId}`);
  
  return { reportId, jobId, status: 'scheduled' };
}

// ============================================================================
// BATCH PROCESSING
// ============================================================================

function scheduleAllExports() {
  console.log('Starting export scheduling...');
  
  const reports = discoverReports();
  const scheduled = [];
  const errors = [];
  
  // Load existing progress
  const progressJson = PropertiesService.getScriptProperties().getProperty('EXPORT_PROGRESS');
  let progress = progressJson ? JSON.parse(progressJson) : { scheduled: [], errors: [], processed: 0 };
  
  const startIndex = progress.processed;
  console.log(`Starting from index ${startIndex}`);
  
  for (let i = startIndex; i < reports.length; i++) {
    const report = reports[i];
    console.log(`Processing ${i + 1}/${reports.length}: ${report.Id}`);
    
    try {
      const job = scheduleExport(report.Id);
      scheduled.push({
        ...job,
        reportName: report.Name,
        originalReport: report
      });
      
      // Update progress
      progress.scheduled.push(scheduled[scheduled.length - 1]);
      progress.processed = i + 1;
      
      // Save progress every 5 reports to avoid too much I/O
      if ((i + 1) % 5 === 0 || i === reports.length - 1) {
        PropertiesService.getScriptProperties().setProperty('EXPORT_PROGRESS', JSON.stringify(progress));
        console.log(`Progress saved: ${i + 1}/${reports.length}`);
      }
      
      console.log(`✅ ${i + 1}/${reports.length} completed`);
      
      // Minimal delay
      if (i < reports.length - 1) {
        Utilities.sleep(500); // Just 500ms
      }
      
    } catch (error) {
      console.error(`❌ Failed ${report.Id}: ${error.message}`);
      errors.push({
        reportId: report.Id,
        reportName: report.Name,
        error: error.message
      });
      
      // Update progress even on error
      progress.errors.push(errors[errors.length - 1]);
      progress.processed = i + 1;
      
      if ((i + 1) % 5 === 0 || i === reports.length - 1) {
        PropertiesService.getScriptProperties().setProperty('EXPORT_PROGRESS', JSON.stringify(progress));
      }
    }
  }
  
  // Final save
  PropertiesService.getScriptProperties().setProperty('EXPORT_PROGRESS', JSON.stringify(progress));
  
  console.log(`Scheduling complete: ${scheduled.length} scheduled, ${errors.length} errors`);
  return { scheduled, errors };
}

// ============================================================================
// MAIN FUNCTIONS
// ============================================================================

function runCompleteDiscovery() {
  console.log('Starting discovery...');
  
  try {
    const results = scheduleAllExports();
    console.log(`Discovery complete: ${results.scheduled.length} scheduled`);
    return results;
  } catch (error) {
    console.error('Discovery failed:', error.message);
    throw error;
  }
}

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
    processed: progress.processed
  };
}

function clearProgress() {
  PropertiesService.getScriptProperties().deleteProperty('EXPORT_PROGRESS');
  console.log('Progress cleared');
}

function resumeDiscovery() {
  console.log('Resuming discovery...');
  return scheduleAllExports();
}
