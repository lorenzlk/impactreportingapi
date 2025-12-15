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
