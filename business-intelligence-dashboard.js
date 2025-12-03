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
      if (sheetName === 'DISCOVERY SUMMARY') return;

      try {
        const data = this.extractSheetData(sheet);
        if (data.length === 0) return;

        // Categorize data based on sheet name
        if (sheetName.toLowerCase().includes('partner') || sheetName.toLowerCase().includes('subid')) {
          processedData.partnerPerformance = this.processPartnerData(data);
        } else if (sheetName.toLowerCase().includes('campaign')) {
          processedData.campaignPerformance = this.processCampaignData(data);
        } else if (sheetName.toLowerCase().includes('click')) {
          processedData.clickPerformance = this.processClickData(data);
        } else if (sheetName.toLowerCase().includes('conversion')) {
          processedData.conversionPerformance = this.processConversionData(data);
        } else if (sheetName.toLowerCase().includes('creative')) {
          processedData.creativePerformance = this.processCreativeData(data);
        } else if (sheetName.toLowerCase().includes('skulevelaction')) {
          // Process Team data from SkuLevelAction reports
          const teamData = this.processTeamData(data);
          if (teamData && teamData.length > 0) {
            processedData.teamPerformance = processedData.teamPerformance.concat(teamData);
          }
        }

        // Update summary
        this.updateSummary(processedData.summary, data);

      } catch (error) {
        Logger.log('Error processing sheet ' + sheetName + ': ' + error.message);
      }
    });

    return processedData;
  }

  extractSheetData(sheet) {
    const data = sheet.getDataRange().getValues();
    if (data.length < 2) return [];

    const headers = data[0];
    const rows = data.slice(1);

    return rows.map(row => {
      const obj = {};
      headers.forEach((header, index) => {
        obj[header] = row[index];
      });
      return obj;
    });
  }

  processPartnerData(data) {
    return data.map(row => ({
      partner: row['Partner'] || row['SubID'] || row['subid'] || 'Unknown',
      revenue: this.parseNumber(row['Sale_amount'] || row['Revenue'] || row['revenue'] || 0),
      conversions: this.parseNumber(row['Actions'] || row['Conversions'] || row['conversions'] || 0),
      clicks: this.parseNumber(row['Clicks'] || row['clicks'] || 0),
      earnings: this.parseNumber(row['Earnings'] || row['earnings'] || 0),
      epc: this.parseNumber(row['EPC'] || row['epc'] || 0),
      conversionRate: this.calculateConversionRate(
        this.parseNumber(row['Clicks'] || row['clicks'] || 0),
        this.parseNumber(row['Actions'] || row['Conversions'] || row['conversions'] || 0)
      ),
      aov: this.calculateAOV(
        this.parseNumber(row['Sale_amount'] || row['Revenue'] || row['revenue'] || 0),
        this.parseNumber(row['Actions'] || row['Conversions'] || row['conversions'] || 0)
      ),
      date: this.parseDate(row['Date'] || row['date'] || row['Period'] || row['period'])
    }));
  }

  processCampaignData(data) {
    return data.map(row => ({
      campaign: row['Campaign'] || row['campaign'] || 'Unknown',
      revenue: this.parseNumber(row['Sale_amount'] || row['Revenue'] || row['revenue'] || 0),
      conversions: this.parseNumber(row['Actions'] || row['Conversions'] || row['conversions'] || 0),
      clicks: this.parseNumber(row['Clicks'] || row['clicks'] || 0),
      earnings: this.parseNumber(row['Earnings'] || row['earnings'] || 0),
      cpc: this.parseNumber(row['CPC_Cost'] || row['Click_Cost'] || row['cpc'] || 0),
      date: this.parseDate(row['Date'] || row['date'] || row['Period'] || row['period'])
    }));
  }

  processClickData(data) {
    return data.map(row => ({
      partner: row['Partner'] || row['SubID'] || row['subid'] || 'Unknown',
      campaign: row['Campaign'] || row['campaign'] || 'Unknown',
      clicks: this.parseNumber(row['Clicks'] || row['clicks'] || 0),
      impressions: this.parseNumber(row['Impressions'] || row['impressions'] || 0),
      ctr: this.parseNumber(row['CTR'] || row['ctr'] || 0),
      date: this.parseDate(row['Date'] || row['date'] || row['Period'] || row['period'])
    }));
  }

  processConversionData(data) {
    return data.map(row => ({
      partner: row['Partner'] || row['SubID'] || row['subid'] || 'Unknown',
      campaign: row['Campaign'] || row['campaign'] || 'Unknown',
      conversions: this.parseNumber(row['Actions'] || row['Conversions'] || row['conversions'] || 0),
      revenue: this.parseNumber(row['Sale_amount'] || row['Revenue'] || row['revenue'] || 0),
      earnings: this.parseNumber(row['Earnings'] || row['earnings'] || 0),
      date: this.parseDate(row['Date'] || row['date'] || row['Period'] || row['period'])
    }));
  }

  processCreativeData(data) {
    return data.map(row => ({
      creative: row['Creative'] || row['creative'] || 'Unknown',
      campaign: row['Campaign'] || row['campaign'] || 'Unknown',
      clicks: this.parseNumber(row['Clicks'] || row['clicks'] || 0),
      conversions: this.parseNumber(row['Actions'] || row['Conversions'] || row['conversions'] || 0),
      revenue: this.parseNumber(row['Sale_amount'] || row['Revenue'] || row['revenue'] || 0),
      ctr: this.parseNumber(row['CTR'] || row['ctr'] || 0),
      date: this.parseDate(row['Date'] || row['date'] || row['Period'] || row['period'])
    }));
  }

  processTeamData(data) {
    // Extract Team data from SkuLevelAction reports
    // Looks for 'Team' column which is added by team-sku-analysis.js
    // OR falls back to PubSubid3 if Team column is missing (though it should be there)

    return data.map(row => {
      let team = row['Team'] || 'Unassigned';
      const pubSubid1 = (row['PubSubid1'] || row['pubsubid1'] || '').toString().toLowerCase();
      const pubSubid3 = (row['PubSubid3'] || row['pubsubid3'] || '').toString();

      // Fallback logic if Team column is missing but PubSubid3 exists
      if ((team === 'Unassigned' || !team) && pubSubid3) {
        // Simple formatter fallback
        team = pubSubid3.split('-')
          .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
          .join(' ');
      }

      // Robust column mapping
      const revenue = this.parseNumber(
        row['Sale Amount'] || row['Sale_amount'] || row['SaleAmount'] ||
        row['Revenue'] || row['revenue'] || 0
      );

      const conversions = this.parseNumber(
        row['Actions'] || row['Conversions'] || row['conversions'] ||
        row['Action Count'] || 0
      );

      const earnings = this.parseNumber(
        row['Earnings'] || row['earnings'] || row['Commission'] ||
        row['Pub Commission'] || row['Pub_Commission'] || 0
      );

      const quantity = this.parseNumber(
        row['Quantity'] || row['quantity'] || row['Items'] || 1
      );

      return {
        team: team,
        isMula: pubSubid1.includes('mula'),
        revenue: revenue,
        conversions: conversions,
        earnings: earnings,
        quantity: quantity,
        date: this.parseDate(row['Date'] || row['date'] || row['Period'] || row['period'])
      };
    });
  }

  updateSummary(summary, data) {
    summary.reportCount++;

    data.forEach(row => {
      summary.totalRevenue += this.parseNumber(
        row['Sale Amount'] || row['Sale_amount'] || row['SaleAmount'] ||
        row['Revenue'] || row['revenue'] || 0
      );

      summary.totalConversions += this.parseNumber(
        row['Actions'] || row['Conversions'] || row['conversions'] ||
        row['Action Count'] || 0
      );

      summary.totalClicks += this.parseNumber(
        row['Clicks'] || row['clicks'] || 0
      );

      summary.totalEarnings += this.parseNumber(
        row['Earnings'] || row['earnings'] || row['Commission'] ||
        row['Pub Commission'] || row['Pub_Commission'] || 0
      );

      const date = this.parseDate(row['Date'] || row['date'] || row['Period'] || row['period']);
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
    this.createPerformanceAnalytics(data);
    this.createTrendAnalysis(data);
    this.createTrendAnalysis(data);
    this.createTeamAnalysis(data); // New Team Analysis Sheet
    this.createPartnerAnalysis(data);
    this.createCampaignAnalysis(data);
    this.createFinancialOverview(data);
    this.createAlertsAndInsights(data);

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

    sheet.autoResizeColumns(1, 10);
  }

  createPerformanceAnalytics(data) {
    const sheet = this.getOrCreateSheet('📈 Performance Analytics');

    // Clear existing content
    sheet.clear();

    // Header
    sheet.getRange('A1').setValue('Performance Analytics');
    sheet.getRange('A1').setFontSize(16).setFontWeight('bold');

    // Timeframe
    const dateRange = data.summary.dateRange;
    const timeframe = dateRange.start && dateRange.end
      ? `${dateRange.start.toLocaleDateString()} - ${dateRange.end.toLocaleDateString()}`
      : 'All Time';
    sheet.getRange('A2').setValue(`Timeframe: ${timeframe}`);
    sheet.getRange('A2').setFontStyle('italic');

    // Partner Performance Table
    const partnerData = this.aggregatePartnerData(data.partnerPerformance);
    this.createPartnerPerformanceTable(sheet, partnerData, 3);

    // Campaign Performance Table
    const campaignData = this.aggregateCampaignData(data.campaignPerformance);
    this.createCampaignPerformanceTable(sheet, campaignData, 15);

    // Performance Charts
    this.addPartnerPerformanceChart(sheet, partnerData, 'H3');
    this.addCampaignPerformanceChart(sheet, campaignData, 'H15');

    sheet.autoResizeColumns(1, 10);
  }

  createTrendAnalysis(data) {
    const sheet = this.getOrCreateSheet('📅 Trend Analysis');

    sheet.clear();
    sheet.getRange('A1').setValue('Trend Analysis');
    sheet.getRange('A1').setFontSize(16).setFontWeight('bold');

    // Timeframe
    const dateRange = data.summary.dateRange;
    const timeframe = dateRange.start && dateRange.end
      ? `${dateRange.start.toLocaleDateString()} - ${dateRange.end.toLocaleDateString()}`
      : 'All Time';
    sheet.getRange('A2').setValue(`Timeframe: ${timeframe}`);
    sheet.getRange('A2').setFontStyle('italic');

    // Daily trends
    const dailyTrends = this.calculateDailyTrends(data);

    // Weekly trends (New)
    const weeklyTrends = this.calculateWeeklyTrends(dailyTrends);

    // --- Section 1: Daily Trends ---
    sheet.getRange('A3').setValue('Daily Performance');
    sheet.getRange('A3').setFontSize(12).setFontWeight('bold');
    this.createTrendTable(sheet, dailyTrends, 4);
    this.addRevenueTrendChart(sheet, dailyTrends, 'H4', 'Daily Revenue Trend');

    // --- Section 2: Weekly Trends (Optimization Tracking) ---
    const weeklyRow = 4 + Math.min(dailyTrends.length, 30) + 4;
    sheet.getRange('A' + weeklyRow).setValue('Weekly Performance (Optimization Tracking)');
    sheet.getRange('A' + weeklyRow).setFontSize(12).setFontWeight('bold').setFontColor('#E65100');

    this.createTrendTable(sheet, weeklyTrends, weeklyRow + 1);
    this.addRevenueTrendChart(sheet, weeklyTrends, 'H' + (weeklyRow + 1), 'Weekly Revenue Trend');

    sheet.autoResizeColumns(1, 10);
  }

  calculateWeeklyTrends(dailyTrends) {
    const weekly = {};

    dailyTrends.forEach(day => {
      const date = new Date(day.date);
      // Get start of week (Sunday)
      const startOfWeek = new Date(date);
      startOfWeek.setDate(date.getDate() - date.getDay());
      const weekKey = startOfWeek.toISOString().split('T')[0];

      if (!weekly[weekKey]) {
        weekly[weekKey] = {
          date: weekKey,
          revenue: 0,
          conversions: 0,
          clicks: 0,
          earnings: 0
        };
      }

      weekly[weekKey].revenue += day.revenue;
      weekly[weekKey].conversions += day.conversions;
      weekly[weekKey].clicks += day.clicks;
      weekly[weekKey].earnings += day.earnings;
    });

    return Object.values(weekly).sort((a, b) => new Date(a.date) - new Date(b.date));
  }

  createPartnerAnalysis(data) {
    const sheet = this.getOrCreateSheet('🤝 Partner Analysis');

    sheet.clear();
    sheet.getRange('A1').setValue('Partner Performance Analysis');
    sheet.getRange('A1').setFontSize(16).setFontWeight('bold');

    // Timeframe
    const dateRange = data.summary.dateRange;
    const timeframe = dateRange.start && dateRange.end
      ? `${dateRange.start.toLocaleDateString()} - ${dateRange.end.toLocaleDateString()}`
      : 'All Time';
    sheet.getRange('A2').setValue(`Timeframe: ${timeframe}`);
    sheet.getRange('A2').setFontStyle('italic');

    // Partner tier analysis
    const partnerTiers = this.analyzePartnerTiers(data.partnerPerformance);
    this.createPartnerTierTable(sheet, partnerTiers, 3);

    // Top performers
    const topPerformers = this.getTopPerformers(data.partnerPerformance, 10);
    this.createTopPerformersTable(sheet, topPerformers, 10);

    // Partner charts
    this.addPartnerTierChart(sheet, partnerTiers, 'H3');
    this.addTopPerformersChart(sheet, topPerformers, 'H15');

    sheet.autoResizeColumns(1, 10);
  }

  createCampaignAnalysis(data) {
    const sheet = this.getOrCreateSheet('🎯 Campaign Analysis');

    sheet.clear();
    sheet.getRange('A1').setValue('Campaign Performance Analysis');
    sheet.getRange('A1').setFontSize(16).setFontWeight('bold');

    // Timeframe
    const dateRange = data.summary.dateRange;
    const timeframe = dateRange.start && dateRange.end
      ? `${dateRange.start.toLocaleDateString()} - ${dateRange.end.toLocaleDateString()}`
      : 'All Time';
    sheet.getRange('A2').setValue(`Timeframe: ${timeframe}`);
    sheet.getRange('A2').setFontStyle('italic');

    // Campaign performance
    const campaignData = this.aggregateCampaignData(data.campaignPerformance);
    this.createCampaignTable(sheet, campaignData, 3);

    // Campaign efficiency
    const efficiencyData = this.calculateCampaignEfficiency(data.campaignPerformance);
    this.createEfficiencyTable(sheet, efficiencyData, 15);

    // Campaign charts
    this.addCampaignRevenueChart(sheet, campaignData, 'H3');
    this.addEfficiencyChart(sheet, efficiencyData, 'H15');

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

  createAlertsAndInsights(data) {
    const sheet = this.getOrCreateSheet('🚨 Alerts & Insights');

    sheet.clear();
    sheet.getRange('A1').setValue('Alerts & Business Insights');
    sheet.getRange('A1').setFontSize(16).setFontWeight('bold');

    // Timeframe
    const dateRange = data.summary.dateRange;
    const timeframe = dateRange.start && dateRange.end
      ? `${dateRange.start.toLocaleDateString()} - ${dateRange.end.toLocaleDateString()}`
      : 'All Time';
    sheet.getRange('A2').setValue(`Timeframe: ${timeframe}`);
    sheet.getRange('A2').setFontStyle('italic');

    // Generate alerts
    const alerts = this.generateAlerts(data);
    this.createAlertsTable(sheet, alerts, 3);

    // Generate insights
    const insights = this.generateInsights(data);
    this.createInsightsTable(sheet, insights, 10);

    // Performance recommendations
    const recommendations = this.generateRecommendations(data);
    this.createRecommendationsTable(sheet, recommendations, 17);

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
          conversions: 0,
          earnings: 0,
          quantity: 0
        };
      }

      aggregated[key].revenue += item.revenue;
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
    const headers = ['Rank', 'Team', 'Revenue', 'Conversions', 'Earnings', 'AOV', 'Comm. Rate'];

    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#1976D2').setFontColor('white');

    const rows = data.map((team, index) => [
      index + 1,
      team.team,
      this.formatCurrency(team.revenue),
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
      sheet.getRange(startRow + 1, 1).setValue('No data available');
    }
  }

  createAlertsTable(sheet, data, startRow) {
    const headers = ['Type', 'Severity', 'Message', 'Recommendation'];
    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#F44336').setFontColor('white');

    const tableData = data.map(a => [
      a.type,
      a.severity,
      a.message,
      a.recommendation
    ]);

    if (tableData.length > 0) {
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setValues(tableData);
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setBorder(true, true, true, true, true, true);
    } else {
      sheet.getRange(startRow + 1, 1).setValue('No data available');
    }
  }

  createInsightsTable(sheet, data, startRow) {
    const headers = ['Type', 'Insight', 'Action'];
    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#4CAF50').setFontColor('white');

    const tableData = data.map(i => [
      i.type,
      i.insight,
      i.action
    ]);

    if (tableData.length > 0) {
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setValues(tableData);
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setBorder(true, true, true, true, true, true);
    } else {
      sheet.getRange(startRow + 1, 1).setValue('No data available');
    }
  }

  createRecommendationsTable(sheet, data, startRow) {
    const headers = ['Category', 'Recommendation', 'Priority', 'Impact'];
    sheet.getRange(startRow, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(startRow, 1, 1, headers.length).setFontWeight('bold').setBackground('#FF9800').setFontColor('white');

    const tableData = data.map(r => [
      r.category,
      r.recommendation,
      r.priority,
      r.impact
    ]);

    if (tableData.length > 0) {
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setValues(tableData);
      sheet.getRange(startRow + 1, 1, tableData.length, headers.length).setBorder(true, true, true, true, true, true);
    } else {
      sheet.getRange(startRow + 1, 1).setValue('No data available');
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
