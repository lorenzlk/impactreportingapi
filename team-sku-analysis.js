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
  }

  /**
   * Map a conversation/row to a team based on various identifiers
   */
  mapToTeam(row) {
    const rules = this.teamConfig.get('teamMappingRules', {});
    
    // Extract identifiers from row
    const partner = (row['Partner'] || row['partner'] || '').toString().toLowerCase();
    const subid = (row['SubID'] || row['subid'] || row['SubId'] || '').toString().toLowerCase();
    const campaign = (row['Campaign'] || row['campaign'] || '').toString().toLowerCase();
    const conversationId = (row['ConversationID'] || row['conversation_id'] || '').toString().toLowerCase();
    
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
    
    // Add team attribution to each row
    const enrichedData = this.teamMapper.batchMapToTeams(skuData);
    
    // Aggregate by team
    const teamStats = {};
    const skuByTeam = {};
    
    enrichedData.forEach(row => {
      const team = row.team;
      const sku = row['SKU'] || row['sku'] || row['Product_SKU'] || 'Unknown';
      const revenue = this.parseNumber(row['Sale_amount'] || row['Revenue'] || row['revenue'] || 0);
      const quantity = this.parseNumber(row['Quantity'] || row['quantity'] || row['Items'] || 1);
      const conversions = this.parseNumber(row['Actions'] || row['Conversions'] || row['conversions'] || 1);
      
      // Initialize team stats
      if (!teamStats[team]) {
        teamStats[team] = {
          team: team,
          totalRevenue: 0,
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
          conversions: 0,
          quantity: 0,
          avgOrderValue: 0
        };
      }
      
      skuByTeam[team][sku].revenue += revenue;
      skuByTeam[team][sku].conversions += conversions;
      skuByTeam[team][sku].quantity += quantity;
    });
    
    // Calculate derived metrics and convert Sets to counts
    Object.keys(teamStats).forEach(team => {
      teamStats[team].uniqueSKUCount = teamStats[team].uniqueSKUs.size;
      teamStats[team].avgOrderValue = teamStats[team].totalConversions > 0 ? 
        teamStats[team].totalRevenue / teamStats[team].totalConversions : 0;
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
    
    // Prepare summary data
    const summaryData = [
      ['Team Performance Summary - Mula Conversations', '', '', '', '', '', '', ''],
      ['Generated: ' + new Date().toLocaleString(), '', '', '', '', '', '', ''],
      ['', '', '', '', '', '', '', ''],
      ['Team', 'Total Revenue', 'Conversions', 'Quantity', 'Unique SKUs', 'Avg Order Value', 'Target', '% of Target']
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
    const totalConversions = teams.reduce((sum, t) => sum + t.totalConversions, 0);
    const totalQuantity = teams.reduce((sum, t) => sum + t.totalQuantity, 0);
    
    summaryData.push(['', '', '', '', '', '', '', '']);
    summaryData.push([
      'TOTAL',
      '$' + totalRevenue.toFixed(2),
      totalConversions,
      totalQuantity,
      '',
      '$' + (totalConversions > 0 ? (totalRevenue / totalConversions).toFixed(2) : '0.00'),
      '',
      ''
    ]);
    
    // Write data
    sheet.getRange(1, 1, summaryData.length, 8).setValues(summaryData);
    
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
    
    // Prepare data
    const data = [
      ['SKU Performance by Team', '', '', '', '', ''],
      ['Generated: ' + new Date().toLocaleString(), '', '', '', '', ''],
      ['', '', '', '', '', ''],
      ['Team', 'SKU', 'Revenue', 'Conversions', 'Quantity', 'Avg Order Value']
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
        sku.conversions,
        sku.quantity,
        '$' + sku.avgOrderValue.toFixed(2)
      ]);
    });
    
    // Write data
    sheet.getRange(1, 1, data.length, 6).setValues(data);
    
    // Format sheet
    this.formatDataSheet(sheet, 6, data.length);
    
    console.log('SKU performance sheet created with ' + allSKUs.length + ' SKUs');
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
    
    const data = [
      ['Team Performance Comparison', ...padding],
      ['Generated: ' + new Date().toLocaleString(), ...padding],
      ['', ...padding],
      ['Metric', ...teams.map(t => t.team)],
      ['Total Revenue', ...teams.map(t => '$' + t.totalRevenue.toFixed(2))],
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
    const titleRange = sheet.getRange(1, 1, 1, 8);
    titleRange.setFontSize(14);
    titleRange.setFontWeight('bold');
    titleRange.setBackground('#1976D2');
    titleRange.setFontColor('#FFFFFF');
    sheet.getRange(1, 1, 1, 8).merge();
    
    // Format header row
    const headerRange = sheet.getRange(4, 1, 1, 8);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#4CAF50');
    headerRange.setFontColor('#FFFFFF');
    sheet.setFrozenRows(4);
    
    // Auto-resize columns
    sheet.autoResizeColumns(1, 8);
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
 * This modifies the raw CSV data before it's written to the sheet
 * 
 * @param {Array} csvData - Raw CSV data (array of arrays, first row is headers)
 * @returns {Array} - Enhanced CSV data with Team column added
 */
function enrichSKUDataWithTeams(csvData) {
  if (!csvData || csvData.length === 0) {
    console.log('No data to enrich');
    return csvData;
  }
  
  const formatter = new TeamDisplayFormatter();
  const headers = csvData[0];
  const dataRows = csvData.slice(1);
  
  // Find PubSubid3 column
  const pubSubid3Index = headers.findIndex(h => 
    h && h.toLowerCase() === 'pubsubid3'
  );
  
  if (pubSubid3Index === -1) {
    console.log('Warning: PubSubid3 column not found. Cannot add Team column.');
    return csvData;
  }
  
  // Add Team column header (insert after PubSubid4 if it exists, otherwise at the end)
  const pubSubid4Index = headers.findIndex(h => 
    h && h.toLowerCase() === 'pubsubid4'
  );
  const teamColumnIndex = pubSubid4Index >= 0 ? pubSubid4Index + 1 : headers.length;
  
  const newHeaders = [...headers];
  newHeaders.splice(teamColumnIndex, 0, 'Team');
  
  // Process each data row and add team name
  const newDataRows = dataRows.map(row => {
    const newRow = [...row];
    const pubSubid3Value = row[pubSubid3Index];
    
    let teamName = 'Unassigned';
    if (pubSubid3Value && typeof pubSubid3Value === 'string' && pubSubid3Value.trim()) {
      const urlFriendlyName = pubSubid3Value.trim();
      teamName = formatter.toDisplayName(urlFriendlyName);
    }
    
    newRow.splice(teamColumnIndex, 0, teamName);
    return newRow;
  });
  
  console.log('✅ Added Team column at position ' + (teamColumnIndex + 1));
  console.log('   Teams found: ' + [...new Set(newDataRows.map(row => row[teamColumnIndex]))].filter(t => t !== 'Unassigned').join(', '));
  
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

