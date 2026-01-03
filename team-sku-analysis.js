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

