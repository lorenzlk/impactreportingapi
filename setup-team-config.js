/**
 * Quick Setup Template for Team Configuration
 * 
 * INSTRUCTIONS:
 * 1. Edit the teams and mapping patterns below to match YOUR team structure
 * 2. Run setupTeamsQuickStart() in the Apps Script editor
 * 3. Then run runCompleteTeamAnalysisPipeline() to generate reports
 */

/**
 * Quick Start Setup - ON3 COLLEGE TEAMS CONFIGURATION
 */
function setupTeamsQuickStart() {
  console.log('Setting up ON3 college team configuration...');
  
  const teamConfig = new TeamConfig();
  
  // ============================================================================
  // STEP 1: DEFINE YOUR TEAMS (ON3 College Teams)
  // ============================================================================
  
  teamConfig.addTeam('USC Trojans', {
    description: 'USC Trojans NIL Team',
    lead: '',  // Add team lead name if applicable
    target: 100000,  // Monthly revenue target in dollars - CUSTOMIZE THIS
    active: true
  });
  
  teamConfig.addTeam('NC State Wolfpack', {
    description: 'NC State Wolfpack NIL Team',
    lead: '',
    target: 100000,  // CUSTOMIZE THIS
    active: true
  });
  
  teamConfig.addTeam('Ole Miss Rebels', {
    description: 'Ole Miss Rebels NIL Team',
    lead: '',
    target: 100000,  // CUSTOMIZE THIS
    active: true
  });
  
  teamConfig.addTeam('Florida Gators', {
    description: 'Florida Gators NIL Team',
    lead: '',
    target: 100000,  // CUSTOMIZE THIS
    active: true
  });
  
  teamConfig.addTeam('LSU Tigers', {
    description: 'LSU Tigers NIL Team',
    lead: '',
    target: 100000,  // CUSTOMIZE THIS
    active: true
  });
  
  teamConfig.addTeam('Penn State Nittany Lions', {
    description: 'Penn State Nittany Lions NIL Team',
    lead: '',
    target: 100000,  // CUSTOMIZE THIS
    active: true
  });
  
  teamConfig.addTeam('Notre Dame Fighting Irish', {
    description: 'Notre Dame Fighting Irish NIL Team',
    lead: '',
    target: 100000,  // CUSTOMIZE THIS
    active: true
  });
  
  teamConfig.addTeam('Michigan Wolverines', {
    description: 'Michigan Wolverines NIL Team',
    lead: '',
    target: 100000,  // CUSTOMIZE THIS
    active: true
  });
  
  teamConfig.addTeam('Ohio State Buckeyes', {
    description: 'Ohio State Buckeyes NIL Team',
    lead: '',
    target: 100000,  // CUSTOMIZE THIS
    active: true
  });
  
  teamConfig.addTeam('Auburn Tigers', {
    description: 'Auburn Tigers NIL Team',
    lead: '',
    target: 100000,  // CUSTOMIZE THIS
    active: true
  });
  
  // ============================================================================
  // STEP 2: SET UP MAPPING RULES
  // ============================================================================
  // These patterns will identify which Mula conversations belong to each team
  // The system checks if SubID, Partner, or Campaign contains any of these patterns
  
  // IMPORTANT: Run discoverDataPatterns() first to see your actual SubID values!
  // Then customize these patterns to match what you see in your data.
  
  // USC Trojans - Multiple pattern variations
  teamConfig.addTeamMappingRule('USC Trojans', 'subidPatterns', [
    'usc',          // Matches: "usc_123", "team_usc", "usc_promo"
    'trojans',      // Matches: "trojans_456", "trojan"
    'trojan',       // Singular form
    'southern_cal', // Alternative name
    'sc'            // Common abbreviation (may need refinement)
  ]);
  
  // NC State Wolfpack
  teamConfig.addTeamMappingRule('NC State Wolfpack', 'subidPatterns', [
    'ncstate',      // Matches: "ncstate_123", "team_ncstate"
    'nc_state',     // With underscore
    'wolfpack',     // Matches: "wolfpack_456"
    'ncst',         // Abbreviation
    'state'         // Common reference (may need refinement if conflicts)
  ]);
  
  // Ole Miss Rebels
  teamConfig.addTeamMappingRule('Ole Miss Rebels', 'subidPatterns', [
    'olemiss',      // Matches: "olemiss_123"
    'ole_miss',     // With underscore
    'rebels',       // Matches: "rebels_456"
    'rebel',        // Singular
    'mississippi'   // Full name
  ]);
  
  // Florida Gators
  teamConfig.addTeamMappingRule('Florida Gators', 'subidPatterns', [
    'florida',      // Matches: "florida_123", "team_florida"
    'gators',       // Matches: "gators_456"
    'gator',        // Singular
    'uf',           // University of Florida abbreviation
    'fl_gators'     // Alternative format
  ]);
  
  // LSU Tigers
  teamConfig.addTeamMappingRule('LSU Tigers', 'subidPatterns', [
    'lsu',          // Matches: "lsu_123", "team_lsu"
    'tigers',       // Matches: "tigers_456" (may conflict with other Tigers teams)
    'tiger',        // Singular
    'louisiana',    // Full state name
    'lsu_tigers'    // Combined
  ]);
  
  // Penn State Nittany Lions
  teamConfig.addTeamMappingRule('Penn State Nittany Lions', 'subidPatterns', [
    'pennstate',    // Matches: "pennstate_123"
    'penn_state',   // With underscore
    'penn',         // Short form
    'psu',          // Abbreviation
    'nittany',      // Matches: "nittany_lions"
    'lions'         // May need refinement if conflicts with other Lions teams
  ]);
  
  // Notre Dame Fighting Irish
  teamConfig.addTeamMappingRule('Notre Dame Fighting Irish', 'subidPatterns', [
    'notredame',    // Matches: "notredame_123"
    'notre_dame',   // With underscore
    'notre',        // Short form
    'nd',           // Common abbreviation
    'fighting_irish', // Full mascot name
    'irish'         // Mascot
  ]);
  
  // Michigan Wolverines
  teamConfig.addTeamMappingRule('Michigan Wolverines', 'subidPatterns', [
    'michigan',     // Matches: "michigan_123", "team_michigan"
    'wolverines',   // Matches: "wolverines_456"
    'wolverine',    // Singular
    'um',           // University of Michigan
    'mich',         // Abbreviation
    'go_blue'       // Common phrase
  ]);
  
  // Ohio State Buckeyes
  teamConfig.addTeamMappingRule('Ohio State Buckeyes', 'subidPatterns', [
    'ohiostate',    // Matches: "ohiostate_123"
    'ohio_state',   // With underscore
    'ohio',         // Short form
    'osu',          // Common abbreviation
    'buckeyes',     // Matches: "buckeyes_456"
    'buckeye'       // Singular
  ]);
  
  // Auburn Tigers
  teamConfig.addTeamMappingRule('Auburn Tigers', 'subidPatterns', [
    'auburn',       // Matches: "auburn_123", "team_auburn"
    'tigers',       // Matches: "tigers_456" (note: conflicts with LSU - may need refinement)
    'tiger',        // Singular
    'war_eagle',    // Battle cry
    'au'            // Abbreviation
  ]);
  
  // OPTION B: Map by Partner name patterns
  // Uncomment and edit if your partner names indicate teams
  /*
  teamConfig.addTeamMappingRule('USC Trojans', 'partnerPatterns', [
    'partner_usc',
    'usc_partner'
  ]);
  */
  
  // OPTION C: Map by Campaign patterns
  // Uncomment and edit if your campaigns indicate teams
  /*
  teamConfig.addTeamMappingRule('Team Alpha', 'campaignPatterns', [
    'campaign_alpha',
    'promo_a'
  ]);
  */
  
  // OPTION D: Manual exact mappings (highest priority)
  // For specific IDs that need explicit team assignment
  const rules = teamConfig.get('teamMappingRules');
  rules.manualMappings = {
    // 'specific_subid_123': 'Team Alpha',
    // 'special_partner_456': 'Team Beta',
  };
  teamConfig.set('teamMappingRules', rules);
  
  console.log('✅ ON3 College Teams configuration complete!');
  console.log('\n📚 Teams configured (10 total):');
  const teams = teamConfig.getActiveTeams();
  teams.forEach(team => {
    console.log('  🏈 ' + team.name + ' (Target: $' + team.target.toLocaleString() + '/month)');
  });
  
  console.log('\n⚠️  IMPORTANT NOTES:');
  console.log('  • Some teams share mascots (LSU Tigers & Auburn Tigers)');
  console.log('  • Pattern conflicts may occur - review after first analysis');
  console.log('  • Run discoverDataPatterns() to see your actual SubID values');
  console.log('  • Customize revenue targets above based on your goals');
  
  console.log('\n📋 Next steps:');
  console.log('  1. (Optional) Review patterns: viewTeamConfiguration()');
  console.log('  2. Test mapping: testTeamMapping()');
  console.log('  3. Run full analysis: runCompleteTeamAnalysisPipeline()');
  console.log('  4. Check results in your spreadsheet');
  
  return {
    success: true,
    teamsConfigured: teams.length,
    teams: teams.map(t => t.name)
  };
}

/**
 * Alternative: Setup for teams organized by rep/person
 * Use this if each person/rep is effectively their own team
 */
function setupTeamsByRep() {
  console.log('Setting up teams by individual reps...');
  
  const teamConfig = new TeamConfig();
  
  // Define each rep as a team
  const reps = [
    { name: 'John Smith', target: 25000, pattern: 'john' },
    { name: 'Jane Doe', target: 30000, pattern: 'jane' },
    { name: 'Bob Johnson', target: 22000, pattern: 'bob' },
    { name: 'Alice Williams', target: 28000, pattern: 'alice' }
  ];
  
  reps.forEach(rep => {
    // Create team for rep
    teamConfig.addTeam(rep.name, {
      description: 'Individual contributor',
      lead: rep.name,
      target: rep.target,
      active: true
    });
    
    // Map by their name pattern in SubID
    teamConfig.addTeamMappingRule(rep.name, 'subidPatterns', [
      rep.pattern,
      rep.name.toLowerCase().replace(' ', '_')
    ]);
  });
  
  console.log('✅ ' + reps.length + ' reps configured as teams');
  console.log('\nNext: Run runCompleteTeamAnalysisPipeline()');
  
  return { success: true, teamsConfigured: reps.length };
}

/**
 * Setup for regional teams
 * Use this if your teams are organized by geography
 */
function setupRegionalTeams() {
  console.log('Setting up regional teams...');
  
  const teamConfig = new TeamConfig();
  
  const regions = [
    {
      name: 'North Region',
      patterns: ['north', 'northern', 'n_region'],
      target: 75000
    },
    {
      name: 'South Region',
      patterns: ['south', 'southern', 's_region'],
      target: 65000
    },
    {
      name: 'East Region',
      patterns: ['east', 'eastern', 'e_region'],
      target: 70000
    },
    {
      name: 'West Region',
      patterns: ['west', 'western', 'w_region'],
      target: 80000
    }
  ];
  
  regions.forEach(region => {
    teamConfig.addTeam(region.name, {
      description: region.name + ' sales team',
      lead: '',
      target: region.target,
      active: true
    });
    
    teamConfig.addTeamMappingRule(region.name, 'subidPatterns', region.patterns);
  });
  
  console.log('✅ ' + regions.length + ' regional teams configured');
  console.log('\nNext: Run runCompleteTeamAnalysisPipeline()');
  
  return { success: true, teamsConfigured: regions.length };
}

/**
 * Helper: Discover what patterns exist in your data
 * Run this FIRST to see what SubIDs, Partners, etc. you have
 */
function discoverDataPatterns() {
  console.log('Discovering patterns in your SKU data...');
  
  try {
    const impactConfig = new ImpactConfig();
    const metrics = new PerformanceMetrics();
    const logger = new EnhancedLogger(impactConfig, metrics);
    const spreadsheetManager = new EnhancedSpreadsheetManager(impactConfig, logger, metrics);
    
    const spreadsheet = spreadsheetManager.getSpreadsheet();
    const skuSheet = spreadsheet.getSheetByName('SkuLevelAction') || 
                     spreadsheet.getSheetByName('SkuLevelActions');
    
    if (!skuSheet) {
      console.log('❌ No SKU data found. Run runSkuLevelActionOnly() first.');
      return;
    }
    
    const data = skuSheet.getDataRange().getValues();
    const headers = data[0];
    const rows = data.slice(1);
    
    // Find SubID column
    const subidIndex = headers.findIndex(h => 
      h && (h.toLowerCase() === 'subid' || h.toLowerCase() === 'sub_id')
    );
    
    // Find Partner column
    const partnerIndex = headers.findIndex(h => 
      h && h.toLowerCase() === 'partner'
    );
    
    // Find Campaign column
    const campaignIndex = headers.findIndex(h => 
      h && h.toLowerCase() === 'campaign'
    );
    
    console.log('\n=== DATA PATTERNS DISCOVERED ===');
    console.log('Total Records: ' + rows.length);
    
    if (subidIndex >= 0) {
      const subids = rows.map(row => row[subidIndex]).filter(v => v);
      const uniqueSubids = [...new Set(subids)].slice(0, 20);
      console.log('\nSample SubIDs (first 20):');
      uniqueSubids.forEach(subid => console.log('  - ' + subid));
    }
    
    if (partnerIndex >= 0) {
      const partners = rows.map(row => row[partnerIndex]).filter(v => v);
      const uniquePartners = [...new Set(partners)].slice(0, 20);
      console.log('\nSample Partners (first 20):');
      uniquePartners.forEach(partner => console.log('  - ' + partner));
    }
    
    if (campaignIndex >= 0) {
      const campaigns = rows.map(row => row[campaignIndex]).filter(v => v);
      const uniqueCampaigns = [...new Set(campaigns)].slice(0, 20);
      console.log('\nSample Campaigns (first 20):');
      uniqueCampaigns.forEach(campaign => console.log('  - ' + campaign));
    }
    
    console.log('\n=== RECOMMENDATIONS ===');
    console.log('Look at the patterns above and identify common prefixes, keywords, or identifiers');
    console.log('that indicate different teams. Use these to configure your mapping rules.');
    console.log('\nExample: If you see subids like "team_a_001", "team_a_002", "team_b_001"');
    console.log('Your patterns would be: ["team_a"] for Team A and ["team_b"] for Team B');
    
  } catch (error) {
    console.error('Error discovering patterns: ' + error.message);
  }
}

/**
 * Test your configuration before running full analysis
 */
function testTeamMapping() {
  console.log('Testing team mapping configuration...');
  
  try {
    const teamConfig = new TeamConfig();
    const teamMapper = new TeamMapper(teamConfig);
    const impactConfig = new ImpactConfig();
    const metrics = new PerformanceMetrics();
    const logger = new EnhancedLogger(impactConfig, metrics);
    const spreadsheetManager = new EnhancedSpreadsheetManager(impactConfig, logger, metrics);
    
    const spreadsheet = spreadsheetManager.getSpreadsheet();
    const skuSheet = spreadsheet.getSheetByName('SkuLevelAction') || 
                     spreadsheet.getSheetByName('SkuLevelActions');
    
    if (!skuSheet) {
      console.log('❌ No SKU data found. Run runSkuLevelActionOnly() first.');
      return;
    }
    
    // Read sample data
    const data = skuSheet.getDataRange().getValues();
    const headers = data[0];
    const sampleRows = data.slice(1, 11); // Test first 10 rows
    
    console.log('\n=== TESTING MAPPING ON SAMPLE DATA ===');
    console.log('Testing first 10 records...\n');
    
    const teamCounts = {};
    
    sampleRows.forEach((row, index) => {
      const rowObj = {};
      headers.forEach((header, i) => {
        rowObj[header] = row[i];
      });
      
      const team = teamMapper.mapToTeam(rowObj);
      
      if (!teamCounts[team]) {
        teamCounts[team] = 0;
      }
      teamCounts[team]++;
      
      const subid = rowObj['SubID'] || rowObj['subid'] || 'N/A';
      const partner = rowObj['Partner'] || rowObj['partner'] || 'N/A';
      
      console.log('Record ' + (index + 1) + ':');
      console.log('  SubID: ' + subid);
      console.log('  Partner: ' + partner);
      console.log('  → Mapped to: ' + team);
      console.log('');
    });
    
    console.log('=== TEAM DISTRIBUTION (sample) ===');
    Object.entries(teamCounts).forEach(([team, count]) => {
      console.log(team + ': ' + count + ' records');
    });
    
    console.log('\n✅ Mapping test complete!');
    
    if (teamCounts['Unassigned'] > 0) {
      console.log('\n⚠️  WARNING: ' + teamCounts['Unassigned'] + ' records mapped to "Unassigned"');
      console.log('Review your mapping patterns to ensure all records are assigned.');
    }
    
  } catch (error) {
    console.error('Error testing mapping: ' + error.message);
  }
}

/**
 * Complete setup wizard - walks through all steps
 */
function runSetupWizard() {
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║       TEAM SKU ANALYSIS - SETUP WIZARD                         ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');
  
  console.log('\nStep 1: Checking for SKU data...');
  const impactConfig = new ImpactConfig();
  const metrics = new PerformanceMetrics();
  const logger = new EnhancedLogger(impactConfig, metrics);
  const spreadsheetManager = new EnhancedSpreadsheetManager(impactConfig, logger, metrics);
  
  const spreadsheet = spreadsheetManager.getSpreadsheet();
  const skuSheet = spreadsheet.getSheetByName('SkuLevelAction') || 
                   spreadsheet.getSheetByName('SkuLevelActions');
  
  if (!skuSheet) {
    console.log('❌ No SKU data found.');
    console.log('\n🔧 ACTION REQUIRED:');
    console.log('Run this command first: runSkuLevelActionOnly()');
    console.log('\nThen come back and run runSetupWizard() again.');
    return { step: 1, action: 'Pull SKU data first' };
  }
  
  console.log('✅ SKU data found!');
  
  console.log('\nStep 2: Discovering data patterns...');
  discoverDataPatterns();
  
  console.log('\n\nStep 3: Configure your teams');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('📝 NEXT ACTION: Edit the setupTeamsQuickStart() function');
  console.log('   in setup-team-config.js with your team information');
  console.log('');
  console.log('   Then run: setupTeamsQuickStart()');
  console.log('');
  console.log('   Then run: testTeamMapping()');
  console.log('');
  console.log('   Finally run: runCompleteTeamAnalysisPipeline()');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  
  return { step: 3, action: 'Configure teams in setupTeamsQuickStart()' };
}

