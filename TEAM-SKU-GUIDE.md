# Team-Level SKU Analysis Guide

## Overview

This guide will help you track which teams are driving the most revenue from Mula conversations and associate each conversation to a team. The system provides:

- **Team Attribution**: Automatically map each Mula conversation to a sales team
- **Revenue Tracking**: See exactly how much revenue each team is generating
- **SKU Analysis**: Understand which products (SKUs) each team is selling
- **Performance Dashboards**: Visual reports showing team performance and comparisons

## Quick Start

### 1. Configure Your Teams

First, you need to tell the system about your teams. Edit the `configureTeamMapping()` function in `team-sku-analysis.js` or run this in the Apps Script editor:

```javascript
function setupMyTeams() {
  const teamConfig = new TeamConfig();
  
  // Add your teams
  teamConfig.addTeam('Team Alpha', {
    description: 'East Coast Sales',
    lead: 'John Doe',
    target: 50000, // Monthly revenue target in dollars
    active: true
  });
  
  teamConfig.addTeam('Team Beta', {
    description: 'West Coast Sales',
    lead: 'Jane Smith',
    target: 40000,
    active: true
  });
  
  // Add more teams as needed...
}
```

### 2. Set Up Mapping Rules

The system needs to know how to identify which team each conversation belongs to. You can map by:

- **SubID patterns** (recommended for Mula conversations)
- **Partner name patterns**
- **Campaign patterns**
- **Manual mappings** for specific IDs

```javascript
function setupMappingRules() {
  const teamConfig = new TeamConfig();
  
  // Map by SubID patterns (most common for Mula)
  // If a conversation's SubID contains any of these patterns, it's assigned to that team
  teamConfig.addTeamMappingRule('Team Alpha', 'subidPatterns', [
    'alpha',      // matches: "alpha_123", "team_alpha", etc.
    'team_a',     // matches: "team_a_456", etc.
    'east'        // matches: "east_coast", etc.
  ]);
  
  teamConfig.addTeamMappingRule('Team Beta', 'subidPatterns', [
    'beta',
    'team_b',
    'west'
  ]);
  
  // You can also map by partner name
  teamConfig.addTeamMappingRule('Team Alpha', 'partnerPatterns', [
    'partner_alpha',
    'alpha_partner'
  ]);
  
  // Or by campaign name
  teamConfig.addTeamMappingRule('Team Alpha', 'campaignPatterns', [
    'campaign_a',
    'promo_alpha'
  ]);
}
```

### 3. Run the Analysis

Once configured, run the complete pipeline:

```javascript
// In Apps Script editor
runCompleteTeamAnalysisPipeline();
```

Or if you want to analyze a specific date range:

```javascript
runCompleteTeamAnalysisPipeline('2025-10-01', '2025-10-31');
```

## Understanding Your Reports

After running the analysis, you'll get several new sheets in your spreadsheet:

### 1. TEAM_SUMMARY_DASHBOARD
**Purpose**: High-level overview of all team performance

**Columns**:
- `Team` - Team name
- `Total Revenue` - Total dollars generated
- `Conversions` - Number of successful conversions
- `Quantity` - Total items sold
- `Unique SKUs` - Number of different products sold
- `Avg Order Value` - Average revenue per conversion
- `Target` - Monthly revenue target
- `% of Target` - Performance vs target

**Use this to**:
- See which teams are performing best
- Identify teams exceeding or missing targets
- Quick comparison of team revenue

### 2. TEAM_[TeamName] Sheets
**Purpose**: Detailed conversation-level data for each team

**Contains**: Every Mula conversation attributed to that team with all original data fields plus team attribution

**Use this to**:
- Drill down into specific team's conversations
- Audit team attribution
- Export team-specific data
- Analyze conversation patterns per team

### 3. SKU_PERFORMANCE_BY_TEAM
**Purpose**: Product-level breakdown showing which teams sell which products

**Columns**:
- `Team` - Team name
- `SKU` - Product SKU/ID
- `Revenue` - Revenue from this SKU
- `Conversions` - Number of times sold
- `Quantity` - Total units sold
- `Avg Order Value` - Average value per sale

**Use this to**:
- Identify best-selling products per team
- See which teams specialize in which products
- Find cross-selling opportunities
- Inventory planning by team

### 4. TEAM_COMPARISON
**Purpose**: Side-by-side comparison of all teams

**Rows**:
- Total Revenue
- Total Conversions
- Total Quantity
- Unique SKUs
- Avg Order Value
- Market Share %

**Use this to**:
- Compare teams head-to-head
- Identify performance gaps
- Set benchmarks
- Plan team strategies

## Common Use Cases

### Finding Your Top Revenue Team

```javascript
function getTopTeam() {
  const result = runTeamSKUAnalysis();
  const analyzer = new SKUTeamAnalyzer(new TeamConfig(), new TeamMapper(new TeamConfig()));
  const topTeams = analyzer.getTopTeamsByRevenue(result.analysis, 1);
  
  console.log('Top Revenue Team: ' + topTeams[0].team);
  console.log('Revenue: $' + topTeams[0].totalRevenue.toFixed(2));
  console.log('Conversions: ' + topTeams[0].totalConversions);
}
```

### Finding Top Products for a Specific Team

```javascript
function getTopProductsForTeam(teamName) {
  const result = runTeamSKUAnalysis();
  const analyzer = new SKUTeamAnalyzer(new TeamConfig(), new TeamMapper(new TeamConfig()));
  const topSKUs = analyzer.getTopSKUsForTeam(result.analysis, teamName, 10);
  
  console.log('Top 10 Products for ' + teamName + ':');
  topSKUs.forEach((sku, index) => {
    console.log((index + 1) + '. ' + sku.sku + ': $' + sku.revenue.toFixed(2));
  });
}
```

### Getting All Conversations for a Team

```javascript
function getTeamData(teamName) {
  const result = runTeamSKUAnalysis();
  const analyzer = new SKUTeamAnalyzer(new TeamConfig(), new TeamMapper(new TeamConfig()));
  const conversations = analyzer.getTeamConversations(result.analysis, teamName);
  
  console.log('Found ' + conversations.length + ' conversations for ' + teamName);
  return conversations;
}
```

## Mapping Strategy

### How to Identify the Right Patterns

1. **Run SKU report first** to see what your data looks like:
   ```javascript
   runSkuLevelActionOnly();
   ```

2. **Check the SubID column** - Look for patterns that identify teams:
   - Do you use prefixes? (e.g., "team_a_", "north_")
   - Do you use team names? (e.g., "alpha", "beta")
   - Do you use rep names? (e.g., "john_", "jane_")

3. **Look at Partner names** if SubIDs aren't team-specific

4. **Check Campaign names** for team indicators

### Example Patterns

If your SubIDs look like this:
- `team_alpha_001`, `team_alpha_002`, `alpha_promo_123`
- `team_beta_001`, `beta_lead_456`
- `john_smith_789`, `jane_doe_012`

Your mapping would be:
```javascript
// By team prefix
teamConfig.addTeamMappingRule('Team Alpha', 'subidPatterns', ['alpha', 'team_alpha']);
teamConfig.addTeamMappingRule('Team Beta', 'subidPatterns', ['beta', 'team_beta']);

// By rep name (if reps = teams)
teamConfig.addTeamMappingRule('John Smith Team', 'subidPatterns', ['john_smith', 'john']);
teamConfig.addTeamMappingRule('Jane Doe Team', 'subidPatterns', ['jane_doe', 'jane']);
```

## Advanced Configuration

### Priority of Mapping Rules

The system checks in this order:
1. **Manual Mappings** (highest priority)
2. **SubID Patterns**
3. **Partner Patterns**
4. **Campaign Patterns**
5. **Default Team** ("Unassigned")

### Manual Mappings for Specific IDs

For exact matches (highest priority):

```javascript
const teamConfig = new TeamConfig();
const rules = teamConfig.get('teamMappingRules');

rules.manualMappings = {
  'specific_partner_123': 'Team Alpha',
  'special_subid_456': 'Team Beta',
  'conversation_789': 'Team Gamma'
};

teamConfig.set('teamMappingRules', rules);
```

### Handling Unassigned Conversations

Conversations that don't match any rule go to the "Unassigned" team. To see what's unassigned:

1. Check the **TEAM_Unassigned** sheet
2. Review the SubIDs/Partners
3. Add new mapping rules as needed
4. Re-run the analysis

## Automation

### Scheduled Daily Reports

Set up a time-based trigger to run analysis daily:

1. In Apps Script Editor, go to **Triggers** (clock icon)
2. Click **+ Add Trigger**
3. Select function: `runCompleteTeamAnalysisPipeline`
4. Event source: **Time-driven**
5. Type: **Day timer**
6. Time: Pick your preferred time (e.g., 6:00 AM)

### Monthly Team Reports

```javascript
function runMonthlyTeamReports() {
  // Get current month
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  
  const startDate = `${year}-${month}-01`;
  
  // Calculate last day of month
  const lastDay = new Date(year, now.getMonth() + 1, 0).getDate();
  const endDate = `${year}-${month}-${lastDay}`;
  
  console.log('Running monthly team report for ' + month + '/' + year);
  return runCompleteTeamAnalysisPipeline(startDate, endDate);
}
```

## Troubleshooting

### "No SKU data sheet found"

**Problem**: The system can't find the SKU data.

**Solution**:
1. Run `runSkuLevelActionOnly()` first to pull SKU data
2. Check that the sheet is named "SkuLevelAction" or "SkuLevelActions"

### All conversations show as "Unassigned"

**Problem**: Your mapping rules aren't matching any data.

**Solution**:
1. View your current configuration: `viewTeamConfiguration()`
2. Check your SKU data sheet to see actual SubID values
3. Verify your patterns match the actual data (case-insensitive matching)
4. Use broader patterns (e.g., just "alpha" instead of "team_alpha_123")

### Teams showing zero revenue

**Problem**: Data isn't being aggregated correctly.

**Solution**:
1. Check column names in your SKU sheet
2. System looks for: `Sale_amount`, `Revenue`, or `revenue`
3. Verify these columns exist and contain numeric values

### Performance is slow

**Problem**: Large datasets taking too long.

**Solution**:
1. Use date range filtering to analyze smaller periods
2. Process one month at a time
3. Limit the number of sheets created (configure in TeamConfig)

## API Reference

### Key Functions

```javascript
// Main pipeline
runCompleteTeamAnalysisPipeline(startDate, endDate)

// Just analysis (if you already have SKU data)
runTeamSKUAnalysis()

// Configuration
configureTeamMapping()
viewTeamConfiguration()

// Quick checks
getTeamPerformanceSummary()

// Pull fresh SKU data
runSkuLevelActionOnly()
runSkuLevelActionWithDateRange('2025-10-01', '2025-10-31')
```

### Classes

- **TeamConfig**: Manages team and mapping configuration
- **TeamMapper**: Maps conversations to teams
- **SKUTeamAnalyzer**: Analyzes SKU data by team
- **TeamReportGenerator**: Creates spreadsheet reports

## Best Practices

1. **Start with broad patterns**, refine later
2. **Review "Unassigned" regularly** to catch missed conversations
3. **Set realistic targets** for accurate performance tracking
4. **Run daily or weekly** to track trends
5. **Use date ranges** for comparing time periods
6. **Document your mapping logic** for team members

## Next Steps

1. ✅ Configure your teams
2. ✅ Set up mapping rules
3. ✅ Run initial analysis
4. ✅ Review the reports
5. ✅ Refine mappings based on "Unassigned"
6. ✅ Set up automation
7. ✅ Share dashboards with team leads

## Questions?

Check the main README.md for general Impact.com script documentation.

For team-specific analysis questions, review:
- Team mapping configuration in `team-sku-analysis.js`
- Example patterns in this guide
- Your actual data in the SkuLevelAction sheet

