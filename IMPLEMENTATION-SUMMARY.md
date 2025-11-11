# Team-Level SKU Tracking - Implementation Summary

## What Was Built

I've created a comprehensive team tracking system that allows you to:

1. **Associate each Mula conversation to a team**
2. **Track revenue by team**
3. **Analyze which SKUs each team is selling**
4. **Compare team performance**
5. **Track team progress vs targets**

## Files Created

### 1. `team-sku-analysis.js` (Main Module)
**Purpose**: Core functionality for team attribution and analysis

**Key Components**:
- `TeamConfig`: Manages team definitions and mapping rules
- `TeamMapper`: Maps conversations/SKUs to teams based on patterns
- `SKUTeamAnalyzer`: Analyzes revenue and SKU data by team
- `TeamReportGenerator`: Creates comprehensive spreadsheet reports

**Key Functions**:
```javascript
runTeamSKUAnalysis()              // Analyze existing SKU data by team
runCompleteTeamAnalysisPipeline() // Pull data + analyze + generate reports
configureTeamMapping()             // Set up team configurations
viewTeamConfiguration()            // View current config
getTeamPerformanceSummary()        // Quick performance check
```

### 2. `setup-team-config.js` (Setup Templates)
**Purpose**: Easy-to-customize templates for team configuration

**Templates Included**:
- `setupTeamsQuickStart()` - General team setup
- `setupTeamsByRep()` - Individual rep/person as teams
- `setupRegionalTeams()` - Geographic region-based teams
- `discoverDataPatterns()` - Discover patterns in your data
- `testTeamMapping()` - Test your configuration
- `runSetupWizard()` - Guided setup process

### 3. `TEAM-SKU-GUIDE.md` (Complete Documentation)
**Purpose**: Comprehensive guide covering:
- Detailed configuration instructions
- Mapping strategy guidelines
- Report explanations
- Use cases and examples
- Troubleshooting
- API reference

### 4. `TEAM-TRACKING-QUICKSTART.md` (Quick Reference)
**Purpose**: Fast-access reference for:
- 5-minute quick start
- Common patterns
- Regular use commands
- Troubleshooting tips
- Automation setup

### 5. `IMPLEMENTATION-SUMMARY.md` (This File)
**Purpose**: Overview of what was built and how to use it

## How It Works

### 1. Data Flow

```
Impact.com API
    ↓
SKU Report (SkuLevelActions)
    ↓
Team Mapper (applies rules)
    ↓
SKU Team Analyzer (aggregates)
    ↓
Team Report Generator (creates sheets)
    ↓
Your Spreadsheet (new team sheets)
```

### 2. Mapping Logic

The system maps conversations to teams using pattern matching:

**Priority Order** (highest to lowest):
1. **Manual Mappings** - Exact ID matches
2. **SubID Patterns** - Pattern matching on SubID field
3. **Partner Patterns** - Pattern matching on Partner field
4. **Campaign Patterns** - Pattern matching on Campaign field
5. **Default Team** - "Unassigned" if no match

**Example**:
- SubID: "team_alpha_conversation_123"
- Pattern: ["alpha", "team_a"]
- Result: Maps to "Team Alpha" (contains "alpha")

### 3. Reports Generated

After running analysis, you get 4 new sheets:

#### A. TEAM_SUMMARY_DASHBOARD
**Overview of all teams**

Columns:
- Team name
- Total Revenue
- Total Conversions (# of Mula conversations)
- Total Quantity (items sold)
- Unique SKUs (products sold)
- Avg Order Value
- Revenue Target
- % of Target

Sorted by revenue (highest first)

#### B. TEAM_[TeamName] (one per team)
**Detailed conversation-level data**

Contains every conversation attributed to that team with all original data plus team assignment.

Use for:
- Drilling into specific team's activity
- Auditing team attribution
- Exporting team-specific data

#### C. SKU_PERFORMANCE_BY_TEAM
**Product-level breakdown**

Shows which products (SKUs) each team is selling and how much revenue each SKU generates per team.

Columns:
- Team
- SKU
- Revenue
- Conversions
- Quantity
- Avg Order Value

Sorted by revenue (highest first)

#### D. TEAM_COMPARISON
**Side-by-side metrics**

Compares all teams across key metrics:
- Total Revenue
- Total Conversions
- Total Quantity
- Unique SKUs
- Avg Order Value
- Market Share %

## Setup Process

### Quick Setup (Recommended Path)

```javascript
// Step 1: Discover patterns in your data
discoverDataPatterns()

// Step 2: Configure teams (edit in setup-team-config.js first)
setupTeamsQuickStart()

// Step 3: Test your configuration
testTeamMapping()

// Step 4: Run full analysis
runCompleteTeamAnalysisPipeline()
```

### Detailed Setup Steps

1. **Pull SKU Data** (if you don't have it yet)
   ```javascript
   runSkuLevelActionOnly()
   ```

2. **Discover Your Patterns**
   ```javascript
   discoverDataPatterns()
   ```
   This shows you sample SubIDs, Partners, and Campaigns from your actual data.

3. **Configure Teams**
   Edit `setup-team-config.js` → `setupTeamsQuickStart()`:
   - Add your teams
   - Set revenue targets
   - Add mapping patterns based on what you saw in step 2

4. **Run Configuration**
   ```javascript
   setupTeamsQuickStart()
   ```

5. **Test Mapping**
   ```javascript
   testTeamMapping()
   ```
   Verify that conversations map correctly. Ideally, very few should be "Unassigned".

6. **Run Full Analysis**
   ```javascript
   runCompleteTeamAnalysisPipeline()
   ```

7. **Review Results**
   Check your spreadsheet for the new team sheets.

8. **Refine if Needed**
   - Look at TEAM_Unassigned sheet
   - Add more patterns for missed conversations
   - Re-run analysis

## Configuration Examples

### Example 1: Teams in SubID

**Your Data**:
- `team_north_001`, `team_north_002`
- `team_south_001`, `team_south_002`

**Configuration**:
```javascript
teamConfig.addTeam('North Team', {
  description: 'Northern region',
  target: 50000,
  active: true
});

teamConfig.addTeamMappingRule('North Team', 'subidPatterns', [
  'north',
  'team_north'
]);

teamConfig.addTeam('South Team', {
  description: 'Southern region',
  target: 45000,
  active: true
});

teamConfig.addTeamMappingRule('South Team', 'subidPatterns', [
  'south',
  'team_south'
]);
```

### Example 2: Rep Names as Teams

**Your Data**:
- `john_smith_123`, `john_456`
- `jane_doe_789`, `jane_012`

**Configuration**:
```javascript
teamConfig.addTeam('John Smith', {
  description: 'Individual contributor',
  target: 25000,
  active: true
});

teamConfig.addTeamMappingRule('John Smith', 'subidPatterns', [
  'john',
  'john_smith'
]);

teamConfig.addTeam('Jane Doe', {
  description: 'Individual contributor',
  target: 30000,
  active: true
});

teamConfig.addTeamMappingRule('Jane Doe', 'subidPatterns', [
  'jane',
  'jane_doe'
]);
```

### Example 3: Mixed Patterns

**Your Data**:
- Some have team prefixes in SubID
- Some have team indicators in Partner name
- Some specific IDs need manual assignment

**Configuration**:
```javascript
// SubID patterns
teamConfig.addTeamMappingRule('Team Alpha', 'subidPatterns', ['alpha', 'team_a']);

// Partner patterns
teamConfig.addTeamMappingRule('Team Alpha', 'partnerPatterns', ['partner_alpha']);

// Manual exact matches (highest priority)
const rules = teamConfig.get('teamMappingRules');
rules.manualMappings = {
  'special_id_123': 'Team Alpha',
  'vip_partner_456': 'Team Beta'
};
teamConfig.set('teamMappingRules', rules);
```

## Common Use Cases

### Use Case 1: Monthly Team Review

```javascript
// Pull data for October
runCompleteTeamAnalysisPipeline('2025-10-01', '2025-10-31')

// Review:
// - TEAM_SUMMARY_DASHBOARD for overview
// - Check "% of Target" column
// - Identify top and bottom performers
```

### Use Case 2: Find Top Products per Team

```javascript
// After running analysis
const result = runTeamSKUAnalysis();
const analyzer = new SKUTeamAnalyzer(new TeamConfig(), new TeamMapper(new TeamConfig()));

// Get top 10 SKUs for specific team
const topSKUs = analyzer.getTopSKUsForTeam(result.analysis, 'Team Alpha', 10);

topSKUs.forEach((sku, index) => {
  console.log((index + 1) + '. ' + sku.sku + ': $' + sku.revenue.toFixed(2));
});
```

### Use Case 3: Identify Unassigned Conversations

```javascript
// Run analysis
runTeamSKUAnalysis()

// Check TEAM_Unassigned sheet in spreadsheet
// Review SubIDs that didn't match any pattern
// Add new patterns and re-run
```

### Use Case 4: Compare Teams Head-to-Head

```javascript
// Run analysis
runTeamSKUAnalysis()

// Check TEAM_COMPARISON sheet
// Shows side-by-side metrics for all teams
// See market share %
```

### Use Case 5: Team Performance Alerts

```javascript
function checkTeamTargets() {
  const result = runTeamSKUAnalysis();
  const analyzer = new SKUTeamAnalyzer(new TeamConfig(), new TeamMapper(new TeamConfig()));
  const teams = analyzer.getTopTeamsByRevenue(result.analysis);
  
  teams.forEach(team => {
    const teamMeta = new TeamConfig().get('teams')[team.team];
    if (teamMeta && teamMeta.target) {
      const percentOfTarget = (team.totalRevenue / teamMeta.target) * 100;
      
      if (percentOfTarget < 80) {
        console.log('⚠️ ' + team.team + ' is at ' + percentOfTarget.toFixed(1) + '% of target');
      } else if (percentOfTarget >= 100) {
        console.log('✅ ' + team.team + ' has met their target! (' + percentOfTarget.toFixed(1) + '%)');
      }
    }
  });
}
```

## Integration with Existing Scripts

This module integrates seamlessly with your existing Impact.com scripts:

### Works With:
- `optimized-impact-script-v4.js` - Main data pulling script
- `runSkuLevelActionOnly()` - SKU data source
- Existing spreadsheet structure
- Date range filtering

### How to Use Together:

```javascript
// Option 1: Use existing SKU data
runSkuLevelActionOnly()  // Pull data
runTeamSKUAnalysis()     // Analyze it

// Option 2: Complete pipeline (recommended)
runCompleteTeamAnalysisPipeline()  // Does both

// Option 3: With date ranges
setDateRange('2025-10-01', '2025-10-31')
runSkuLevelActionOnly()
runTeamSKUAnalysis()
```

## Automation Options

### Daily Automated Reports

1. Open Apps Script Editor
2. Click **Triggers** (clock icon)
3. **+ Add Trigger**
4. Select: `runCompleteTeamAnalysisPipeline`
5. Event: Time-driven → Day timer → 6:00 AM
6. Save

Now team reports update automatically every morning!

### Weekly Summary Email

```javascript
function sendWeeklyTeamSummary() {
  // Get last 7 days
  const endDate = new Date();
  const startDate = new Date(endDate);
  startDate.setDate(startDate.getDate() - 7);
  
  // Run analysis
  const result = runCompleteTeamAnalysisPipeline(
    startDate.toISOString().split('T')[0],
    endDate.toISOString().split('T')[0]
  );
  
  // Send email (configure recipient)
  MailApp.sendEmail({
    to: 'team-leads@yourcompany.com',
    subject: 'Weekly Team Performance Summary',
    body: 'Team performance reports have been updated. Check the spreadsheet for details.'
  });
}
```

## Performance Considerations

### For Large Datasets

- Use date range filtering to process smaller periods
- Process one month at a time for historical analysis
- The system handles large datasets well but date filtering speeds things up

### Memory Optimization

- Built-in memory management
- Processes data in batches
- Suitable for datasets with 10,000+ SKU records

## Troubleshooting

### Issue: All conversations show "Unassigned"

**Cause**: Mapping patterns don't match your data

**Fix**:
1. Run `discoverDataPatterns()`
2. Look at actual SubID values
3. Update patterns in `setupTeamsQuickStart()`
4. Use broader patterns (just "alpha" not "team_alpha_region_123")
5. Re-run `setupTeamsQuickStart()`
6. Test with `testTeamMapping()`

### Issue: Missing SKU sheet

**Cause**: Need to pull SKU data first

**Fix**: Run `runSkuLevelActionOnly()`

### Issue: Performance is slow

**Cause**: Processing large dataset

**Fix**: Use date ranges
```javascript
runCompleteTeamAnalysisPipeline('2025-10-01', '2025-10-31')
```

## Next Steps

### Immediate (First Run):
1. ✅ Run `discoverDataPatterns()` to see your data
2. ✅ Edit `setup-team-config.js` with your teams
3. ✅ Run `setupTeamsQuickStart()`
4. ✅ Run `testTeamMapping()` to verify
5. ✅ Run `runCompleteTeamAnalysisPipeline()`
6. ✅ Review your new team sheets

### Ongoing:
- Run daily/weekly for updated reports
- Review TEAM_Unassigned regularly
- Refine mapping patterns as needed
- Track teams vs targets
- Share team-specific sheets with team leads

### Advanced:
- Set up automated daily runs
- Create custom alert functions
- Build team comparison views
- Export for external BI tools

## Documentation Reference

- **Quick Start**: `TEAM-TRACKING-QUICKSTART.md` - Fast reference
- **Complete Guide**: `TEAM-SKU-GUIDE.md` - Detailed documentation
- **Code Examples**: `setup-team-config.js` - Templates to customize
- **Core Module**: `team-sku-analysis.js` - Main code (usually don't need to edit)

## Support

For questions:
1. Check `TEAM-TRACKING-QUICKSTART.md` for quick answers
2. Review `TEAM-SKU-GUIDE.md` for detailed explanations
3. Use `discoverDataPatterns()` to understand your data
4. Use `testTeamMapping()` to debug mapping issues

## Summary

You now have a complete team-level SKU tracking system that:

✅ Maps each Mula conversation to a team automatically  
✅ Tracks revenue by team  
✅ Shows which SKUs each team sells  
✅ Compares team performance  
✅ Tracks progress vs targets  
✅ Creates comprehensive dashboards  
✅ Integrates with your existing Impact.com data pipeline  

**Get started now**: Run `runSetupWizard()` for guided setup!

