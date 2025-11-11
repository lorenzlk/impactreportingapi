# Team Tracking Quick Start Guide

## 🎯 What This Does

Track which teams drive the most revenue from Mula conversations and associate each SKU sale to a specific team.

## 🚀 Quick Start (5 Minutes)

### 1. Pull SKU Data (if you haven't already)

```javascript
runSkuLevelActionOnly()
```

### 2. See What Data You Have

```javascript
discoverDataPatterns()
```

This shows you sample SubIDs, Partners, and Campaigns so you can identify team patterns.

### 3. Configure Your Teams

Edit `setup-team-config.js` → `setupTeamsQuickStart()` function:

```javascript
// Example: If your SubIDs look like "team_alpha_123", "team_beta_456"

teamConfig.addTeam('Team Alpha', {
  description: 'East Coast Sales',
  lead: 'John Doe',
  target: 50000,  // Monthly target
  active: true
});

teamConfig.addTeamMappingRule('Team Alpha', 'subidPatterns', [
  'alpha',     // Matches any SubID containing "alpha"
  'team_a'     // Matches any SubID containing "team_a"
]);
```

Then run:
```javascript
setupTeamsQuickStart()
```

### 4. Test Your Configuration

```javascript
testTeamMapping()
```

This shows how the first 10 records would be mapped. Make sure no records go to "Unassigned" (or very few).

### 5. Run Full Analysis

```javascript
runCompleteTeamAnalysisPipeline()
```

Or with date range:
```javascript
runCompleteTeamAnalysisPipeline('2025-10-01', '2025-10-31')
```

## 📊 What You Get

New sheets in your spreadsheet:

1. **TEAM_SUMMARY_DASHBOARD** - Overview of all teams
2. **TEAM_[Name]** - Detailed data for each team  
3. **SKU_PERFORMANCE_BY_TEAM** - Which teams sell which products
4. **TEAM_COMPARISON** - Side-by-side team comparison

## 🔍 Key Insights You Can Get

### Which team drives the most revenue?
Check **TEAM_SUMMARY_DASHBOARD** - sorted by revenue

### What products does each team sell?
Check **SKU_PERFORMANCE_BY_TEAM** - shows SKUs per team

### How many Mula conversations per team?
Check **TEAM_SUMMARY_DASHBOARD** - "Conversions" column

### Which team is closest to their target?
Check **TEAM_SUMMARY_DASHBOARD** - "% of Target" column

### Detailed view of a specific team?
Check **TEAM_[TeamName]** sheet - all their conversations

## ⚙️ Common Patterns

### Pattern 1: Teams in SubID
If SubIDs are like: `team_north_001`, `team_south_002`

```javascript
teamConfig.addTeamMappingRule('North Team', 'subidPatterns', ['north', 'team_north']);
teamConfig.addTeamMappingRule('South Team', 'subidPatterns', ['south', 'team_south']);
```

### Pattern 2: Rep Names in SubID
If SubIDs are like: `john_smith_123`, `jane_doe_456`

```javascript
teamConfig.addTeam('John Smith', { target: 25000, active: true });
teamConfig.addTeamMappingRule('John Smith', 'subidPatterns', ['john', 'john_smith']);

teamConfig.addTeam('Jane Doe', { target: 30000, active: true });
teamConfig.addTeamMappingRule('Jane Doe', 'subidPatterns', ['jane', 'jane_doe']);
```

### Pattern 3: Partner Names Indicate Teams
If partners are like: `partner_alpha`, `partner_beta`

```javascript
teamConfig.addTeamMappingRule('Team Alpha', 'partnerPatterns', ['partner_alpha', 'alpha']);
```

### Pattern 4: Specific Exact Matches
For specific IDs that need exact assignment:

```javascript
const rules = teamConfig.get('teamMappingRules');
rules.manualMappings = {
  'specific_id_123': 'Team Alpha',
  'special_partner': 'Team Beta'
};
teamConfig.set('teamMappingRules', rules);
```

## 🔄 Regular Use

### Daily Update
```javascript
runCompleteTeamAnalysisPipeline()
```

### Weekly Review
```javascript
// Get last 7 days
const endDate = new Date();
const startDate = new Date(endDate);
startDate.setDate(startDate.getDate() - 7);

runCompleteTeamAnalysisPipeline(
  startDate.toISOString().split('T')[0],
  endDate.toISOString().split('T')[0]
);
```

### Monthly Report
```javascript
runCompleteTeamAnalysisPipeline('2025-10-01', '2025-10-31')
```

## 🎨 Alternative Setup Options

### Setup by Individual Reps
```javascript
setupTeamsByRep()
```

### Setup by Region
```javascript
setupRegionalTeams()
```

### Complete Wizard (Step-by-step)
```javascript
runSetupWizard()
```

## 🔧 Troubleshooting

### Everything shows "Unassigned"
**Problem**: Your patterns don't match your data.

**Solution**: 
1. Run `discoverDataPatterns()` to see actual SubID values
2. Look for common words/prefixes
3. Use those in your patterns (matching is case-insensitive)
4. Use broader patterns (e.g., just "alpha" not "team_alpha_region_east")

### Can't find SKU sheet
**Problem**: Need to pull SKU data first.

**Solution**: Run `runSkuLevelActionOnly()`

### Analysis is slow
**Problem**: Processing lots of data.

**Solution**: Use date ranges to process smaller periods
```javascript
runCompleteTeamAnalysisPipeline('2025-10-01', '2025-10-31')
```

## 📈 Advanced: Automation

Set up daily automated reports:

1. Open Apps Script Editor
2. Click **Triggers** (clock icon on left)
3. **+ Add Trigger**
4. Function: `runCompleteTeamAnalysisPipeline`
5. Event: Time-driven → Day timer → 6:00 AM (or your choice)
6. Save

Now reports update automatically every day!

## 💡 Pro Tips

1. **Start broad** - Use simple patterns like "alpha", "beta" first
2. **Test first** - Always run `testTeamMapping()` before full analysis
3. **Review Unassigned** - Check TEAM_Unassigned sheet to find missed patterns
4. **Set realistic targets** - Use for % of Target tracking
5. **Combine with date ranges** - Great for monthly/quarterly reviews
6. **Share specific team sheets** - Each team can see just their data

## 📞 Next Steps

1. ✅ Discover your data patterns
2. ✅ Configure teams  
3. ✅ Test mapping
4. ✅ Run full analysis
5. ✅ Share dashboards with team leads
6. ✅ Set up automation

See **TEAM-SKU-GUIDE.md** for complete documentation.

