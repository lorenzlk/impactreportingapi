# ON3 College Teams - Quick Setup Guide

## Your Teams (10 Total)

Your configuration is ready for these college teams:

1. 🏈 **USC Trojans**
2. 🏈 **NC State Wolfpack**
3. 🏈 **Ole Miss Rebels**
4. 🏈 **Florida Gators**
5. 🏈 **LSU Tigers**
6. 🏈 **Penn State Nittany Lions**
7. 🏈 **Notre Dame Fighting Irish**
8. 🏈 **Michigan Wolverines**
9. 🏈 **Ohio State Buckeyes**
10. 🏈 **Auburn Tigers**

## 🚀 Quick Start (3 Steps)

### Step 1: Discover Your Data Patterns

```javascript
discoverDataPatterns()
```

This shows you sample SubIDs from your actual Mula conversations so you can see if the default patterns match.

### Step 2: Run Setup

```javascript
setupTeamsQuickStart()
```

This configures all 10 teams with pattern matching rules.

### Step 3: Test & Run

```javascript
// Test first (recommended)
testTeamMapping()

// Then run full analysis
runCompleteTeamAnalysisPipeline()
```

## 📊 What You'll Get

4 new sheets in your spreadsheet:

1. **TEAM_SUMMARY_DASHBOARD**
   - Revenue per team
   - Conversion count (Mula conversations)
   - SKU metrics per team
   - % of target achieved

2. **TEAM_[SchoolName]** (10 sheets, one per team)
   - All Mula conversations for that team
   - Complete SKU details

3. **SKU_PERFORMANCE_BY_TEAM**
   - Which products each team sells
   - Revenue per SKU per team

4. **TEAM_COMPARISON**
   - Side-by-side team metrics
   - Market share by team

## 🎯 Pattern Matching

The system automatically maps Mula conversations to teams based on SubID patterns:

### USC Trojans
Matches if SubID contains: `usc`, `trojans`, `trojan`, `southern_cal`, or `sc`

### NC State Wolfpack
Matches if SubID contains: `ncstate`, `nc_state`, `wolfpack`, `ncst`, or `state`

### Ole Miss Rebels
Matches if SubID contains: `olemiss`, `ole_miss`, `rebels`, `rebel`, or `mississippi`

### Florida Gators
Matches if SubID contains: `florida`, `gators`, `gator`, `uf`, or `fl_gators`

### LSU Tigers
Matches if SubID contains: `lsu`, `tigers`, `tiger`, `louisiana`, or `lsu_tigers`

### Penn State Nittany Lions
Matches if SubID contains: `pennstate`, `penn_state`, `penn`, `psu`, `nittany`, or `lions`

### Notre Dame Fighting Irish
Matches if SubID contains: `notredame`, `notre_dame`, `notre`, `nd`, `fighting_irish`, or `irish`

### Michigan Wolverines
Matches if SubID contains: `michigan`, `wolverines`, `wolverine`, `um`, `mich`, or `go_blue`

### Ohio State Buckeyes
Matches if SubID contains: `ohiostate`, `ohio_state`, `ohio`, `osu`, `buckeyes`, or `buckeye`

### Auburn Tigers
Matches if SubID contains: `auburn`, `tigers`, `tiger`, `war_eagle`, or `au`

> **Note**: Pattern matching is case-insensitive. `USC` = `usc` = `Usc`

## ⚠️ Important Notes

### Potential Conflicts

1. **Tigers Mascot** - Both LSU and Auburn use "Tigers"
   - LSU has priority: `lsu`, `lsu_tigers`, `louisiana`
   - Auburn has priority: `auburn`, `war_eagle`, `au`
   - If a SubID is just "tigers_123", the first match wins (LSU)
   - **Fix**: Ensure your SubIDs are more specific (e.g., "lsu_tigers_123" or "auburn_tigers_123")

2. **Lions Mascot** - Penn State uses "Lions"
   - If another school also uses Lions, same issue applies

3. **Generic abbreviations**
   - `sc` for USC might conflict if you have South Carolina
   - `um` for Michigan might be too generic
   - `ohio` might match other Ohio schools
   - **Fix**: Remove overly generic patterns if conflicts occur

### How to Handle Conflicts

After your first run, check the **TEAM_Unassigned** sheet:
1. If you see conversations that should belong to a team, note the SubID pattern
2. Add that pattern to the team's mapping rules in `setup-team-config.js`
3. Re-run `setupTeamsQuickStart()` and `runCompleteTeamAnalysisPipeline()`

## 🔧 Customization

### Set Revenue Targets

Edit `setup-team-config.js` and change the target values:

```javascript
teamConfig.addTeam('USC Trojans', {
  description: 'USC Trojans NIL Team',
  lead: 'John Smith',  // Add team lead name
  target: 150000,      // Change to your actual target
  active: true
});
```

### Add More Pattern Variations

If you discover additional patterns in your data:

```javascript
// In setup-team-config.js, add more patterns
teamConfig.addTeamMappingRule('USC Trojans', 'subidPatterns', [
  'usc',
  'trojans',
  'trojan',
  'southern_cal',
  'sc',
  'your_new_pattern'  // Add here
]);
```

### Use Partner or Campaign Patterns

If your team identification is in Partner or Campaign fields instead:

```javascript
// Map by Partner name
teamConfig.addTeamMappingRule('USC Trojans', 'partnerPatterns', [
  'usc_partner',
  'partner_usc'
]);

// Map by Campaign name
teamConfig.addTeamMappingRule('USC Trojans', 'campaignPatterns', [
  'usc_campaign',
  'campaign_usc'
]);
```

### Manual Exact Mappings (Highest Priority)

For specific IDs that need explicit assignment:

```javascript
const rules = teamConfig.get('teamMappingRules');
rules.manualMappings = {
  'special_usc_id_123': 'USC Trojans',
  'vip_florida_456': 'Florida Gators',
  'specific_lsu_789': 'LSU Tigers'
};
teamConfig.set('teamMappingRules', rules);
```

## 📅 Regular Use

### Daily Update
```javascript
runCompleteTeamAnalysisPipeline()
```

### Weekly Review
```javascript
// Last 7 days
const endDate = new Date();
const startDate = new Date(endDate);
startDate.setDate(startDate.getDate() - 7);

runCompleteTeamAnalysisPipeline(
  startDate.toISOString().split('T')[0],
  endDate.toISOString().split('T')[0]
);
```

### Monthly Team Reports
```javascript
// October 2025
runCompleteTeamAnalysisPipeline('2025-10-01', '2025-10-31')

// November 2025
runCompleteTeamAnalysisPipeline('2025-11-01', '2025-11-30')
```

### Football Season Analysis
```javascript
// Entire season (example: Sept - Dec)
runCompleteTeamAnalysisPipeline('2025-09-01', '2025-12-31')
```

## 🔍 Analysis Examples

### Find Top Revenue Team
```javascript
runTeamSKUAnalysis()
// Check TEAM_SUMMARY_DASHBOARD - sorted by revenue
```

### Compare SEC Teams
After running analysis, check **TEAM_COMPARISON** sheet for:
- LSU Tigers
- Florida Gators
- Auburn Tigers
- Ole Miss Rebels

### Find Which Team Sells Most of a Specific Product
Check **SKU_PERFORMANCE_BY_TEAM** sheet and filter by SKU

### Track Team Progress to Target
Check **TEAM_SUMMARY_DASHBOARD** - "% of Target" column

## 🤖 Automation

### Set Up Daily Automated Reports

1. Open Apps Script Editor
2. Click **Triggers** (clock icon)
3. **+ Add Trigger**
4. Function: `runCompleteTeamAnalysisPipeline`
5. Event: Time-driven → Day timer
6. Time: 6:00 AM (or your choice)
7. Save

Now your team reports update automatically every morning!

## 🐛 Troubleshooting

### Issue: All conversations show "Unassigned"

**Solution**: Your SubID patterns don't match your data
1. Run `discoverDataPatterns()` to see actual SubID values
2. Look for school identifiers in the SubIDs
3. Update patterns in `setup-team-config.js`
4. Re-run setup

### Issue: Wrong team assignments

**Solution**: Pattern conflict
1. Check which patterns are matching
2. Remove generic patterns (like just "tiger" or "lions")
3. Use more specific patterns (like "lsu_tigers" or "penn_nittany")
4. Use manual mappings for specific IDs

### Issue: One team getting all conversations

**Solution**: Overly broad pattern
1. Find the pattern that's matching everything
2. Make it more specific
3. Example: Change "state" to "ncstate" or "nc_state"

### Issue: Can't find SKU data

**Solution**: Pull data first
```javascript
runSkuLevelActionOnly()
```

## 📚 Complete Documentation

- **This file**: Quick reference for ON3 teams
- **TEAM-TRACKING-QUICKSTART.md**: General quick start guide
- **TEAM-SKU-GUIDE.md**: Complete detailed documentation
- **setup-team-config.js**: Configuration file (edit this)
- **team-sku-analysis.js**: Core code (don't usually need to edit)

## 🎯 Success Checklist

- [ ] Run `discoverDataPatterns()` to see your data
- [ ] Review patterns match your SubID format
- [ ] Customize revenue targets if needed
- [ ] Run `setupTeamsQuickStart()`
- [ ] Run `testTeamMapping()` to verify
- [ ] Run `runCompleteTeamAnalysisPipeline()`
- [ ] Review **TEAM_SUMMARY_DASHBOARD**
- [ ] Check **TEAM_Unassigned** for missed conversations
- [ ] Refine patterns if needed
- [ ] Set up automated daily runs
- [ ] Share team-specific sheets with stakeholders

## 💡 Pro Tips

1. **Start with the test**: Always run `testTeamMapping()` before full analysis
2. **Check Unassigned first**: It will show you what patterns you're missing
3. **Be specific**: "auburn_tigers" is better than just "tiger"
4. **Use manual mappings**: For VIP or special conversations
5. **Set realistic targets**: Makes the "% of Target" metric useful
6. **Run by date range**: Great for comparing months or quarters
7. **Export team sheets**: Each team can get their own data

## 📞 Quick Commands Reference

```javascript
// Setup & Configuration
setupTeamsQuickStart()           // Configure all 10 teams
viewTeamConfiguration()          // View current setup
discoverDataPatterns()           // See your data

// Testing
testTeamMapping()                // Test on first 10 records

// Analysis
runCompleteTeamAnalysisPipeline()                    // Full pipeline
runCompleteTeamAnalysisPipeline('2025-10-01', '2025-10-31')  // Date range
runTeamSKUAnalysis()             // Analysis only (if data exists)

// Quick Checks
getTeamPerformanceSummary()      // Quick performance view
runSetupWizard()                 // Guided setup

// Data Pull
runSkuLevelActionOnly()          // Pull SKU data if needed
```

---

## 🏈 Ready to Start?

Run this complete sequence:

```javascript
// 1. See what data you have
discoverDataPatterns()

// 2. Set up your teams
setupTeamsQuickStart()

// 3. Test the mapping
testTeamMapping()

// 4. Run full analysis
runCompleteTeamAnalysisPipeline()

// 5. Check your spreadsheet for new team sheets!
```

**Questions?** Check the detailed guide: `TEAM-SKU-GUIDE.md`

