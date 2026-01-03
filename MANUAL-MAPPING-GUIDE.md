# Manual Team Mapping Guide

## Overview

The team tracking system uses a **priority-based approach**:

1. **Manual Mappings** (Highest Priority) - For historical data without PubSubid3
2. **PubSubid3 Automatic** - For new data going forward ✅ **This is the default**
3. **Unassigned** - Fallback for records without team info

## Data Structure Definitions

For documentation purposes, the system relies on the following structure:
- **PubSubid1**: Must be `mula` (identifies the partner).
- **PubSubid3**: Contains the **Team Name** (e.g., `ole-miss-rebels`, `michigan-wolverines`).

## Why Manual Mappings?

Manual mappings are **only needed for historical data** that was created before PubSubid3 was implemented. All new data going forward will automatically have the correct team from PubSubid3.

## Workflow: Fix Historical Data

### Step 1: Find Unassigned Records

```javascript
getUnassignedRecords()
```

This shows:
- Total unassigned records
- Top 20 by revenue with ActionId, SKU, Category, and Sale Amount
- Returns array of all unassigned records

**Example Output:**
```
📊 UNASSIGNED RECORDS SUMMARY:
Total unassigned: 45

Top 20 by revenue:
ActionId    SKU         Category        Sale Amount
────────────────────────────────────────────────────
123456      SKU-123     Jerseys         $125.00
123457      SKU-456     Hats            $45.00
...
```

### Step 2: Assign Teams

#### Option A: Single Record
```javascript
assignTeamManually('123456', 'Auburn Tigers')
```

#### Option B: Multiple Records
```javascript
assignTeamManually(['123456', '123457', '123458'], 'Florida Gators')
```

#### Option C: Bulk Assignment (alias)
```javascript
bulkAssignTeam([
  '123456',
  '123457',
  '123458'
], 'LSU Tigers')
```

### Step 3: Apply Changes

**Important:** Manual mappings are saved but won't appear in the sheet until you refresh:

```javascript
forceRefreshSkuDataWithTeams()
```

This will:
- Pull fresh data from Impact.com
- Apply manual mappings (priority 1)
- Apply PubSubid3 auto-detection (priority 2)
- Update the Team column in the sheet

### Step 4: Verify

Check the console output to see:
```
✅ Added Team column at position 13
   📌 Manual mappings: 45      ← Your manual fixes
   🔄 PubSubid3 auto: 91      ← Automatic from new data
   ❓ Unassigned: 0           ← Should be 0 if all fixed!
```

## Managing Manual Mappings

### View Current Mappings
```javascript
viewManualMappings()
```

Shows all manual assignments grouped by team.

### Remove a Single Mapping
```javascript
removeManualMapping('123456')
```

Removes manual override. Record will use PubSubid3 or become Unassigned.

### Clear All Mappings
```javascript
clearAllManualMappings()  // Shows warning
clearAllManualMappingsConfirmed()  // Actually clears
```

**Warning:** Only do this if you want to start over!

## Available Team Names

Use these exact names when assigning:
- `Auburn Tigers`
- `Florida Gators`
- `LSU Tigers`
- `USC Trojans`
- `Michigan Wolverines`
- `Ohio State Buckeyes`
- `Penn State Nittany Lions`
- `Notre Dame Fighting Irish`
- `NC State Wolfpack`
- `Ole Miss Rebels`

## How It Works

### Priority System

When enriching data with teams:

```javascript
// 1. Check manual mapping FIRST
if (manualMapping exists for ActionId) {
  return manualMapping.team;  // ← Overrides everything
}

// 2. Check PubSubid3 (for new data)
if (PubSubid3 has value) {
  return convertToTeamName(PubSubid3);  // ← Auto detection
}

// 3. Fallback
return "Unassigned";
```

### Why This Works

- **Historical data** (no PubSubid3): You manually map once, saved forever
- **New data** (has PubSubid3): Automatically assigned, no manual work needed
- **Future data**: PubSubid3 handles everything automatically

## Storage

Manual mappings are stored in Google Apps Script PropertiesService:
- **Key:** `MANUAL_TEAM_MAPPINGS`
- **Format:** JSON object `{ "ActionId": "TeamName" }`
- **Persistence:** Survives script updates and reruns
- **Size limit:** ~500KB total (thousands of mappings)

## Example Workflow

```javascript
// Day 1: Fix historical data
var unassigned = getUnassignedRecords();  // See what needs fixing

// Manually research and assign teams
assignTeamManually(['123', '456', '789'], 'Auburn Tigers');
assignTeamManually(['111', '222'], 'Florida Gators');

// Apply changes
forceRefreshSkuDataWithTeams();

// Day 2+: New data comes in automatically
runCompleteTeamAnalysisPipeline();  // New data auto-assigned via PubSubid3
```

## Troubleshooting

### Manual mapping not showing in sheet?
**Solution:** Run `forceRefreshSkuDataWithTeams()` to apply changes.

### Wrong team assigned?
**Solution:** 
```javascript
assignTeamManually('123456', 'Correct Team Name')
forceRefreshSkuDataWithTeams()
```

### Want to go back to automatic?
**Solution:**
```javascript
removeManualMapping('123456')  // Remove manual override
forceRefreshSkuDataWithTeams()  // Let PubSubid3 handle it
```

### How many records have I manually mapped?
**Solution:**
```javascript
viewManualMappings()  // Shows count and breakdown by team
```

## Best Practices

1. **Only map historical data** - New data handles itself
2. **Map in bulk** - More efficient than one-by-one
3. **Verify after mapping** - Run `forceRefreshSkuDataWithTeams()` and check output
4. **Document why** - Keep notes on which ActionIds you mapped and why
5. **Regular cleanup** - Once PubSubid3 is fully deployed, historical mappings become less important

## Questions?

See the main documentation:
- `TEAM-SKU-GUIDE.md` - Complete system guide
- `TEAM-TRACKING-QUICKSTART.md` - Quick reference
- `ON3-TEAMS-SETUP.md` - ON3-specific setup

