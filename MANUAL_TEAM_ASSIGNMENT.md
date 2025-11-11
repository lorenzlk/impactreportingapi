# Manual Team Assignment Feature

## Overview

The manual team assignment feature allows you to assign teams to specific items (rows) in your Impact.com reports and have those assignments **persist through data refresh cycles**. Manual assignments have the **highest priority** in the team assignment logic.

## Problem Solved

Previously, when you manually edited the "Team" column in the spreadsheet, those edits would be lost when the data refreshed because the system would completely recreate the sheet. Now, you can make manual assignments that will be preserved even when data is refreshed.

## How It Works

### Priority Order (Highest to Lowest)

1. **Manual row assignments** ⭐ NEW - Set via the manual assignment functions
2. Manual mappings - Exact ID matches in configuration
3. SubID patterns - Substring matching in SubID field
4. Partner patterns - Substring matching in Partner field
5. Campaign patterns - Substring matching in Campaign field
6. Default team - "Unassigned"

### Persistence Mechanism

Manual assignments are stored in Google Apps Script's PropertiesService using unique row identifiers:
- Primary: ActionTrackerId
- Fallback: ActionId
- Fallback: OrderId
- Fallback: Composite key (Partner + SubID + Date + Amount)

When data refreshes, the `TeamMapper.mapToTeam()` function checks for manual assignments **FIRST** before applying any automatic rules.

## Usage

### Method 1: Interactive UI (Recommended)

1. Open your Google Sheet with the Impact.com data
2. Click **"Team Management"** menu (appears at the top)
3. Select **"Manually Assign Teams"**
4. Follow the prompts:
   - View count of unassigned items
   - See list of available teams
   - Enter the team name to assign to all unassigned items
5. Done! Assignments are saved and will persist through refreshes

### Method 2: Programmatic Assignment (Specific Rows)

```javascript
// Assign specific row numbers
assignTeamToRows([2, 5, 10], 'Team Alpha', 'Fixing unassigned orders');

// Assign specific ActionTrackerIds
assignTeamToRows(['12345', '12346'], 'Team Beta', 'Manual correction');
```

### Method 3: Bulk Assignment (All Unassigned)

```javascript
// Assign all currently unassigned items to a team
assignTeamToAllUnassigned('Team Gamma', 'Initial cleanup');
```

### Method 4: View and Manage Assignments

```javascript
// View all manual assignments
viewManualTeamAssignments();

// Clear all manual assignments (with confirmation dialog)
clearAllManualTeamAssignments();
```

Or use the **"Team Management"** menu in Google Sheets.

## Workflow Example

### One-Time Cleanup of Unassigned Items

1. **Run the pipeline** to get fresh data:
   ```javascript
   runCompleteTeamAnalysisPipeline();
   ```

2. **Identify unassigned items**:
   - Look at the "SkuLevelAction" sheet
   - Filter for "Unassigned" in the Team column
   - Or check the "TEAM_Unassigned" sheet

3. **Manually assign teams**:
   - Use the "Team Management" menu > "Manually Assign Teams"
   - Enter the team name for all unassigned items
   - Or assign specific rows programmatically

4. **Future refreshes**:
   - When you refresh data (via the menu or `forceRefreshSkuDataWithTeams()`)
   - Manual assignments are preserved automatically
   - New items follow the dynamic assignment rules

## Important Notes

- ✅ **Manual assignments persist through data refreshes**
- ✅ **Highest priority in assignment logic**
- ✅ **Can be overridden by reassigning**
- ✅ **Stored persistently in PropertiesService**
- ⚠️ **Clearing manual assignments reverts to automatic rules**
- ⚠️ **Manual assignments are per-row, not per-pattern**

## Technical Details

### Files Modified

- `team-sku-analysis.js` - Added manual assignment functionality

### Key Functions Added

1. **TeamConfig methods**:
   - `setManualRowAssignment(uniqueKey, teamName, reason)`
   - `getManualRowAssignment(uniqueKey)`
   - `removeManualRowAssignment(uniqueKey)`
   - `getAllManualRowAssignments()`
   - `clearAllManualRowAssignments()`

2. **TeamMapper methods**:
   - `generateRowKey(row)` - Creates unique identifier for each row
   - Modified `mapToTeam(row)` - Checks manual assignments first

3. **Public API functions**:
   - `manuallyAssignTeamsUI()` - Interactive UI
   - `assignTeamToRows(rowIdentifiers, teamName, reason)`
   - `assignTeamToAllUnassigned(teamName, reason)`
   - `viewManualTeamAssignments()`
   - `clearAllManualTeamAssignments()`

4. **Menu**:
   - `onOpen()` - Creates "Team Management" menu in Google Sheets

### Storage Format

Stored in PropertiesService under `TEAM_SKU_CONFIG.manualRowAssignments`:

```json
{
  "ActionTrackerId_12345": {
    "team": "Team Alpha",
    "assignedAt": "2025-01-15T10:30:00.000Z",
    "reason": "Manual assignment"
  },
  "ActionTrackerId_12346": {
    "team": "Team Beta",
    "assignedAt": "2025-01-15T10:31:00.000Z",
    "reason": "Bulk manual assignment"
  }
}
```

## FAQ

**Q: Will my manual assignments be lost when I refresh data?**
A: No! Manual assignments are stored separately and have the highest priority. They persist through all data refreshes.

**Q: Can I change a manual assignment?**
A: Yes! Just reassign the same row to a different team. The new assignment will overwrite the old one.

**Q: What happens if I delete a row from the source data?**
A: The manual assignment will remain in storage but won't affect anything since the row no longer exists.

**Q: Can I revert a manual assignment to automatic rules?**
A: Yes, use `clearAllManualTeamAssignments()` to clear all manual assignments, or delete specific assignments by calling `teamConfig.removeManualRowAssignment(rowKey)`.

**Q: How do I know which items have manual assignments?**
A: Call `viewManualTeamAssignments()` to see all manual assignments with their details.

## Support

For issues or questions, please check:
1. The inline documentation in `team-sku-analysis.js`
2. The console logs when running functions
3. The "Team Management" menu in your Google Sheet
