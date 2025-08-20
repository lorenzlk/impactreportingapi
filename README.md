# Impact.com Data Discovery Tool

A Google Apps Script tool for discovering and exporting all available data from the Impact.com affiliate tracking platform API.

## Overview

This tool performs a comprehensive discovery of your Impact.com account to understand what data signals are available. It automatically identifies all accessible reports, exports them, and organizes the data in a Google Spreadsheet for analysis.

## What This Tool Does

1. **Discovers all available reports** in your Impact.com account
2. **Identifies which reports are API accessible** vs restricted
3. **Exports all accessible reports** with your data
4. **Creates organized spreadsheet** with each report in its own tab
5. **Provides summary overview** of all discovered data sources

## Prerequisites

### Impact.com API Credentials
You need:
- **SID** (Site ID) from your Impact.com account
- **TOKEN** (API Token) from your Impact.com account

### Google Apps Script Setup
1. Create a new Google Apps Script project
2. Create a Google Spreadsheet to receive the data
3. Set up script properties with your credentials

## Installation & Setup

### 1. Set Up Script Properties
In Google Apps Script, go to Project Settings > Script Properties and add:

```
IMPACT_SID = your_impact_sid_here
IMPACT_TOKEN = your_impact_token_here
```

### 2. Update Target Spreadsheet
In the code, update this line with your Google Spreadsheet ID:
```javascript
const ss = SpreadsheetApp.openById('YOUR_SPREADSHEET_ID_HERE');
```

### 3. Paste the Code
Copy the complete script into your Google Apps Script project.

## Usage

### Quick Start - Complete Discovery
Run this single function to discover everything:

```javascript
runCompleteDiscovery()
```

This will:
- Find all available reports
- Export all accessible ones
- Create a comprehensive discovery spreadsheet

### Step-by-Step Discovery
If you prefer to run it in phases:

1. **Discover available reports:**
   ```javascript
   discoverAllReports()
   ```

2. **Schedule exports:**
   ```javascript
   exportAllReportsRaw()
   ```

3. **Fetch and organize data:**
   ```javascript
   fetchAllDiscoveryData()
   ```

## What You'll Get

### Discovery Summary Sheet
- Overview of all reports found
- Success/failure status for each report
- Row counts and column information
- Key headers preview

### Individual Report Sheets
- One sheet per accessible report type
- Complete data export for each report
- Formatted headers and metadata
- Report details in cell notes

### Sample Output Structure
```
📊 Your Discovery Spreadsheet:
├── DISCOVERY SUMMARY (overview of all findings)
├── partner_performance_by_subid (your main performance data)
├── campaign_performance (campaign-level metrics)
├── click_performance (click tracking data)
└── [additional report types based on your account access]
```

## Understanding Your Data

### Common Report Types You Might Find:
- **partner_performance_by_subid** - Main affiliate performance metrics
- **campaign_performance** - Campaign-level data
- **creative_performance** - Ad creative performance
- **click_performance** - Click tracking details
- **conversion_performance** - Conversion tracking

### Key Metrics to Look For:
- **Clicks** - Traffic volume
- **Actions/Conversions** - Successful conversions
- **Sale_amount** - Revenue generated
- **Earnings** - Your commission earnings
- **CPC_Cost/Click_Cost** - Cost per click
- **EPC** - Earnings per click
- **AOV** - Average order value

## Important Notes

### API Limitations
- Some reports may be restricted based on your account level
- Date filters may not work on all report types (many return cumulative data)
- API rate limits apply - the script includes appropriate delays

### Data Freshness
- Data reflects your Impact.com account status at time of export
- Some metrics may have reporting delays
- Historical data availability depends on your account settings

### Troubleshooting
- **403 Errors**: Report not accessible with your account permissions
- **Timeouts**: Large datasets may require multiple attempts
- **Empty Data**: Some reports may have no data for your account

## Next Steps After Discovery

### 1. Analyze Your Data Universe
Review the Discovery Summary to understand:
- Which reports have the most valuable data
- What metrics are available for tracking
- Which signals could inform business decisions

### 2. Identify Key Performance Indicators
Look for metrics that matter to your business:
- Revenue and commission tracking
- Conversion rates and trends
- Partner/campaign performance
- Cost efficiency metrics

### 3. Set Up Targeted Tracking
After discovery, consider setting up:
- Daily snapshots of key reports
- Automated monitoring of important metrics
- Trend analysis and alerting

## Code Structure

### Main Functions:
- `runCompleteDiscovery()` - One-click complete discovery
- `discoverAllReports()` - Find available reports
- `exportAllReportsRaw()` - Schedule all exports
- `fetchAllDiscoveryData()` - Collect and organize data

### Configuration:
- Spreadsheet ID (update in code)
- API credentials (in Script Properties)
- Polling timeouts and retry logic

## Security Notes

- API credentials are stored in Script Properties (encrypted)
- No sensitive data is logged to console
- Spreadsheet access controlled by your Google account permissions

## Support

This tool provides a foundation for understanding your Impact.com data landscape. After running the discovery, you'll have a complete picture of available data sources and can make informed decisions about ongoing tracking and analysis needs.

For Impact.com API documentation and account-specific questions, consult your Impact.com account manager or their developer documentation.
