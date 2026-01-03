# Product Context

## Why this project exists
Generic affiliate reporting shows *total* revenue but lacks internal attribution. We need to know **who** drove the sale.
-   Did the "North Team" sell more merchandise than the "South Team"?
-   Which individual rep is hitting their target?

## Problems Solved
-   **Attribution Gaps**: Impact.com doesn't inherently know about our internal organizational structure. This project bridges that gap using SubIDs.
-   **Performance Visibility**: Transforms raw CSV dumps into a "Leaderboard" style dashboard.
-   **SKU Intelligence**: Moves beyond just "Revenue" to understanding "Product Mix" per team.

## How it works
1.  **Ingest**: Pulls "Action" and "SKU" reports from Impact.com API (`optimized-impact-script-v4.js`).
2.  **Map**: Applies Regex/String matching rules (`TeamMapper`) to SubID, Partner, or Campaign fields to find a "Team" match.
3.  **Analyze**: Aggregates totals by Team, SKU, and Team+SKU (`SKUTeamAnalyzer`).
4.  **Report**: Generates specific Google Sheets (Summary, Team Detail, Comparisons).

## User Experience
-   **User**: Sales Managers / Team Leads.
-   **Key Insight**: "Team Alpha is at 80% of their monthly target, driving mostly high-ticket electronics."
