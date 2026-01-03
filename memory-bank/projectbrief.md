# Project Brief: on3 Impact Reporting ("Team SKU Tracking")

## Overview
A sophisticated Business Intelligence (BI) and data discovery system for the Impact.com API. While capable of generic data discovery, its primary business function is **Team-Level SKU Tracking & Attribution**.

## Core Objective
To attribute revenue and specific SKU sales to individual teams or representatives, allowing for granular performance analysis, target tracking, and "team vs. team" gamification.

## Success Criteria
1.  **Attribution Accuracy**: Correctly maps Impact conversations to Teams based on SubID/Campaign patterns.
2.  **Dashboarding**: Generates "Team Summary" and "SKU Performance" dashboards in Google Sheets.
3.  **Stability**: Runs reliably with "Circuit Breaker" and retry logic for API resilience.
4.  **Security**: Zero hardcoded credentials; fully uses ScriptProperties/Environment variables.

## Key Features
-   **Team Mapper**: Configurable rules engine to assign revenue to teams (e.g., "team_north" -> "North Team").
-   **BI Dashboard**: Executive view of Revenue, Conversions, AOV, and performance vs. targets.
-   **SKU Analysis**: Breakdown of exactly *what* each team is selling.
