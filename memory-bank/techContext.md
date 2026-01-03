# Tech Context

## Tech Stack
-   **Runtime**: Google Apps Script (ES6+ features used).
-   **API**: Impact.com REST API.
-   **Storage**: Google Sheets / ScriptProperties.

## Configuration
-   **Global Config**: `IMPACT_OPTIMIZED_CONFIG` (Script Property).
-   **Credentials**: `IMPACT_SID`, `IMPACT_TOKEN` (Script Properties).
-   **Output**: Google Sheet (ID configured during setup).

## Constraints
-   **Apps Script Execution Time**: 6 minutes per execution. (Mitigated by "Smart Resume" feature).
-   **API Rate Limits**: Impact.com limits must be respected (handled by "Smart Polling" and backoff).
-   **API Date Formats**: Strict requirement for `YYYY-MM-DDTHH:mm:ssZ`. Milliseconds cause 400 Bad Request. Future dates may cause silent empty returns.
-   **Memory**: Large SKU reports can hit GAS memory limits (Mitigated by batch processing).
