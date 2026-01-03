# System Patterns

## Architecture
**Modular ETL & Analytics** platform using Google Apps Script.

`Impact API -> Extraction Loop (Optimized V4) -> Logic Layer (Analysis/Mapping) -> Visualization (Sheets)`

## Key Design Decisions
1.  **Configuration Object**: To bypass Apps Script's 50-property limit, all config is stored as a single JSON blob in `IMPACT_OPTIMIZED_CONFIG`.
2.  **Enterprise Resilience**:
    -   *Circuit Breaker*: Stops requests if API fails repeatedly.
    -   *Smart Resume*: Can pick up where it left off if execution time limit is hit.
3.  **Pattern-Based Attribution**: Team mapping is not hardcoded but rules-based (Regex/Substring), allowing flexibility as teams change.
4.  **Security First**: Explicit design pattern to fetch credentials via `PropertiesService` or `process.env`, never string literals.

## Code Structure
-   `optimized-impact-script-v4.js`: The "Engine" (Extract). Handles API, pagination, resilience.
-   `team-sku-analysis.js`: The "Brain" (Transform). Handles attribution logic and metrics.
-   `setup-team-config.js`: The "Settings". User-defined rules.
-   `business-intelligence-dashboard.js`: The "Face". Visual reporting layer.

## API Constraints & Patterns
-   **Date Handling**: The Impact API rejects future dates beyond a certain threshold. It also requires `YYYY-MM-DDTHH:mm:ssZ` format (no milliseconds). The system now dynamically generates this at runtime.
-   **Monolithic Deployment**: `optimized-impact-script-v4.js` is the single source of truth for deployment, bundling all necessary classes and logic to simplify Apps Script management.
