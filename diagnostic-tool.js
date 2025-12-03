/**
 * Diagnostic Tool for Impact Script
 * Run this to debug why data isn't showing up
 */
function runDiagnostics() {
    console.log('=== IMPACT SCRIPT DIAGNOSTICS ===');

    const props = PropertiesService.getScriptProperties();

    // 1. Check Credentials & Config
    const sid = props.getProperty('IMPACT_SID');
    const token = props.getProperty('IMPACT_TOKEN');
    const sheetId = props.getProperty('IMPACT_SPREADSHEET_ID');

    console.log('\n1. CONFIGURATION');
    console.log('SID:', sid ? 'Set' : 'MISSING');
    console.log('Token:', token ? 'Set' : 'MISSING');
    console.log('Spreadsheet ID:', sheetId);
    console.log('Target Sheet URL:', 'https://docs.google.com/spreadsheets/d/' + sheetId);

    // 2. Check Freshness Data
    const freshnessJson = props.getProperty('IMPACT_DATA_FRESHNESS');
    let freshness = {};
    if (freshnessJson) {
        try {
            freshness = JSON.parse(freshnessJson);
            console.log('\n2. DATA FRESHNESS');
            console.log('Tracked Reports:', Object.keys(freshness).length);
            console.log('Sample (first 3):');
            Object.entries(freshness).slice(0, 3).forEach(([id, data]) => {
                console.log(`- ${id}: Last updated ${data.lastUpdated}`);
            });
        } catch (e) {
            console.log('Error parsing freshness data:', e.message);
        }
    } else {
        console.log('\n2. DATA FRESHNESS: No data found (Clean state)');
    }

    // 3. Check Completed Reports
    const completedJson = props.getProperty('IMPACT_COMPLETED_V4');
    let completed = [];
    if (completedJson) {
        try {
            completed = JSON.parse(completedJson);
            console.log('\n3. COMPLETED REPORTS');
            console.log('Count:', completed.length);
        } catch (e) {
            console.log('Error parsing completed data:', e.message);
        }
    } else {
        console.log('\n3. COMPLETED REPORTS: No data found');
    }

    // 4. Check Progress
    const progressJson = props.getProperty('IMPACT_PROGRESS_V4');
    if (progressJson) {
        console.log('\n4. ACTIVE PROGRESS');
        console.log('Found active progress data - script might think it is resuming');
        console.log('Progress:', progressJson.substring(0, 200) + '...');
    } else {
        console.log('\n4. ACTIVE PROGRESS: None');
    }

    // 5. Test Spreadsheet Access
    console.log('\n5. SPREADSHEET ACCESS TEST');
    try {
        const ss = SpreadsheetApp.openById(sheetId);
        console.log('✅ Successfully opened spreadsheet: ' + ss.getName());
        console.log('Sheets:', ss.getSheets().map(s => s.getName()).join(', '));
    } catch (e) {
        console.error('❌ FAILED to open spreadsheet:', e.message);
    }

    console.log('\n=== DIAGNOSTICS COMPLETE ===');
    console.log('Recommendation: If you see "Tracked Reports" or "Completed Reports" > 0,');
    console.log('try running: clearAllProgress()');
}

function clearAllProgress() {
    const props = PropertiesService.getScriptProperties();
    props.deleteProperty('IMPACT_DATA_FRESHNESS');
    props.deleteProperty('IMPACT_COMPLETED_V4');
    props.deleteProperty('IMPACT_PROGRESS_V4');
    props.deleteProperty('IMPACT_CHECKPOINT');
    console.log('✅ Cleared all progress and freshness data. Run completediscovery again.');
}
