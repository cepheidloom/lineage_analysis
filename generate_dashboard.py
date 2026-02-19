import json
import os

# ==========================================
# CONFIGURATION - EDIT THESE VALUES
# ==========================================

INPUT_JSON_FILE  = "local_files/lineage_statistics.json"
OUTPUT_DASHBOARD = "local_files/lineage_dashboard.html"

# ==========================================
# DASHBOARD GENERATION
# ==========================================

def generate_dashboard(input_json: str, output_file: str):
    """
    Reads lineage_statistics.json and injects it into the HTML dashboard template,
    producing a fully self-contained HTML file openable in any browser.
    """
    # Read the statistics JSON
    with open(input_json, 'r', encoding='utf-8') as f:
        report = json.load(f)

    # Read the HTML template (expected in same folder as this script)
    template_path = os.path.join(os.path.dirname(__file__), "dashboard_template.html")
    with open(template_path, 'r', encoding='utf-8') as f:
        html = f.read()

    # Inject the report JSON directly into the HTML
    report_json = json.dumps(report, indent=2)
    html = html.replace('__STATS_DATA__', report_json)

    # Write the final dashboard
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"✓ Dashboard saved to: {output_file}")


# ==========================================
# EXECUTION
# ==========================================

if __name__ == "__main__":
    generate_dashboard(INPUT_JSON_FILE, OUTPUT_DASHBOARD)