import os
import json
import glob
import yaml
from collections import defaultdict

# ==========================================
# CONFIGURATION
# ==========================================
FOLDER_PATH = "lineage_outputs"
OUTPUT_HTML = "local_files/lineage_visualization.html"

# Load target object from YAML
with open("local_files/target_object.yaml") as f:
    target_object_yaml = yaml.safe_load(f)

TARGET_OBJECT = target_object_yaml['target_tables']['schema'] + '.' + target_object_yaml['target_tables']['object_name']

# Choose Flow Direction:
# "UPSTREAM"   = Reverse Lineage (What is this made from?)
# "DOWNSTREAM" = Impact Analysis (What uses this?)
DIRECTION = "DOWNSTREAM"

# ==========================================
# 1. BUILD THE GRAPH
# ==========================================
# Using defaultdict for cleaner code
forward_graph = defaultdict(list)  # Source -> [Targets]
reverse_graph = defaultdict(list)  # Target -> [Sources]

json_files = glob.glob(os.path.join(FOLDER_PATH, "*.json"))
print(f"Loading {len(json_files)} lineage files...")

for file_path in json_files:
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
            # Extract the lineage list from the "lineage" key
            relationships = data.get('lineage', [])
            
            for item in relationships:
                src = item['source']
                tgt = item['target']
                
                # Add to forward graph (avoid duplicates)
                if tgt not in forward_graph[src]:
                    forward_graph[src].append(tgt)
                
                # Add to reverse graph (avoid duplicates)
                if src not in reverse_graph[tgt]:
                    reverse_graph[tgt].append(src)
                
    except Exception as e:
        print(f"Skipping file {file_path}: {e}")

print(f"Graph built: {len(forward_graph)} sources, {len(reverse_graph)} targets")

# ==========================================
# 2. RECURSIVE LINEAGE TRACER
# ==========================================
def get_lineage_edges(node, direction, visited=None, edges=None):
    """Recursively trace lineage in the specified direction."""
    if visited is None:
        visited = set()
    if edges is None:
        edges = []
    
    if node in visited:
        return edges
    
    visited.add(node)
    
    # Select appropriate graph based on direction
    graph = forward_graph if direction == "DOWNSTREAM" else reverse_graph
    relatives = graph.get(node, [])
    
    for relative in relatives:
        # Always store edges as (source, target) for consistent arrow direction
        if direction == "DOWNSTREAM":
            edge = (node, relative)
        else:  # UPSTREAM
            edge = (relative, node)
        
        edges.append(edge)
        get_lineage_edges(relative, direction, visited, edges)
    
    return edges

# ==========================================
# 3. GENERATE VISUALIZATION
# ==========================================
def sanitize_node_id(name):
    """Create a safe node ID for Mermaid."""
    return name.replace(".", "_").replace("-", "_")

# Check if target object exists in the graph
graph_to_check = forward_graph if DIRECTION == "DOWNSTREAM" else reverse_graph
all_nodes = set(forward_graph.keys()) | set(reverse_graph.keys())

if TARGET_OBJECT not in all_nodes:
    print(f"Error: Object '{TARGET_OBJECT}' not found in the lineage data.")
    exit(1)

# Get lineage edges
edges = get_lineage_edges(TARGET_OBJECT, DIRECTION)

if not edges:
    print(f"Object found, but has no {DIRECTION.lower()} dependencies.")
    exit(0)

# Generate Mermaid diagram
mermaid_lines = ["graph LR"]

# Style definition for root node
safe_root = sanitize_node_id(TARGET_OBJECT)
mermaid_lines.append(f"    classDef rootNode fill:#ff6b6b,color:#fff,stroke:#c92a2a,stroke-width:3px;")
mermaid_lines.append(f"    {safe_root}[\"{TARGET_OBJECT}\"]:::rootNode")

# Add all edges
for src, tgt in edges:
    s_safe = sanitize_node_id(src)
    t_safe = sanitize_node_id(tgt)
    mermaid_lines.append(f'    {s_safe}["{src}"] --> {t_safe}["{tgt}"]')

mermaid_content = "\n".join(mermaid_lines)

# HTML Template with improved styling
html_template = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{DIRECTION} Lineage: {TARGET_OBJECT}</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <script>mermaid.initialize({{startOnLoad:true}});</script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            padding: 30px;
            background: #f5f5f5;
        }}
        .container {{
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        h2 {{
            color: #333;
            margin-bottom: 20px;
        }}
        .badge {{
            background-color: #228be6;
            color: white;
            padding: 6px 12px;
            border-radius: 4px;
            font-size: 0.85em;
            font-weight: 600;
        }}
        .info {{
            margin-top: 20px;
            padding: 15px;
            background: #e7f5ff;
            border-left: 4px solid #228be6;
            border-radius: 4px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h2>{TARGET_OBJECT} <span class="badge">{DIRECTION}</span></h2>
        <div class="mermaid">
{mermaid_content}
        </div>
        <div class="info">
            <strong>Stats:</strong> {len(edges)} dependencies found
        </div>
    </div>
</body>
</html>
"""

# Write output
with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html_template)

print(f"\n✓ Success! {DIRECTION} graph generated: {OUTPUT_HTML}")
print(f"  Found {len(edges)} dependencies for '{TARGET_OBJECT}'")