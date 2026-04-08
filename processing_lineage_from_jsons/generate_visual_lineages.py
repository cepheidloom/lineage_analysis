import json
import yaml
import os
from pathlib import Path
import networkx as nx
from networkx.drawing.nx_agraph import to_agraph
import glob

# Import from existing script
import sys
sys.path.append('.')
from process_lineage_json import build_lineage_graph

# ==========================================
# CONFIGURATION
# ==========================================

with open(r'local_files\target_object.yaml', 'r') as file:
    graph_viz_path = yaml.safe_load(file)

os.environ["PATH"] += os.pathsep + graph_viz_path["graphviz_path"]

SQL_LINEAGE_FOLDERS = "lineage_outputs"  # Your existing SQL JSONs
POWER_BI_INVENTORY = Path("lineage_outputs/power_bi_inventory")
OUTPUT_BASE = Path("local_files/visuals_lineage")

# ==========================================
# GRAPH BUILDING
# ==========================================

def merge_visual_into_sql_graph(sql_graph: nx.DiGraph, visual_json_path: Path) -> tuple:
    """
    Merges a visual's lineage into the pre-loaded SQL graph.
    Returns (merged_graph, visual_name)
    """
    with open(visual_json_path, 'r', encoding='utf-8') as f:
        visual_data = json.load(f)
    
    # Create a copy of the SQL graph to avoid mutation
    G = sql_graph.copy()
    
    visual_name = visual_data.get("name", "Unknown Visual")
    
    # Add the visual as a node
    G.add_node(visual_name, node_type='powerbi_visual')
    
    # 1. Add SQL to Power BI lineage edges
    sql_to_pbi = visual_data.get("sql_to_powerbi_lineage", [])
    for edge in sql_to_pbi:
        src = edge.get("source")
        tgt = edge.get("target")
        if src and tgt:
            G.add_edge(src, tgt)
            if src not in G.nodes:
                G.add_node(src, node_type='sql_table')
            if tgt not in G.nodes:
                G.add_node(tgt, node_type='powerbi_column')
    
    # 2. Add Power BI column-to-column lineage edges
    column_lineage = visual_data.get("lineage", [])
    for edge in column_lineage:
        src = edge.get("source")
        tgt = edge.get("target")
        if src and tgt:
            G.add_edge(src, tgt)
            if src not in G.nodes:
                G.add_node(src, node_type='powerbi_column')
            if tgt not in G.nodes:
                G.add_node(tgt, node_type='powerbi_column')
    
    # 3. Add visual to column lineage edges
    visual_to_column = visual_data.get("visual_to_column_lineage", [])
    for edge in visual_to_column:
        src = edge.get("source")
        tgt = edge.get("target")
        if src and tgt:
            G.add_edge(src, tgt)
            if src not in G.nodes:
                G.add_node(src, node_type='powerbi_column')
    
    return G, visual_name


def extract_visual_subgraph(full_graph: nx.DiGraph, visual_name: str) -> nx.DiGraph:
    """
    Extracts only the nodes relevant to this visual (upstream lineage).
    """
    if visual_name not in full_graph:
        return nx.DiGraph()
    
    # Get all upstream nodes (ancestors) of the visual
    upstream_nodes = nx.ancestors(full_graph, visual_name)
    
    # Include the visual itself
    nodes_to_include = upstream_nodes | {visual_name}
    
    # Create subgraph
    subgraph = full_graph.subgraph(nodes_to_include).copy()
    
    return subgraph


# ==========================================
# VISUALIZATION
# ==========================================

def visualize_visual_lineage(G: nx.DiGraph, visual_name: str, output_file: Path):
    """
    Creates a Graphviz visualization for a visual's lineage.
    """
    if G.number_of_nodes() == 0:
        print(f"  ⚠ No lineage data for visual")
        return
    
    # Convert to AGraph
    A = to_agraph(G)
    
    # Set graph attributes
    A.graph_attr.update({
        'rankdir': 'LR',
        'bgcolor': 'white',
        'splines': 'ortho',
        'nodesep': '0.8',
        'ranksep': '1.5',
        'fontname': 'Arial',
        'fontsize': '12'
    })
    
    A.node_attr.update({
        'fontname': 'Arial',
        'fontsize': '11',
        'height': '0.6',
        'width': '1.2',
        'style': 'filled',
        'penwidth': '2'
    })
    
    A.edge_attr.update({
        'color': '#666666',
        'penwidth': '1.5',
        'arrowsize': '0.8'
    })
    
    # Style nodes based on type
    for node in G.nodes():
        n = A.get_node(node)
        node_type = G.nodes[node].get('node_type', 'unknown')
        
        if node_type == 'powerbi_visual':
            # Visual: Bright red
            n.attr.update({
                'shape': 'box',
                'fillcolor': '#ff6b6b',
                'color': '#cc0000',
                'penwidth': '3',
                'style': 'filled',
                'fontcolor': 'white'
            })
        elif node_type == 'powerbi_column':
            # Power BI columns: Purple
            n.attr.update({
                'shape': 'box',
                'fillcolor': '#9d4edd',
                'color': '#7209b7',
                'penwidth': '2',
                'style': 'filled',
                'fontcolor': 'white'
            })
        elif node_type == 'powerbi_table':
            # Power BI tables: Light purple
            n.attr.update({
                'shape': 'box',
                'fillcolor': '#c77dff',
                'color': '#9d4edd',
                'penwidth': '2',
                'style': 'filled,rounded'
            })
        elif node_type == 'stored_procedure':
            # Stored procedures: Orange diamond
            n.attr.update({
                'shape': 'diamond',
                'fillcolor': '#ff9500',
                'color': '#cc7700',
                'penwidth': '2',
                'style': 'filled'
            })
        elif node_type == 'sql_table' or node_type == 'table':
            # SQL tables: Blue
            n.attr.update({
                'shape': 'box',
                'fillcolor': '#4dabf7',
                'color': '#1971c2',
                'penwidth': '2',
                'style': 'filled'
            })
        else:
            # Unknown: Gray
            n.attr.update({
                'shape': 'box',
                'fillcolor': '#e9ecef',
                'color': '#868e96',
                'penwidth': '1',
                'style': 'filled'
            })
    
    # Generate SVG
    output_file.parent.mkdir(parents=True, exist_ok=True)
    A.draw(str(output_file), prog='dot', format='svg')


# ==========================================
# MAIN PROCESSING
# ==========================================

def process_all_visuals():
    """
    Processes all Power BI visual JSONs and generates lineage SVGs.
    Pre-loads SQL graph once for efficiency.
    """
    print(f"\n{'='*70}")
    print(f"GENERATING VISUAL LINEAGE DIAGRAMS WITH FULL SQL INTEGRATION")
    print(f"{'='*70}\n")
    
    # Step 1: Load SQL lineage graph ONCE
    print("�� Loading SQL lineage graph...")
    sql_graph = build_lineage_graph(SQL_LINEAGE_FOLDERS)
    print(f"   Loaded SQL graph: {sql_graph.number_of_nodes()} nodes, {sql_graph.number_of_edges()} edges\n")
    
    if not POWER_BI_INVENTORY.exists():
        print(f"❌ Power BI inventory folder not found: {POWER_BI_INVENTORY}")
        return
    
    total_visuals = 0
    total_reports = 0
    
    # Step 2: Process each report
    for report_folder in POWER_BI_INVENTORY.iterdir():
        if not report_folder.is_dir():
            continue
        
        report_name = report_folder.name.replace("_visuals_with_full_lineage", "")
        total_reports += 1
        
        print(f"�� Processing Report: {report_name}")
        
        output_folder = OUTPUT_BASE / report_name
        output_folder.mkdir(parents=True, exist_ok=True)
        
        visual_files = list(report_folder.glob("*.json"))
        
        for visual_file in visual_files:
            try:
                # Merge visual into SQL graph
                full_graph, visual_name = merge_visual_into_sql_graph(sql_graph, visual_file)
                
                # Extract only relevant subgraph for this visual
                visual_subgraph = extract_visual_subgraph(full_graph, visual_name)
                
                # Generate safe filename
                safe_filename = visual_name.replace(" || ", "_").replace(" ", "_").replace("/", "_")
                safe_filename = safe_filename[:200]
                output_file = output_folder / f"{safe_filename}.svg"
                
                # Visualize
                visualize_visual_lineage(visual_subgraph, visual_name, output_file)
                
                total_visuals += 1
                print(f"  ✓ {visual_file.name} ({visual_subgraph.number_of_nodes()} nodes)")
                
            except Exception as e:
                print(f"  ❌ Error processing {visual_file.name}: {e}")
        
        print()
    
    print(f"{'='*70}")
    print(f"✓ Complete!")
    print(f"  Reports processed: {total_reports}")
    print(f"  Visuals generated: {total_visuals}")
    print(f"  Output location: {OUTPUT_BASE.absolute()}")
    print(f"{'='*70}\n")


# ==========================================
# EXECUTION
# ==========================================

if __name__ == "__main__":
    process_all_visuals()