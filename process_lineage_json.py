import json
import yaml
from collections import defaultdict, Counter
import glob
import os
import networkx as nx
from networkx.drawing.nx_agraph import to_agraph


with open(r'local_files\target_object.yaml', 'r') as file:
    graph_viz_path = yaml.safe_load(file)

os.environ["PATH"] += os.pathsep + graph_viz_path["graphviz_path"]

from collections import Counter

def build_lineage_graph(folder_path: str) -> nx.DiGraph:
    """
    Reads all lineage JSON files in a folder and builds a directed graph.
    Handles VIEWs and SQL_STORED_PROCEDUREs differently.
    Normalizes node names to lowercase internally, but stores the most
    frequently seen casing as the display label.
    """
    G = nx.DiGraph()

    json_files = glob.glob(os.path.join(folder_path, "*.json"))
    print(f"Found {len(json_files)} lineage files.")

    # -------------------------------------------------------
    # PASS 1: Count casing occurrences for every node name
    # -------------------------------------------------------
    casing_votes = defaultdict(Counter)  # { lowercase_name: Counter({casing: count}) }

    for file_path in json_files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            obj_type = data.get('type', '')
            obj_name = data.get('name', '')

            # "name" field counts as a vote
            if obj_name:
                casing_votes[obj_name.lower()][obj_name] += 1

            for item in data.get('lineage', []):
                src = item.get('source')
                tgt = item.get('target')
                # Skip special keywords
                if src and src not in ('CHANGING',):
                    casing_votes[src.lower()][src] += 1
                if tgt and tgt not in ('REFERENCED',):
                    casing_votes[tgt.lower()][tgt] += 1

        except Exception as e:
            print(f"Error in pass 1 for {file_path}: {e}")

    # Resolve canonical display name (most frequent casing wins, random on tie)
    canonical = {
        lower: counter.most_common(1)[0][0]
        for lower, counter in casing_votes.items()
    }

    # -------------------------------------------------------
    # Helper: normalize a raw name to its lowercase key,
    # then return the canonical display name
    # -------------------------------------------------------
    def resolve(name: str) -> str:
        return canonical.get(name.lower(), name)

    # -------------------------------------------------------
    # PASS 2: Build the graph using canonical names
    # -------------------------------------------------------
    for file_path in json_files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            obj_type = data.get('type', '')
            obj_name = resolve(data.get('name', '')) if data.get('name') else ''
            relationships = data.get('lineage', [])

            if obj_type == 'VIEW':
                for item in relationships:
                    src = item.get('source')
                    tgt = item.get('target')
                    if src and tgt:
                        src_c, tgt_c = resolve(src), resolve(tgt)
                        G.add_edge(src_c, tgt_c)
                        if src_c not in G.nodes:
                            G.add_node(src_c, node_type='table')
                        if tgt_c not in G.nodes:
                            G.add_node(tgt_c, node_type='table')

            elif obj_type == 'SQL_STORED_PROCEDURE':
                if obj_name:
                    G.add_node(obj_name, node_type='stored_procedure')

                for item in relationships:
                    src = item.get('source')
                    tgt = item.get('target')

                    if src == 'CHANGING' and tgt:
                        tgt_c = resolve(tgt)
                        G.add_edge(obj_name, tgt_c)
                        if tgt_c not in G.nodes:
                            G.add_node(tgt_c, node_type='table')

                    elif tgt == 'REFERENCED' and src:
                        src_c = resolve(src)
                        G.add_edge(src_c, obj_name)
                        if src_c not in G.nodes:
                            G.add_node(src_c, node_type='table')

                    elif src and tgt:
                        src_c, tgt_c = resolve(src), resolve(tgt)
                        G.add_edge(src_c, obj_name)
                        G.add_edge(obj_name, tgt_c)
                        if src_c not in G.nodes:
                            G.add_node(src_c, node_type='table')
                        if tgt_c not in G.nodes:
                            G.add_node(tgt_c, node_type='table')
            
            elif obj_type == 'POWER_BI_TABLE':
                # Power BI tables: simple pass-through like VIEWs
                # Add the PBI table itself as a node
                if obj_name:
                    G.add_node(obj_name, node_type='powerbi_table')
                
                for item in relationships:
                    src = item.get('source')
                    tgt = item.get('target')
                    
                    # Skip if source is empty (calculated tables)
                    if src and tgt:
                        src_c, tgt_c = resolve(src), resolve(tgt)
                        G.add_edge(src_c, tgt_c)
                        if src_c not in G.nodes:
                            G.add_node(src_c, node_type='table')
                        if tgt_c not in G.nodes:
                            G.add_node(tgt_c, node_type='table')

        except Exception as e:
            print(f"Error in pass 2 for {file_path}: {e}")

    print(f"Graph built successfully with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")
    return G

# ==========================================
# LINEAGE EXTRACTION WITH DIRECTION
# ==========================================

def get_lineage_subgraph(G: nx.DiGraph, target_tables: list, direction: str = 'both') -> nx.DiGraph:
    """
    Extracts a subgraph based on target tables and direction.
    
    Args:
        G: Full lineage graph
        target_tables: List of table names to analyze
        direction: 'upstream', 'downstream', or 'both'
    
    Returns:
        Subgraph containing relevant nodes
    """
    nodes_to_include = set()
    stored_procedures_to_expand = set()
    
    for target_table in target_tables:
        if target_table not in G:
            print(f"Warning: Table '{target_table}' not found in the graph. Skipping.")
            continue
        
        # Always include the target table itself
        nodes_to_include.add(target_table)
        
        if direction in ['upstream', 'both']:
            # Get all upstream nodes (ancestors)
            upstream_nodes = nx.ancestors(G, target_table)
            nodes_to_include.update(upstream_nodes)
            
            # Identify stored procedures in upstream
            for node in upstream_nodes:
                if G.nodes[node].get('node_type') == 'stored_procedure':
                    stored_procedures_to_expand.add(node)
        
        if direction in ['downstream', 'both']:
            # Get all downstream nodes (descendants)
            downstream_nodes = nx.descendants(G, target_table)
            nodes_to_include.update(downstream_nodes)
            
            # Identify stored procedures in downstream
            for node in downstream_nodes:
                if G.nodes[node].get('node_type') == 'stored_procedure':
                    stored_procedures_to_expand.add(node)
    
    # Expand stored procedures: include ALL their connections
    for sp_node in stored_procedures_to_expand:
        # Get all predecessors (nodes pointing to this SP)
        predecessors = set(G.predecessors(sp_node))
        nodes_to_include.update(predecessors)
        
        # Get all successors (nodes this SP points to)
        successors = set(G.successors(sp_node))
        nodes_to_include.update(successors)
    
    # Create and return the subgraph
    subgraph = G.subgraph(nodes_to_include).copy()
    print(f"\nSubgraph created with {subgraph.number_of_nodes()} nodes and {subgraph.number_of_edges()} edges.")
    return subgraph

# ==========================================
# VISUALIZATION WITH GRAPHVIZ
# ==========================================

def visualize_lineage_graphviz(G: nx.DiGraph, target_tables: list, output_file: str = "lineage_output.png"):
    """
    Creates a beautiful static visualization using Graphviz.
    
    Args:
        G: The lineage subgraph to visualize
        target_tables: List of target tables (for highlighting)
        output_file: Output file path (supports .png, .svg, .pdf)
    """
    # Convert NetworkX graph to AGraph (Graphviz)
    A = to_agraph(G)
    
    # Set global graph attributes for a clean, professional look
    A.graph_attr.update({
        'rankdir': 'LR',  # Left to Right layout
        'bgcolor': 'white',
        'splines': 'ortho',  # Orthogonal edges (clean right angles) can be changed to 'polyline'
        'nodesep': '0.8',
        'ranksep': '1.5',
        'fontname': 'Arial',
        'fontsize': '12'
    })
    
    # Set default node attributes
    A.node_attr.update({
        'fontname': 'Arial',
        'fontsize': '11',
        'height': '0.6',
        'width': '1.2',
        'style': 'filled',
        'penwidth': '2'
    })
    
    # Set default edge attributes
    A.edge_attr.update({
        'color': '#666666',
        'penwidth': '1.5',
        'arrowsize': '0.8'
    })
    
    # Style individual nodes based on their type and whether they're targets
    for node in G.nodes():
        n = A.get_node(node)
        node_type = G.nodes[node].get('node_type', 'table')
        
        if node in target_tables:
            # Target tables: Red with bold border
            n.attr.update({
                'shape': 'box',
                'fillcolor': '#ffcccc',
                'color': '#cc0000',
                'penwidth': '3',
                'style': 'filled'
            })
        elif node_type == 'stored_procedure':
            # Stored procedures: Diamond shape with yellow/orange
            n.attr.update({
                'shape': 'diamond',
                'fillcolor': '#ffd966',
                'color': '#cc8800',
                'penwidth': '2',
                'style': 'filled'
            })
        elif node_type == 'powerbi_table':
            # Power BI tables: Rounded box with purple/violet
            n.attr.update({
                'shape': 'box',
                'fillcolor': '#e6d5ff',
                'color': '#9966ff',
                'penwidth': '2',
                'style': 'filled,rounded',
                'peripheries': '1'
            })
        else:
            # Regular tables: Box with light blue
            n.attr.update({
                'shape': 'box',
                'fillcolor': '#e6f2ff',
                'color': '#4d94ff',
                'penwidth': '2',
                'style': 'filled'
            })
    
    # Determine output format from file extension
    output_format = output_file.split('.')[-1] if '.' in output_file else 'png'
    
    # Generate the visualization
    A.draw(output_file, prog='dot', format=output_format)
    print(f"✓ Success! Beautiful visualization saved to: {output_file}")

# ==========================================
# EXECUTION
# ==========================================
if __name__ == "__main__":
    # ==========================================
    # CONFIGURATION - EDIT THESE VALUES
    # ==========================================
    
    # Folder containing your JSON lineage files
    FOLDER_WITH_JSONS = "lineage_outputs"
    
    # List of target tables to analyze (can be one or multiple)
    with open("local_files/target_object.yaml", "r") as f:
        yaml_read = yaml.safe_load(f)
    TARGET_TABLES = yaml_read["target_tables"]["target_tables_list"]
    
    # Direction: 'upstream', 'downstream', or 'both'
    DIRECTION = "downstream"
    
    # Output file (supports .png, .svg, .pdf)
    OUTPUT_FILE = "local_files/lineage_output.svg"
    
    # ==========================================
    # EXECUTION - DON'T EDIT BELOW
    # ==========================================
    
    print(f"\n{'='*60}")
    print(f"DATA LINEAGE VISUALIZATION")
    print(f"{'='*60}")
    print(f"Folder: {FOLDER_WITH_JSONS}")
    print(f"Target Tables: {', '.join(TARGET_TABLES)}")
    print(f"Direction: {DIRECTION}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"{'='*60}\n")
    
    # Build the full graph
    full_graph = build_lineage_graph(FOLDER_WITH_JSONS)
    
    # Extract relevant subgraph based on direction
    subgraph = get_lineage_subgraph(full_graph, TARGET_TABLES, DIRECTION)
    
    # Visualize
    visualize_lineage_graphviz(subgraph, TARGET_TABLES, OUTPUT_FILE)