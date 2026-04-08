import json
import networkx as nx
from collections import defaultdict
import yaml
# Import graph builder from our existing script
import sys
sys.path.append('.')
from  processing_lineage_from_jsons.process_lineage_json import build_lineage_graph, get_lineage_subgraph

# ==========================================
# CONFIGURATION - EDIT THESE VALUES
# ==========================================

FOLDER_WITH_JSONS = "_DATA_AND_OUTPUTS/lineage_outputs"
OUTPUT_JSON_FILE  = "_DATA_AND_OUTPUTS/local_files/lineage_statistics.json"
DIRECTION         = "both"          # 'upstream', 'downstream', or 'both'

with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
    yaml_read = yaml.safe_load(f)

TARGET_TABLES     = yaml_read["target_tables"]["target_tables_list"]

# ==========================================
# STATISTICS FUNCTIONS
# ==========================================

def get_base_tables(G: nx.DiGraph) -> list:
    """
    Returns all nodes with no incoming edges (depth 0 / root sources).
    These are the base tables that nothing feeds into.
    """
    return sorted([
        node for node in G.nodes()
        if G.in_degree(node) == 0
    ])


def get_terminal_tables(G: nx.DiGraph) -> list:
    """
    Returns all nodes with no outgoing edges (final destinations).
    These are the end points that feed nothing downstream.
    """
    return sorted([
        node for node in G.nodes()
        if G.out_degree(node) == 0
    ])


def get_schema_breakdown(G: nx.DiGraph) -> dict:
    """
    Auto-detects all schemas by splitting node names on '.' and taking index 0.
    Returns a breakdown per schema: tables, stored procedures, views, and their names.
    """
    schemas = defaultdict(lambda: {
        "total": 0,
        "tables": [],
        "stored_procedures": [],
    })

    for node in G.nodes():
        # Detect schema from name
        schema = node.split('.')[0] if '.' in node else '__no_schema__'
        node_type = G.nodes[node].get('node_type', 'table')

        schemas[schema]["total"] += 1

        if node_type == 'stored_procedure':
            schemas[schema]["stored_procedures"].append(node)
        else:
            schemas[schema]["tables"].append(node)

    # Sort names within each schema for readability
    result = {}
    for schema, data in sorted(schemas.items()):
        result[schema] = {
            "total"                  : data["total"],
            "table_count"            : len(data["tables"]),
            "stored_procedure_count" : len(data["stored_procedures"]),
            "tables"                 : sorted(data["tables"]),
            "stored_procedures"      : sorted(data["stored_procedures"]),
        }

    return result


def get_most_connected_nodes(G: nx.DiGraph, top_n: int = 10) -> dict:
    """
    Returns top N nodes by:
    - Incoming edges (most depended upon - breaking these affects many)
    - Outgoing edges (most impactful - changes here propagate far)
    """
    nodes = list(G.nodes())

    by_incoming = sorted(nodes, key=lambda n: G.in_degree(n), reverse=True)[:top_n]
    by_outgoing = sorted(nodes, key=lambda n: G.out_degree(n), reverse=True)[:top_n]

    return {
        "most_depended_upon (highest incoming)": [
            {
                "node"           : n,
                "node_type"      : G.nodes[n].get('node_type', 'table'),
                "incoming_edges" : G.in_degree(n)
            }
            for n in by_incoming
        ],
        "most_impactful (highest outgoing)": [
            {
                "node"           : n,
                "node_type"      : G.nodes[n].get('node_type', 'table'),
                "outgoing_edges" : G.out_degree(n)
            }
            for n in by_outgoing
        ],
    }


def get_sp_impact_analysis(G: nx.DiGraph, top_n: int = 10) -> list:
    """
    Ranks stored procedures by total connections (incoming + outgoing edges).
    The higher the number, the more tables this SP touches.
    """
    sp_nodes = [
        node for node in G.nodes()
        if G.nodes[node].get('node_type') == 'stored_procedure'
    ]

    sp_impact = []
    for sp in sp_nodes:
        incoming = G.in_degree(sp)
        outgoing = G.out_degree(sp)
        sp_impact.append({
            "stored_procedure"  : sp,
            "incoming_edges"    : incoming,
            "outgoing_edges"    : outgoing,
            "total_connections" : incoming + outgoing,
        })

    return sorted(sp_impact, key=lambda x: x["total_connections"], reverse=True)[:top_n]


def get_lineage_depth(G: nx.DiGraph) -> dict:
    """
    Finds the longest lineage chain (critical path) in the graph.
    Uses DAG longest path algorithm.
    Falls back gracefully if the graph has cycles.
    """
    try:
        longest_path = nx.dag_longest_path(G)
        return {
            "longest_chain_length" : len(longest_path),
            "critical_path"        : longest_path,
        }
    except nx.NetworkXUnfeasible:
        # Graph has cycles - find approximate longest path
        return {
            "longest_chain_length" : None,
            "critical_path"        : [],
            "note"                 : "Graph contains cycles, exact longest path could not be computed."
        }


# ==========================================
# REPORT GENERATION
# ==========================================

def generate_statistics(G: nx.DiGraph) -> dict:
    """
    Runs all statistics functions and assembles the full report.
    """
    print("Computing statistics...\n")

    base_tables     = get_base_tables(G)
    terminal_tables = get_terminal_tables(G)
    schema_breakdown = get_schema_breakdown(G)
    most_connected  = get_most_connected_nodes(G, top_n=10)
    sp_impact       = get_sp_impact_analysis(G, top_n=10)
    lineage_depth   = get_lineage_depth(G)

    report = {
        "summary": {
            "total_nodes"             : G.number_of_nodes(),
            "total_edges"             : G.number_of_edges(),
            "total_schemas"           : len(schema_breakdown),
            "total_base_tables"       : len(base_tables),
            "total_terminal_tables"   : len(terminal_tables),
            "total_stored_procedures" : sum(
                s["stored_procedure_count"] for s in schema_breakdown.values()
            ),
            "longest_lineage_chain"   : lineage_depth["longest_chain_length"],
        },
        "base_tables"      : base_tables,
        "terminal_tables"  : terminal_tables,
        "schema_breakdown" : schema_breakdown,
        "most_connected_nodes" : most_connected,
        "sp_impact_analysis"   : sp_impact,
        "lineage_depth"        : lineage_depth,
    }

    return report


def print_report_summary(report: dict):
    """
    Prints a clean, readable summary to the console.
    """
    s = report["summary"]

    print("=" * 60)
    print("LINEAGE STATISTICS REPORT")
    print("=" * 60)

    print(f"\n📊 SUMMARY")
    print(f"  Total Nodes             : {s['total_nodes']}")
    print(f"  Total Edges             : {s['total_edges']}")
    print(f"  Total Schemas           : {s['total_schemas']}")
    print(f"  Base Tables (sources)   : {s['total_base_tables']}")
    print(f"  Terminal Tables (sinks) : {s['total_terminal_tables']}")
    print(f"  Stored Procedures       : {s['total_stored_procedures']}")
    print(f"  Longest Lineage Chain   : {s['longest_lineage_chain']} nodes")

    print(f"\n📂 SCHEMA BREAKDOWN")
    for schema, data in report["schema_breakdown"].items():
        print(f"  [{schema}]  {data['total']} objects  "
              f"({data['table_count']} tables, "
              f"{data['stored_procedure_count']} stored procedures)")

    print(f"\n🔗 MOST DEPENDED UPON (highest incoming edges)")
    for item in report["most_connected_nodes"]["most_depended_upon (highest incoming)"]:
        print(f"  {item['node']:<50} incoming: {item['incoming_edges']}")

    print(f"\n💥 MOST IMPACTFUL (highest outgoing edges)")
    for item in report["most_connected_nodes"]["most_impactful (highest outgoing)"]:
        print(f"  {item['node']:<50} outgoing: {item['outgoing_edges']}")

    print(f"\n⚙️  TOP STORED PROCEDURES BY TOTAL CONNECTIONS")
    for item in report["sp_impact_analysis"]:
        print(f"  {item['stored_procedure']:<50} "
              f"in: {item['incoming_edges']}  "
              f"out: {item['outgoing_edges']}  "
              f"total: {item['total_connections']}")

    print(f"\n🔗 LONGEST LINEAGE CHAIN ({report['lineage_depth']['longest_chain_length']} nodes)")
    path = report["lineage_depth"]["critical_path"]
    if path:
        print("  " + " →\n  ".join(path))

    print("\n" + "=" * 60)


# ==========================================
# EXECUTION - DON'T EDIT BELOW
# ==========================================

if __name__ == "__main__":
    # Build the full graph
    G = build_lineage_graph(FOLDER_WITH_JSONS)

     # Extract the relevant subgraph
    subgraph = get_lineage_subgraph(G, TARGET_TABLES, DIRECTION)

    # Generate all statistics
    report = generate_statistics(subgraph)

    # Print summary to console
    print_report_summary(report)

    # Save full report to JSON
    with open(OUTPUT_JSON_FILE, 'w') as f:
        json.dump(report, f, indent=4)
    print(f"\n✓ Full report saved to: {OUTPUT_JSON_FILE}")