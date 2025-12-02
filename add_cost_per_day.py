#!/usr/bin/env python3
"""
Add COST_PER_DAY calculation to the prepare_final_export function
"""
import json

# Read the Snowflake notebook
with open('/home/user/work_collab/nrs_beta_2_snowflake.ipynb', 'r') as f:
    nb = json.load(f)

# Find the cell with prepare_final_export function and modify it
for i, cell in enumerate(nb['cells']):
    if cell['cell_type'] == 'code':
        source = ''.join(cell['source'])
        if 'def prepare_final_export' in source:
            print(f"Found prepare_final_export in cell {i}")

            # Split into lines
            lines = cell['source']

            # Find the line with "return newborns_out" and insert before it
            new_lines = []
            for line in lines:
                if 'return newborns_out' in line and 'def ' not in line:
                    # Insert the COST_PER_DAY calculation before the return
                    new_lines.append("    # Calculate cost per day (NET_PD_AMT / LOS)\n")
                    new_lines.append("    newborns_out = newborns_out.with_column(\n")
                    new_lines.append("        \"COST_PER_DAY\",\n")
                    new_lines.append("        when(col(\"LOS\") > lit(0), col(\"NET_PD_AMT\") / col(\"LOS\"))\n")
                    new_lines.append("        .otherwise(lit(None))\n")
                    new_lines.append("    )\n")
                    new_lines.append("    \n")
                new_lines.append(line)

            # Update the cell
            cell['source'] = new_lines
            print(f"Updated cell {i} with COST_PER_DAY calculation")
            print(f"New cell has {len(new_lines)} lines (was {len(lines)})")
            break

# Save the modified notebook
with open('/home/user/work_collab/nrs_beta_2_snowflake.ipynb', 'w') as f:
    json.dump(nb, f, indent=2)

print("✓ Successfully added COST_PER_DAY calculation to prepare_final_export()")
