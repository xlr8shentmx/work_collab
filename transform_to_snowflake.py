#!/usr/bin/env python3
"""
Transform nrs_beta_2.ipynb into a Snowflake notebook-optimized version
"""
import json
import re

def create_code_cell(source, metadata=None):
    """Create a code cell"""
    if isinstance(source, str):
        source = [source]
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": metadata or {},
        "outputs": [],
        "source": source
    }

def create_markdown_cell(source):
    """Create a markdown cell"""
    if isinstance(source, str):
        source = [source]
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": source
    }

def split_large_code_cell(source_lines):
    """
    Split the large code cell into logical sections based on function definitions
    Returns a list of (description, code_lines) tuples
    """
    sections = []
    current_section = []
    current_desc = "Imports and Setup"

    i = 0
    while i < len(source_lines):
        line = source_lines[i]

        # Check for major section boundaries
        if 'def get_snowflake_session' in line:
            if current_section:
                sections.append((current_desc, current_section))
            current_section = [line]
            current_desc = "Connection Management Functions"
        elif 'def calculate_birth_window' in line:
            if current_section:
                sections.append((current_desc, current_section))
            current_section = [line]
            current_desc = "Date Window Calculation"
        elif 'def process_membership' in line:
            if current_section:
                sections.append((current_desc, current_section))
            current_section = [line]
            current_desc = "Membership Processing Functions"
        elif 'def fetch_newborn_keys' in line:
            if current_section:
                sections.append((current_desc, current_section))
            current_section = [line]
            current_desc = "Newborn Identification Functions"
        elif 'def assign_claim_type' in line:
            if current_section:
                sections.append((current_desc, current_section))
            current_section = [line]
            current_desc = "Claim Classification and Reference Tagging"
        elif 'def build_hosp_rollup' in line:
            if current_section:
                sections.append((current_desc, current_section))
            current_section = [line]
            current_desc = "Hospital Rollup and Episode Building"
        elif 'def build_nicu_rollup' in line:
            if current_section:
                sections.append((current_desc, current_section))
            current_section = [line]
            current_desc = "NICU Analysis and Rollup"
        elif 'def prepare_final_export' in line:
            if current_section:
                sections.append((current_desc, current_section))
            current_section = [line]
            current_desc = "Final Export Preparation"
        elif 'def main(' in line:
            if current_section:
                sections.append((current_desc, current_section))
            current_section = [line]
            current_desc = "Main Pipeline Orchestration"
        else:
            current_section.append(line)

        i += 1

    # Add the last section
    if current_section:
        sections.append((current_desc, current_section))

    return sections

def main():
    # Read the original notebook
    with open('/home/user/work_collab/nrs_beta_2.ipynb', 'r') as f:
        nb = json.load(f)

    # Create new Snowflake-optimized notebook
    snowflake_nb = {
        "cells": [],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.8.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }

    cells = nb['cells']

    # Add title and overview
    snowflake_nb['cells'].append(create_markdown_cell([
        "# NRS Beta 2: Newborn Risk Stratification Analytics\n",
        "## Snowflake Notebook Edition\n",
        "\n",
        "This notebook processes newborn and NICU claims data through a comprehensive 6-step pipeline:\n",
        "\n",
        "1. **Membership Processing** - Load and process member eligibility data\n",
        "2. **Newborn Identification** - Identify newborns using diagnosis and revenue codes\n",
        "3. **Claims Processing** - Load and enrich claims with reference flags\n",
        "4. **Hospital Rollup** - Aggregate hospital stays and calculate length of stay\n",
        "5. **NICU Analysis** - Identify and analyze NICU admissions with cost breakdowns\n",
        "6. **Final Export** - Combine newborn and NICU data for output\n",
        "\n",
        "**Optimized for Snowflake Notebooks**: This version features modular cells for better execution control and debugging.\n",
        "\n",
        "---"
    ]))

    # Add configuration section
    snowflake_nb['cells'].append(create_markdown_cell([
        "## 1. Configuration and Setup\n",
        "\n",
        "Define client settings, database parameters, clinical thresholds, and code ranges."
    ]))

    # Add the configuration cell (cell 0 from original)
    snowflake_nb['cells'].append(create_code_cell(cells[0]['source']))

    # Add note about Snowflake session
    snowflake_nb['cells'].append(create_markdown_cell([
        "## 2. Pipeline Implementation\n",
        "\n",
        "The following cells contain the pipeline functions organized into logical groups.\n",
        "\n",
        "**Note for Snowflake Notebooks**: If running in a Snowflake notebook environment, you can use the built-in `snowflake.snowpark.session` instead of creating a new connection."
    ]))

    # Split the large code cell (cell 2 from original)
    sections = split_large_code_cell(cells[2]['source'])

    for desc, code_lines in sections:
        # Add a markdown header for each section
        snowflake_nb['cells'].append(create_markdown_cell([
            f"### {desc}\n"
        ]))

        # Add the code cell
        snowflake_nb['cells'].append(create_code_cell(code_lines))

    # Add execution section from original cells[3] and cells[4]
    snowflake_nb['cells'].append(create_markdown_cell(cells[3]['source']))
    snowflake_nb['cells'].append(create_code_cell(cells[4]['source']))

    # Add results section from original cells[5] and cells[6]
    snowflake_nb['cells'].append(create_markdown_cell(cells[5]['source']))
    snowflake_nb['cells'].append(create_code_cell(cells[6]['source']))

    # Write the new notebook
    output_path = '/home/user/work_collab/nrs_beta_2_snowflake.ipynb'
    with open(output_path, 'w') as f:
        json.dump(snowflake_nb, f, indent=2)

    print(f"✓ Created Snowflake-optimized notebook: {output_path}")
    print(f"  Original cells: {len(cells)}")
    print(f"  New cells: {len(snowflake_nb['cells'])}")
    print(f"  Code sections created: {len(sections)}")

if __name__ == '__main__':
    main()
