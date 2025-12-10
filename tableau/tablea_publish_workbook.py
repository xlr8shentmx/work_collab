import os
import zipfile
import shutil
import re
import logging
import tableauserverclient as TSC
from pathlib import Path


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurations
wb_new_client = 'SMTHFDS'
wb_new_filename = 'SmithfieldFoods_FafPH_202512'
wb_template_filename = 'Employer_PSA_Template_Prod'  # Template to use and pull from repository
PUBLISH = True  # Controls whether the workbook will be published
PROJECT_FOLDER = 'Pre-Sales'  # Project folder to publish to (Default | Pre-Sales | Spine and Joint)

# Load environment variables
wb_repo_path = os.getenv('GITHUB_TEMPLATE_PATH')
wb_template_path = os.getenv('NAS_DRIVE_PATH')
tableau_user = os.getenv('TABLEAU_USERNAME')
tableau_pw = os.getenv('TABLEAU_PW')
sf_user = os.getenv('SF_USERNAME')
sf_pw = os.getenv('SF_PW')
sf_server = os.getenv('SF_SERVER')  # Snowflake server address (e.g., account.snowflakecomputing.com)

tableau_server_url = 'https://orbitbi-tableau.optum.com'

# Validate environment variables
required_env_vars = {
    'GITHUB_TEMPLATE_PATH': wb_repo_path,
    'NAS_DRIVE_PATH': wb_template_path,
    'TABLEAU_USERNAME': tableau_user,
    'TABLEAU_PW': tableau_pw,
    'SF_USERNAME': sf_user,
    'SF_PW': sf_pw,
    'SF_SERVER': sf_server,
}
missing_vars = [key for key, value in required_env_vars.items() if not value]
if missing_vars:
    raise EnvironmentError(f"Missing environment variables: {', '.join(missing_vars)}")

# Paths and new filenames
twbx_file = Path(wb_repo_path) / f"{wb_template_filename}.twbx"
twbx_file_out = Path(wb_template_path) / f"{wb_new_filename}.twbx"


def publish_workbook():
    """Publish workbook to Tableau Server."""
    logger.info("Publishing workbook to Tableau Server...")
    server = None
    try:
        auth = TSC.TableauAuth(tableau_user, tableau_pw, site_id='Optum_HCA')
        server = TSC.Server(tableau_server_url, use_server_version=True)

        with server.auth.sign_in(auth):
            # Find project
            project_id = next(
                (proj.id for proj in TSC.Pager(server.projects) if proj.name == PROJECT_FOLDER),
                None
            )
            if not project_id:
                raise ValueError(f"Project '{PROJECT_FOLDER}' not found on Tableau Server.")

            # Prepare workbook item
            workbook_item = TSC.WorkbookItem(name=wb_new_filename, project_id=project_id, show_tabs=True)

            # Step 1: Publish workbook without credentials
            new_workbook = server.workbooks.publish(
                workbook_item,
                str(twbx_file_out),
                mode=TSC.Server.PublishMode.Overwrite,
                skip_connection_check=True
            )
            logger.info(f"Published workbook: {new_workbook.name}")

            # Step 2: Update connection credentials after publishing
            server.workbooks.populate_connections(new_workbook)
            for conn in new_workbook.connections:
                conn.username = sf_user
                conn.password = sf_pw
                conn.embed_password = True
                server.workbooks.update_connection(new_workbook, conn)
                logger.info(f"Updated credentials for connection: {conn.id} ({conn.connection_type})")

            logger.info(f"Successfully published workbook with embedded credentials: {new_workbook.name}")

    except Exception as e:
        logger.error(f"Failed to publish workbook: {e}")
        raise
    finally:
        if server is not None:
            server.auth.sign_out()


def update_workbook():
    """Update workbook with new client configuration."""
    logger.info("Updating workbook...")
    try:
        # Copy template workbook
        shutil.copy(twbx_file, wb_template_path)

        # Extract TWB
        with zipfile.ZipFile(twbx_file, 'r') as zip_in:
            zip_in.extract(f"{wb_template_filename}.twb", wb_template_path)

        twb_path = Path(wb_template_path) / f"{wb_template_filename}.twb"
        with open(twb_path, 'r', encoding='utf-8') as file:
            data = file.read()

        # Replace client names
        pattern = re.escape('VW_MEMBERSHIP_SUMMARY_') + r'(\S+)'
        match = re.search(pattern, data)
        if not match:
            raise ValueError("Original client name not found in TWB file.")
        wb_orig_client = match.group(1)
        updated_data = data.replace(wb_orig_client, wb_new_client)

        # Save updated TWB
        with open(twb_path, 'w', encoding='utf-8') as file:
            file.write(updated_data)

        # Create updated TWBX
        with zipfile.ZipFile(twbx_file_out, 'w') as zip_out:
            with zipfile.ZipFile(twbx_file, 'r') as zip_in:
                for item in zip_in.infolist():
                    if item.filename != f"{wb_template_filename}.twb":
                        zip_out.writestr(item, zip_in.read(item.filename))
            zip_out.write(twb_path, f'{wb_new_filename}.twb')

        logger.info("Workbook update completed successfully.")

        if PUBLISH:
            publish_workbook()

    except Exception as e:
        logger.error(f"Error during workbook update: {e}")
        raise


# Main entry point
if __name__ == "__main__":
    update_workbook()
