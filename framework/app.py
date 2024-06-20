import click
import os
import time
from business.compare_cus_with_tf import DataComparator
from business.version_control import EltVcsManager
from business.recon import ReconWorkflowManager
from business.ingestion import IngestionManager

current_path = os.getcwd()
print(current_path)


@click.group()
def cli():
    """Command line tool for clear integration elt framework."""
    pass


@click.command(name="compare_output")
def execute_comparison():
    """Execute the data comparison."""
    comparator = DataComparator()
    comparator.execute_comparison()


@click.command(name="compare_mapping_suggester")
def customer_input_suggestion():
    """Execute the data comparison."""
    comparator = DataComparator()
    comparator.find_suggestion()


@click.command(name="push")
def version_push():
    """Execute the data comparison."""
    elt_instance_path = os.path.basename(current_path)
    version_manager = EltVcsManager(current_path, elt_instance_path)
    version_manager.check_and_push_to_vcs()


@click.command(name="pull")
@click.option('--version_id', default=None, help='Version ID to pull (optional).')
def version_pull(version_id):
    """Execute the data comparison."""
    elt_instance_path = os.path.basename(current_path)
    version_manager = EltVcsManager(current_path, elt_instance_path)
    version_manager.pull(version_id)


@click.command(name="pull_presign")
@click.option('--version_id', default=None, help='Version ID to pull (optional).')
def pull_presign(version_id):
    """Execute the data comparison."""
    elt_instance_path = os.path.basename(current_path)
    version_manager = EltVcsManager(current_path, elt_instance_path)
    version_manager.generate_presign_url(version_id)


@click.command(name="versions")
def get_versions():
    """Execute the data comparison."""
    elt_instance_path = os.path.basename(current_path)
    version_manager = EltVcsManager(current_path, elt_instance_path)
    version_manager.list_versions_in_path()

@click.command(name = "recon_workflow")    
def start_recon_worflow():
    """Recon workflow """
    extracted_url = os.path.dirname(current_path)
    file_path = f'{extracted_url}/dev-testing/config/workflow-request.json'
    recon_workflow_manager = ReconWorkflowManager(file_path)
    api_response = recon_workflow_manager.trigger_and_poll_workflow()
    report_folder_path = os.path.join(current_path, 'output', 'recon-result')
    os.makedirs(report_folder_path, exist_ok=True) 
    recon_workflow_manager.process_summary_and_custom_report(api_response,report_folder_path)
            

@click.command(name = "ingestion")
def start_ingestion_workflow():
    """Ingestion workflow """
    elt_instance_path = os.path.basename(current_path)
    current_folder = os.path.dirname(current_path)
    file_path = f'/dev-testing/config/workflow-request.json'
    ingestion_folder_path = os.path.join(current_path, 'output', 'ingestion')
    ingestion_manager = IngestionManager(current_folder,file_path)
    ingestion_manager.trigger(ingestion_folder_path)
          
      


cli.add_command(execute_comparison)
cli.add_command(customer_input_suggestion)
cli.add_command(version_push)
cli.add_command(version_pull)
cli.add_command(get_versions)
cli.add_command(pull_presign)
cli.add_command(start_ingestion_workflow)
cli.add_command(start_recon_worflow)

if __name__ == "__main__":
    cli()
