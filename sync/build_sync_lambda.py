import os
import shutil
import zipfile
import subprocess

def build_sync_lambda_package():
    """Build Lambda package for Salesforce to PostgreSQL sync"""
    
    package_dir = "lambda_package_sync"
    zip_filename = "sync_lambda_deployment.zip"
    
    # Clean up
    if os.path.exists(package_dir):
        shutil.rmtree(package_dir)
    if os.path.exists(zip_filename):
        os.remove(zip_filename)
    
    os.makedirs(package_dir)
    
    print("Installing packages for Linux Lambda environment...")
    print("  - requests (for Salesforce)")
    print("  - psycopg2-binary (for PostgreSQL)")
    print()
    
    # Install dependencies
    result = subprocess.run([
        "pip", "install",
        "--target", package_dir,
        "--platform", "manylinux2014_x86_64",
        "--implementation", "cp",
        "--python-version", "3.9",
        "--only-binary=:all:",
        "--upgrade",
        "requests",
        "psycopg2-binary"
    ])
    
    if result.returncode != 0:
        print("ERROR: Failed to install packages")
        return
    
    print("\nSuccessfully installed all packages")
    
    # Copy all Python modules
    print("Copying Python modules...")
    modules = [
        "lambda_function.py",
        "salesforce_accessor.py",
        "postgres_accessor.py",
        "data_syncer.py"
    ]
    
    for module in modules:
        shutil.copy2(module, os.path.join(package_dir, module))
        print(f"  - {module}")
    
    # Create zip file
    print("\nCreating zip file...")
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(package_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, package_dir)
                zipf.write(file_path, arcname)
    
    # Clean up
    shutil.rmtree(package_dir)
    
    # Get file size
    size_mb = os.path.getsize(zip_filename) / (1024 * 1024)
    print(f"\n{'='*80}")
    print(f"SUCCESS: Sync Lambda package created!")
    print(f"{'='*80}")
    print(f"File: {zip_filename}")
    print(f"Size: {size_mb:.2f} MB")
    print(f"\nArchitecture:")
    print(f"  - salesforce_accessor.py : Salesforce API & pagination")
    print(f"  - postgres_accessor.py   : PostgreSQL upsert & delete")
    print(f"  - data_syncer.py         : Sync orchestration")
    print(f"  - lambda_function.py     : AWS Lambda entry point")
    print(f"\nFeatures:")
    print(f"  + Batch processing (2000 records at a time)")
    print(f"  + Automatic pagination for large datasets")
    print(f"  + Upsert based on composite primary keys")
    print(f"  + Handles IsDeleted flag for cleanup")
    print(f"  + Modular config for multiple tables")
    print(f"\nDeployment:")
    print(f"  1. Upload {zip_filename} to AWS Lambda")
    print(f"  2. Runtime: Python 3.9")
    print(f"  3. Handler: lambda_function.lambda_handler")
    print(f"  4. Timeout: 300 seconds (5 minutes) minimum")
    print(f"  5. Memory: 512 MB recommended")
    print(f"  6. VPC: Configure with NAT Gateway + RDS access")
    print(f"\nTest Event:")
    print(f'  {{}}                           - Sync all tables')
    print(f'  {{"table": "inventory"}}       - Sync specific table')
    print(f'  {{"batch_size": 1000}}         - Custom batch size')
    print(f"\nReady to deploy!")
    print(f"{'='*80}")

if __name__ == "__main__":
    build_sync_lambda_package()

