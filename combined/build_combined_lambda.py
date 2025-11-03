import os
import shutil
import zipfile
import subprocess

def build_combined_lambda_package():
    """Build Lambda package with both requests and psycopg2"""
    
    package_dir = "lambda_package_combined"
    zip_filename = "combined_lambda_deployment.zip"
    
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
    
    # Install both packages
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
    
    # Copy lambda function
    print("Copying lambda function...")
    shutil.copy2("Combined_Connection_Test.py", os.path.join(package_dir, "lambda_function.py"))
    
    # Create zip file
    print("Creating zip file...")
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
    print(f"SUCCESS: Combined Lambda package created!")
    print(f"{'='*80}")
    print(f"File: {zip_filename}")
    print(f"Size: {size_mb:.2f} MB")
    print(f"\nFeatures included:")
    print(f"  1. Salesforce OAuth 2.0 authentication")
    print(f"  2. Salesforce SOQL query (WOD_2__Inventory__c)")
    print(f"  3. PostgreSQL/Aurora connection with SSL")
    print(f"  4. PostgreSQL data query")
    print(f"\nDeployment Instructions:")
    print(f"  1. Upload {zip_filename} to AWS Lambda")
    print(f"  2. Set Runtime: Python 3.9")
    print(f"  3. Set Handler: lambda_function.lambda_handler")
    print(f"  4. Configure VPC (needs NAT Gateway for Salesforce + RDS access)")
    print(f"  5. Set timeout to at least 60 seconds")
    print(f"\nReady to deploy!")
    print(f"{'='*80}")

if __name__ == "__main__":
    build_combined_lambda_package()


