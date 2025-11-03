import os
import shutil
import zipfile
import subprocess

def build_salesforce_lambda_package():
    """Build Lambda package for Salesforce connection test with internet connectivity check"""
    
    package_dir = "lambda_package_sf"
    zip_filename = "salesforce_lambda_deployment.zip"
    
    # Clean up
    if os.path.exists(package_dir):
        shutil.rmtree(package_dir)
    if os.path.exists(zip_filename):
        os.remove(zip_filename)
    
    os.makedirs(package_dir)
    
    # Install requests library for Linux
    print("Installing requests library for Linux...")
    
    result = subprocess.run([
        "pip", "install",
        "--target", package_dir,
        "--platform", "manylinux2014_x86_64",
        "--implementation", "cp",
        "--python-version", "3.9",
        "--only-binary=:all:",
        "--upgrade",
        "requests"
    ])
    
    if result.returncode != 0:
        print("ERROR: Failed to install requests")
        return
    
    print("Successfully installed requests library")
    
    # Copy lambda function
    print("\nCopying lambda function...")
    shutil.copy2("Salesforce_Connection_Test.py", os.path.join(package_dir, "lambda_function.py"))
    
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
    print(f"SUCCESS: Salesforce Lambda package created successfully!")
    print(f"{'='*80}")
    print(f"File: {zip_filename}")
    print(f"Size: {size_mb:.2f} MB")
    print(f"\nFeatures included:")
    print(f"  1. HTTPS connectivity test (Google, Amazon, ipify)")
    print(f"  2. Salesforce OAuth 2.0 authentication")
    print(f"  3. Salesforce API query test")
    print(f"\nDeployment Instructions:")
    print(f"  1. Upload {zip_filename} to AWS Lambda")
    print(f"  2. Set Runtime: Python 3.9")
    print(f"  3. Set Handler: lambda_function.lambda_handler")
    print(f"  4. Configure VPC with NAT Gateway")
    print(f"  5. Set timeout to at least 60 seconds")
    print(f"\nReady to deploy!")
    print(f"{'='*80}")

if __name__ == "__main__":
    build_salesforce_lambda_package()

