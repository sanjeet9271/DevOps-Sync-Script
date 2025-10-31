import os
import shutil
import zipfile
import subprocess

def build_lambda_package():
    """Build Lambda package with psycopg2 that has SSL support"""
    
    package_dir = "lambda_package"
    zip_filename = "lambda_deployment.zip"
    
    # Clean up
    if os.path.exists(package_dir):
        shutil.rmtree(package_dir)
    if os.path.exists(zip_filename):
        os.remove(zip_filename)
    
    os.makedirs(package_dir)
    
    # Install psycopg2-binary for Linux using Docker-like approach
    print("Installing psycopg2-binary for Linux (with SSL support)...")
    
    # Use pip to download the Linux wheel
    result = subprocess.run([
        "pip", "install",
        "--target", package_dir,
        "--platform", "manylinux2014_x86_64",
        "--implementation", "cp",
        "--python-version", "3.9",
        "--only-binary=:all:",
        "--upgrade",
        "psycopg2-binary"
    ])
    
    if result.returncode != 0:
        print("ERROR: Failed to install psycopg2-binary")
        return
    
    print("Successfully installed psycopg2-binary with SSL support")
    
    # Copy lambda function
    print("\nCopying lambda function...")
    shutil.copy2("Salesforce_Syncer_test.py", os.path.join(package_dir, "lambda_function.py"))
    
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
    print(f"\nLambda package created: {zip_filename} ({size_mb:.2f} MB)")
    print("\nIMPORTANT: Set your Lambda runtime to Python 3.9")
    print("Ready to upload to AWS Lambda!")

if __name__ == "__main__":
    build_lambda_package()
