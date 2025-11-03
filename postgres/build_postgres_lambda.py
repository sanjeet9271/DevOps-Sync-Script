import os
import shutil
import zipfile
import subprocess

def build_postgres_lambda_package():
    """Build Lambda package with psycopg2 that has SSL support"""
    
    package_dir = "lambda_package_pg"
    zip_filename = "postgres_lambda_deployment.zip"
    
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
    shutil.copy2("Postgres_Connection_Test.py", os.path.join(package_dir, "lambda_function.py"))
    
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
    print(f"SUCCESS: PostgreSQL Lambda package created successfully!")
    print(f"{'='*80}")
    print(f"File: {zip_filename}")
    print(f"Size: {size_mb:.2f} MB")
    print(f"\nFeatures included:")
    print(f"  1. PostgreSQL connection with SSL support")
    print(f"  2. Database version check")
    print(f"  3. Schema listing")
    print(f"\nDeployment Instructions:")
    print(f"  1. Upload {zip_filename} to AWS Lambda")
    print(f"  2. Set Runtime: Python 3.9")
    print(f"  3. Set Handler: lambda_function.lambda_handler")
    print(f"  4. Configure VPC with access to RDS")
    print(f"  5. Set timeout to at least 30 seconds")
    print(f"\nReady to deploy!")
    print(f"{'='*80}")

if __name__ == "__main__":
    build_postgres_lambda_package()

