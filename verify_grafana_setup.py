import os
import sys
from pathlib import Path

def verify_project_structure():
    """Verify the project structure and files exist."""
    project_root = Path(__file__).parent.absolute()
    
    required_files = [
        'analysis/__init__.py',
        'analysis/eda/__init__.py',
        'analysis/eda/subject_analyzer.py',
        'analysis/eda/grafana_publisher.py',
        'mongo_loader.py',
        'run_analysis.py'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not (project_root / file_path).exists():
            missing_files.append(file_path)
    
    if missing_files:
        print("Missing required files:")
        for file in missing_files:
            print(f"  - {file}")
        return False
    
    print("✓ All required files present")
    return True

def verify_imports():
    """Verify that imports work correctly."""
    try:
        sys.path.insert(0, str(Path(__file__).parent.absolute()))
        
        # Test imports
        from analysis.eda.subject_analyzer import EmailSubjectAnalyzer
        from analysis.eda.grafana_publisher import GrafanaPublisher
        
        print("✓ Imports working correctly")
        return True
    except ImportError as e:
        print(f"Import error: {str(e)}")
        return False

if __name__ == "__main__":
    print("Verifying project setup...")
    structure_ok = verify_project_structure()
    imports_ok = verify_imports()
    
    if structure_ok and imports_ok:
        print("\nProject setup verified successfully!")
    else:
        print("\nProject setup verification failed!")
        sys.exit(1)