import os
import sys
import unittest


def run_all_tests():
    # Get the directory where this script is located
    tests_dir = os.path.dirname(os.path.abspath(__file__))

    # Get the project root (two levels up from tests directory)
    project_root = os.path.dirname(os.path.dirname(tests_dir))

    # Add project root to sys.path if not already there
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Create a test loader
    loader = unittest.TestLoader()

    # Discover all tests starting from the tests directory
    # Pattern matches files with 'Test' in the name
    suite = loader.discover(
        start_dir=tests_dir, pattern="*Test*.py", top_level_dir=project_root
    )

    # Create a test runner with detailed output
    runner = unittest.TextTestRunner(verbosity=2)

    # Run the tests
    print(f"Running tests from: {tests_dir}\n")
    result = runner.run(suite)

    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(run_all_tests())
