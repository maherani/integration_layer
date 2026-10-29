# -*- coding: utf-8 -*-
"""Test Runner for Integration Layer."""

import sys
import unittest

def main():
    print("=" * 70)
    print("Running Integration Layer Comprehensive Test Suite")
    print("=" * 70)

    loader = unittest.TestLoader()
    suite = loader.discover("tests", pattern="test_*.py")

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("=" * 70)

    if result.wasSuccessful():
        print(">> ALL INTEGRATION LAYER TESTS PASSED SUCCESSFULLY! <<")
        sys.exit(0)
    else:
        print(">> TEST FAILURES DETECTED! <<")
        sys.exit(1)

if __name__ == "__main__":
    main()
