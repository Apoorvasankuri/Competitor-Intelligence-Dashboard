from pathlib import Path
import sys

FILE = Path("main.py")

CHECKS = {
    "Source Metadata API Exposure": [
        "source_domain",
        "source_type",
        "source_category",
        "source_priority",
        "source_authority_score",
        "preferred_for_executive_summary",
        "source_match_method",
    ],

    "Search Lens API Exposure": [
        "search_query",
        "search_query_type",
        "detected_client_authority",
        "detected_strategic_theme",
        "accepted_by_gate",
    ],

    "Expected API Endpoints Still Present": [
        "/api/data",
        "/api/data/profile",
        "/api/export-csv",
    ],
}


def read():
    if not FILE.exists():
        print(f"❌ Missing file: {FILE}")
        sys.exit(1)
    return FILE.read_text(encoding="utf-8", errors="ignore")


def main():
    text = read()
    total_failures = 0

    print("\n=== Audit: main.py ===")

    for section, checks in CHECKS.items():
        print(f"\n{section}")
        print("-" * len(section))

        failures = 0
        for item in checks:
            if item in text:
                print(f"✅ {item}")
            else:
                print(f"❌ {item}")
                failures += 1

        total_failures += failures

    print("\nSummary:")
    print(f"Total failures: {total_failures}")

    if total_failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
