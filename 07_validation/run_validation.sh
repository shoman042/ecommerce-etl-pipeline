#!/bin/bash
# ============================================================
# Run all validation queries (MariaDB + Hive) and save
# clean query+result output to a single log file.
# Usage: bash run_validation.sh
# ============================================================

cd "$(dirname "$0")" || exit 1

OUTPUT_FILE="validation_results.log"

echo "====================================" > "$OUTPUT_FILE"
echo "MariaDB Validation - Source Layer" >> "$OUTPUT_FILE"
echo "====================================" >> "$OUTPUT_FILE"
mysql -u student -pstudent ecommerce < validation_mariadb.sql >> "$OUTPUT_FILE" 2>&1

echo "" >> "$OUTPUT_FILE"
echo "====================================" >> "$OUTPUT_FILE"
echo "Hive Validation - Analytics Layer" >> "$OUTPUT_FILE"
echo "====================================" >> "$OUTPUT_FILE"
hive -S -f validation_hive.sql >> "$OUTPUT_FILE" 2>/dev/null

echo ""
echo "Done. Results saved to: $(pwd)/$OUTPUT_FILE"
