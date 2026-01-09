#!/bin/bash
# Monitor invoice upload progress in real-time
# Usage: ./monitor_upload.sh [gr|bc|both]

GR_FILE="/tmp/claude/-Users-dan-Desktop-Ryan-England-Workspace-retail-analytics-dashboard/tasks/beed565.output"
BC_FILE="/tmp/claude/-Users-dan-Desktop-Ryan-England-Workspace-retail-analytics-dashboard/tasks/bc6dead.output"

MODE="${1:-both}"

echo "=== Invoice Upload Monitor ==="
echo "Press Ctrl+C to stop"
echo ""

show_status() {
    local file=$1
    local name=$2
    if [ -f "$file" ]; then
        echo "=== $name Status ==="
        grep -v "FontBBox" "$file" | grep "Processing\|COMPLETE\|Successful\|Duplicates\|Failed:" | tail -3
        echo ""
    fi
}

case $MODE in
    gr)
        show_status "$GR_FILE" "Grass Roots"
        echo "=== Live Updates ==="
        tail -f "$GR_FILE" 2>/dev/null | while read line; do
            [[ ! "$line" =~ "FontBBox" ]] && echo "[GR] $line"
        done
        ;;
    bc)
        show_status "$BC_FILE" "Barbary Coast"
        echo "=== Live Updates ==="
        tail -f "$BC_FILE" 2>/dev/null | while read line; do
            [[ ! "$line" =~ "FontBBox" ]] && echo "[BC] $line"
        done
        ;;
    both|*)
        show_status "$GR_FILE" "Grass Roots"
        show_status "$BC_FILE" "Barbary Coast"
        echo "=== Live Updates (both) ==="
        tail -f "$GR_FILE" "$BC_FILE" 2>/dev/null | while read line; do
            [[ ! "$line" =~ "FontBBox" ]] && echo "$line"
        done
        ;;
esac
