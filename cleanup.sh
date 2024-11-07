servers=(
    "orion02"
    "orion03"
    "orion04"
    "orion05"
    "orion06"
    "orion07"
    "orion08"
    "orion09"
    "orion10"
    "orion11"
    "orion12"
)

for server in "${servers[@]}"; do
    echo "Processing ${server}..."
    ssh "${server}" "pkill -u $(whoami) node; rm -rf /bigdata/students/$(whoami)/*"
    if [ $? -eq 0 ]; then
        echo "Successfully cleaned /bigdata/students/$(whoami) on ${server}."
    else
        echo "Failed to clean /bigdata/students/$(whoami) on ${server}."
    fi
done
