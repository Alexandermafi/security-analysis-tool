#/bin/bash

folder="dabs"
env_vars=false


# Loop through all arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --env-vars) env_vars="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

version=$(python -c "import sys; print(sys.version_info[:])" 2>&1)
if [[ -z "$version" ]]; then
    echo "Python not found"
    exit 1
fi

major=$(echo $version | cut -d ',' -f 1 | tr -d '(')
minor=$(echo $version | cut -d ',' -f 2)

if [[ $major -lt 3 || $minor -lt 9 ]]; then
    echo "Python 3.9 or higher is required"
    exit 1
fi

cd $folder
pip install -r requirements.txt
python main.py $env_vars