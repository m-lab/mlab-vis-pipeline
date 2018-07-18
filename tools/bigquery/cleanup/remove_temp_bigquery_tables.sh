USAGE="$0 [production|staging|sandbox]"

set -e
set -x

source "${HOME}/google-cloud-sdk/path.bash.inc"

# Initialize correct environment variables based on type of server being run
if [[ "$1" == production ]]; then
  source ./environments/production.sh
elif [[ "$1" == staging ]]; then
  source ./environments/staging.sh
elif [[ "$1" == sandbox ]]; then
  source ./environments/sandbox.sh
else
  echo "BAD ARGUMENT TO $0"
  exit 1
fi

KEY_FILE=${KEY_FILE} \
PROJECT=${PROJECT} \
python -m tools.bigquery.cleanup.remove_temp_bigquery_tables