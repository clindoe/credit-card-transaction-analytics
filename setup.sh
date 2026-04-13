#!/usr/bin/env bash
# =============================================================================
# Credit Card Transaction Analytics — One-Command Infrastructure Deployment
#
# Usage:
#   ./setup.sh                        # deploy all stacks (prod)
#   ENVIRONMENT=dev ./setup.sh        # deploy to dev environment
#   ALERT_EMAIL=you@email.com ./setup.sh  # with SNS email alerts
#   ./setup.sh --skip-tests           # skip unit test run
#   ./setup.sh --dry-run              # validate templates only, no deploy
#
# Prerequisites: aws-cli v2, python3, pip3, AWS credentials configured.
# =============================================================================
set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
PROJECT_NAME="${PROJECT_NAME:-credit-txn-analytics}"
ENVIRONMENT="${ENVIRONMENT:-prod}"
BUCKET_NAME="${BUCKET_NAME:-credit-txn-analytics-adil}"
REGION="${AWS_REGION:-us-east-1}"
ALERT_EMAIL="${ALERT_EMAIL:-}"
SKIP_TESTS=false
DRY_RUN=false

for arg in "$@"; do
  case $arg in
    --skip-tests) SKIP_TESTS=true ;;
    --dry-run)    DRY_RUN=true ;;
    *) echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

STACK_PREFIX="${PROJECT_NAME}-${ENVIRONMENT}"
CFN_DIR="infrastructure/cloudformation"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

log()    { echo -e "${BLUE}▶${NC} $*"; }
ok()     { echo -e "${GREEN}✔${NC} $*"; }
warn()   { echo -e "${YELLOW}⚠${NC}  $*"; }
err()    { echo -e "${RED}✖${NC} $*"; exit 1; }
header() { echo -e "\n${BOLD}${BLUE}── $* ──${NC}"; }

# ── Prerequisites ──────────────────────────────────────────────────────────────
check_prerequisites() {
  header "Prerequisites"

  command -v aws     >/dev/null 2>&1 || err "AWS CLI not found. https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html"
  command -v python3 >/dev/null 2>&1 || err "python3 not found."
  command -v pip3    >/dev/null 2>&1 || err "pip3 not found."

  aws sts get-caller-identity >/dev/null 2>&1 \
    || err "AWS credentials not configured. Run: aws configure"

  ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
  ok "AWS Account ${ACCOUNT_ID} | Region ${REGION} | Environment ${ENVIRONMENT}"

  [[ -z "${ALERT_EMAIL}" ]] \
    && warn "ALERT_EMAIL not set — SNS email subscription will be skipped.\n   Re-run with: ALERT_EMAIL=you@email.com ./setup.sh"
}

# ── Python deps ───────────────────────────────────────────────────────────────
install_python_deps() {
  header "Python Dependencies"
  pip3 install -q -r requirements.txt
  ok "Dependencies installed from requirements.txt"
}

# ── Unit tests ────────────────────────────────────────────────────────────────
run_tests() {
  if [[ "${SKIP_TESTS}" == "true" ]]; then
    warn "Tests skipped (--skip-tests)"
    return
  fi
  header "Unit Tests"
  python3 -m pytest tests/ -v --tb=short -q 2>&1 | tail -20
  ok "All tests passed"
}

# ── CloudFormation validate (always) + deploy (unless --dry-run) ──────────────
cfn_action() {
  local stack_name="$1"
  local template="$2"
  shift 2
  local params=("$@")

  log "Validating ${template}..."
  aws cloudformation validate-template \
    --template-body "file://${template}" \
    --region "${REGION}" >/dev/null
  ok "Template valid: ${template}"

  if [[ "${DRY_RUN}" == "true" ]]; then
    warn "DRY RUN — skipping deploy of ${stack_name}"
    return
  fi

  log "Deploying stack: ${stack_name}..."
  aws cloudformation deploy \
    --region "${REGION}" \
    --stack-name "${stack_name}" \
    --template-file "${template}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides "${params[@]}" \
    --tags Project="${PROJECT_NAME}" Environment="${ENVIRONMENT}" \
    --no-fail-on-empty-changeset
  ok "Stack deployed: ${stack_name}"
}

# ── Stack: IAM Roles ──────────────────────────────────────────────────────────
deploy_iam() {
  header "Stack 1/4 — IAM Roles"
  cfn_action \
    "${STACK_PREFIX}-iam" \
    "${CFN_DIR}/iam_roles.yaml" \
    "ProjectName=${PROJECT_NAME}" \
    "Environment=${ENVIRONMENT}" \
    "BucketName=${BUCKET_NAME}"
}

# ── Stack: S3 Buckets ─────────────────────────────────────────────────────────
deploy_s3() {
  header "Stack 2/4 — S3 Data Lake"
  cfn_action \
    "${STACK_PREFIX}-s3" \
    "${CFN_DIR}/s3_buckets.yaml" \
    "ProjectName=${PROJECT_NAME}" \
    "Environment=${ENVIRONMENT}" \
    "BucketName=${BUCKET_NAME}"
}

# ── Upload Glue scripts ───────────────────────────────────────────────────────
upload_glue_scripts() {
  header "Glue Scripts → S3"
  if [[ "${DRY_RUN}" == "true" ]]; then
    warn "DRY RUN — skipping script upload"
    return
  fi
  for script in src/glue_jobs/*.py; do
    [[ "${script}" == *"__init__"* ]] && continue
    fname=$(basename "${script}")
    aws s3 cp "${script}" \
      "s3://${BUCKET_NAME}/glue-scripts/${fname}" \
      --region "${REGION}" --quiet
    ok "Uploaded: glue-scripts/${fname}"
  done
}

# ── Stack: Glue Resources ─────────────────────────────────────────────────────
deploy_glue() {
  header "Stack 3/4 — Glue Jobs & Workflow"

  if [[ "${DRY_RUN}" == "true" ]]; then
    cfn_action \
      "${STACK_PREFIX}-glue" \
      "${CFN_DIR}/glue_resources.yaml" \
      "ProjectName=${PROJECT_NAME}" \
      "Environment=${ENVIRONMENT}" \
      "BucketName=${BUCKET_NAME}" \
      "GlueRoleArn=arn:aws:iam::123456789012:role/placeholder-for-validation"
    return
  fi

  GLUE_ROLE_ARN=$(aws cloudformation describe-stacks \
    --region "${REGION}" \
    --stack-name "${STACK_PREFIX}-iam" \
    --query "Stacks[0].Outputs[?OutputKey=='GlueRoleArn'].OutputValue" \
    --output text)

  [[ -z "${GLUE_ROLE_ARN}" ]] && err "Could not read GlueRoleArn from IAM stack. Was it deployed?"

  cfn_action \
    "${STACK_PREFIX}-glue" \
    "${CFN_DIR}/glue_resources.yaml" \
    "ProjectName=${PROJECT_NAME}" \
    "Environment=${ENVIRONMENT}" \
    "BucketName=${BUCKET_NAME}" \
    "GlueRoleArn=${GLUE_ROLE_ARN}"
}

# ── Stack: Monitoring ─────────────────────────────────────────────────────────
deploy_monitoring() {
  header "Stack 4/4 — CloudWatch & SNS Monitoring"
  local extra_params=()
  [[ -n "${ALERT_EMAIL}" ]] && extra_params+=("AlertEmail=${ALERT_EMAIL}")
  cfn_action \
    "${STACK_PREFIX}-monitoring" \
    "${CFN_DIR}/monitoring.yaml" \
    "ProjectName=${PROJECT_NAME}" \
    "Environment=${ENVIRONMENT}" \
    "${extra_params[@]+"${extra_params[@]}"}"
}

# ── Launch Streamlit ──────────────────────────────────────────────────────────
launch_dashboard() {
  if command -v streamlit >/dev/null 2>&1; then
    header "Streamlit Dashboard"
    ok "Launch with: streamlit run src/dashboard/app.py"
  fi
}

# ── Summary ───────────────────────────────────────────────────────────────────
print_summary() {
  local action_word
  [[ "${DRY_RUN}" == "true" ]] && action_word="Validated (dry-run)" || action_word="Deployed"

  echo ""
  echo -e "${GREEN}${BOLD}═══════════════════════════════════════════════════════${NC}"
  echo -e "${GREEN}${BOLD}  ${action_word} Successfully!${NC}"
  echo -e "${GREEN}${BOLD}═══════════════════════════════════════════════════════${NC}"
  echo ""
  echo "  Stacks         : ${STACK_PREFIX}-iam/s3/glue/monitoring"
  echo "  Data Lake      : s3://${BUCKET_NAME}/"
  echo "  Glue Workflow  : ${PROJECT_NAME}-pipeline"
  echo "  Region         : ${REGION}"
  if [[ "${DRY_RUN}" != "true" ]]; then
    echo ""
    echo "  Next steps:"
    echo "    1. Ingest data    : python3 src/ingestion/transaction_simulator.py data/fraudTrain.csv train"
    echo "    2. Run pipeline   : aws glue start-workflow-run --name ${PROJECT_NAME}-pipeline --region ${REGION}"
    echo "    3. Monitor        : aws cloudwatch get-dashboard --dashboard-name ${STACK_PREFIX}-pipeline"
    echo "    4. Dashboard      : streamlit run src/dashboard/app.py"
    [[ -n "${ALERT_EMAIL}" ]] && echo "    5. Confirm SNS    : check ${ALERT_EMAIL} for subscription confirmation"
  fi
  echo ""
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  echo ""
  echo -e "${BOLD}${BLUE}═══════════════════════════════════════════════════════${NC}"
  echo -e "${BOLD}${BLUE}  Credit Card Transaction Analytics — Deploy${NC}"
  echo -e "${BOLD}${BLUE}═══════════════════════════════════════════════════════${NC}"
  [[ "${DRY_RUN}" == "true" ]] && echo -e "  ${YELLOW}DRY RUN — templates will be validated, not deployed${NC}"
  echo ""

  check_prerequisites
  install_python_deps
  run_tests
  deploy_iam
  deploy_s3
  upload_glue_scripts
  deploy_glue
  deploy_monitoring
  launch_dashboard
  print_summary
}

main "$@"
