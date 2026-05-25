"""Real-environment test runner for Astro DR Failover.

This script tests the ENTIRE failover system against REAL AWS services,
sends REAL Slack notifications, and optionally updates REAL Astro deployments.

Prerequisites:
  - AWS CLI configured (`aws configure`) or AWS env vars set
  - .env file filled with real values (copy from .env.example)
  - `pip install -e ".[dev]"` already done
  - Terraform infrastructure deployed (Step 1 in the guide)

Usage:
  python scripts/test_real_env.py                     # Run all tests
  python scripts/test_real_env.py --step 1             # Run only step 1
  python scripts/test_real_env.py --step 1 2 3         # Run steps 1, 2, 3
  python scripts/test_real_env.py --skip-teardown      # Keep AWS resources
  python scripts/test_real_env.py --dry-run             # Validate config only
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Load .env file if present
_env_file = Path(__file__).resolve().parent.parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

import boto3

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════

PRIMARY_REGION = os.environ.get("ASTRO_DR_PRIMARY_REGION", "us-east-1")
DR_REGION = os.environ.get("ASTRO_DR_DR_REGION", "us-east-2")
SSM_PARAM = os.environ.get(
    "ASTRO_DR_ACTIVE_REGION_SSM_PARAM", "/airflow/active_region"
)
SECRET_TEMPLATE = os.environ.get(
    "ASTRO_DR_SECRET_NAME_TEMPLATE", "/airflow/config/{region}/app_config"
)
SLACK_WEBHOOK = os.environ.get("ASTRO_DR_SLACK_WEBHOOK_URL", "")
SLACK_CHANNEL = os.environ.get("ASTRO_DR_SLACK_CHANNEL", "#dr-failover-alerts")
ASTRO_API_KEY = os.environ.get("ASTRO_DR_ASTRO_API_KEY", "")
ASTRO_DEPLOYMENT_ID = os.environ.get("ASTRO_DR_ASTRO_DEPLOYMENT_ID", "")
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL", "")
STATE_TABLE = os.environ.get("STATE_TABLE_NAME", "")
LOCK_TABLE = os.environ.get("LOCK_TABLE_NAME", "")

PRIMARY_CONFIG = {
    "databricks_url": "https://primary-test.cloud.databricks.com",
    "s3_bucket": "samir-primary-dr-test-bucket",
    "kafka_bootstrap": "primary-kafka-test.us-east-1.amazonaws.com:9092",
    "environment": "primary",
}
DR_CONFIG = {
    "databricks_url": "https://dr-test.cloud.databricks.com",
    "s3_bucket": "samir-dr-dr-test-bucket",
    "kafka_bootstrap": "dr-kafka-test.us-east-2.amazonaws.com:9092",
    "environment": "dr",
}


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    END = "\033[0m"


def header(text: str) -> None:
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'═' * 70}")
    print(f"  {text}")
    print(f"{'═' * 70}{Colors.END}\n")


def step(num: int, text: str) -> None:
    print(f"\n{Colors.BOLD}[Step {num}]{Colors.END} {text}")
    print(f"{'─' * 60}")


def ok(text: str) -> None:
    print(f"  {Colors.GREEN}✅ {text}{Colors.END}")


def fail(text: str) -> None:
    print(f"  {Colors.RED}❌ {text}{Colors.END}")


def warn(text: str) -> None:
    print(f"  {Colors.YELLOW}⚠️  {text}{Colors.END}")


def info(text: str) -> None:
    print(f"  ℹ️  {text}")


def pause(msg: str = "Press Enter to continue..."):
    input(f"\n  {Colors.YELLOW}⏸️  {msg}{Colors.END}")


# ═══════════════════════════════════════════════════════════════════════════
# STEP 0: VALIDATE CONFIG
# ═══════════════════════════════════════════════════════════════════════════


def step_0_validate_config() -> bool:
    step(0, "Validating configuration")
    all_ok = True

    # AWS credentials
    try:
        sts = boto3.client("sts", region_name=PRIMARY_REGION)
        identity = sts.get_caller_identity()
        ok(f"AWS credentials valid — Account: {identity['Account']}")
        ok(f"  IAM ARN: {identity['Arn']}")
    except Exception as e:
        fail(f"AWS credentials FAILED: {e}")
        fail("  Run: aws configure")
        all_ok = False

    # Regions
    info(f"Primary region: {PRIMARY_REGION}")
    info(f"DR region:      {DR_REGION}")

    # Slack
    if SLACK_WEBHOOK:
        ok(f"Slack webhook: {SLACK_WEBHOOK[:50]}...")
    else:
        warn("Slack webhook NOT set — notifications will be skipped")

    # Astro
    if ASTRO_API_KEY and ASTRO_DEPLOYMENT_ID:
        ok(f"Astro API key: {ASTRO_API_KEY[:10]}...")
        ok(f"Astro deployment: {ASTRO_DEPLOYMENT_ID}")
    else:
        warn("Astro API key/deployment NOT set — Astro updates will be skipped")

    # SQS / DynamoDB (optional — only for ACID tests)
    if SQS_QUEUE_URL:
        ok(f"SQS queue: {SQS_QUEUE_URL}")
    else:
        warn("SQS queue URL not set — ACID tests will use basic failover")

    if STATE_TABLE and LOCK_TABLE:
        ok(f"DynamoDB state table: {STATE_TABLE}")
        ok(f"DynamoDB lock table:  {LOCK_TABLE}")
    else:
        warn("DynamoDB tables not set — ACID tests will use basic failover")

    return all_ok


# ═══════════════════════════════════════════════════════════════════════════
# STEP 1: CREATE AWS RESOURCES (Secrets + SSM)
# ═══════════════════════════════════════════════════════════════════════════


def step_1_create_aws_resources() -> bool:
    step(1, "Creating AWS resources (Secrets Manager + SSM)")

    try:
        # Create secrets in both regions
        for region, config_values in [
            (PRIMARY_REGION, PRIMARY_CONFIG),
            (DR_REGION, DR_CONFIG),
        ]:
            sm = boto3.client("secretsmanager", region_name=region)
            secret_name = SECRET_TEMPLATE.format(region=region)

            try:
                sm.describe_secret(SecretId=secret_name)
                # Secret exists — update it
                sm.put_secret_value(
                    SecretId=secret_name,
                    SecretString=json.dumps(config_values),
                )
                ok(f"Updated existing secret: {secret_name} in {region}")
            except sm.exceptions.ResourceNotFoundException:
                # Create new secret
                sm.create_secret(
                    Name=secret_name,
                    SecretString=json.dumps(config_values),
                    Description=f"DR test config for {region}",
                )
                ok(f"Created secret: {secret_name} in {region}")

        # Create SSM parameter
        ssm = boto3.client("ssm", region_name=PRIMARY_REGION)
        ssm.put_parameter(
            Name=SSM_PARAM,
            Value=PRIMARY_REGION,
            Type="String",
            Overwrite=True,
            Description="Active AWS region for Airflow DR failover (test)",
        )
        ok(f"SSM parameter set: {SSM_PARAM} = {PRIMARY_REGION}")

        return True
    except Exception as e:
        fail(f"Failed to create AWS resources: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# STEP 2: VERIFY AWS RESOURCES EXIST
# ═══════════════════════════════════════════════════════════════════════════


def step_2_verify_resources() -> bool:
    step(2, "Verifying AWS resources are accessible")
    all_ok = True

    # Verify secrets
    for region in [PRIMARY_REGION, DR_REGION]:
        sm = boto3.client("secretsmanager", region_name=region)
        secret_name = SECRET_TEMPLATE.format(region=region)
        try:
            resp = sm.get_secret_value(SecretId=secret_name)
            values = json.loads(resp["SecretString"])
            ok(f"Secret {secret_name} in {region}: {len(values)} keys")
            for k, v in values.items():
                info(f"  {k}: {v}")
        except Exception as e:
            fail(f"Cannot read secret {secret_name} in {region}: {e}")
            all_ok = False

    # Verify SSM
    ssm = boto3.client("ssm", region_name=PRIMARY_REGION)
    try:
        resp = ssm.get_parameter(Name=SSM_PARAM)
        active = resp["Parameter"]["Value"]
        ok(f"SSM {SSM_PARAM} = {active}")
    except Exception as e:
        fail(f"Cannot read SSM parameter: {e}")
        all_ok = False

    return all_ok


# ═══════════════════════════════════════════════════════════════════════════
# STEP 3: TEST PYTHON MODULES AGAINST REAL AWS
# ═══════════════════════════════════════════════════════════════════════════


def step_3_test_python_modules() -> bool:
    step(3, "Testing Python modules against REAL AWS")

    from astro_dr.aws_client import AWSClientFactory
    from astro_dr.config import DRConfig
    from astro_dr.health_checker import RegionHealthChecker
    from astro_dr.secrets_manager import SecretsManager

    config = DRConfig(
        primary_region=PRIMARY_REGION,
        dr_region=DR_REGION,
        secret_name_template=SECRET_TEMPLATE,
        active_region_ssm_param=SSM_PARAM,
        slack_webhook_url=SLACK_WEBHOOK,
        slack_channel=SLACK_CHANNEL,
        astro_api_key=ASTRO_API_KEY or "",
        astro_deployment_id=ASTRO_DEPLOYMENT_ID or "",
    )
    factory = AWSClientFactory()
    all_ok = True

    # Test SecretsManager
    info("Testing SecretsManager...")
    sm = SecretsManager(factory, config)
    try:
        primary_secret = sm.get_secret(PRIMARY_REGION)
        ok(f"SecretsManager.get_secret({PRIMARY_REGION}): {list(primary_secret.keys())}")

        dr_secret = sm.get_secret(DR_REGION)
        ok(f"SecretsManager.get_secret({DR_REGION}): {list(dr_secret.keys())}")

        assert primary_secret != dr_secret, "Secrets should be different!"
        ok("Secrets are correctly different per region")
    except Exception as e:
        fail(f"SecretsManager test failed: {e}")
        all_ok = False

    # Test HealthChecker
    info("Testing RegionHealthChecker...")
    checker = RegionHealthChecker(factory, config)
    try:
        for region in [PRIMARY_REGION, DR_REGION]:
            health = checker.check_region(region)
            if health.is_healthy:
                ok(f"Region {region}: HEALTHY (latency: {health.latency_ms:.1f}ms)")
            else:
                warn(f"Region {region}: UNHEALTHY — {health.error_message}")
    except Exception as e:
        fail(f"HealthChecker test failed: {e}")
        all_ok = False

    # Test VariableSwitcher (read only)
    info("Testing VariableSwitcher (read-only)...")
    from astro_dr.variable_switcher import VariableSwitcher

    try:
        switcher = VariableSwitcher(config)
        active = switcher.get_active_region()
        ok(f"Active region from SSM: {active}")

        region_config = switcher.fetch_region_config(active)
        ok(f"Config for {active}: {list(region_config.keys())}")
    except Exception as e:
        fail(f"VariableSwitcher test failed: {e}")
        all_ok = False

    return all_ok


# ═══════════════════════════════════════════════════════════════════════════
# STEP 4: TEST SLACK NOTIFICATION (REAL)
# ═══════════════════════════════════════════════════════════════════════════


def step_4_test_slack() -> bool:
    step(4, "Sending REAL Slack test notification")

    if not SLACK_WEBHOOK:
        warn("Slack webhook not configured — skipping")
        return True

    from astro_dr.slack_notifier import SlackNotifier

    notifier = SlackNotifier(SLACK_WEBHOOK, SLACK_CHANNEL)

    try:
        notifier.send_health_check_alert(
            region=PRIMARY_REGION,
            status="TEST",
            details=(
                "🧪 This is a TEST notification from the DR failover system.\n"
                f"Timestamp: {datetime.now(tz=timezone.utc).isoformat()}\n"
                "If you see this, Slack integration is working!"
            ),
        )
        ok("Slack test notification sent!")
        info("Check your Slack channel to confirm receipt")
        pause("Press Enter after you've verified the Slack message...")
        return True
    except Exception as e:
        fail(f"Slack notification failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# STEP 5: EXECUTE REAL FAILOVER (Primary → DR)
# ═══════════════════════════════════════════════════════════════════════════


def step_5_failover_to_dr() -> bool:
    step(5, f"EXECUTING REAL FAILOVER: {PRIMARY_REGION} → {DR_REGION}")

    warn("This will SWITCH the active region in AWS SSM!")
    pause(f"Press Enter to failover {PRIMARY_REGION} → {DR_REGION}...")

    from astro_dr.config import DRConfig
    from astro_dr.variable_switcher import VariableSwitcher

    config = DRConfig(
        primary_region=PRIMARY_REGION,
        dr_region=DR_REGION,
        secret_name_template=SECRET_TEMPLATE,
        active_region_ssm_param=SSM_PARAM,
        slack_webhook_url=SLACK_WEBHOOK,
        slack_channel=SLACK_CHANNEL,
        astro_api_key=ASTRO_API_KEY or "",
        astro_deployment_id=ASTRO_DEPLOYMENT_ID or "",
    )

    # Choose ACID or basic failover
    if SQS_QUEUE_URL and STATE_TABLE and LOCK_TABLE:
        info("Using ACID TransactionalFailover (SQS + DynamoDB)")
        from astro_dr.transactional_failover import TransactionalFailover

        txn = TransactionalFailover(
            config=config,
            sqs_queue_url=SQS_QUEUE_URL,
            state_table_name=STATE_TABLE,
            lock_table_name=LOCK_TABLE,
        )
        result = txn.execute(
            target_region=DR_REGION,
            reason="real_environment_test",
        )
        if result.success:
            ok(f"ACID Failover SUCCEEDED!")
            ok(f"  Failover ID: {result.failover_id}")
            ok(f"  {result.from_region} → {result.to_region}")
            ok(f"  Duration: {result.duration_ms:.1f}ms")
            ok(f"  State: {result.final_state}")
        else:
            fail(f"ACID Failover FAILED: {result.error_message}")
            if result.rolled_back:
                warn("Rollback was executed — SSM restored to original")
            return False
    else:
        info("Using basic VariableSwitcher (no SQS/DynamoDB)")
        switcher = VariableSwitcher(config)
        result = switcher.execute_failover(
            target_region=DR_REGION,
            reason="real_environment_test",
        )
        if result.success:
            ok(f"Failover SUCCEEDED!")
            ok(f"  {result.from_region} → {result.to_region}")
            ok(f"  Duration: {result.duration_ms:.1f}ms")
        else:
            fail(f"Failover FAILED: {result.error_message}")
            return False

    return True


# ═══════════════════════════════════════════════════════════════════════════
# STEP 6: VALIDATE POST-FAILOVER STATE
# ═══════════════════════════════════════════════════════════════════════════


def step_6_validate_post_failover() -> bool:
    step(6, "Validating post-failover state")
    all_ok = True

    # Check SSM now points to DR
    ssm = boto3.client("ssm", region_name=PRIMARY_REGION)
    resp = ssm.get_parameter(Name=SSM_PARAM)
    active = resp["Parameter"]["Value"]

    if active == DR_REGION:
        ok(f"SSM {SSM_PARAM} = {active} (correctly switched to DR)")
    else:
        fail(f"SSM {SSM_PARAM} = {active} (expected {DR_REGION})")
        all_ok = False

    # Read config from DR region
    from astro_dr.aws_client import AWSClientFactory
    from astro_dr.config import DRConfig
    from astro_dr.secrets_manager import SecretsManager

    config = DRConfig(
        primary_region=PRIMARY_REGION,
        dr_region=DR_REGION,
        secret_name_template=SECRET_TEMPLATE,
    )
    factory = AWSClientFactory()
    sm = SecretsManager(factory, config)

    dr_config = sm.get_secret(DR_REGION)
    if dr_config.get("environment") == "dr":
        ok(f"Config reads from DR: environment={dr_config['environment']}")
    else:
        fail(f"Config doesn't look like DR config: {dr_config}")
        all_ok = False

    info("If you have Slack configured, check for the failover notification! 🔔")

    return all_ok


# ═══════════════════════════════════════════════════════════════════════════
# STEP 7: EXECUTE REAL FAILBACK (DR → Primary)
# ═══════════════════════════════════════════════════════════════════════════


def step_7_failback_to_primary() -> bool:
    step(7, f"EXECUTING REAL FAILBACK: {DR_REGION} → {PRIMARY_REGION}")

    pause(f"Press Enter to failback {DR_REGION} → {PRIMARY_REGION}...")

    from astro_dr.config import DRConfig
    from astro_dr.variable_switcher import VariableSwitcher

    config = DRConfig(
        primary_region=PRIMARY_REGION,
        dr_region=DR_REGION,
        secret_name_template=SECRET_TEMPLATE,
        active_region_ssm_param=SSM_PARAM,
        slack_webhook_url=SLACK_WEBHOOK,
        slack_channel=SLACK_CHANNEL,
        astro_api_key=ASTRO_API_KEY or "",
        astro_deployment_id=ASTRO_DEPLOYMENT_ID or "",
    )

    if SQS_QUEUE_URL and STATE_TABLE and LOCK_TABLE:
        info("Using ACID TransactionalFailover for failback")
        from astro_dr.transactional_failover import TransactionalFailover

        txn = TransactionalFailover(
            config=config,
            sqs_queue_url=SQS_QUEUE_URL,
            state_table_name=STATE_TABLE,
            lock_table_name=LOCK_TABLE,
        )
        result = txn.execute(
            target_region=PRIMARY_REGION,
            reason="real_environment_failback_test",
        )
        if result.success:
            ok(f"ACID Failback SUCCEEDED!")
            ok(f"  {result.from_region} → {result.to_region}")
            ok(f"  Duration: {result.duration_ms:.1f}ms")
        else:
            fail(f"Failback FAILED: {result.error_message}")
            return False
    else:
        switcher = VariableSwitcher(config)
        result = switcher.execute_failover(
            target_region=PRIMARY_REGION,
            reason="real_environment_failback_test",
        )
        if result.success:
            ok(f"Failback SUCCEEDED!")
            ok(f"  {result.from_region} → {result.to_region}")
        else:
            fail(f"Failback FAILED: {result.error_message}")
            return False

    # Verify SSM restored
    ssm = boto3.client("ssm", region_name=PRIMARY_REGION)
    resp = ssm.get_parameter(Name=SSM_PARAM)
    active = resp["Parameter"]["Value"]
    if active == PRIMARY_REGION:
        ok(f"SSM restored: {SSM_PARAM} = {active}")
    else:
        fail(f"SSM NOT restored: {SSM_PARAM} = {active}")
        return False

    return True


# ═══════════════════════════════════════════════════════════════════════════
# STEP 8: CLEANUP (optional)
# ═══════════════════════════════════════════════════════════════════════════


def step_8_cleanup() -> bool:
    step(8, "Cleaning up test AWS resources")

    pause("Press Enter to DELETE all test resources (or Ctrl+C to keep them)...")

    try:
        # Delete secrets
        for region in [PRIMARY_REGION, DR_REGION]:
            sm = boto3.client("secretsmanager", region_name=region)
            secret_name = SECRET_TEMPLATE.format(region=region)
            try:
                sm.delete_secret(
                    SecretId=secret_name,
                    ForceDeleteWithoutRecovery=True,
                )
                ok(f"Deleted secret: {secret_name} in {region}")
            except Exception:
                warn(f"Could not delete secret {secret_name} in {region}")

        # Delete SSM parameter
        ssm = boto3.client("ssm", region_name=PRIMARY_REGION)
        try:
            ssm.delete_parameter(Name=SSM_PARAM)
            ok(f"Deleted SSM parameter: {SSM_PARAM}")
        except Exception:
            warn(f"Could not delete SSM parameter: {SSM_PARAM}")

        ok("Cleanup complete!")
        return True
    except Exception as e:
        fail(f"Cleanup failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════


ALL_STEPS = {
    0: ("Validate Config", step_0_validate_config),
    1: ("Create AWS Resources", step_1_create_aws_resources),
    2: ("Verify Resources", step_2_verify_resources),
    3: ("Test Python Modules", step_3_test_python_modules),
    4: ("Test Slack Notification", step_4_test_slack),
    5: ("Failover: Primary → DR", step_5_failover_to_dr),
    6: ("Validate Post-Failover", step_6_validate_post_failover),
    7: ("Failback: DR → Primary", step_7_failback_to_primary),
    8: ("Cleanup Resources", step_8_cleanup),
}


def main():
    parser = argparse.ArgumentParser(
        description="Real-environment test runner for Astro DR Failover"
    )
    parser.add_argument(
        "--step",
        type=int,
        nargs="+",
        help="Run only specific steps (e.g. --step 1 2 3)",
    )
    parser.add_argument(
        "--skip-teardown",
        action="store_true",
        help="Skip the cleanup step (keep AWS resources)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only validate config (step 0)",
    )
    args = parser.parse_args()

    header("🚀 Astro DR Failover — Real Environment Test")
    print(f"  Time:    {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Primary: {PRIMARY_REGION}")
    print(f"  DR:      {DR_REGION}")
    print()

    if args.dry_run:
        step_0_validate_config()
        return

    steps_to_run = args.step or list(ALL_STEPS.keys())
    if args.skip_teardown and 8 in steps_to_run:
        steps_to_run.remove(8)

    results: dict[int, bool] = {}

    for step_num in sorted(steps_to_run):
        if step_num not in ALL_STEPS:
            warn(f"Unknown step {step_num}, skipping")
            continue

        name, func = ALL_STEPS[step_num]
        try:
            results[step_num] = func()
        except KeyboardInterrupt:
            warn(f"\nStep {step_num} interrupted by user")
            results[step_num] = False
            break
        except Exception as e:
            fail(f"Step {step_num} crashed: {e}")
            results[step_num] = False

        if not results[step_num] and step_num < 5:
            fail(f"Step {step_num} failed — stopping (fix the issue and re-run)")
            break

    # Summary
    header("📊 Test Results Summary")
    passed = 0
    failed = 0
    for step_num in sorted(results.keys()):
        name = ALL_STEPS[step_num][0]
        if results[step_num]:
            ok(f"Step {step_num}: {name}")
            passed += 1
        else:
            fail(f"Step {step_num}: {name}")
            failed += 1

    print(f"\n  {Colors.BOLD}Total: {passed} passed, {failed} failed{Colors.END}")

    if failed == 0:
        print(f"\n  {Colors.GREEN}{Colors.BOLD}🎉 ALL TESTS PASSED! Your DR failover system is production-ready.{Colors.END}")
    else:
        print(f"\n  {Colors.RED}{Colors.BOLD}⚠️  Some tests failed. Review the output above.{Colors.END}")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
