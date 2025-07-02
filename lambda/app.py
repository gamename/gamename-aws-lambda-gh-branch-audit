"""
AWS Lambda function to identify non-main branches in GitHub repositories across all organizations owned by the authenticated user.

This function:
- Retrieves a GitHub Personal Access Token (PAT) from AWS Secrets Manager.
- Uses the PAT to authenticate with the GitHub API via PyGithub.
- Fetches all organizations owned by the user.
- Iterates through all repositories in these organizations (expected ~200 repos).
- Identifies branches other than 'main' in each repository.
- Sends a plain-text email with the list of non-main branches (or a "none found" message) via AWS SES.
- Handles GitHub API rate limits by pausing if remaining requests drop below 100.
- Is designed to be triggered by a CloudWatch Events schedule (e.g., daily).

Environment Variables:
- GITHUB_SECRET_NAME: Name of the Secrets Manager secret containing the GitHub PAT (e.g., 'github-token').
- SENDER_EMAIL: SES-verified email address to send the report from (e.g., 'noreply@example.com').
- RECIPIENT_EMAIL: Email address to receive the report.

Dependencies:
- boto3: For AWS Secrets Manager and SES interactions.
- PyGithub: For GitHub API interactions (provided via Lambda layer or package).
- Python 3.12 runtime.

GitHub PAT Requirements:
- Scopes: 'read:org' (to list organizations), 'repo' (for private repos) or 'public_repo' (for public repos).
- Stored in Secrets Manager as JSON: {"github_token": "your-pat"}.

IAM Permissions Required:
- secretsmanager:GetSecretValue: To retrieve the GitHub PAT.
- ses:SendEmail: To send the report via SES.
- AWSLambdaBasicExecutionRole: For CloudWatch Logs.

Rate Limit Handling:
- Checks GitHub API rate limit before processing organizations and repositories.
- Pauses execution if remaining requests < 100 until the limit resets.

Error Handling:
- Catches and logs (via print) errors at the organization, repository, and branch levels.
- Continues processing remaining orgs/repos if individual failures occur.
- Returns HTTP 500 status code on unhandled exceptions.

Output:
- Sends an email with a list of non-main branches in the format:
  "Org: <org>, Repo: <repo>, Branch: <branch>"
- If no non-main branches are found, sends: "No non-main branches found as of <timestamp>."
- Logs progress and errors to CloudWatch via print statements.

Setup Notes:
- Lambda timeout: 5 minutes (sufficient for ~200 repos, ~1-2 minutes execution).
- Lambda memory: 512 MB.
- PyGithub layer: arn:aws:lambda:<region>:770693421928:layer:Klayers-p39-github:1.
- SES: Sender and recipient emails must be verified (if in sandbox mode).
- CloudWatch: Schedule with 'rate(1 day)' for daily execution.

Example Email Output:
  Subject: GitHub Non-Main Branches Report
  Non-main branches found as of 2025-07-02T14:49:00:
  Org: myorg1, Repo: repo1, Branch: dev
  Org: myorg1, Repo: repo2, Branch: feature/x
  Org: myorg2, Repo: repo3, Branch: staging
"""

import json
import os
import boto3
from github import Github
from datetime import datetime
import time

def get_secret(secret_name):
    """Retrieve secret from AWS Secrets Manager"""
    client = boto3.client('secretsmanager')
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        print(f"Error retrieving secret: {str(e)}")
        raise e

def send_email(sender, recipient, subject, body):
    """Send email via AWS SES"""
    ses = boto3.client('ses')
    try:
        ses.send_email(
            Source=sender,
            Destination={'ToAddresses': [recipient]},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body}}
            }
        )
        print("Email sent successfully")
    except Exception as e:
        print(f"Error sending email: {str(e)}")
        raise e

def check_rate_limit(github_client):
    """Check GitHub API rate limit and pause if needed"""
    rate_limit = github_client.get_rate_limit().core
    if rate_limit.remaining < 100:  # Buffer to avoid hitting limit
        reset_time = rate_limit.reset.timestamp()
        sleep_time = reset_time - time.time() + 10  # Add buffer
        if sleep_time > 0:
            print(f"Rate limit low ({rate_limit.remaining}). Sleeping for {sleep_time} seconds.")
            time.sleep(sleep_time)
    return rate_limit.remaining

def lambda_handler(event, context):
    """Lambda handler function"""
    try:
        # Environment variables
        github_secret_name = os.environ['GITHUB_SECRET_NAME']
        sender_email = os.environ['SENDER_EMAIL']
        recipient_email = os.environ['RECIPIENT_EMAIL']

        # Get GitHub token from Secrets Manager
        secrets = get_secret(github_secret_name)
        github_token = secrets['github_token']

        # Initialize GitHub client
        g = Github(github_token)

        # Get all organizations owned by the user
        non_main_branches = []
        try:
            check_rate_limit(g)
            orgs = g.get_user().get_orgs()
        except Exception as e:
            print(f"Error fetching organizations: {str(e)}")
            raise e

        # Collect non-main branches from all orgs
        for org in orgs:
            try:
                check_rate_limit(g)
                for repo in org.get_repos():
                    try:
                        check_rate_limit(g)
                        branches = repo.get_branches()
                        for branch in branches:
                            if branch.name != 'main':
                                non_main_branches.append({
                                    'org': org.login,
                                    'repo': repo.name,
                                    'branch': branch.name
                                })
                    except Exception as e:
                        print(f"Error processing repo {repo.name}: {str(e)}")
            except Exception as e:
                print(f"Error processing organization {org.login}: {str(e)}")

        # Prepare email content
        if non_main_branches:
            branch_list = "\n".join(
                f"Org: {b['org']}, Repo: {b['repo']}, Branch: {b['branch']}"
                for b in non_main_branches
            )
            email_body = (
                f"Non-main branches found as of {datetime.utcnow().isoformat()}:\n\n"
                f"{branch_list}"
            )
        else:
            email_body = f"No non-main branches found as of {datetime.utcnow().isoformat()}."

        # Send email
        subject = "GitHub Non-Main Branches Report"
        send_email(sender_email, recipient_email, subject, email_body)

        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed and sent email')
        }

    except Exception as e:
        print(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }