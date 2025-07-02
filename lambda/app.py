"""
AWS Lambda function to identify non-main branches in GitHub repositories across all organizations owned by the
authenticated user, indicating whether each branch is older than 72 hours, with a formatted hierarchical report and
repository count.

This function:
- Retrieves a GitHub Personal Access Token (PAT) from AWS Secrets Manager.
- Uses the PAT to authenticate with the GitHub API via PyGithub.
- Fetches all organizations owned by the user.
- Iterates through all repositories in these organizations (expected ~200 repos).
- Identifies branches other than 'main' and checks if their HEAD commit is older than 72 hours based on committer.date.
- Sends a multipart email (HTML and plain-text) with a hierarchical report of non-main branches and a count of
processed repositories, via AWS SES.
- Lists each organization once, followed by its repositories (indented), and their branches (further indented) with
+72hrs status (Y/N).
- Includes the day and date (e.g., 'Wednesday, July 02, 2025') in the email subject line.
- Logs each organization and repository being processed to CloudWatch via print statements.
- Handles GitHub API rate limits by pausing if remaining requests drop below 100.
- Is designed to be triggered by a CloudWatch Events schedule (e.g., daily).

Environment Variables:
- GITHUB_SECRET_NAME: Name of the Secrets Manager secret containing the GitHub PAT (e.g., 'github-token').
- SENDER_EMAIL: SES-verified email address to send the report from (e.g., 'tennis.n.smith@gmail.com').
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
- Checks GitHub API rate limit before processing organizations, repositories, and commits.
- Pauses execution if remaining requests < 100 until the limit resets.
- Expected ~2,210 API calls for 200 repos (~1,000 branches), within 5,000/hour limit.

Error Handling:
- Catches and logs (via print) errors at the organization, repository, branch, and commit levels.
- Continues processing remaining orgs/repos if individual failures occur.
- Returns HTTP 500 status code on unhandled exceptions.

Logging:
- Prints each organization and repository as they are processed (e.g., 'Processing organization: myorg1', 'Processing
repo: repo1 in myorg1').
- Logs errors and email success to CloudWatch via print statements.

Output:
- Sends a multipart email with:
  - HTML: Org in <h3>, repos in <p> with margin-left: 20px, branches in <p> with margin-left: 40px, format: <branch>,
  Y/N, followed by repository count.
  - Plain-text: Org as header, repos indented with 4 spaces, branches indented with 8 spaces, format: <branch>, Y/N,
  followed by repository count.
  - Subject: Includes day and date, e.g., 'GitHub Non-Main Branches Report - Wednesday, July 02, 2025'.
  Example:
    myorg1
        repo1
            dev, Y
            feature/x, N
        repo2
            test, Y
    org_name_with_length
        repo3
            staging, Y
        repo4
            test2, N
    Total repositories processed: 200
- If no non-main branches are found, sends: "No non-main branches found as of <timestamp>." with the repository count.

Setup Notes:
- Lambda timeout: 5 minutes (sufficient for ~200 repos, ~2-3 minutes execution with commit checks).
- Lambda memory: 512 MB.
- PyGithub layer: arn:aws:lambda:<region>:770693421928:layer:Klayers-p39-github:1.
- SES: Sender and recipient emails must be verified in us-east-1 (if in sandbox mode).
- CloudWatch: Schedule with 'rate(1 day)' for daily execution.

Example Email Output (Plain-text):
  Subject: GitHub Non-Main Branches Report - Wednesday, July 02, 2025
  Non-main branches found as of 2025-07-02T15:58:00:
  myorg1
      repo1
          dev, Y
          feature/x, N
      repo2
          test, Y
  org_name_with_length
      repo3
          staging, Y
      repo4
          test2, N
  Total repositories processed: 200
Example HTML Output (rendered):
  <p>Non-main branches found as of 2025-07-02T15:58:00:</p>
  <div style="font-family: Arial, sans-serif;">
  <h3>myorg1</h3>
  <p style="margin-left: 20px;">repo1</p>
  <p style="margin-left: 40px;">dev, Y</p>
  <p style="margin-left: 40px;">feature/x, N</p>
  <p style="margin-left: 20px;">repo2</p>
  <p style="margin-left: 40px;">test, Y</p>
  <h3>org_name_with_length</h3>
  <p style="margin-left: 20px;">repo3</p>
  <p style="margin-left: 40px;">staging, Y</p>
  <p style="margin-left: 20px;">repo4</p>
  <p style="margin-left: 40px;">test2, N</p>
  <p>Total repositories processed: 200</p>
  </div>
"""

import json
import os
import time
from datetime import datetime, timezone

import boto3
from github import Github


def get_secret(secret_name):
    """Retrieve secret from AWS Secrets Manager"""
    client = boto3.client('secretsmanager')
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        print(f"Error retrieving secret: {str(e)}")
        raise e


def send_email(sender, recipient, subject, html_body, text_body):
    """Send multipart email (HTML and plain-text) via AWS SES"""
    ses = boto3.client('ses')
    try:
        ses.send_email(
            Source=sender,
            Destination={'ToAddresses': [recipient]},
            Message={
                'Subject': {'Data': subject},
                'Body': {
                    'Text': {'Data': text_body},
                    'Html': {'Data': html_body}
                }
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

        # Collect non-main branches, grouped by org and repo
        branches_by_org = {}
        repo_count = 0
        try:
            check_rate_limit(g)
            print("Fetching organizations")
            orgs = g.get_user().get_orgs()
        except Exception as e:
            print(f"Error fetching organizations: {str(e)}")
            raise e

        for org in orgs:
            try:
                check_rate_limit(g)
                print(f"Processing organization: {org.login}")
                branches_by_org[org.login] = {}
                for repo in org.get_repos():
                    try:
                        check_rate_limit(g)
                        print(f"Processing repo: {repo.name} in {org.login}")
                        repo_count += 1
                        branches = repo.get_branches()
                        for branch in branches:
                            if branch.name != 'main':
                                try:
                                    check_rate_limit(g)
                                    # Get the HEAD commit for the branch
                                    commit = branch.commit
                                    commit_date = commit.commit.committer.date
                                    # Calculate age in seconds
                                    age_seconds = (datetime.now(timezone.utc) - commit_date).total_seconds()
                                    is_stale = age_seconds > 72 * 3600  # 72 hours in seconds
                                    if repo.name not in branches_by_org[org.login]:
                                        branches_by_org[org.login][repo.name] = []
                                    branches_by_org[org.login][repo.name].append({
                                        'branch': branch.name,
                                        'stale': is_stale
                                    })
                                except Exception as e:
                                    print(f"Error processing branch {branch.name} in repo {repo.name}: {str(e)}")
                    except Exception as e:
                        print(f"Error processing repo {repo.name}: {str(e)}")
            except Exception as e:
                print(f"Error processing organization {org.login}: {str(e)}")

        # Prepare email content
        if branches_by_org:
            # Plain-text report
            text_lines = []
            html_lines = ['<div style="font-family: Arial, sans-serif;">']
            for org_name, repos in sorted(branches_by_org.items()):
                if repos:  # Only include orgs with non-main branches
                    text_lines.append(org_name)
                    html_lines.append(f'<h3>{org_name}</h3>')
                    for repo_name, branches in sorted(repos.items()):
                        text_lines.append(f"    {repo_name}")
                        html_lines.append(f'<p style="margin-left: 20px;">{repo_name}</p>')
                        for branch in sorted(branches, key=lambda x: x['branch']):
                            text_lines.append(f"        {branch['branch']}, {'Y' if branch['stale'] else 'N'}")
                            html_lines.append(
                                f'<p style="margin-left: 40px;">{branch["branch"]}, '
                                f'{"Y" if branch["stale"] else "N"}</p>')
            text_lines.append(f"\nTotal repositories processed: {repo_count}")
            html_lines.append(f'<p>Total repositories processed: {repo_count}</p>')
            html_lines.append('</div>')
            text_body = (
                f"Non-main branches found as of {datetime.utcnow().isoformat()}:\n\n"
                f"\n".join(text_lines)
            )
            html_body = (
                f'<p>Non-main branches found as of {datetime.utcnow().isoformat()}:</p>'
                f'\n{"".join(html_lines)}'
            )
        else:
            text_body = (
                f"No non-main branches found as of {datetime.utcnow().isoformat()}.\n\n"
                f"Total repositories processed: {repo_count}"
            )
            html_body = (
                f"<p>No non-main branches found as of {datetime.utcnow().isoformat()}.</p>"
                f"<p>Total repositories processed: {repo_count}</p>"
            )

        # Send email with day and date in subject
        subject = f"GitHub Non-Main Branches Report - {datetime.now().strftime('%A, %B %d, %Y')}"
        send_email(sender_email, recipient_email, subject, html_body, text_body)

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