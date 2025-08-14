#!/usr/bin/env python3
"""
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import argparse
import os
import subprocess
import sys
import tempfile
import requests
from pathlib import Path
from typing import List, Dict, Any

import git
from github import Github


def run_command(cmd: List[str], cwd: str = None, check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and return the result."""
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=False)

    if result.returncode != 0 and check:
        print(f"Command failed with return code {result.returncode}")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        sys.exit(1)

    return result


def get_commits_between_tags(repo_path: str, previous_tag: str, commit_sha: str) -> List[Dict[str, str]]:
    """Get commits between previous tag and current commit."""
    try:
        repo = git.Repo(repo_path)

        # Get commits from previous tag to current commit
        commits = list(repo.iter_commits(f"{previous_tag}..{commit_sha}"))

        commit_info = []
        for commit in commits:
            commit_info.append({
                'sha': commit.hexsha[:8],  # Short commit ID
                'author': commit.author.name,
                'message': commit.message.split('\n')[0]  # Subject line only
            })

        return commit_info

    except Exception as e:
        print(f"Error getting commits: {e}")
        return []


def create_release_notes(commits: List[Dict[str, str]]) -> str:
    """Create initial release notes from commits."""
    if not commits:
        return "## Changes\n\nNo commits found between releases.\n"

    notes = "## Changes\n\n"
    for commit in commits:
        notes += f"* {commit['sha']} - {commit['message']} ({commit['author']})\n"

    return notes


def verify_gpg_key(gpg_user: str) -> bool:
    """Verify that the GPG key exists and can be used for signing."""
    try:
        result = run_command(['gpg', '--list-secret-keys', gpg_user], check=False)
        return result.returncode == 0
    except Exception:
        return False


def sign_file(file_path: str, gpg_user: str) -> str:
    """Create a detached GPG signature for a file."""
    signature_path = f"{file_path}.asc"

    cmd = [
        'gpg',
        '--local-user', gpg_user,
        '--armor',
        '--detach-sign',
        file_path
    ]

    run_command(cmd)

    if not os.path.exists(signature_path):
        raise RuntimeError(f"Signature file {signature_path} was not created")

    return signature_path


def verify_signature(file_path: str, signature_path: str) -> bool:
    """Verify a GPG signature."""
    try:
        result = run_command(['gpg', '--verify', signature_path, file_path], check=False)
        return result.returncode == 0
    except Exception:
        return False


def download_file(url: str, local_path: str):
    """Download a file from URL to local path."""
    print(f"Downloading {url} to {local_path}")

    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


def upload_release_asset(release, file_path: str):
    """Upload a file as a release asset."""
    print(f"Uploading {file_path} to release")

    filename = os.path.basename(file_path)

    # Use the release object's upload_asset method
    # PyGithub expects: upload_asset(path, label=None, content_type=None, name=None)
    release.upload_asset(file_path, label=filename, name=filename)


def main():
    parser = argparse.ArgumentParser(description='Create and sign Apache Spark Connect Go release')
    parser.add_argument('--tag', required=True, help='New tag version (e.g., v0.2.0)')
    parser.add_argument('--prev-tag', required=True, help='Previous tag version (e.g., v0.1.0)')
    parser.add_argument('--commit', required=True, help='Commit SHA for the tag')
    parser.add_argument('--gpg-user', required=True, help='GPG user ID for signing')
    parser.add_argument('--prerelease', action='store_true', help='Mark as pre-release')
    parser.add_argument('--repo', default='apache/spark-connect-go', help='GitHub repository (owner/name)')
    parser.add_argument('--token', help='GitHub token (or set GITHUB_TOKEN env var)')

    args = parser.parse_args()

    # Get GitHub token
    github_token = args.token or os.environ.get('GITHUB_TOKEN')
    if not github_token:
        print("Error: GitHub token is required. Use --token or set GITHUB_TOKEN environment variable.")
        sys.exit(1)

    # Verify GPG key exists
    if not verify_gpg_key(args.gpg_user):
        print(f"Error: GPG key for user '{args.gpg_user}' not found or not usable")
        sys.exit(1)

    # Initialize GitHub client
    github_client = Github(github_token)
    repo = github_client.get_repo(args.repo)

    print(f"Creating release for {args.repo}")
    print(f"Tag: {args.tag}")
    print(f"Commit: {args.commit}")
    print(f"Previous tag: {args.prev_tag}")
    print(f"GPG user: {args.gpg_user}")
    print(f"Pre-release: {args.prerelease}")

    # Step 1: Create and push tag
    print("\n=== Step 1: Creating and pushing tag ===")
    repo_path = os.getcwd()

    try:
        local_repo = git.Repo(repo_path)

        # Create tag
        new_tag = local_repo.create_tag(args.tag, ref=args.commit, message=f"Release {args.tag}")
        print(f"Created tag {args.tag} at commit {args.commit}")

        # Push tag
        origin = local_repo.remote('origin')
        origin.push(new_tag)
        print(f"Pushed tag {args.tag} to GitHub")

    except Exception as e:
        print(f"Error creating/pushing tag: {e}")
        sys.exit(1)

    # Step 2: Get commits for release notes
    print("\n=== Step 2: Generating release notes ===")
    commits = get_commits_between_tags(repo_path, args.prev_tag, args.commit)
    initial_release_notes = create_release_notes(commits)

    # Step 3: Prompt user for release description
    print("\n=== Step 3: Release description ===")
    print("Initial release notes based on commits:")
    print(initial_release_notes)
    print("\nPlease enter the final release description (press Ctrl+D when done):")

    lines = []
    try:
        while True:
            line = input()
            lines.append(line)
    except EOFError:
        pass

    # Join the lines and add the initial release notes
    final_release_notes = '\n'.join(lines).strip()
    spacer = "\n\n" if final_release_notes else ""
    final_release_notes += spacer + initial_release_notes

    # Step 4: Create GitHub release
    print("\n=== Step 4: Creating GitHub release ===")
    try:
        release = repo.create_git_release(
            tag=args.tag,
            name=f"Release {args.tag}",
            message=final_release_notes,
            draft=True,
            prerelease=args.prerelease
        )
        print(f"Created draft release: {release.html_url}")

    except Exception as e:
        print(f"Error creating release: {e}")
        sys.exit(1)

    # Step 5: Download release artifacts
    print("\n=== Step 5: Downloading release artifacts ===")

    # GitHub automatically creates source archives
    artifacts = [
        f"{args.tag}.tar.gz",
        f"{args.tag}.zip"
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        downloaded_files = []

        for artifact in artifacts:
            # Construct download URL for source archive
            download_url = f"https://github.com/{args.repo}/archive/refs/tags/{artifact}"
            local_file = os.path.join(temp_dir, f"spark-connect-go-{artifact}")

            try:
                download_file(download_url, local_file)
                downloaded_files.append(local_file)
            except Exception as e:
                print(f"Error downloading {artifact}: {e}")
                continue

        if not downloaded_files:
            print("Error: No artifacts were downloaded")
            sys.exit(1)

        # Step 6: Sign artifacts
        print("\n=== Step 6: Signing artifacts ===")
        signatures = []

        for file_path in downloaded_files:
            try:
                print(f"Signing {os.path.basename(file_path)}")
                signature_path = sign_file(file_path, args.gpg_user)
                signatures.append(signature_path)
                print(f"Created signature: {os.path.basename(signature_path)}")

            except Exception as e:
                print(f"Error signing {file_path}: {e}")
                continue

        # Step 7: Verify signatures
        print("\n=== Step 7: Verifying signatures ===")
        for i, file_path in enumerate(downloaded_files):
            if i < len(signatures):
                signature_path = signatures[i]
                if verify_signature(file_path, signature_path):
                    print(f"✓ Signature verified for {os.path.basename(file_path)}")
                else:
                    print(f"✗ Signature verification failed for {os.path.basename(file_path)}")
                    sys.exit(1)

        # Step 8: Upload signatures to release
        print("\n=== Step 8: Uploading signatures to release ===")
        for signature_path in signatures:
            try:
                upload_release_asset(release, signature_path)
                print(f"Uploaded {os.path.basename(signature_path)}")
            except Exception as e:
                print(f"Error uploading {signature_path}: {e}")
                continue

    print(f"\n=== Release created successfully ===")
    print(f"Release URL: {release.html_url}")
    print(f"Tag: {args.tag}")
    print(f"Status: Draft")
    print(f"Pre-release: {args.prerelease}")
    print("\nNext steps:")
    print("1. Review the release on GitHub")
    print("2. Test the release artifacts")
    print("3. Publish the release when ready")


if __name__ == '__main__':
    main()