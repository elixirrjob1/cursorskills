"""
Publish a governance vocabulary .md file to OpenMetadata Classifications and Tags.

Usage:
    python scripts/publish_vocab.py \
        --file <path-to-vocab.md> \
        --base-url <openmetadata-url> \
        --username <email> \
        --password <password>
"""

import argparse
import base64
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Optional

import requests


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Tag:
    name: str
    description: str  # plain text + domain context joined with \n\n


@dataclass
class Classification:
    name: str
    mutually_exclusive: bool
    tags: list[Tag] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_vocab(path: str) -> list[Classification]:
    """Parse a governance vocabulary .md file into a list of Classifications."""
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()

    classifications: list[Classification] = []
    current_cls: Optional[Classification] = None
    current_tag: Optional[Tag] = None
    pending_description: Optional[str] = None

    def flush_tag():
        nonlocal current_tag, pending_description
        if current_tag and current_cls:
            if pending_description:
                current_tag.description = pending_description.strip()
            current_cls.tags.append(current_tag)
        current_tag = None
        pending_description = None

    for raw_line in lines:
        line = raw_line.rstrip("\n")

        # Classification heading
        if re.match(r"^## .+", line):
            flush_tag()
            if current_cls:
                classifications.append(current_cls)
            cls_name = line[3:].strip()
            current_cls = Classification(name=cls_name, mutually_exclusive=True)
            continue

        # Mutually exclusive / multi-select flag
        if line.strip() == "*Mutually exclusive*":
            if current_cls:
                current_cls.mutually_exclusive = True
            continue
        if line.strip() == "*Multi-select*":
            if current_cls:
                current_cls.mutually_exclusive = False
            continue

        # Tag heading
        if re.match(r"^### .+", line):
            flush_tag()
            tag_name = line[4:].strip()
            current_tag = Tag(name=tag_name, description="")
            pending_description = None
            continue

        # Horizontal rule — end of classification block
        if line.strip() == "---":
            flush_tag()
            continue

        # Skip the document title
        if re.match(r"^# .+", line):
            continue

        # Skip blank lines
        if not line.strip():
            continue

        # Domain context blockquote — append to description
        if line.startswith("> ") and current_tag is not None:
            context = line[2:].strip()
            if pending_description:
                pending_description = pending_description.rstrip() + "\n\n" + context
            else:
                pending_description = context
            continue

        # Plain text description line
        if current_tag is not None:
            if pending_description:
                pending_description = pending_description.rstrip() + " " + line.strip()
            else:
                pending_description = line.strip()

    # Flush any remaining tag / classification
    flush_tag()
    if current_cls:
        classifications.append(current_cls)

    return classifications


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def login(base_url: str, username: str, password: str) -> str:
    """Authenticate with OpenMetadata and return a bearer token."""
    url = f"{base_url}/api/v1/users/login"
    encoded_password = base64.b64encode(password.encode()).decode()
    payload = {"email": username, "password": encoded_password}
    resp = requests.post(url, json=payload)
    if resp.status_code == 401:
        print("ERROR: Login failed — check your username and password.", file=sys.stderr)
        sys.exit(1)
    resp.raise_for_status()
    token = resp.json().get("accessToken")
    if not token:
        print("ERROR: Login succeeded but no accessToken was returned.", file=sys.stderr)
        sys.exit(1)
    return token


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def make_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def get_classification(base_url: str, headers: dict, name: str) -> Optional[dict]:
    resp = requests.get(f"{base_url}/api/v1/classifications/name/{name}", headers=headers)
    if resp.status_code == 200:
        return resp.json()
    if resp.status_code == 404:
        return None
    resp.raise_for_status()


def put_classification(base_url: str, headers: dict, cls: Classification) -> dict:
    payload = {
        "name": cls.name,
        "displayName": cls.name,
        "description": f"Governance classification: {cls.name}.",
        "mutuallyExclusive": cls.mutually_exclusive,
        "provider": "user",
    }
    resp = requests.put(f"{base_url}/api/v1/classifications", headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()


def get_tag(base_url: str, headers: dict, fqn: str) -> Optional[dict]:
    resp = requests.get(f"{base_url}/api/v1/tags/name/{fqn}", headers=headers)
    if resp.status_code == 200:
        return resp.json()
    if resp.status_code == 404:
        return None
    resp.raise_for_status()


def put_tag(base_url: str, headers: dict, cls_name: str, tag: Tag) -> dict:
    payload = {
        "name": tag.name,
        "displayName": tag.name,
        "description": tag.description,
        "classification": cls_name,
        "provider": "user",
    }
    resp = requests.put(f"{base_url}/api/v1/tags", headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Publish logic
# ---------------------------------------------------------------------------

def publish(classifications: list[Classification], base_url: str, token: str) -> None:
    headers = make_headers(token)
    base_url = base_url.rstrip("/")

    cls_created = cls_updated = cls_skipped = 0
    tag_created = tag_updated = tag_skipped = 0

    for cls in classifications:
        existing_cls = get_classification(base_url, headers, cls.name)

        if existing_cls is None:
            put_classification(base_url, headers, cls)
            print(f"  [CREATED] Classification: {cls.name}")
            cls_created += 1
        elif existing_cls.get("mutuallyExclusive") != cls.mutually_exclusive:
            put_classification(base_url, headers, cls)
            print(f"  [UPDATED] Classification: {cls.name} (mutuallyExclusive changed)")
            cls_updated += 1
        else:
            print(f"  [SKIPPED] Classification: {cls.name} (no change)")
            cls_skipped += 1

        for tag in cls.tags:
            fqn = f"{cls.name}.{tag.name}"
            existing_tag = get_tag(base_url, headers, fqn)

            if existing_tag is None:
                put_tag(base_url, headers, cls.name, tag)
                print(f"    [CREATED] Tag: {fqn}")
                tag_created += 1
            elif (existing_tag.get("description") or "").strip() != tag.description.strip():
                put_tag(base_url, headers, cls.name, tag)
                print(f"    [UPDATED] Tag: {fqn} (description changed)")
                tag_updated += 1
            else:
                print(f"    [SKIPPED] Tag: {fqn} (no change)")
                tag_skipped += 1

    print()
    print("=" * 50)
    print("Summary")
    print("=" * 50)
    print(f"  Classifications : {cls_created} created  {cls_updated} updated  {cls_skipped} skipped")
    print(f"  Tags            : {tag_created} created  {tag_updated} updated  {tag_skipped} skipped")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Publish a governance vocabulary .md to OpenMetadata Classifications and Tags."
    )
    parser.add_argument("--file", required=True, help="Path to the governance vocabulary .md file.")
    parser.add_argument("--base-url", required=True, help="OpenMetadata base URL (e.g. https://sandbox.open-metadata.org).")
    parser.add_argument("--username", required=True, help="OpenMetadata login email.")
    parser.add_argument("--password", required=True, help="OpenMetadata login password.")
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        print(f"ERROR: File not found: {args.file}", file=sys.stderr)
        sys.exit(1)

    print(f"Authenticating as {args.username} ...")
    token = login(args.base_url.rstrip("/"), args.username, args.password)
    print("Authenticated.\n")

    print(f"Parsing: {args.file}")
    classifications = parse_vocab(args.file)
    print(f"Found {len(classifications)} classification(s)\n")

    publish(classifications, args.base_url, token)


if __name__ == "__main__":
    main()
