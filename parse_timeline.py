#!/usr/bin/env python3
import json
import os
from pathlib import Path

# Read all message files
import sys
if len(sys.argv) > 1:
    msg_dir = Path(sys.argv[1])
else:
    msg_dir = Path(r"C:\Users\Juan\.local\share\opencode\storage\message\ses_3f882aeadffepWHayAIkjZLa5B")
messages = []

for msg_file in sorted(msg_dir.glob("msg_*.json")):
    try:
        with open(msg_file, 'r', encoding='utf-8') as f:
            msg = json.load(f)
            messages.append(msg)
    except Exception as e:
        print(f"Error reading {msg_file}: {e}")

# Sort by creation time
messages.sort(key=lambda m: m.get('time', {}).get('created', 0))

# Print timeline
print("=" * 80)
print(f"Timeline for session: 'Project Status: Arena repo, query, and materialization gaps'")
print(f"Total messages: {len(messages)}")
print("=" * 80)

for i, msg in enumerate(messages, 1):
    role = msg.get('role', 'unknown')
    created_ts = msg.get('time', {}).get('created', 0)
    summary = msg.get('summary', {})
    title = summary.get('title', msg.get('prompt', '')[:100] if msg.get('prompt') else '')
    prompt_snippet = msg.get('prompt', '')[:150] if msg.get('prompt') else ''

    print(f"\n{i}. [{role.upper()}] Message ID: {msg['id']}")
    print(f"   Created: {created_ts}")
    print(f"   Title/Prompt: {title}")
    if prompt_snippet and not title:
        print(f"   Prompt: {prompt_snippet}")

    # Show diffs if any
    if 'diffs' in summary and summary['diffs']:
        for diff in summary['diffs']:
            print(f"   - Modified: {diff.get('file', 'unknown')} (+{diff.get('additions', 0)}/-{diff.get('deletions', 0)})")

    # Show tool calls if any
    if 'toolCalls' in msg:
        print(f"   Tool calls: {len(msg['toolCalls'])}")
        for tc in msg['toolCalls'][:3]:  # Show first 3
            print(f"     - {tc.get('name', 'unknown')}")
