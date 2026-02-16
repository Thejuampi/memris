#!/usr/bin/env python3
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

def percent(missed, covered):
    m = int(missed)
    c = int(covered)
    total = m + c
    if total == 0:
        return 100.0
    return (c / total) * 100.0

def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('memris-core/target/site/jacoco/jacoco.xml')
    if not path.exists():
        print(f'ERROR: file not found: {path}', file=sys.stderr)
        sys.exit(2)
    tree = ET.parse(path)
    root = tree.getroot()
    classes = []
    for pkg in root.findall('.//package'):
        for cls in pkg.findall('class'):
            name = cls.get('name')
            # find LINE counter
            line_counter = None
            for c in cls.findall('counter'):
                if c.get('type') == 'LINE':
                    line_counter = c
                    break
            if line_counter is None:
                # fallback to INSTRUCTION
                for c in cls.findall('counter'):
                    if c.get('type') == 'INSTRUCTION':
                        line_counter = c
                        break
            if line_counter is None:
                continue
            missed = int(line_counter.get('missed'))
            covered = int(line_counter.get('covered'))
            pct = percent(missed, covered)
            classes.append((name.replace('/', '.'), missed, covered, pct))
    # sort by pct ascending, then by missed desc
    classes.sort(key=lambda x: (x[3], -x[1]))
    print(f"Found {len(classes)} classes")
    print("Rank  Coverage%  Missed Covered  Class")
    for i, (name, missed, covered, pct) in enumerate(classes[:20], start=1):
        print(f"{i:2d}.   {pct:6.2f}%   {missed:6d} {covered:7d}  {name}")

if __name__ == '__main__':
    main()
