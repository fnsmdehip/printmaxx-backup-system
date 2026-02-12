#!/usr/bin/env python3
"""
PRINTMAXX Backup System
========================
Handles incremental backups, full snapshots, and restore operations.
Works with the guardrails module for integrated safety.

USAGE:
  python3 backup_system.py --full              Full project backup (excludes node_modules, .git)
  python3 backup_system.py --incremental       Only files changed since last backup
  python3 backup_system.py --list              List all backups
  python3 backup_system.py --restore <id>      Restore from a backup
  python3 backup_system.py --diff <id>         Show what changed since backup
  python3 backup_system.py --verify <id>       Verify backup integrity
  python3 backup_system.py --auto              Smart auto-backup (runs before risky ops)
  python3 backup_system.py --prune             Remove backups older than retention period
  python3 backup_system.py --size              Show backup disk usage

BACKUP LOCATION:
  ~/PRINTMAXX_BACKUPS/  (OUTSIDE project to survive project-level disasters)
  Also: .guardrails/checkpoints/ (inside project for quick reverts)

SCHEDULE:
  - Auto-backup before overnight runner
  - Auto-backup before any bulk delete/move
  - Nightly cron backup at 9 PM
  - Manual checkpoint before risky experiments
"""

import os
import sys
import json
import shutil
import hashlib
import argparse
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict

# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = "/Users/macbookpro/Documents/p/PRINTMAXX_STARTER_KITttttt"
BACKUP_ROOT = os.path.expanduser("~/PRINTMAXX_BACKUPS")
MANIFEST_DIR = os.path.join(BACKUP_ROOT, "_manifests")

# Directories to EXCLUDE from backups (huge, regenerable, or not our code)
EXCLUDE_DIRS = {
    "node_modules",
    ".git",
    ".guardrails",
    "__pycache__",
    ".next",
    ".vercel",
    "venv",
    ".venv",
    "dist",
    ".DS_Store",
    ".Trash",
    "app factory",  # Legacy dir with node_modules bloat
    "cal ai",  # Separate project
}

# File extensions to skip (binary blobs that are regenerable)
EXCLUDE_EXTENSIONS = {
    ".pyc", ".pyo", ".so", ".dylib",
    ".o", ".a",
    ".whl",
}

# Max file size to include (skip huge binary files)
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB

# Retention
MAX_FULL_BACKUPS = 5
MAX_INCREMENTAL_BACKUPS = 14
RETENTION_DAYS = 14


# ============================================================
# HELPERS
# ============================================================

def file_hash(filepath: str) -> str:
    """SHA-256 hash of a file for change detection."""
    h = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                h.update(chunk)
        return h.hexdigest()
    except (PermissionError, OSError):
        return "ERROR"


def human_size(size_bytes: int) -> str:
    """Convert bytes to human readable."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def should_exclude(path: str, rel_path: str) -> bool:
    """Check if a file/directory should be excluded from backup."""
    basename = os.path.basename(path)

    # Directory exclusions
    parts = rel_path.split(os.sep)
    for part in parts:
        if part in EXCLUDE_DIRS:
            return True

    # Hidden files/dirs (except important ones)
    if basename.startswith('.') and basename not in {'.claude', '.env', '.gitignore', '.guardrails'}:
        # Allow .claude directory
        if not any(p == '.claude' for p in parts):
            return True

    # Extension exclusions
    _, ext = os.path.splitext(basename)
    if ext.lower() in EXCLUDE_EXTENSIONS:
        return True

    # Size check
    try:
        if os.path.isfile(path) and os.path.getsize(path) > MAX_FILE_SIZE:
            return True
    except OSError:
        return True

    return False


# ============================================================
# BACKUP OPERATIONS
# ============================================================

def full_backup(quiet: bool = False) -> str:
    """
    Create a full backup of the entire project.
    Returns backup ID.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_id = f"full_{timestamp}"
    backup_path = os.path.join(BACKUP_ROOT, backup_id)

    os.makedirs(backup_path, exist_ok=True)
    os.makedirs(MANIFEST_DIR, exist_ok=True)

    manifest = {
        "id": backup_id,
        "type": "full",
        "created": datetime.now().isoformat(),
        "project_root": PROJECT_ROOT,
        "files": {},
        "stats": {"total_files": 0, "total_size": 0, "skipped": 0},
    }

    if not quiet:
        print(f"Creating full backup: {backup_id}")
        print(f"Source: {PROJECT_ROOT}")
        print(f"Destination: {backup_path}")
        print("Scanning files...")

    for dirpath, dirnames, filenames in os.walk(PROJECT_ROOT):
        rel_dir = os.path.relpath(dirpath, PROJECT_ROOT)

        # Skip excluded directories
        dirnames[:] = [d for d in dirnames if not should_exclude(
            os.path.join(dirpath, d), os.path.join(rel_dir, d)
        )]

        for filename in filenames:
            src_file = os.path.join(dirpath, filename)
            rel_file = os.path.relpath(src_file, PROJECT_ROOT)

            if should_exclude(src_file, rel_file):
                manifest["stats"]["skipped"] += 1
                continue

            dst_file = os.path.join(backup_path, rel_file)

            try:
                os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                shutil.copy2(src_file, dst_file)

                file_size = os.path.getsize(src_file)
                manifest["files"][rel_file] = {
                    "hash": file_hash(src_file),
                    "size": file_size,
                    "mtime": os.path.getmtime(src_file),
                }
                manifest["stats"]["total_files"] += 1
                manifest["stats"]["total_size"] += file_size

            except (PermissionError, OSError) as e:
                manifest["stats"]["skipped"] += 1
                if not quiet:
                    print(f"  Skip: {rel_file} ({e})")

    # Save manifest
    manifest_path = os.path.join(MANIFEST_DIR, f"{backup_id}.json")
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    if not quiet:
        print(f"\nBackup complete: {backup_id}")
        print(f"  Files: {manifest['stats']['total_files']}")
        print(f"  Size: {human_size(manifest['stats']['total_size'])}")
        print(f"  Skipped: {manifest['stats']['skipped']}")
        print(f"  Location: {backup_path}")

    return backup_id


def incremental_backup(quiet: bool = False) -> str:
    """
    Create an incremental backup (only changed files since last backup).
    """
    # Find the most recent backup manifest
    last_manifest = _get_latest_manifest()
    last_files = {}
    if last_manifest:
        last_files = last_manifest.get("files", {})

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_id = f"incr_{timestamp}"
    backup_path = os.path.join(BACKUP_ROOT, backup_id)

    os.makedirs(backup_path, exist_ok=True)
    os.makedirs(MANIFEST_DIR, exist_ok=True)

    manifest = {
        "id": backup_id,
        "type": "incremental",
        "parent": last_manifest.get("id") if last_manifest else None,
        "created": datetime.now().isoformat(),
        "project_root": PROJECT_ROOT,
        "files": {},
        "changes": {"new": 0, "modified": 0, "deleted": 0, "unchanged": 0},
        "stats": {"total_files": 0, "total_size": 0},
    }

    if not quiet:
        print(f"Creating incremental backup: {backup_id}")
        if last_manifest:
            print(f"  Based on: {last_manifest['id']}")
        else:
            print("  No previous backup found (will backup all files)")

    current_files = set()

    for dirpath, dirnames, filenames in os.walk(PROJECT_ROOT):
        rel_dir = os.path.relpath(dirpath, PROJECT_ROOT)
        dirnames[:] = [d for d in dirnames if not should_exclude(
            os.path.join(dirpath, d), os.path.join(rel_dir, d)
        )]

        for filename in filenames:
            src_file = os.path.join(dirpath, filename)
            rel_file = os.path.relpath(src_file, PROJECT_ROOT)

            if should_exclude(src_file, rel_file):
                continue

            current_files.add(rel_file)
            current_hash = file_hash(src_file)

            # Check if file changed
            old_info = last_files.get(rel_file, {})
            old_hash = old_info.get("hash", "")

            if current_hash != old_hash:
                # File is new or modified - back it up
                dst_file = os.path.join(backup_path, rel_file)
                try:
                    os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                    shutil.copy2(src_file, dst_file)

                    file_size = os.path.getsize(src_file)
                    manifest["files"][rel_file] = {
                        "hash": current_hash,
                        "size": file_size,
                        "mtime": os.path.getmtime(src_file),
                        "change": "new" if not old_hash else "modified",
                    }
                    manifest["stats"]["total_files"] += 1
                    manifest["stats"]["total_size"] += file_size

                    if not old_hash:
                        manifest["changes"]["new"] += 1
                    else:
                        manifest["changes"]["modified"] += 1

                except (PermissionError, OSError):
                    pass
            else:
                manifest["changes"]["unchanged"] += 1

    # Track deleted files
    for old_file in last_files:
        if old_file not in current_files:
            manifest["changes"]["deleted"] += 1

    # Save manifest
    manifest_path = os.path.join(MANIFEST_DIR, f"{backup_id}.json")
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    if not quiet:
        print(f"\nIncremental backup complete: {backup_id}")
        print(f"  New files: {manifest['changes']['new']}")
        print(f"  Modified: {manifest['changes']['modified']}")
        print(f"  Deleted: {manifest['changes']['deleted']}")
        print(f"  Unchanged: {manifest['changes']['unchanged']}")
        print(f"  Backed up: {manifest['stats']['total_files']} files ({human_size(manifest['stats']['total_size'])})")

    return backup_id


def restore_backup(backup_id: str, target: str = None, dry_run: bool = False) -> Dict:
    """
    Restore from a backup. Optionally to a different target directory.
    """
    manifest_path = os.path.join(MANIFEST_DIR, f"{backup_id}.json")
    if not os.path.exists(manifest_path):
        print(f"ERROR: Backup manifest not found: {backup_id}")
        sys.exit(1)

    with open(manifest_path, 'r') as f:
        manifest = json.load(f)

    backup_path = os.path.join(BACKUP_ROOT, backup_id)
    if not os.path.exists(backup_path):
        print(f"ERROR: Backup data not found: {backup_path}")
        sys.exit(1)

    restore_target = target or PROJECT_ROOT
    results = {"restored": 0, "errors": 0, "skipped": 0}

    print(f"Restoring backup: {backup_id}")
    print(f"  Type: {manifest['type']}")
    print(f"  Created: {manifest['created']}")
    print(f"  Target: {restore_target}")

    if dry_run:
        print("  DRY RUN - no files will be modified")

    for rel_file, info in manifest["files"].items():
        src = os.path.join(backup_path, rel_file)
        dst = os.path.join(restore_target, rel_file)

        if not os.path.exists(src):
            results["skipped"] += 1
            continue

        if dry_run:
            print(f"  Would restore: {rel_file}")
            results["restored"] += 1
            continue

        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
            results["restored"] += 1
        except Exception as e:
            print(f"  Error restoring {rel_file}: {e}")
            results["errors"] += 1

    print(f"\nRestore complete: {results['restored']} files restored, {results['errors']} errors")
    return results


def list_backups() -> List[Dict]:
    """List all available backups."""
    if not os.path.exists(MANIFEST_DIR):
        return []

    backups = []
    for fname in sorted(os.listdir(MANIFEST_DIR)):
        if fname.endswith('.json'):
            with open(os.path.join(MANIFEST_DIR, fname), 'r') as f:
                try:
                    manifest = json.load(f)
                    # Calculate actual backup size on disk
                    bp = os.path.join(BACKUP_ROOT, manifest['id'])
                    if os.path.exists(bp):
                        size = sum(
                            os.path.getsize(os.path.join(dp, fn))
                            for dp, _, fns in os.walk(bp)
                            for fn in fns
                        )
                        manifest['disk_size'] = size
                    else:
                        manifest['disk_size'] = 0
                    backups.append(manifest)
                except json.JSONDecodeError:
                    pass

    return backups


def diff_backup(backup_id: str):
    """Show what changed since a specific backup."""
    manifest_path = os.path.join(MANIFEST_DIR, f"{backup_id}.json")
    if not os.path.exists(manifest_path):
        print(f"ERROR: Backup not found: {backup_id}")
        sys.exit(1)

    with open(manifest_path, 'r') as f:
        manifest = json.load(f)

    backup_files = manifest.get("files", {})
    current_files = {}

    # Scan current project
    for dirpath, dirnames, filenames in os.walk(PROJECT_ROOT):
        rel_dir = os.path.relpath(dirpath, PROJECT_ROOT)
        dirnames[:] = [d for d in dirnames if not should_exclude(
            os.path.join(dirpath, d), os.path.join(rel_dir, d)
        )]
        for filename in filenames:
            src_file = os.path.join(dirpath, filename)
            rel_file = os.path.relpath(src_file, PROJECT_ROOT)
            if not should_exclude(src_file, rel_file):
                current_files[rel_file] = file_hash(src_file)

    # Compare
    new_files = set(current_files.keys()) - set(backup_files.keys())
    deleted_files = set(backup_files.keys()) - set(current_files.keys())
    modified_files = {
        f for f in current_files
        if f in backup_files and current_files[f] != backup_files[f].get("hash", "")
    }

    print(f"\nChanges since backup {backup_id} ({manifest['created']}):")
    print(f"  New files: {len(new_files)}")
    print(f"  Modified: {len(modified_files)}")
    print(f"  Deleted: {len(deleted_files)}")

    if new_files:
        print(f"\n  NEW ({len(new_files)}):")
        for f in sorted(list(new_files)[:20]):
            print(f"    + {f}")
        if len(new_files) > 20:
            print(f"    ... and {len(new_files) - 20} more")

    if modified_files:
        print(f"\n  MODIFIED ({len(modified_files)}):")
        for f in sorted(list(modified_files)[:20]):
            print(f"    ~ {f}")
        if len(modified_files) > 20:
            print(f"    ... and {len(modified_files) - 20} more")

    if deleted_files:
        print(f"\n  DELETED ({len(deleted_files)}):")
        for f in sorted(list(deleted_files)[:20]):
            print(f"    - {f}")
        if len(deleted_files) > 20:
            print(f"    ... and {len(deleted_files) - 20} more")


def verify_backup(backup_id: str) -> bool:
    """Verify backup integrity by checking file hashes."""
    manifest_path = os.path.join(MANIFEST_DIR, f"{backup_id}.json")
    if not os.path.exists(manifest_path):
        print(f"ERROR: Backup not found: {backup_id}")
        return False

    with open(manifest_path, 'r') as f:
        manifest = json.load(f)

    backup_path = os.path.join(BACKUP_ROOT, backup_id)
    total = len(manifest["files"])
    good = 0
    bad = 0
    missing = 0

    print(f"Verifying backup: {backup_id} ({total} files)")

    for rel_file, info in manifest["files"].items():
        backup_file = os.path.join(backup_path, rel_file)
        if not os.path.exists(backup_file):
            missing += 1
            continue

        current_hash = file_hash(backup_file)
        if current_hash == info.get("hash", ""):
            good += 1
        else:
            bad += 1
            print(f"  CORRUPT: {rel_file}")

    print(f"\nVerification: {good} OK, {bad} corrupt, {missing} missing out of {total}")
    return bad == 0 and missing == 0


def prune_backups():
    """Remove old backups beyond retention limits."""
    backups = list_backups()

    full_backups = [b for b in backups if b['type'] == 'full']
    incr_backups = [b for b in backups if b['type'] == 'incremental']

    removed = 0

    # Remove excess full backups (keep most recent MAX_FULL_BACKUPS)
    if len(full_backups) > MAX_FULL_BACKUPS:
        for old in full_backups[:-MAX_FULL_BACKUPS]:
            bp = os.path.join(BACKUP_ROOT, old['id'])
            mp = os.path.join(MANIFEST_DIR, f"{old['id']}.json")
            if os.path.exists(bp):
                shutil.rmtree(bp)
            if os.path.exists(mp):
                os.unlink(mp)
            print(f"  Pruned: {old['id']}")
            removed += 1

    # Remove excess incremental backups
    if len(incr_backups) > MAX_INCREMENTAL_BACKUPS:
        for old in incr_backups[:-MAX_INCREMENTAL_BACKUPS]:
            bp = os.path.join(BACKUP_ROOT, old['id'])
            mp = os.path.join(MANIFEST_DIR, f"{old['id']}.json")
            if os.path.exists(bp):
                shutil.rmtree(bp)
            if os.path.exists(mp):
                os.unlink(mp)
            print(f"  Pruned: {old['id']}")
            removed += 1

    # Remove by age
    cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)
    for b in backups:
        try:
            created = datetime.fromisoformat(b['created'])
            if created < cutoff:
                bp = os.path.join(BACKUP_ROOT, b['id'])
                mp = os.path.join(MANIFEST_DIR, f"{b['id']}.json")
                if os.path.exists(bp):
                    shutil.rmtree(bp)
                if os.path.exists(mp):
                    os.unlink(mp)
                print(f"  Pruned (age): {b['id']}")
                removed += 1
        except (ValueError, KeyError):
            pass

    print(f"\nPruned {removed} old backups")


def show_size():
    """Show total backup disk usage."""
    if not os.path.exists(BACKUP_ROOT):
        print("No backups found.")
        return

    total = 0
    for dirpath, _, filenames in os.walk(BACKUP_ROOT):
        for fn in filenames:
            total += os.path.getsize(os.path.join(dirpath, fn))

    backups = list_backups()
    print(f"\nBackup Storage: {human_size(total)}")
    print(f"  Full backups: {sum(1 for b in backups if b['type'] == 'full')}")
    print(f"  Incremental: {sum(1 for b in backups if b['type'] == 'incremental')}")
    print(f"  Location: {BACKUP_ROOT}")


def auto_backup() -> str:
    """
    Smart auto-backup: incremental if recent full exists, otherwise full.
    Called automatically before risky operations.
    """
    backups = list_backups()
    full_backups = [b for b in backups if b['type'] == 'full']

    # If no full backup in last 7 days, do full
    if not full_backups:
        print("No full backup found. Creating full backup first...")
        return full_backup(quiet=True)

    last_full = full_backups[-1]
    try:
        last_date = datetime.fromisoformat(last_full['created'])
        if (datetime.now() - last_date).days > 7:
            print("Full backup older than 7 days. Creating new full backup...")
            return full_backup(quiet=True)
    except (ValueError, KeyError):
        pass

    # Otherwise incremental
    return incremental_backup(quiet=True)


# ============================================================
# INTERNAL HELPERS
# ============================================================

def _get_latest_manifest() -> Optional[Dict]:
    """Get the most recent backup manifest."""
    if not os.path.exists(MANIFEST_DIR):
        return None

    manifests = sorted(os.listdir(MANIFEST_DIR))
    if not manifests:
        return None

    with open(os.path.join(MANIFEST_DIR, manifests[-1]), 'r') as f:
        return json.load(f)


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="PRINTMAXX Backup System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 backup_system.py --full              Create full project backup
  python3 backup_system.py --incremental       Backup only changed files
  python3 backup_system.py --auto              Smart auto-backup
  python3 backup_system.py --list              List all backups
  python3 backup_system.py --restore <id>      Restore from backup
  python3 backup_system.py --diff <id>         Show changes since backup
  python3 backup_system.py --verify <id>       Verify backup integrity
  python3 backup_system.py --prune             Remove old backups
  python3 backup_system.py --size              Show backup disk usage
        """
    )

    parser.add_argument("--full", action="store_true", help="Create full backup")
    parser.add_argument("--incremental", action="store_true", help="Create incremental backup")
    parser.add_argument("--auto", action="store_true", help="Smart auto-backup")
    parser.add_argument("--list", action="store_true", help="List all backups")
    parser.add_argument("--restore", type=str, help="Restore from backup ID")
    parser.add_argument("--diff", type=str, help="Show changes since backup")
    parser.add_argument("--verify", type=str, help="Verify backup integrity")
    parser.add_argument("--prune", action="store_true", help="Remove old backups")
    parser.add_argument("--size", action="store_true", help="Show backup disk usage")
    parser.add_argument("--dry-run", action="store_true", help="Restore dry-run (show what would change)")
    parser.add_argument("--target", type=str, help="Restore target directory (for non-destructive restore)")

    args = parser.parse_args()

    if args.full:
        full_backup()
    elif args.incremental:
        incremental_backup()
    elif args.auto:
        backup_id = auto_backup()
        print(f"Auto-backup: {backup_id}")
    elif args.list:
        backups = list_backups()
        if not backups:
            print("No backups found.")
            print(f"Create one: python3 backup_system.py --full")
        else:
            print(f"\n{'ID':<30} {'Type':<12} {'Created':<25} {'Files':<8} {'Size'}")
            print("-" * 95)
            for b in backups:
                files = b.get('stats', {}).get('total_files', len(b.get('files', {})))
                size = human_size(b.get('disk_size', 0))
                print(f"{b['id']:<30} {b['type']:<12} {b['created']:<25} {files:<8} {size}")
    elif args.restore:
        restore_backup(args.restore, target=args.target, dry_run=args.dry_run)
    elif args.diff:
        diff_backup(args.diff)
    elif args.verify:
        verify_backup(args.verify)
    elif args.prune:
        prune_backups()
    elif args.size:
        show_size()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
