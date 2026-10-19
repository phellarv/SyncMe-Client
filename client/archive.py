import os
import shutil
import time
from pathlib import Path
from typing import List, Dict

def get_archive_dir(sync_dir: Path) -> Path:
    archive_dir = sync_dir / ".syncme" / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    return archive_dir

def archive_file(sync_dir: Path, rel_path: str):
    full_path = sync_dir / rel_path
    if not full_path.exists() or not full_path.is_file():
        return None

    timestamp = str(int(time.time()))
    safe_name = rel_path.replace("/", "__").replace("\\", "__")
    archive_name = f"{safe_name}_{timestamp}"
    archive_dir = get_archive_dir(sync_dir)
    archive_path = archive_dir / archive_name
    n = 0
    while archive_path.exists():
        n += 1
        archive_name = f"{safe_name}_{timestamp}-{n}"
        archive_path = archive_dir / archive_name

    shutil.copy2(full_path, archive_path)
    return archive_name

def list_archive(sync_dir: Path) -> List[Dict[str, str]]:
    archive_dir = get_archive_dir(sync_dir)
    results = []
    if not archive_dir.exists():
        return results
        
    for item in archive_dir.iterdir():
        if item.is_file():
            results.append({
                "name": item.name,
                "size": str(item.stat().st_size),
                "mtime": str(item.stat().st_mtime)
            })
    return results

def restore_file(sync_dir: Path, archive_name: str, target_rel_path: str):
    from client.pathsafe import safe_join
    archive_dir = get_archive_dir(sync_dir)
    archive_path = safe_join(archive_dir, archive_name)
    
    if not archive_path.exists():
        raise FileNotFoundError(f"Archive {archive_name} not found")
        
    clean_target = target_rel_path.lstrip("/\\")
    target_path = safe_join(sync_dir, clean_target)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    
    shutil.copy2(archive_path, target_path)
