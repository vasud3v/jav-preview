"""
Progress tracking for resumable extractions.
Persists state to disk for recovery after interruptions.
"""

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from models import ProgressState


class ProgressTracker:
    """Manages persistent state for resumable extractions."""
    
    def __init__(self, state_dir: str = "scraper_state"):
        """
        Initialize tracker with state directory.
        
        Args:
            state_dir: Directory to store state files
        """
        self.state_dir = Path(state_dir)
        self.state_file = self.state_dir / "progress.json"
        self.backup_file = self.state_dir / "progress.backup.json"
        self._state: Optional[ProgressState] = None
        self._ensure_dir()
    
    def _ensure_dir(self):
        """Ensure state directory exists."""
        self.state_dir.mkdir(parents=True, exist_ok=True)
    
    def load_state(self) -> Optional[ProgressState]:
        """
        Load existing state from disk.
        
        Returns:
            ProgressState if exists and valid, None otherwise
        """
        if not self.state_file.exists():
            return None
        
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self._state = ProgressState(
                started_at=data.get('started_at', ''),
                last_updated=data.get('last_updated', ''),
                mode=data.get('mode', 'full'),
                total_discovered=data.get('total_discovered', 0),
                completed_codes=data.get('completed_codes', []),
                pending_codes=data.get('pending_codes', []),
                current_page=data.get('current_page', 1),
                total_pages=data.get('total_pages', 0)
            )
            return self._state
            
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print(f"Progress file corrupted: {e}")
            self._backup_corrupted()
            return None
    
    def _backup_corrupted(self):
        """Create backup of corrupted state file."""
        if self.state_file.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.state_dir / f"progress.corrupted.{timestamp}.json"
            try:
                shutil.copy2(self.state_file, backup_path)
                print(f"Backed up corrupted state to {backup_path}")
            except Exception as e:
                print(f"Failed to backup corrupted state: {e}")
    
    def save_state(self, state: ProgressState):
        """
        Atomically save state to disk.
        
        Args:
            state: ProgressState to persist
        """
        self._state = state
        state.last_updated = datetime.now().isoformat()
        
        data = {
            'started_at': state.started_at,
            'last_updated': state.last_updated,
            'mode': state.mode,
            'total_discovered': state.total_discovered,
            'completed_codes': state.completed_codes,
            'pending_codes': state.pending_codes,
            'current_page': state.current_page,
            'total_pages': state.total_pages
        }
        
        # Atomic write: write to temp file, then rename
        temp_file = self.state_dir / "progress.tmp.json"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())  # Ensure data is written to disk
            
            # Atomic rename with Windows workaround
            if os.name == 'nt':  # Windows
                # Use replace() which is atomic on Windows Python 3.3+
                import shutil
                if self.state_file.exists():
                    backup = self.state_dir / "progress.prev.json"
                    shutil.copy2(self.state_file, backup)
                temp_file.replace(self.state_file)
            else:
                temp_file.rename(self.state_file)
            
        except Exception as e:
            print(f"Failed to save state: {e}")
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass
            raise
    
    def mark_completed(self, code: str):
        """
        Mark a video code as completed and persist immediately.
        
        Args:
            code: Video code that was successfully scraped
        """
        if self._state is None:
            return
        
        if code not in self._state.completed_codes:
            self._state.completed_codes.append(code)
        
        if code in self._state.pending_codes:
            self._state.pending_codes.remove(code)
        
        self.save_state(self._state)
    
    def get_pending(self) -> List[str]:
        """
        Get list of pending video codes.
        
        Returns:
            List of codes not yet completed
        """
        if self._state is None:
            return []
        return list(self._state.pending_codes)
    
    def set_pending(self, codes: List[str]):
        """
        Set the pending codes list, excluding already completed codes.
        
        Args:
            codes: List of discovered video codes
        """
        if self._state is None:
            return
        
        completed_set = set(self._state.completed_codes)
        self._state.pending_codes = [c for c in codes if c not in completed_set]
        self._state.total_discovered = len(codes)
        self.save_state(self._state)
    
    def create_new_state(self, mode: str) -> ProgressState:
        """
        Create a new progress state for a fresh extraction.
        
        Args:
            mode: Extraction mode (full, incremental, etc.)
            
        Returns:
            New ProgressState instance
        """
        self._state = ProgressState(
            started_at=datetime.now().isoformat(),
            last_updated=datetime.now().isoformat(),
            mode=mode,
            total_discovered=0,
            completed_codes=[],
            pending_codes=[],
            current_page=1,
            total_pages=0
        )
        self.save_state(self._state)
        return self._state
    
    def reset(self):
        """Clear all progress state (with backup)."""
        if self.state_file.exists():
            # Create backup before reset
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.state_dir / f"progress.reset.{timestamp}.json"
            try:
                shutil.copy2(self.state_file, backup_path)
                print(f"Backed up state before reset to {backup_path}")
            except Exception as e:
                print(f"Failed to backup before reset: {e}")
            
            self.state_file.unlink()
        
        self._state = None
    
    def get_stats(self) -> dict:
        """
        Get current progress statistics.
        
        Returns:
            Dict with progress stats
        """
        if self._state is None:
            return {
                'completed': 0,
                'pending': 0,
                'total': 0,
                'percent': 0.0
            }
        
        total = len(self._state.completed_codes) + len(self._state.pending_codes)
        completed = len(self._state.completed_codes)
        
        return {
            'completed': completed,
            'pending': len(self._state.pending_codes),
            'total': total,
            'percent': (completed / total * 100) if total > 0 else 0.0
        }
