"""Checkpoint tracking for resumable sync operations."""

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from dr_sync.exceptions import SyncError


@dataclass
class SyncCheckpoint:
    """Checkpoint data for tracking sync progress.

    Attributes:
        sync_type: Type of sync operation (e.g., "tables", "jobs").
        source_host: Source workspace host.
        target_host: Target workspace host.
        started_at: Timestamp when sync started.
        completed_items: Set of completed item identifiers.
        failed_items: Dict mapping item identifier to error message.
        last_checkpoint_time: Timestamp of last checkpoint update.
        metadata: Optional additional metadata (catalogs, filters, etc.).
    """

    sync_type: str
    source_host: str
    target_host: str
    started_at: str
    completed_items: Set[str] = field(default_factory=set)
    failed_items: Dict[str, str] = field(default_factory=dict)
    last_checkpoint_time: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "sync_type": self.sync_type,
            "source_host": self.source_host,
            "target_host": self.target_host,
            "started_at": self.started_at,
            "completed_items": list(self.completed_items),
            "failed_items": self.failed_items,
            "last_checkpoint_time": self.last_checkpoint_time,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SyncCheckpoint":
        """Create from JSON-serializable dict."""
        return cls(
            sync_type=data["sync_type"],
            source_host=data["source_host"],
            target_host=data["target_host"],
            started_at=data["started_at"],
            completed_items=set(data.get("completed_items", [])),
            failed_items=data.get("failed_items", {}),
            last_checkpoint_time=data.get("last_checkpoint_time", data["started_at"]),
            metadata=data.get("metadata", {}),
        )


class CheckpointManager:
    """Manages checkpoint state file for resumable sync operations."""

    def __init__(self, state_dir: str = ".dr_sync_state"):
        """Initialize checkpoint manager.

        Args:
            state_dir: Directory to store checkpoint state files.
        """
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def _get_checkpoint_path(self, sync_type: str, source_host: str, target_host: str) -> Path:
        """Generate checkpoint file path based on sync parameters."""
        # Create unique filename from sync parameters
        safe_source = source_host.replace("https://", "").replace("http://", "").replace("/", "_")
        safe_target = target_host.replace("https://", "").replace("http://", "").replace("/", "_")
        filename = f"{sync_type}_{safe_source}_to_{safe_target}.json"
        return self.state_dir / filename

    def load(self, sync_type: str, source_host: str, target_host: str) -> Optional[SyncCheckpoint]:
        """Load existing checkpoint if available.

        Args:
            sync_type: Type of sync operation.
            source_host: Source workspace host.
            target_host: Target workspace host.

        Returns:
            SyncCheckpoint if exists, None otherwise.
        """
        path = self._get_checkpoint_path(sync_type, source_host, target_host)

        if not path.exists():
            return None

        try:
            with open(path, "r") as f:
                data = json.load(f)
            return SyncCheckpoint.from_dict(data)
        except (json.JSONDecodeError, KeyError) as e:
            raise SyncError(
                resource_type="checkpoint",
                resource_name=str(path),
                message=f"Invalid checkpoint file: {e}",
            ) from e

    def save(self, checkpoint: SyncCheckpoint):
        """Save checkpoint to file.

        Args:
            checkpoint: Checkpoint to save.
        """
        path = self._get_checkpoint_path(
            checkpoint.sync_type,
            checkpoint.source_host,
            checkpoint.target_host,
        )

        checkpoint.last_checkpoint_time = datetime.utcnow().isoformat()

        with open(path, "w") as f:
            json.dump(checkpoint.to_dict(), f, indent=2)

    def delete(self, sync_type: str, source_host: str, target_host: str):
        """Delete checkpoint file.

        Args:
            sync_type: Type of sync operation.
            source_host: Source workspace host.
            target_host: Target workspace host.
        """
        path = self._get_checkpoint_path(sync_type, source_host, target_host)
        if path.exists():
            path.unlink()

    def list_checkpoints(self) -> List[Dict[str, Any]]:
        """List all checkpoint files with metadata.

        Returns:
            List of checkpoint metadata dicts.
        """
        checkpoints = []

        for path in self.state_dir.glob("*.json"):
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                checkpoints.append(
                    {
                        "file": str(path),
                        "sync_type": data.get("sync_type"),
                        "source_host": data.get("source_host"),
                        "target_host": data.get("target_host"),
                        "started_at": data.get("started_at"),
                        "last_checkpoint_time": data.get("last_checkpoint_time"),
                        "completed_count": len(data.get("completed_items", [])),
                        "failed_count": len(data.get("failed_items", {})),
                    }
                )
            except (json.JSONDecodeError, KeyError):
                # Skip invalid checkpoint files
                continue

        return checkpoints
