"""
Watchlist router — user-managed ticker watch list.

Routes
------
GET    /api/watchlist       — all entries ordered by added_date desc
POST   /api/watchlist       — add a ticker (idempotent)
PATCH  /api/watchlist/{id}  — update status and/or notes
DELETE /api/watchlist/{id}  — remove an entry
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy.orm import Session

from database import get_db
from models import WatchlistEntry, WatchlistStatus

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])

# Valid status values exposed to the API layer (mirrors WatchlistStatus enum)
_VALID_STATUSES = {s.value for s in WatchlistStatus}


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class WatchlistOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:          int
    ticker:      str
    added_date:  date
    entry_price: float | None
    notes:       str   | None
    status:      str              # "watching" | "entered" | "closed"


class WatchlistCreate(BaseModel):
    ticker:      str
    entry_price: float | None = None
    notes:       str   | None = None

    @field_validator("ticker")
    @classmethod
    def normalise_ticker(cls, v: str) -> str:
        return v.upper().strip()


class WatchlistUpdate(BaseModel):
    """All fields optional — send only what you want to change."""
    status: Optional[Literal["watching", "entered", "closed"]] = None
    notes:  Optional[str]                                       = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_out(entry: WatchlistEntry) -> WatchlistOut:
    """Convert a WatchlistEntry ORM object to the API response model."""
    return WatchlistOut(
        id          = entry.id,
        ticker      = entry.ticker,
        added_date  = entry.added_date,
        entry_price = entry.entry_price,
        notes       = entry.notes,
        status      = entry.status.value if isinstance(entry.status, WatchlistStatus) else str(entry.status),
    )


def _get_entry_or_404(db: Session, entry_id: int) -> WatchlistEntry:
    entry = db.get(WatchlistEntry, entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Watchlist entry {entry_id} not found")
    return entry


# ---------------------------------------------------------------------------
# GET /api/watchlist
# ---------------------------------------------------------------------------

@router.get("/", response_model=list[WatchlistOut])
def list_watchlist(db: Session = Depends(get_db)):
    """Return all watchlist entries ordered by most-recently added first."""
    entries = (
        db.query(WatchlistEntry)
        .order_by(WatchlistEntry.added_date.desc(), WatchlistEntry.id.desc())
        .all()
    )
    return [_to_out(e) for e in entries]


# ---------------------------------------------------------------------------
# POST /api/watchlist
# ---------------------------------------------------------------------------

@router.post("/", response_model=WatchlistOut, status_code=200)
def add_to_watchlist(
    body: WatchlistCreate,
    db:   Session = Depends(get_db),
):
    """
    Add *ticker* to the watchlist.

    Idempotent: if the ticker is already present, updates ``entry_price``
    and ``notes`` with the new values (if provided) and returns the entry.
    """
    existing = (
        db.query(WatchlistEntry)
        .filter(WatchlistEntry.ticker == body.ticker)
        .first()
    )

    if existing:
        # Update mutable fields if supplied
        if body.entry_price is not None:
            existing.entry_price = body.entry_price
        if body.notes is not None:
            existing.notes = body.notes
        db.commit()
        db.refresh(existing)
        return _to_out(existing)

    entry = WatchlistEntry(
        ticker      = body.ticker,
        added_date  = date.today(),
        entry_price = body.entry_price,
        notes       = body.notes,
        status      = WatchlistStatus.watching,
    )
    db.add(entry)
    db.commit()
    db.refresh(entry)
    return _to_out(entry)


# ---------------------------------------------------------------------------
# PATCH /api/watchlist/{id}
# ---------------------------------------------------------------------------

@router.patch("/{entry_id}", response_model=WatchlistOut)
def update_watchlist_entry(
    entry_id: int,
    body:     WatchlistUpdate,
    db:       Session = Depends(get_db),
):
    """
    Partially update *status* and/or *notes* on a watchlist entry.

    Only supplied (non-None) fields are changed.
    """
    entry = _get_entry_or_404(db, entry_id)

    if body.status is not None:
        entry.status = WatchlistStatus(body.status)
    if body.notes is not None:
        entry.notes = body.notes

    db.commit()
    db.refresh(entry)
    return _to_out(entry)


# ---------------------------------------------------------------------------
# DELETE /api/watchlist/{id}
# ---------------------------------------------------------------------------

@router.delete("/{entry_id}", status_code=204)
def delete_watchlist_entry(
    entry_id: int,
    db:       Session = Depends(get_db),
):
    """
    Remove a watchlist entry by ID.

    Returns 204 on success.  Returns 404 if the entry does not exist.
    """
    entry = _get_entry_or_404(db, entry_id)
    db.delete(entry)
    db.commit()
    return Response(status_code=204)
