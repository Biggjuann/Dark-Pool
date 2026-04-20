"""
REST endpoint for combined swing-trade recommendations.

GET /api/recommendations/
    Returns tickers ranked by a 60% dark-pool + 40% Twitter-sentiment score.
    Use ?min_score= to change the floor (default 50).
    Use ?limit= to change the result count (default 20).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from database import get_db
from signals.recommender import get_recommendations

router = APIRouter(prefix="/api/recommendations", tags=["recommendations"])


class RecommendationOut(BaseModel):
    ticker:             str
    combined_score:     float
    level:              str            # "high" | "medium" | "low"
    dp_score:           float
    dp_pct:             float | None
    volume_spike_ratio: float | None
    sentiment_score:    float | None
    tweet_count:        int
    bullish_count:      int
    bearish_count:      int
    has_sentiment:      bool
    price_close:        float | None
    week_ending:        str
    name:               str | None
    sector:             str | None


@router.get("/", response_model=list[RecommendationOut])
def list_recommendations(
    min_score: float = 50.0,
    limit: int = 20,
    db: Session = Depends(get_db),
):
    """
    Return ranked swing-trade recommendations.

    Scores are computed as: 0.60 × dark_pool_score + 0.40 × sentiment_score.
    Tickers without Twitter coverage are capped at 70 and still returned
    if they exceed min_score.
    """
    return get_recommendations(db, min_score=min_score, limit=limit)
