"""LeadSet: the result of every fetch (flat DataFrame + raw payloads). Nothing is written until you save it."""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from . import clock


@dataclass
class LeadSet:
    """What every fetch returns. Nothing is written to disk until you call CopyTradeService.save().

    df    flat rows (one per lead), see normalize.flatten_lead
    raws  portfolioId -> raw payload (detail, performance{tr}, chart, positionHistory, ...). Empty for quick fetches.
    date  snapshot date (UTC) used as the folder name when saved
    meta  how it was produced (filter, source, stage) — stored in meta.json
    """
    df: pd.DataFrame
    raws: dict = field(default_factory=dict)
    date: str = field(default_factory=clock.today)
    meta: dict = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.df)

    @property
    def ids(self) -> list[str]:
        return [] if self.df.empty else self.df["portfolioId"].astype(str).tolist()

    def raw(self, portfolio_id: str) -> dict | None:
        return self.raws.get(str(portfolio_id))
