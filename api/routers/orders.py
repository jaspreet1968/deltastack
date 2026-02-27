"""Order lifecycle endpoints – view and track orders."""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Query

from deltastack.db.dao_orders import list_orders, get_order

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/orders", tags=["orders"])


@router.get("")
def get_orders(limit: int = Query(50, ge=1, le=500)):
    """Return recent orders from the orders table."""
    orders = list_orders(limit=limit)
    for o in orders:
        for k in ("created_at", "updated_at"):
            if o.get(k):
                o[k] = str(o[k])
        for k in ("request_json", "response_json"):
            if isinstance(o.get(k), str):
                try:
                    o[k] = json.loads(o[k])
                except (json.JSONDecodeError, TypeError):
                    pass
    return {"orders": orders, "count": len(orders)}


@router.get("/{order_id}")
def get_order_detail(order_id: str):
    """Return a single order by ID."""
    order = get_order(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    for k in ("created_at", "updated_at"):
        if order.get(k):
            order[k] = str(order[k])
    for k in ("request_json", "response_json"):
        if isinstance(order.get(k), str):
            try:
                order[k] = json.loads(order[k])
            except (json.JSONDecodeError, TypeError):
                pass
    return order
