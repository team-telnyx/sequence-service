"""Sequence management endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from typing import Optional
import uuid

from src.models.base import get_db
from src.models.models import Sequence, SequenceStep, SequenceStatus

router = APIRouter()


class SequenceCreate(BaseModel):
    name: str
    description: Optional[str] = None


class SequenceUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[SequenceStatus] = None


class SequenceResponse(BaseModel):
    id: str
    name: str
    description: Optional[str]
    status: SequenceStatus
    
    class Config:
        from_attributes = True


@router.get("/")
async def list_sequences(
    request: Request,
    db: AsyncSession = Depends(get_db),
    limit: int = 50,
    offset: int = 0,
):
    """List all sequences for the tenant."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(Sequence)
        .where(Sequence.tenant_id == tenant_id)
        .limit(limit)
        .offset(offset)
    )
    sequences = result.scalars().all()
    
    return {"data": [SequenceResponse.model_validate(s) for s in sequences]}


@router.get("/{sequence_id}")
async def get_sequence(
    sequence_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Get a specific sequence."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(Sequence)
        .where(Sequence.id == sequence_id, Sequence.tenant_id == tenant_id)
    )
    sequence = result.scalar_one_or_none()
    
    if not sequence:
        raise HTTPException(status_code=404, detail="Sequence not found")
    
    return {"data": SequenceResponse.model_validate(sequence)}


@router.post("/", status_code=201)
async def create_sequence(
    data: SequenceCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Create a new sequence."""
    tenant_id = request.state.tenant_id
    
    sequence = Sequence(
        id=str(uuid.uuid4()),
        tenant_id=tenant_id,
        name=data.name,
        description=data.description,
    )
    
    db.add(sequence)
    await db.commit()
    await db.refresh(sequence)
    
    return {"data": SequenceResponse.model_validate(sequence)}


@router.put("/{sequence_id}")
async def update_sequence(
    sequence_id: str,
    data: SequenceUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Update a sequence."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(Sequence)
        .where(Sequence.id == sequence_id, Sequence.tenant_id == tenant_id)
    )
    sequence = result.scalar_one_or_none()
    
    if not sequence:
        raise HTTPException(status_code=404, detail="Sequence not found")
    
    if data.name is not None:
        sequence.name = data.name
    if data.description is not None:
        sequence.description = data.description
    if data.status is not None:
        sequence.status = data.status
    
    await db.commit()
    await db.refresh(sequence)
    
    return {"data": SequenceResponse.model_validate(sequence)}


class StepCreate(BaseModel):
    step_number: int
    subject_template: Optional[str] = None
    body_template: Optional[str] = None
    subject: Optional[str] = None
    body: Optional[str] = None
    delay_days: int = 0
    delay_hours: int = 0


class StepResponse(BaseModel):
    id: str
    step_number: int
    subject: str
    body: str
    delay_days: int
    delay_hours: int

    class Config:
        from_attributes = True


@router.post("/{sequence_id}/steps", status_code=201)
async def create_step(
    sequence_id: str,
    data: StepCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Create a step for a sequence."""
    tenant_id = request.state.tenant_id

    result = await db.execute(
        select(Sequence).where(Sequence.id == sequence_id, Sequence.tenant_id == tenant_id)
    )
    sequence = result.scalar_one_or_none()
    if not sequence:
        raise HTTPException(status_code=404, detail="Sequence not found")

    # Check for existing step with same number
    existing = await db.execute(
        select(SequenceStep).where(
            SequenceStep.sequence_id == sequence_id,
            SequenceStep.step_number == data.step_number,
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail=f"Step {data.step_number} already exists")

    step = SequenceStep(
        id=str(uuid.uuid4()),
        sequence_id=sequence_id,
        step_number=data.step_number,
        subject=data.subject_template or data.subject or "{{subject}}",
        body=data.body_template or data.body or "{{body}}",
        delay_days=data.delay_days,
        delay_hours=data.delay_hours,
    )
    db.add(step)
    await db.commit()
    await db.refresh(step)

    return {"data": StepResponse.model_validate(step)}


@router.get("/{sequence_id}/steps")
async def list_steps(
    sequence_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """List steps for a sequence."""
    tenant_id = request.state.tenant_id

    result = await db.execute(
        select(Sequence).where(Sequence.id == sequence_id, Sequence.tenant_id == tenant_id)
    )
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Sequence not found")

    steps_result = await db.execute(
        select(SequenceStep)
        .where(SequenceStep.sequence_id == sequence_id)
        .order_by(SequenceStep.step_number)
    )
    steps = steps_result.scalars().all()
    return {"data": [StepResponse.model_validate(s) for s in steps]}


@router.delete("/{sequence_id}", status_code=204)
async def delete_sequence(
    sequence_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Delete a sequence."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(Sequence)
        .where(Sequence.id == sequence_id, Sequence.tenant_id == tenant_id)
    )
    sequence = result.scalar_one_or_none()
    
    if not sequence:
        raise HTTPException(status_code=404, detail="Sequence not found")
    
    await db.delete(sequence)
    await db.commit()
