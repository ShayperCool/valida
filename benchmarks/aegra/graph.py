"""Deterministic two-stage async graph for the Aegra/Valida benchmark."""

import asyncio
from typing import TypedDict
from langgraph.graph import END, START, StateGraph


class State(TypedDict, total=False):
    seed: int
    increment: int
    intermediate: int
    result: int


async def add(state: State) -> dict[str, int]:
    await asyncio.sleep(0.2)
    return {"intermediate": state["seed"] + state["increment"]}


async def double(state: State) -> dict[str, int]:
    await asyncio.sleep(0.2)
    return {"result": state["intermediate"] * 2}


builder = StateGraph(State)
builder.add_node("add", add)
builder.add_node("double", double)
builder.add_edge(START, "add")
builder.add_edge("add", "double")
builder.add_edge("double", END)
graph = builder.compile()
