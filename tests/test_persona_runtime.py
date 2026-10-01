from __future__ import annotations

from grouppig.perception.runtime.persona import RuntimeSnapshotBuilder


def persona(version=2):
    return {"persona_id": "pig", "version": version, "personality": {"warmth": 0.8}, "quirks": {"emoji": ["pig"]}}


def test_snapshot_is_stable_and_tracks_versions():
    builder = RuntimeSnapshotBuilder(default_ttl=10)
    first = builder.build(persona(), group_id=1, user_id=7, mood={"valence": 0.2}, source_versions={"mood": 3}, now=100)
    second = builder.build(
        persona(), group_id=1, user_id=7, mood={"valence": 0.2}, source_versions={"mood": 3}, now=200
    )
    assert first.content_hash == second.content_hash
    assert first.snapshot_id == second.snapshot_id
    assert first.persona.version == 2
    assert first.source_versions["mood"] == 3


def test_ttl_and_zero_ttl_are_explicit():
    builder = RuntimeSnapshotBuilder(default_ttl=5)
    snapshot = builder.build(persona(), now=10)
    assert snapshot.is_valid(14.99)
    assert not snapshot.is_valid(15)
    assert not builder.build(persona(), ttl=0, now=10).is_valid(10)


def test_input_and_output_are_isolated_and_read_only():
    raw = persona()
    snapshot = RuntimeSnapshotBuilder().build(raw, mood={"energy": 0.4})
    raw["personality"]["warmth"] = 0
    assert snapshot.persona.to_dict()["personality"]["warmth"] == 0.8
    try:
        snapshot.components["mood"] = {}  # type: ignore[index]
    except TypeError:
        pass
    else:
        raise AssertionError("components must be immutable")


def test_missing_components_are_degraded_and_scoped():
    snapshot = RuntimeSnapshotBuilder().build(persona(), group_id=11, user_id=22, topic={"topic_id": "t1"})
    assert snapshot.degraded is True
    assert snapshot.missing == ("mood", "relationship", "strategy")
    assert snapshot.degraded_reasons == ("mood_unavailable", "relationship_unavailable", "strategy_unavailable")
    assert (snapshot.group_id, snapshot.user_id) == (11, 22)
