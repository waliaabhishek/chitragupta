from __future__ import annotations

from datetime import date


class TestEmitOutcome:
    def test_emitted_value(self) -> None:
        from core.emitters.models import EmitOutcome

        assert EmitOutcome.EMITTED.value == "emitted"

    def test_failed_value(self) -> None:
        from core.emitters.models import EmitOutcome

        assert EmitOutcome.FAILED.value == "failed"

    def test_skipped_value(self) -> None:
        from core.emitters.models import EmitOutcome

        assert EmitOutcome.SKIPPED.value == "skipped"

    def test_three_members(self) -> None:
        from core.emitters.models import EmitOutcome

        assert len(list(EmitOutcome)) == 3


class TestEmitManifest:
    def test_fields_populated(self) -> None:
        from core.emitters.models import EmitManifest

        m = EmitManifest(
            pending_dates=[date(2025, 1, 1)],
            total_rows_estimate=10,
            is_reemission=False,
        )
        assert m.pending_dates == [date(2025, 1, 1)]
        assert m.total_rows_estimate == 10
        assert m.is_reemission is False

    def test_is_reemission_true(self) -> None:
        from core.emitters.models import EmitManifest

        m = EmitManifest(pending_dates=[], total_rows_estimate=None, is_reemission=True)
        assert m.is_reemission is True

    def test_total_rows_estimate_none(self) -> None:
        from core.emitters.models import EmitManifest

        m = EmitManifest(pending_dates=[], total_rows_estimate=None, is_reemission=False)
        assert m.total_rows_estimate is None

    def test_pending_dates_empty(self) -> None:
        from core.emitters.models import EmitManifest

        m = EmitManifest(pending_dates=[], total_rows_estimate=0, is_reemission=False)
        assert list(m.pending_dates) == []


class TestEmitResult:
    def test_outcomes_default_empty(self) -> None:
        from core.emitters.models import EmitResult

        r = EmitResult()
        assert r.outcomes == {}

    def test_outcomes_with_emitted_value(self) -> None:
        from core.emitters.models import EmitOutcome, EmitResult

        r = EmitResult(outcomes={date(2025, 1, 1): EmitOutcome.EMITTED})
        assert r.outcomes[date(2025, 1, 1)] == EmitOutcome.EMITTED

    def test_outcomes_with_failed_value(self) -> None:
        from core.emitters.models import EmitOutcome, EmitResult

        r = EmitResult(outcomes={date(2025, 1, 2): EmitOutcome.FAILED})
        assert r.outcomes[date(2025, 1, 2)] == EmitOutcome.FAILED


class TestEmissionRecord:
    def test_fields_populated(self) -> None:
        from core.emitters.models import EmissionRecord

        rec = EmissionRecord(
            ecosystem="eco",
            tenant_id="t1",
            emitter_name="csv",
            date=date(2025, 1, 1),
            status="emitted",
        )
        assert rec.ecosystem == "eco"
        assert rec.tenant_id == "t1"
        assert rec.emitter_name == "csv"
        assert rec.date == date(2025, 1, 1)
        assert rec.status == "emitted"

    def test_attempt_count_default_is_1(self) -> None:
        from core.emitters.models import EmissionRecord

        rec = EmissionRecord(
            ecosystem="eco",
            tenant_id="t1",
            emitter_name="csv",
            date=date(2025, 1, 1),
            status="emitted",
        )
        assert rec.attempt_count == 1

    def test_attempt_count_custom(self) -> None:
        from core.emitters.models import EmissionRecord

        rec = EmissionRecord(
            ecosystem="eco",
            tenant_id="t1",
            emitter_name="csv",
            date=date(2025, 1, 1),
            status="failed",
            attempt_count=3,
        )
        assert rec.attempt_count == 3

    def test_status_failed(self) -> None:
        from core.emitters.models import EmissionRecord

        rec = EmissionRecord(
            ecosystem="eco",
            tenant_id="t1",
            emitter_name="csv",
            date=date(2025, 1, 1),
            status="failed",
        )
        assert rec.status == "failed"
