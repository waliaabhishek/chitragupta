from __future__ import annotations

# ---------------------------------------------------------------------------
# Verification 3: TopicAttributionConfig._apply_emitter_defaults
# ---------------------------------------------------------------------------


class TestTopicAttributionConfigEmitterDefaults:
    """Verification test 3 from design doc."""

    def test_csv_default_filename_template_injected(self) -> None:
        from plugins.confluent_cloud.config import TopicAttributionConfig

        config = TopicAttributionConfig(
            enabled=True,
            emitters=[{"type": "csv", "params": {}}],
        )
        csv_spec = next(s for s in config.emitters if s.type == "csv")
        assert csv_spec.params["filename_template"] == "topic_attr_{tenant_id}_{date}.csv"

    def test_csv_default_output_dir_injected(self) -> None:
        from plugins.confluent_cloud.config import TopicAttributionConfig

        config = TopicAttributionConfig(
            enabled=True,
            emitters=[{"type": "csv", "params": {}}],
        )
        csv_spec = next(s for s in config.emitters if s.type == "csv")
        assert csv_spec.params["output_dir"] == "/tmp/topic_attribution"

    def test_user_can_override_output_dir(self) -> None:
        """Verification test 3 (override case) from design doc."""
        from plugins.confluent_cloud.config import TopicAttributionConfig

        config = TopicAttributionConfig(
            enabled=True,
            emitters=[
                {"type": "csv", "params": {"output_dir": "/custom", "filename_template": "custom_{tenant_id}.csv"}}
            ],
        )
        csv_spec = next(s for s in config.emitters if s.type == "csv")
        assert csv_spec.params["output_dir"] == "/custom"

    def test_user_can_override_filename_template(self) -> None:
        from plugins.confluent_cloud.config import TopicAttributionConfig

        config = TopicAttributionConfig(
            enabled=True,
            emitters=[{"type": "csv", "params": {"filename_template": "custom_{tenant_id}.csv"}}],
        )
        csv_spec = next(s for s in config.emitters if s.type == "csv")
        assert csv_spec.params["filename_template"] == "custom_{tenant_id}.csv"

    def test_non_csv_emitter_type_not_modified(self) -> None:
        from plugins.confluent_cloud.config import TopicAttributionConfig

        config = TopicAttributionConfig(
            enabled=True,
            emitters=[{"type": "prometheus", "params": {"port": 8000}}],
        )
        prom_spec = next(s for s in config.emitters if s.type == "prometheus")
        assert "filename_template" not in prom_spec.params
        assert "output_dir" not in prom_spec.params

    def test_multiple_emitters_only_csv_gets_defaults(self) -> None:
        from plugins.confluent_cloud.config import TopicAttributionConfig

        config = TopicAttributionConfig(
            enabled=True,
            emitters=[
                {"type": "csv", "params": {}},
                {"type": "prometheus", "params": {"port": 8000}},
            ],
        )
        csv_spec = next(s for s in config.emitters if s.type == "csv")
        prom_spec = next(s for s in config.emitters if s.type == "prometheus")

        assert csv_spec.params["filename_template"] == "topic_attr_{tenant_id}_{date}.csv"
        assert "filename_template" not in prom_spec.params

    def test_no_emitters_list_no_error(self) -> None:
        from plugins.confluent_cloud.config import TopicAttributionConfig

        config = TopicAttributionConfig(enabled=True)
        assert config.emitters == []

    def test_partial_override_output_dir_filename_template_gets_default(self) -> None:
        from plugins.confluent_cloud.config import TopicAttributionConfig

        config = TopicAttributionConfig(
            enabled=True,
            emitters=[{"type": "csv", "params": {"output_dir": "/my/dir"}}],
        )
        csv_spec = next(s for s in config.emitters if s.type == "csv")
        assert csv_spec.params["output_dir"] == "/my/dir"
        # filename_template not provided → should get default
        assert csv_spec.params["filename_template"] == "topic_attr_{tenant_id}_{date}.csv"
