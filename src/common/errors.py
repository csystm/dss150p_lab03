"""Typed pipeline errors that carry stage and run context."""


class PipelineStageError(RuntimeError):
    """Raised when a named pipeline stage fails.

    Carries the stage name and run_id so log lines and Airflow callbacks can
    identify *which* stage failed *within which* run without parsing free text.
    """

    def __init__(self, stage: str, run_id: str, message: str):
        super().__init__(f'[{stage}] run_id={run_id}: {message}')
        self.stage = stage
        self.run_id = run_id