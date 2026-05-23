from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional


class Token(BaseModel):
    name: str
    value: Any
    description: Optional[str] = None


class ReaderConfig(BaseModel):
    type: str
    view_name: str
    path: Optional[str] = None
    header: bool = True
    infer_schema: bool = True


class TransformationStep(BaseModel):
    id: str
    type: str  # 'sql' or 'python'
    # Make view_name optional so Pydantic doesn't crash if it's missing
    view_name: Optional[str] = None
    # Add target_view as an optional field to support your 'anonymize' logic
    source_view: Optional[str] = None
    target_view: Optional[str] = None
    depends_on: List[str] = []
    query: Optional[str] = None
    function_path: Optional[str] = None  # FIXED: was 'ring'
    input_views: List[str] = []
    # --- SECURITY PROPERTIES (The missing pieces) ---
    # This allows the engine to look up a template in 'security_templates'
    policy_ref: Optional[str] = None
    # This allows the user to define rules directly inside the transformation step
    inline_policy: Optional[Dict[str, Any]] = None
    # A helper property to always get the "output" view regardless of which key was used
    input_views: List[str] = []
    @property
    def effective_view_name(self) -> str:
        return self.target_view or self.view_name


class WriterConfig(BaseModel):
    write_view_name: str
    type: str
    path: str
    mode: str = "append"
    partition_by: List[str] = []


class PipelineConfig(BaseModel):

    reader: Dict[str, ReaderConfig]
    # CHANGE THIS FROM Dict to List
    transformation: List[TransformationStep]
    # This is the missing piece!
    # It allows Pydantic to capture the templates from your JSON.
    security_templates: Dict[str, Any] = Field(default_factory=dict)
    writer: Dict[str, WriterConfig]
    tokens: List[Token]
    security_policy: Optional[Dict[str, Any]] = None

    @property
    def token_dict(self) -> Dict[str, Any]:
        return {t.name: t.value for t in self.tokens}
