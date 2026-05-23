from pydantic import BaseModel
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


class TransformationConfig(BaseModel):
    type: str  # 'sql' or 'python'
    view_name: str
    depends_on: List[str] = []
    query: Optional[str] = None
    function_path: Optional[str] = None  # FIXED: was 'ring'
    input_views: List[str] = []


class WriterConfig(BaseModel):
    write_view_name: str
    type: str
    path: str
    mode: str = "append"
    partition_by: List[str] = []


class PipelineConfig(BaseModel):
    reader: Dict[str, ReaderConfig]
    transformation: Dict[str, TransformationConfig]
    writer: Dict[str, WriterConfig]
    tokens: List[Token]

    @property
    def token_dict(self) -> Dict[str, Any]:
        return {t.name: t.value for t in self.tokens}
