import re
from typing import List, Optional
from domain.chaves_expression import ChavesExpression
from repositories.chaves_expression_repository import ChavesExpressionRepository

class ChavesExpressionService:
    def __init__(self):
        self.repository = ChavesExpressionRepository()

    def get_all_active(self) -> List[ChavesExpression]:
        return self.repository.get_all_active()
    
    def get_patterns(self) -> List[re.Pattern]:
        expressions = self.get_all_active()
        return [re.compile(expr.expressao) for expr in expressions]
    
    def match_any_pattern(self, text: str, patterns: list[re.Pattern]) -> Optional[str]:
        for pattern in patterns:           
            match = pattern.search(text)
            if match:
                return match.group()
        return None