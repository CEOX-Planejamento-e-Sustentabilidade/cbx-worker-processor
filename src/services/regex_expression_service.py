import re
from typing import List, Optional
from domain.regex_expression import RegexExpression
from repositories.regex_expression_repository import RegexExpressionRepository
from services.utils import contem_hora

class RegexExpressionService:    
    def __init__(self):
        self.repository = RegexExpressionRepository()
        self.alvo_nf_chave = 'nf_chave'
        self.alvo_nf_ie_emissor = 'nf_ie_emissor'
        self.alvo_nf_ie_destinatario = 'nf_ie_destinatario'
        self.alvo_nf_tipo = 'nf_tipo'

    def get_all_active(self) -> List[RegexExpression]:
        return self.repository.get_all_active()
       
    def get_pattern_by_alvo(self, expressions: List[RegexExpression], alvo: str) -> List[re.Pattern]:        
        filtered = [expr for expr in expressions if expr.alvo == alvo]
        return [re.compile(expr.expressao) for expr in filtered]
    
    def match_pattern(self, text: str, patterns: list[re.Pattern]) -> re.Match[str]:
        for pattern in patterns:
            match = pattern.search(text)
            if match:
                return match
        return None
    
    def get_group(self, text: str, patterns: list[re.Pattern]) -> Optional[str]:
        match = self.match_pattern(text, patterns)
        if match:
            return match.group()
        return match
    
    def get_after_group(self, text: str, patterns: list[re.Pattern]) -> Optional[str]:
        match = self.match_pattern(text, patterns)
        if match:
            return match.group(1) if len(match.groups()) > 0 else None
        return match
    
    def get_after_group_last_value(self, text: str, patterns: list[re.Pattern]) -> Optional[str]:
        match = self.match_pattern(text, patterns)
        if match:
            result = match.group(1).split()[-1] if len(match.groups()) > 0 else None
            if result:
                if contem_hora(result):
                    return match.group(1).split()[-2]
                return result
        return match    