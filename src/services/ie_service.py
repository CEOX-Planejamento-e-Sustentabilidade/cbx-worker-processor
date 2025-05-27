from functools import lru_cache
import re
from repositories.ie_repository import IeRepository

class IeService:
    def __init__(self):
        self.repository = IeRepository()

    def get_municipio(self, ie_value: int) -> str:
        return self.repository.get_municipio(ie_value)
    
    @lru_cache(maxsize=None)
    def get_municipio_cached(self, ie_value):
        digits = re.sub(r'\D', '', str(ie_value))
        digits = int(digits) if digits.isdigit() else 0
        result = self.get_municipio(digits)
        return result.upper() if result else ''
    