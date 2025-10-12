# gtt_core.py - Core business logic classes

from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ActionType(Enum):
    PLACE = "PLACE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    UNKNOWN = "UNKNOWN"


class TransactionSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class GTTInstruction:
    """Represents a single GTT instruction with validated data"""
    ticker: str
    type: str
    units: int
    gtt_price: float
    live_price: float
    tick_size: float
    action: ActionType
    method: str
    gtt_date: str = ""
    row_number: int = 0
    
    @property
    def exchange(self) -> str:
        """Extract exchange from ticker"""
        if ":" in self.ticker:
            return self.ticker.split(":")[0].strip()
        return "NSE"
    
    @property
    def symbol(self) -> str:
        """Extract symbol from ticker"""
        if ":" in self.ticker:
            return self.ticker.split(":")[1].strip()
        return self.ticker.strip()
    
    @property
    def transaction_side(self) -> TransactionSide:
        """Determine transaction side from type"""
        normalized = TypeNormalizer.normalize_for_matching(self.type)
        return TransactionSide.BUY if normalized == "BUY" else TransactionSide.SELL
    
    @property
    def limit_price(self) -> float:
        """Calculate limit price based on side and tick size"""
        multiplier = 1 if self.transaction_side == TransactionSide.BUY else -1
        return self.gtt_price + multiplier * self.tick_size


@dataclass
class GTTDataRow:
    """Represents a row in the GTT data sheet"""
    ticker: str
    type: str
    units: int
    gtt_price: float
    gtt_date: str
    gtt_id: str
    row_number: int = 0


@dataclass
class ProcessingResult:
    """Result of processing a batch of GTT instructions"""
    total_processed: int
    failed_rows: List[Dict[str, Any]]
    conflict_rows: List[Dict[str, Any]]
    
    def add_failure(self, row_number: int, reason: str):
        """Add a failed row to the result"""
        self.failed_rows.append({"row_number": row_number, "reason": reason})
    
    def add_conflict(self, row_number: int):
        """Add a conflict row to the result"""
        self.conflict_rows.append({"row_number": row_number})


class TypeNormalizer:
    """Handles normalization of trading types for matching"""
    
    BUY_KEYWORDS = [" BUY", "RTP_BUY", "KWK", "SIP_REG"]
    SELL_KEYWORDS = [" SELL", "RTP_SELL"]
    TSL_KEYWORDS = ["TSL"]
    
    @classmethod
    def normalize_for_matching(cls, raw_type: str) -> str:
        """Normalize type string for matching purposes"""
        if raw_type is None:
            return ""
        
        raw = raw_type.strip().upper()
        
        if any(keyword in raw_type for keyword in cls.BUY_KEYWORDS):
            return "BUY"
        
        if any(keyword in raw_type for keyword in cls.SELL_KEYWORDS):
            return "SELL"
        
        if raw_type.startswith("TSL") or raw.startswith("TSL"):
            return "SELL"
        
        return raw


class DataParser:
    """Handles parsing and validation of raw data"""
    
    @staticmethod
    def parse_float_safe(value: Any) -> float:
        """Safely parse a value to float"""
        try:
            return float(str(value).replace(",", "").strip())
        except (ValueError, TypeError):
            return 0.0
    
    @staticmethod
    def parse_int_safe(value: Any) -> int:
        """Safely parse a value to int"""
        try:
            return int(str(value).replace(",", "").strip())
        except (ValueError, TypeError):
            return 0
    
    @staticmethod
    def determine_action(raw_action: str) -> ActionType:
        """Determine action type from raw string"""
        if not raw_action:
            return ActionType.UNKNOWN
        
        raw_action = raw_action.strip().upper()
        
        if "INSERT" in raw_action or "PLACE" in raw_action:
            return ActionType.PLACE
        if "UPDATE" in raw_action:
            return ActionType.UPDATE
        if "DELETE" in raw_action:
            return ActionType.DELETE
        
        return ActionType.UNKNOWN
    
    @classmethod
    def parse_instruction(cls, raw_instruction: Dict[str, Any], row_number: int) -> Optional[GTTInstruction]:
        """Parse raw instruction data into GTTInstruction object"""
        try:
            ticker = raw_instruction.get("TICKER", "").strip()
            type_str = raw_instruction.get("TYPE", "").strip()
            raw_action = raw_instruction.get("ACTION", "").strip()
            
            if not all([ticker, type_str, raw_action]):
                return None
            
            return GTTInstruction(
                ticker=ticker,
                type=type_str,
                units=cls.parse_int_safe(raw_instruction.get("UNITS", "0")),
                gtt_price=cls.parse_float_safe(raw_instruction.get("GTT PRICE", "0")),
                live_price=cls.parse_float_safe(raw_instruction.get("LIVE PRICE", "0")),
                tick_size=cls.parse_float_safe(raw_instruction.get("TICK SIZE", "0")),
                action=cls.determine_action(raw_action),
                method=raw_instruction.get("METHOD", "").strip(),
                gtt_date=raw_instruction.get("GTT DATE", "").strip(),
                row_number=row_number
            )
        except Exception as e:
            logger.error(f"Error parsing instruction at row {row_number}: {e}")
            return None
    
    @classmethod
    def parse_data_row(cls, raw_data: Dict[str, Any], row_number: int) -> GTTDataRow:
        """Parse raw data row into GTTDataRow object"""
        return GTTDataRow(
            ticker=raw_data.get("TICKER", "").strip(),
            type=raw_data.get("TYPE", "").strip(),
            units=cls.parse_int_safe(raw_data.get("UNITS", "0")),
            gtt_price=cls.parse_float_safe(raw_data.get("GTT PRICE", "0")),
            gtt_date=raw_data.get("GTT DATE", "").strip(),
            gtt_id=raw_data.get("GTT_ID", "").strip(),
            row_number=row_number
        )


class GTTMatcher:
    """Handles matching logic between instructions and existing data"""
    
    @staticmethod
    def match_for_exact_comparison(instruction: GTTInstruction, data_row: GTTDataRow) -> bool:
        """Match instruction and data row on all 4 key elements"""
        # Normalize types for comparison
        instr_type = TypeNormalizer.normalize_for_matching(instruction.type)
        data_type = TypeNormalizer.normalize_for_matching(data_row.type)
        
        return (
            instruction.ticker == data_row.ticker and
            instr_type == data_type and
            instruction.units == data_row.units and
            instruction.gtt_price == data_row.gtt_price
        )
    
    @staticmethod
    def match_for_update(instruction: GTTInstruction, data_row: GTTDataRow) -> bool:
        """Match instruction and data row on ticker and type only (for updates)"""
        instr_type = TypeNormalizer.normalize_for_matching(instruction.type)
        data_type = TypeNormalizer.normalize_for_matching(data_row.type)
        
        return (
            instruction.ticker == data_row.ticker and
            instr_type == data_type
        )
    
    @classmethod
    def find_matching_rows(
        cls, 
        instruction: GTTInstruction, 
        data_rows: List[GTTDataRow], 
        update_match: bool = False
    ) -> List[GTTDataRow]:
        """Find all matching data rows for an instruction"""
        if update_match:
            return [row for row in data_rows if cls.match_for_update(instruction, row)]
        else:
            return [row for row in data_rows if cls.match_for_exact_comparison(instruction, row)]
