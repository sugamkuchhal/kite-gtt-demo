# gtt_services.py - Service layer for external API calls and sheet operations

from typing import Dict, List, Optional, Any, Protocol
from kiteconnect import KiteConnect, exceptions as kite_exceptions
import logging

from gtt_core import GTTInstruction, GTTDataRow, TransactionSide, ActionType

logger = logging.getLogger(__name__)


class SheetOperations(Protocol):
    """Protocol for sheet operations to allow for easy testing/mocking"""
    
    def update_status(self, row_number: int, status: str) -> None:
        """Update status for a given row"""
        ...
    
    def update_data_row(self, row_number: int, data: Dict[str, Any], header: List[str]) -> None:
        """Update a data row with new values"""
        ...
    
    def delete_data_row(self, row_number: int) -> None:
        """Delete a data row"""
        ...
    
    def append_data_row(self, data: Dict[str, Any], header: List[str]) -> None:
        """Append a new data row"""
        ...
    
    def find_data_row_by_gtt_id(self, gtt_id: str) -> Optional[int]:
        """Find row number by GTT ID"""
        ...


class GoogleSheetOperations:
    """Implementation of sheet operations for Google Sheets"""
    
    def __init__(self, instruction_sheet, data_sheet):
        self.instruction_sheet = instruction_sheet
        self.data_sheet = data_sheet
    
    def update_status(self, row_number: int, status: str) -> None:
        """Update status column for instruction sheet"""
        try:
            headers = self.instruction_sheet.row_values(1)
            try:
                status_col = headers.index("STATUS") + 1
            except ValueError:
                status_col = len(headers) + 1
                self.instruction_sheet.update_cell(1, status_col, "STATUS")
            
            existing = self.instruction_sheet.cell(row_number, status_col).value
            if existing:
                new_status = existing + " | " + status
            else:
                new_status = status
            
            self.instruction_sheet.update_cell(row_number, status_col, new_status)
        except Exception as e:
            logger.error(f"Failed to update status for row {row_number}: {e}")
    
    def update_data_row(self, row_number: int, data: Dict[str, Any], header: List[str]) -> None:
        """Update data sheet row with new values"""
        try:
            limited_header = header[:5]  # Maintain original behavior
            values = [data.get(col, "") for col in limited_header]
            range_str = f"A{row_number}:E{row_number}"
            self.data_sheet.update(range_name=range_str, values=[values])
        except Exception as e:
            logger.error(f"Failed to update data row {row_number}: {e}")
            raise
    
    def delete_data_row(self, row_number: int) -> None:
        """Delete a row from data sheet"""
        try:
            self.data_sheet.delete_rows(row_number)
        except Exception as e:
            logger.error(f"Failed to delete data row {row_number}: {e}")
            raise
    
    def append_data_row(self, data: Dict[str, Any], header: List[str]) -> None:
        """Append new row to data sheet"""
        try:
            values = [data.get(col, "") for col in header]
            self.data_sheet.append_row(values)
        except Exception as e:
            logger.error(f"Failed to append data row: {e}")
            raise
    
    def find_data_row_by_gtt_id(self, gtt_id: str) -> Optional[int]:
        """Find row number by GTT ID"""
        try:
            data_rows = self.data_sheet.get_all_records()
            for i, row in enumerate(data_rows, start=2):
                if str(row.get("GTT_ID", "")).strip() == str(gtt_id).strip():
                    return i
            return None
        except Exception as e:
            logger.error(f"Failed to find row by GTT ID {gtt_id}: {e}")
            return None


class KiteGTTService:
    """Service for handling Kite GTT operations"""
    
    def __init__(self, kite: KiteConnect):
        self.kite = kite
    
    def _create_order_payload(self, instruction: GTTInstruction) -> Dict[str, Any]:
        """Create order payload from instruction"""
        return {
            "exchange": instruction.exchange,
            "tradingsymbol": instruction.symbol,
            "transaction_type": instruction.transaction_side.value,
            "quantity": instruction.units,
            "order_type": "LIMIT",
            "product": "CNC",
            "price": instruction.limit_price,
            "validity": "DAY",
            "disclosed_quantity": 0,
            "trigger_price": instruction.gtt_price,
            "tag": instruction.method,
        }
    
    def place_gtt(self, instruction: GTTInstruction) -> str:
        """Place a new GTT order"""
        order = self._create_order_payload(instruction)
        
        logger.debug(
            f"Placing GTT for row {instruction.row_number}: "
            f"symbol={instruction.symbol}, exchange={instruction.exchange}, "
            f"trigger_price={instruction.gtt_price}, last_price={instruction.live_price}"
        )
        
        payload = {
            "trigger_type": "single",
            "tradingsymbol": instruction.symbol,
            "exchange": instruction.exchange,
            "trigger_values": [instruction.gtt_price],
            "last_price": instruction.live_price,
            "orders": [order],
        }
        
        logger.debug(f"GTT payload: {payload}")
        
        try:
            response = self.kite.place_gtt(**payload)
            logger.debug(f"Raw GTT response for row {instruction.row_number}: {response}")
            
            gtt_id = response.get("trigger_id")
            if not gtt_id:
                raise Exception(f"GTT response missing ID. Response: {response}")
            
            return str(gtt_id)
            
        except kite_exceptions.KiteException as e:
            logger.error(f"Kite error placing GTT: {e}")
            raise
        except Exception as e:
            logger.error(f"Error placing GTT: {e}")
            raise
    
    def modify_gtt(self, gtt_id: str, instruction: GTTInstruction) -> None:
        """Modify an existing GTT order"""
        order = self._create_order_payload(instruction)
        
        logger.debug(f"Modifying GTT {gtt_id} for row {instruction.row_number}")
        
        try:
            self.kite.modify_gtt(
                gtt_id,
                tradingsymbol=instruction.symbol,
                exchange=instruction.exchange,
                trigger_type="single",
                trigger_values=[instruction.gtt_price],
                last_price=instruction.live_price,
                orders=[order],
            )
        except kite_exceptions.KiteException as e:
            logger.error(f"Kite error modifying GTT {gtt_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error modifying GTT {gtt_id}: {e}")
            raise
    
    def delete_gtt(self, gtt_id: str) -> None:
        """Delete a GTT order"""
        logger.debug(f"Deleting GTT {gtt_id}")
        
        try:
            self.kite.delete_gtt(gtt_id)
        except kite_exceptions.KiteException as e:
            logger.error(f"Kite error deleting GTT {gtt_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error deleting GTT {gtt_id}: {e}")
            raise


class GTTValidationService:
    """Service for validating GTT operations"""
    
    @staticmethod
    def validate_instruction(instruction: GTTInstruction) -> Optional[str]:
        """Validate instruction data, return error message if invalid"""
        if not instruction.ticker:
            return "Missing TICKER"
        
        if not instruction.type:
            return "Missing TYPE"
        
        if instruction.action == ActionType.UNKNOWN:
            return "Unknown ACTION"
        
        if instruction.units <= 0:
            return "Invalid UNITS"
        
        if instruction.gtt_price <= 0:
            return "Invalid GTT PRICE"
        
        return None
    
    @staticmethod
    def should_update_gtt(instruction: GTTInstruction, existing_data: GTTDataRow) -> bool:
        """Check if GTT actually needs updating"""
        return not (
            existing_data.units == instruction.units and
            existing_data.gtt_price == instruction.limit_price
        )


class StatusMessages:
    """Centralized status messages for consistency"""
    
    PLACED = "✅ placed"
    UPDATED = "✅ updated"
    DELETED = "✅ deleted"
    DUPLICATE_FOUND = "⚠️ duplicate found"
    NO_UPDATE_NEEDED = "no update needed"
    
    MISSING_FIELD = "❌ MISSING FIELD"
    NO_GTT_ID_UPDATE = "❌ no gtt_id to update"
    NO_GTT_ID_DELETE = "❌ no gtt_id to delete"
    NO_MATCH_FOUND = "❌ no match found"
    MULTIPLE_MATCHES = "❌ conflict: multiple matches"
    UNKNOWN_ACTION = "❌ unknown action"
    
    @staticmethod
    def error(message: str) -> str:
        return f"❌ error: {message}"
    
    @staticmethod
    def kite_error(message: str) -> str:
        return f"❌ Kite error: {message}"
    
    @staticmethod
    def exception(message: str) -> str:
        return f"❌ exception: {message}"
