# gtt_processor.py - Main processor class with clean separation of concerns

from typing import List, Tuple
import logging
import traceback

from gtt_core import (
    GTTInstruction, GTTDataRow, ProcessingResult, ActionType, 
    DataParser, GTTMatcher
)
from gtt_services import (
    SheetOperations, KiteGTTService, GTTValidationService, StatusMessages
)

logger = logging.getLogger(__name__)


class GTTProcessor:
    """Main processor for handling GTT operations"""
    
    def __init__(
        self, 
        kite_service: KiteGTTService,
        sheet_operations: SheetOperations,
        data_header: List[str]
    ):
        self.kite_service = kite_service
        self.sheet_operations = sheet_operations
        self.data_header = data_header
        self.validation_service = GTTValidationService()
    
    def process_instruction(
        self, 
        instruction: GTTInstruction, 
        data_rows: List[GTTDataRow]
    ) -> None:
        """Process a single GTT instruction"""
        try:
            # Validate instruction
            validation_error = self.validation_service.validate_instruction(instruction)
            if validation_error:
                self.sheet_operations.update_status(
                    instruction.row_number, 
                    StatusMessages.MISSING_FIELD
                )
                return
            
            # Route to appropriate handler based on action
            if instruction.action == ActionType.PLACE:
                self._handle_place_action(instruction, data_rows)
            elif instruction.action == ActionType.UPDATE:
                self._handle_update_action(instruction, data_rows)
            elif instruction.action == ActionType.DELETE:
                self._handle_delete_action(instruction, data_rows)
            else:
                self.sheet_operations.update_status(
                    instruction.row_number, 
                    StatusMessages.UNKNOWN_ACTION
                )
                
        except Exception as e:
            logger.error(
                f"Exception processing row {instruction.row_number}: "
                f"{traceback.format_exc()}"
            )
            self.sheet_operations.update_status(
                instruction.row_number, 
                StatusMessages.exception(str(e))
            )
    
    def _handle_place_action(
        self, 
        instruction: GTTInstruction, 
        data_rows: List[GTTDataRow]
    ) -> None:
        """Handle PLACE action for GTT"""
        # Check for duplicates
        matches = GTTMatcher.find_matching_rows(instruction, data_rows, update_match=False)
        if matches:
            self.sheet_operations.update_status(
                instruction.row_number, 
                StatusMessages.DUPLICATE_FOUND
            )
            return
        
        try:
            # Place GTT via Kite API
            gtt_id = self.kite_service.place_gtt(instruction)
            
            # Update status
            self.sheet_operations.update_status(
                instruction.row_number, 
                StatusMessages.PLACED
            )
            
            # Add to data sheet
            self._add_to_data_sheet(instruction, gtt_id)
            
        except Exception as e:
            self.sheet_operations.update_status(
                instruction.row_number, 
                StatusMessages.error(str(e))
            )
            raise
    
    def _handle_update_action(
        self, 
        instruction: GTTInstruction, 
        data_rows: List[GTTDataRow]
    ) -> None:
        """Handle UPDATE action for GTT"""
        matches = GTTMatcher.find_matching_rows(instruction, data_rows, update_match=True)
        
        if len(matches) == 1:
            matched_row = matches[0]
            
            if not matched_row.gtt_id:
                self.sheet_operations.update_status(
                    instruction.row_number, 
                    StatusMessages.NO_GTT_ID_UPDATE
                )
                return
            
            # Check if update is actually needed
            if not self.validation_service.should_update_gtt(instruction, matched_row):
                self.sheet_operations.update_status(
                    instruction.row_number, 
                    StatusMessages.NO_UPDATE_NEEDED
                )
                return
            
            try:
                # Update GTT via Kite API
                self.kite_service.modify_gtt(matched_row.gtt_id, instruction)
                
                # Update status
                self.sheet_operations.update_status(
                    instruction.row_number, 
                    StatusMessages.UPDATED
                )
                
                # Update data sheet
                self._update_data_sheet(matched_row.gtt_id, instruction)
                
            except Exception as e:
                self.sheet_operations.update_status(
                    instruction.row_number, 
                    StatusMessages.error(str(e))
                )
                raise
                
        elif len(matches) > 1:
            self.sheet_operations.update_status(
                instruction.row_number, 
                StatusMessages.MULTIPLE_MATCHES
            )
        else:
            self.sheet_operations.update_status(
                instruction.row_number, 
                StatusMessages.NO_MATCH_FOUND
            )
    
    def _handle_delete_action(
        self, 
        instruction: GTTInstruction, 
        data_rows: List[GTTDataRow]
    ) -> None:
        """Handle DELETE action for GTT"""
        matches = GTTMatcher.find_matching_rows(instruction, data_rows, update_match=False)
        
        if len(matches) == 1:
            matched_row = matches[0]
            
            if not matched_row.gtt_id:
                self.sheet_operations.update_status(
                    instruction.row_number, 
                    StatusMessages.NO_GTT_ID_DELETE
                )
                return
            
            try:
                # Delete GTT via Kite API
                self.kite_service.delete_gtt(matched_row.gtt_id)
                
                # Update status
                self.sheet_operations.update_status(
                    instruction.row_number, 
                    StatusMessages.DELETED
                )
                
                # Remove from data sheet
                self._remove_from_data_sheet(matched_row.gtt_id)
                
            except Exception as e:
                self.sheet_operations.update_status(
                    instruction.row_number, 
                    StatusMessages.error(str(e))
                )
                raise
                
        elif len(matches) > 1:
            self.sheet_operations.update_status(
                instruction.row_number, 
                StatusMessages.MULTIPLE_MATCHES
            )
        else:
            self.sheet_operations.update_status(
                instruction.row_number, 
                StatusMessages.NO_MATCH_FOUND
            )
    
    def _add_to_data_sheet(self, instruction: GTTInstruction, gtt_id: str) -> None:
        """Add new GTT to data sheet"""
        try:
            # Normalize data for consistent storage
            normalized_quantity = DataParser.parse_int_safe(instruction.units)
            normalized_price = DataParser.parse_float_safe(instruction.gtt_price)
            
            append_row = {
                "TICKER": instruction.ticker,
                "TYPE": instruction.type,
                "UNITS": normalized_quantity,
                "GTT PRICE": normalized_price,
                "GTT DATE": instruction.gtt_date,
                "GTT_ID": gtt_id,
            }
            
            self.sheet_operations.append_data_row(append_row, self.data_header)
            logger.debug(f"Added new GTT to data sheet: row {instruction.row_number}")
            
        except Exception as e:
            logger.error(f"Failed to add GTT to data sheet: {e}")
            raise
    
    def _update_data_sheet(self, gtt_id: str, instruction: GTTInstruction) -> None:
        """Update existing GTT in data sheet"""
        try:
            row_number = self.sheet_operations.find_data_row_by_gtt_id(gtt_id)
            if row_number:
                update_data = {
                    "TICKER": instruction.ticker,
                    "TYPE": instruction.type,
                    "UNITS": instruction.units,
                    "GTT PRICE": instruction.gtt_price,
                    "GTT DATE": instruction.gtt_date,
                }
                self.sheet_operations.update_data_row(row_number, update_data, self.data_header)
            else:
                logger.warning(f"Could not find data sheet row to update for GTT_ID {gtt_id}")
                
        except Exception as e:
            logger.error(f"Failed to update data sheet for GTT_ID {gtt_id}: {e}")
            # Don't re-raise as the GTT was successfully updated in Kite
    
    def _remove_from_data_sheet(self, gtt_id: str) -> None:
        """Remove GTT from data sheet"""
        try:
            row_number = self.sheet_operations.find_data_row_by_gtt_id(gtt_id)
            if row_number:
                self.sheet_operations.delete_data_row(row_number)
            else:
                logger.warning(f"Could not find data sheet row to delete for GTT_ID {gtt_id}")
                
        except Exception as e:
            logger.error(f"Failed to delete data sheet row for GTT_ID {gtt_id}: {e}")
            # Don't re-raise as the GTT was successfully deleted in Kite


class GTTBatchProcessor:
    """High-level batch processor that coordinates the entire operation"""
    
    def __init__(
        self, 
        processor: GTTProcessor,
        instruction_fetcher,
        data_fetcher
    ):
        self.processor = processor
        self.instruction_fetcher = instruction_fetcher
        self.data_fetcher = data_fetcher
    
    def process_batch(self, start_row: int) -> ProcessingResult:
        """Process a batch of GTT instructions"""
        # Fetch instructions and data
        raw_instructions = self.instruction_fetcher(start_row)
        if not raw_instructions:
            logger.info("No GTT instructions found to process.")
            return ProcessingResult(0, [], [])
        
        raw_data_rows = self.data_fetcher(start_row)
        
        # Parse instructions
        instructions = []
        failed_rows = []
        
        for idx, raw_instr in enumerate(raw_instructions):
            row_num = start_row + idx
            instruction = DataParser.parse_instruction(raw_instr, row_num)
            
            if instruction is None:
                failed_rows.append({"row_number": row_num, "reason": "Failed to parse instruction"})
                continue
                
            instructions.append(instruction)
        
        # Parse data rows
        data_rows = []
        for idx, raw_data in enumerate(raw_data_rows):
            data_row = DataParser.parse_data_row(raw_data, start_row + idx)
            data_rows.append(data_row)
        
        # Process each instruction
        conflict_rows = []
        
        for instruction in instructions:
            try:
                self.processor.process_instruction(instruction, data_rows)
            except Exception as e:
                failed_rows.append({
                    "row_number": instruction.row_number, 
                    "reason": str(e)
                })
        
        return ProcessingResult(
            total_processed=len(instructions),
            failed_rows=failed_rows,
            conflict_rows=conflict_rows
        )