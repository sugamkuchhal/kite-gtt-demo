# main.py - Clean main entry point with dependency injection

import logging
from typing import Callable, List, Dict, Any

from kite_session import get_kite
from fetch_google_gtt_instructions import fetch_gtt_instructions_batch, get_instructions_sheet
from fetch_google_existing_gtts import fetch_existing_gtts_batch, get_tracking_sheet
from config import BATCH_SIZE
from google_sheets_utils import get_gsheet_client

from gtt_processor import GTTProcessor, GTTBatchProcessor
from gtt_services import KiteGTTService, GoogleSheetOperations
from gtt_core import ProcessingResult

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class GTTApplicationConfig:
    """Configuration class for the GTT application"""
    
    def __init__(self):
        self.batch_size = BATCH_SIZE
        self.start_row = 2  # Assuming row 1 is header
    
    def setup_dependencies(self):
        """Set up all dependencies for the application"""
        # External services
        kite = get_kite()
        gsheet_client = get_gsheet_client()
        instruction_sheet = get_instructions_sheet()
        data_sheet = get_tracking_sheet()
        
        # Service layer
        kite_service = KiteGTTService(kite)
        sheet_operations = GoogleSheetOperations(instruction_sheet, data_sheet)
        
        # Get data header
        data_header = data_sheet.row_values(1)
        
        # Core processor
        processor = GTTProcessor(
            kite_service=kite_service,
            sheet_operations=sheet_operations,
            data_header=data_header
        )
        
        # Batch processor with function injection
        def instruction_fetcher(start_row: int) -> List[Dict[str, Any]]:
            return fetch_gtt_instructions_batch(instruction_sheet, start_row)
        
        def data_fetcher(start_row: int) -> List[Dict[str, Any]]:
            return fetch_existing_gtts_batch(data_sheet, start_row)
        
        batch_processor = GTTBatchProcessor(
            processor=processor,
            instruction_fetcher=instruction_fetcher,
            data_fetcher=data_fetcher
        )
        
        return batch_processor


class GTTApplication:
    """Main application class that orchestrates the entire process"""
    
    def __init__(self, config: GTTApplicationConfig):
        self.config = config
        self.batch_processor = config.setup_dependencies()
    
    def run(self) -> None:
        """Run the complete GTT processing workflow"""
        logger.info("Starting GTT processing batch script...")
        
        start_row = self.config.start_row
        total_stats = ProcessingResult(0, [], [])
        
        while True:
            logger.info(f"Processing batch starting from row {start_row}")
            
            batch_result = self.batch_processor.process_batch(start_row)
            
            if batch_result.total_processed == 0:
                logger.info("No more instructions found to process.")
                break
            
            # Accumulate statistics
            total_stats.total_processed += batch_result.total_processed
            total_stats.failed_rows.extend(batch_result.failed_rows)
            total_stats.conflict_rows.extend(batch_result.conflict_rows)
            
            # Move to next batch
            start_row += batch_result.total_processed
            
            logger.info(f"Batch completed. Processed: {batch_result.total_processed}")
        
        self._log_final_statistics(total_stats)
    
    def _log_final_statistics(self, stats: ProcessingResult) -> None:
        """Log final processing statistics"""
        logger.info(f"=== FINAL PROCESSING RESULTS ===")
        logger.info(f"Total rows processed: {stats.total_processed}")
        
        if stats.failed_rows:
            logger.warning(f"Failed rows count: {len(stats.failed_rows)}")
            for failed_row in stats.failed_rows:
                logger.warning(f"Failed row {failed_row['row_number']}: {failed_row['reason']}")
        
        if stats.conflict_rows:
            logger.warning(f"Conflict rows count: {len(stats.conflict_rows)}")
            for conflict_row in stats.conflict_rows:
                logger.warning(f"Conflict row: {conflict_row['row_number']}")
        
        if not stats.failed_rows and not stats.conflict_rows:
            logger.info("✅ All rows processed successfully!")


def main():
    """Main entry point"""
    try:
        config = GTTApplicationConfig()
        app = GTTApplication(config)
        app.run()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Application failed with error: {e}")
        raise


if __name__ == "__main__":
    main()
